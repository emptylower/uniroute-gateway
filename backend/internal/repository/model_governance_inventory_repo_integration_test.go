//go:build integration

package repository

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelGovernanceInventoryIncludesAllSourcesAndAggregatesUsageWithoutWriting(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")

	insertInventoryObservation(t, fixture.accountID, "observed-model", "approved", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "used-model", "USD", "inventory-request-30d", fixture.apiKey1, "1.250001", now.Add(-20*24*time.Hour))
	insertInventoryUsage(t, fixture, "used-model", "USD", "inventory-request-7d-a", fixture.apiKey1, "2.000001", now.Add(-6*24*time.Hour))
	insertInventoryUsage(t, fixture, "used-model", "USD", "inventory-request-7d-b", fixture.apiKey2, "3.000001", now.Add(-24*time.Hour))
	insertInventoryUsage(t, fixture, "outside-window", "USD", "inventory-request-old", fixture.apiKey1, "99", now.Add(-31*24*time.Hour))

	before := inventoryBusinessState(t, fixture)
	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	require.Equal(t, before, inventoryBusinessState(t, fixture), "inventory query must not mutate business or governance state")

	byModel := make(map[string]service.InventoryItem, len(items))
	for _, item := range items {
		if item.AccountID == fixture.accountID && item.GroupID != nil && *item.GroupID == fixture.groupID &&
			item.ChannelID != nil && *item.ChannelID == fixture.channelID {
			byModel[item.UpstreamModelID] = item
		}
	}

	for _, model := range []string{"observed-model", "account-mapped-model", "channel-mapped-model", "used-model"} {
		_, ok := byModel[model]
		require.Truef(t, ok, "missing inventory source model %q", model)
	}
	require.NotContains(t, byModel, "outside-window")
	require.Equal(t, "approved", byModel["observed-model"].Classification)
	require.Contains(t, byModel, "priced-model", "concrete default channel-mapped pricing is finite capability evidence")
	require.Equal(t, service.InventoryItem{
		AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
		TargetPlatform: service.PlatformOpenAI, UpstreamModelID: "used-model", Classification: "unknown",
		Requests7d: 2, Requests30d: 3,
		Revenue7dBillingMicros: 5_000_002, Revenue30dBillingMicros: 6_250_003,
		BillingCurrency: "USD", AffectedAPIKeys7d: 2, AffectedAPIKeys30d: 2,
	}, byModel["used-model"])
}

func TestModelGovernanceInventoryUsesPersistedTargetWhenCurrentDimensionIsIneligible(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsageWithTarget(t, fixture, "historical-target-model", service.PlatformGemini, "USD", "historical-target-request", fixture.apiKey1, "1", now.Add(-time.Hour))

	_, err := integrationDB.ExecContext(ctx, `UPDATE groups SET platform = 'anthropic' WHERE id = $1`, fixture.groupID)
	require.NoError(t, err)

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	var found *service.InventoryItem
	for i := range items {
		if items[i].AccountID == fixture.accountID && items[i].UpstreamModelID == "historical-target-model" {
			found = &items[i]
			break
		}
	}
	require.NotNil(t, found)
	require.Equal(t, service.PlatformGemini, found.TargetPlatform)
	require.Equal(t, int64(1), found.Requests30d)
}

func TestModelGovernanceInventoryCompositeRoutesOnlyIncludeFixedUpstreamModels(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"openai_passthrough":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'composite') RETURNING id
	`, "inventory-composite-route-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', '{}'::jsonb) RETURNING id
	`, "inventory-composite-route-channel-"+suffix).Scan(&channelID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO composite_model_routes
			(group_id, public_model, match_type, target_platform, upstream_model, enabled)
		VALUES
			($1, 'dynamic-prefix-', 'prefix', 'openai', '', TRUE),
			($1, 'exact-pass-through', 'exact', 'openai', '', TRUE),
			($1, 'fixed-prefix-', 'prefix', 'openai', 'fixed-prefix-upstream', TRUE)
	`, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	require.NotContains(t, byModel, "dynamic-prefix-")
	require.Contains(t, byModel, "exact-pass-through")
	require.Contains(t, byModel, "fixed-prefix-upstream")
}

func TestModelGovernanceInventoryCompositeProjectionUsesRuntimeRoutePrecedenceAndReachableObservations(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"openai_passthrough":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'composite') RETURNING id
	`, "inventory-composite-precedence-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', '{}'::jsonb) RETURNING id
	`, "inventory-composite-precedence-channel-"+suffix).Scan(&channelID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO composite_model_routes
			(group_id, public_model, match_type, target_platform, upstream_model, endpoint, priority, enabled)
		VALUES
			($1, 'endpoint', 'exact', 'openai', 'endpoint-any', 'any', 1, TRUE),
			($1, 'endpoint', 'exact', 'openai', 'endpoint-specific', 'messages', 50, TRUE),
			($1, 'family-', 'prefix', 'openai', '', 'any', 1, TRUE),
			($1, 'family-long-', 'prefix', 'openai', '', 'any', 50, TRUE),
			($1, 'exact-over-prefix', 'prefix', 'openai', 'prefix-shadowed', 'any', 1, TRUE),
			($1, 'exact-over-prefix', 'exact', 'openai', 'exact-winner', 'any', 50, TRUE)
	`, groupID)
	require.NoError(t, err)
	for _, model := range []string{
		"family-observed", "family-long-observed", "outside-route-domain",
		"endpoint-any", "endpoint-specific", "exact-winner",
	} {
		insertInventoryObservation(t, accountID, model, "approved", now.Add(-time.Hour))
	}
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	for _, model := range []string{
		"endpoint-any", "endpoint-specific", "exact-winner",
		"family-observed", "family-long-observed",
	} {
		require.Containsf(t, byModel, model, "reachable runtime route output %q must be inventoried", model)
	}
	for _, model := range []string{"prefix-shadowed", "outside-route-domain"} {
		require.NotContainsf(t, byModel, model, "unreachable composite model %q leaked into inventory", model)
	}
}

func TestModelGovernanceInventoryIgnoresCompositeRouteRowsAttachedToOrdinaryGroup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, fixture.accountID)
	require.NotContains(t, byModel, "routed-model", "route rows attached to a non-composite group are stale configuration")
	require.Contains(t, byModel, "account-mapped-model")
}

func TestModelGovernanceInventoryPreservesLongExactModelID(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	accountID := insertInventoryAccount(t, `{}`)
	modelID := "inventory/" + strings.Repeat("exact-model-segment-", 16)
	insertInventoryObservation(t, accountID, modelID, "discovered", now)

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Greater(t, len(modelID), 255)
	require.Contains(t, inventoryItemsForAccount(items, accountID), modelID)
}

func TestModelGovernanceInventoryExcludesWildcardCapabilityNamespacesWithoutWriting(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")

	_, err := integrationDB.ExecContext(ctx, `
		UPDATE channels
		SET model_mapping = '{"openai":{"wildcard-request-*":"wildcard-target-*","concrete-request":"exact channel target","concrete-from-wildcard-*":"concrete-from-wildcard","mapping-number":42,"mapping-bool":true,"mapping-null":null,"mapping-array":["fake-mapping-array"],"mapping-object":{"model":"fake-mapping-object"}}}'::jsonb
		WHERE id = $1
	`, fixture.channelID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE accounts
		SET credentials = jsonb_set(credentials, '{model_mapping,exact pricing model}', '"exact pricing model"'::jsonb, TRUE)
		WHERE id = $1
	`, fixture.accountID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE channel_model_pricing
		SET models = '["pricing-*","exact pricing model",42,true,null,["fake-pricing-array"],{"model":"fake-pricing-object"}]'::jsonb
		WHERE channel_id = $1 AND platform = 'openai'
	`, fixture.channelID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE composite_model_routes SET upstream_model = 'routed-*exact' WHERE group_id = $1
	`, fixture.groupID)
	require.NoError(t, err)
	insertInventoryObservation(t, fixture.accountID, "observed-*exact", "approved", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "used-*exact", "USD", "inventory-wildcard-evidence", fixture.apiKey1, "1", now.Add(-time.Hour))

	businessBefore := inventoryBusinessState(t, fixture)
	before := inventoryChannelCapabilityState(t, fixture.channelID)
	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	require.Equal(t, businessBefore, inventoryBusinessState(t, fixture), "inventory query must not mutate business or governance state")
	require.Equal(t, before, inventoryChannelCapabilityState(t, fixture.channelID), "inventory query must not mutate channel capability state")

	byModel := inventoryItemsForAccount(items, fixture.accountID)
	require.NotContains(t, byModel, "wildcard-target-*")
	require.NotContains(t, byModel, "pricing-*")
	for _, fakeModel := range []string{
		"42", "true", "null", `["fake-mapping-array"]`, `{"model": "fake-mapping-object"}`,
		`["fake-pricing-array"]`, `{"model": "fake-pricing-object"}`,
	} {
		require.NotContains(t, byModel, fakeModel)
	}
	require.Contains(t, byModel, "concrete-from-wildcard")
	require.Contains(t, byModel, "exact channel target")
	require.Contains(t, byModel, "exact pricing model")
	require.NotContains(t, byModel, "observed-*exact", "an observation is not current evidence without exact complete-chain replay")
	require.Contains(t, byModel, "used-*exact")
	require.NotContains(t, byModel, "routed-*exact", "stale composite routes on an ordinary group are not capabilities")

	_, err = integrationDB.ExecContext(ctx, `
		UPDATE channels SET model_mapping = '{"openai":["wrong-outer-mapping"]}'::jsonb WHERE id = $1
	`, fixture.channelID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE channel_model_pricing SET models = '{"model":"wrong-outer-pricing"}'::jsonb
		WHERE channel_id = $1 AND platform = 'openai'
	`, fixture.channelID)
	require.NoError(t, err)

	wrongOuterBefore := inventoryChannelCapabilityState(t, fixture.channelID)
	items, err = listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	require.Equal(t, wrongOuterBefore, inventoryChannelCapabilityState(t, fixture.channelID), "inventory query must not mutate wrong-typed channel capability state")
	byModel = inventoryItemsForAccount(items, fixture.accountID)
	require.NotContains(t, byModel, "wrong-outer-mapping")
	require.NotContains(t, byModel, "wrong-outer-pricing")
}

func TestModelGovernanceInventoryUsesHalfOpenUsageWindows(t *testing.T) {
	ctx := context.Background()
	windowEnd := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	cutoff7d := windowEnd.Add(-7 * 24 * time.Hour)
	cutoff30d := windowEnd.Add(-30 * 24 * time.Hour)
	fixture := createModelGovernanceInventoryFixture(t, "USD")

	insertInventoryUsage(t, fixture, "bounded-model", "USD", "inventory-at-30d", fixture.apiKey1, "1", cutoff30d)
	insertInventoryUsage(t, fixture, "bounded-model", "USD", "inventory-at-7d", fixture.apiKey2, "2", cutoff7d)
	insertInventoryUsage(t, fixture, "bounded-model", "USD", "inventory-before-end", fixture.apiKey2, "4", windowEnd.Add(-time.Millisecond))
	insertInventoryUsage(t, fixture, "bounded-model", "USD", "inventory-at-end", fixture.apiKey2, "8", windowEnd)
	insertInventoryUsage(t, fixture, "bounded-model", "USD", "inventory-after-end", fixture.apiKey2, "16", windowEnd.Add(time.Hour))
	insertInventoryUsage(t, fixture, "future-only-model", "USD", "inventory-future-only", fixture.apiKey2, "1", windowEnd.Add(time.Hour))

	items, err := listModelGovernanceInventory(ctx, cutoff7d, cutoff30d, windowEnd)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, fixture.accountID)
	require.NotContains(t, byModel, "future-only-model")
	require.Equal(t, service.InventoryItem{
		AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
		TargetPlatform: service.PlatformOpenAI, UpstreamModelID: "bounded-model", Classification: "unknown",
		Requests7d: 2, Requests30d: 3,
		Revenue7dBillingMicros: 6_000_000, Revenue30dBillingMicros: 7_000_000,
		BillingCurrency: "USD", AffectedAPIKeys7d: 1, AffectedAPIKeys30d: 2,
	}, byModel["bounded-model"])
}

func TestModelGovernanceInventoryFutureRowsCannotAffectValidationOrAggregates(t *testing.T) {
	ctx := context.Background()
	windowEnd := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	cutoff7d := windowEnd.Add(-7 * 24 * time.Hour)
	cutoff30d := windowEnd.Add(-30 * 24 * time.Hour)

	t.Run("mixed currency has positive revenue", func(t *testing.T) {
		fixture := createModelGovernanceInventoryFixture(t, "USD")
		insertInventoryUsage(t, fixture, "future-mixed-model", "USD", "inventory-future-mixed-safe", fixture.apiKey1, "1", windowEnd.Add(-time.Hour))
		insertInventoryUsage(t, fixture, "future-mixed-model", "CNY", "inventory-future-mixed-excluded", fixture.apiKey2, "1", windowEnd)

		items, err := listModelGovernanceInventory(ctx, cutoff7d, cutoff30d, windowEnd)
		require.NoError(t, err)
		require.Equal(t, service.InventoryItem{
			AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
			TargetPlatform: service.PlatformOpenAI, UpstreamModelID: "future-mixed-model", Classification: "unknown",
			Requests7d: 1, Requests30d: 1,
			Revenue7dBillingMicros: 1_000_000, Revenue30dBillingMicros: 1_000_000,
			BillingCurrency: "USD", AffectedAPIKeys7d: 1, AffectedAPIKeys30d: 1,
		}, inventoryItemsForAccount(items, fixture.accountID)["future-mixed-model"])
	})

	t.Run("negative revenue uses in-range currency", func(t *testing.T) {
		fixture := createModelGovernanceInventoryFixture(t, "USD")
		insertInventoryUsage(t, fixture, "future-negative-model", "USD", "inventory-future-negative-safe", fixture.apiKey1, "1", windowEnd.Add(-time.Hour))
		insertInventoryUsage(t, fixture, "future-negative-model", "USD", "inventory-future-negative-excluded", fixture.apiKey2, "-1", windowEnd)

		items, err := listModelGovernanceInventory(ctx, cutoff7d, cutoff30d, windowEnd)
		require.NoError(t, err)
		require.Equal(t, service.InventoryItem{
			AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
			TargetPlatform: service.PlatformOpenAI, UpstreamModelID: "future-negative-model", Classification: "unknown",
			Requests7d: 1, Requests30d: 1,
			Revenue7dBillingMicros: 1_000_000, Revenue30dBillingMicros: 1_000_000,
			BillingCurrency: "USD", AffectedAPIKeys7d: 1, AffectedAPIKeys30d: 1,
		}, inventoryItemsForAccount(items, fixture.accountID)["future-negative-model"])
	})

	t.Run("overflow-only model is excluded", func(t *testing.T) {
		fixture := createModelGovernanceInventoryFixture(t, "USD")
		insertInventoryUsageSeries(t, fixture, "future-overflow-model", "USD", fixture.apiKey2, "9999999999.9999999999", windowEnd.Add(time.Hour), 1000)

		items, err := listModelGovernanceInventory(ctx, cutoff7d, cutoff30d, windowEnd)
		require.NoError(t, err)
		require.NotContains(t, inventoryItemsForAccount(items, fixture.accountID), "future-overflow-model")
	})
}

func TestModelGovernanceInventoryIncludesEnabledAccountWithoutGroup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	accountID := insertInventoryAccount(t, `{"model_mapping":{"public":"ungrouped-model"}}`)
	t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	require.Contains(t, items, service.InventoryItem{
		AccountID: accountID, TargetPlatform: service.PlatformOpenAI,
		UpstreamModelID: "ungrouped-model", Classification: "unknown", BillingCurrency: "CNY",
	})
}

func TestModelGovernanceInventoryRunModeControlsBoundAccountUngroupedDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"public":"bound-simple-model"}}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'anthropic') RETURNING id
	`, "inventory-bound-simple-"+suffix).Scan(&groupID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	standard, err := listModelGovernanceInventoryForRunMode(ctx, config.RunModeStandard, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	simple, err := listModelGovernanceInventoryForRunMode(ctx, config.RunModeSimple, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)

	standardUngrouped := inventoryUngroupedTargets(standard, accountID, "bound-simple-model")
	require.NotContains(t, standardUngrouped, service.PlatformAntigravity,
		"standard mode must not create an ungrouped native dimension for a bound account")
	simpleUngrouped := inventoryUngroupedTargets(simple, accountID, "bound-simple-model")
	for _, target := range []string{service.PlatformAntigravity, service.PlatformAnthropic, service.PlatformGemini} {
		require.Containsf(t, simpleUngrouped, target, "simple mode missing ungrouped target %q", target)
	}
}

func TestModelGovernanceInventoryCompositeAccountMappingsRequireRouteReachability(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{"model_mapping":{"x-exact":"reachable-exact","x-prefix*":"reachable-prefix","y-only":"unreachable-y"}}`)
	var groupID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'composite') RETURNING id
	`, "inventory-composite-reachability-"+suffix).Scan(&groupID))
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO composite_model_routes
			(group_id, public_model, match_type, target_platform, upstream_model, endpoint, priority, enabled)
		VALUES
			($1, 'x-exact', 'exact', 'openai', '', 'any', 0, TRUE),
			($1, 'x-', 'prefix', 'openai', '', 'any', 1, TRUE)
	`, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	models := inventoryItemsForAccount(items, accountID)
	require.Contains(t, models, "reachable-exact")
	require.Contains(t, models, "reachable-prefix")
	require.NotContains(t, models, "unreachable-y")
}

func TestModelGovernanceInventoryProjectsUngroupedOrdinaryRuntimeTargets(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	model := "ungrouped-target-identity-model"
	mixedID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"public":"ungrouped-target-identity-model"}}`)
	disabledID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"public":"ungrouped-target-identity-model"}}`)
	openAIID := insertInventoryAccount(t, `{"model_mapping":{"public":"ungrouped-target-identity-model"}}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1`, mixedID)
	require.NoError(t, err)
	var userID, apiKeyID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO users (email, password_hash, billing_currency) VALUES ($1, 'hash', 'USD') RETURNING id
	`, "inventory-ungrouped-targets-"+suffix+"@example.invalid").Scan(&userID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO api_keys (user_id, key, name) VALUES ($1, $2, 'inventory-ungrouped-targets') RETURNING id
	`, userID, "sk-inventory-ungrouped-targets-"+suffix).Scan(&apiKeyID))
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO usage_logs (
			user_id, api_key_id, account_id, request_id, model, upstream_model,
			governance_target_platform, group_id, channel_id, actual_cost, settlement_currency, created_at
		) VALUES
			($1, $2, $3, $4, $6, $6, 'anthropic', NULL, NULL, 1.25, 'USD', $7),
			($1, $2, $3, $5, $6, $6, 'gemini', NULL, NULL, 2.50, 'USD', $7)
	`, userID, apiKeyID, mixedID, "inventory-ungrouped-anthropic-"+suffix,
		"inventory-ungrouped-gemini-"+suffix, model, now.Add(-time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, userID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id IN ($1, $2, $3)`, mixedID, disabledID, openAIID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)

	for _, target := range []string{service.PlatformAntigravity, service.PlatformAnthropic, service.PlatformGemini} {
		item, ok := inventoryItemForTarget(items, mixedID, model, target)
		require.Truef(t, ok, "mixed-enabled ungrouped account missing target %q", target)
		require.Nil(t, item.GroupID)
		require.Nil(t, item.ChannelID)
		if target == service.PlatformAnthropic {
			require.Equal(t, int64(1), item.Requests7d)
			require.Equal(t, int64(1_250_000), item.Revenue7dBillingMicros)
			require.Equal(t, int64(1), item.AffectedAPIKeys7d)
			require.Equal(t, "USD", item.BillingCurrency)
		}
		if target == service.PlatformGemini {
			require.Equal(t, int64(1), item.Requests7d)
			require.Equal(t, int64(2_500_000), item.Revenue7dBillingMicros)
			require.Equal(t, int64(1), item.AffectedAPIKeys7d)
			require.Equal(t, "USD", item.BillingCurrency)
		}
	}
	for _, target := range []string{service.PlatformAnthropic, service.PlatformGemini} {
		_, ok := inventoryItemForTarget(items, disabledID, model, target)
		require.Falsef(t, ok, "mixed-disabled ungrouped account unexpectedly projected target %q", target)
		_, ok = inventoryItemForTarget(items, openAIID, model, target)
		require.Falsef(t, ok, "OpenAI ungrouped account unexpectedly projected target %q", target)
	}
	_, ok := inventoryItemForTarget(items, disabledID, model, service.PlatformAntigravity)
	require.True(t, ok)
	_, ok = inventoryItemForTarget(items, openAIID, model, service.PlatformOpenAI)
	require.True(t, ok)
}

func TestModelGovernanceInventoryIncludesFiniteEffectiveRuntimeMappings(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		platform    string
		accountType string
		credentials string
		wantModel   string
	}{
		{
			name: "antigravity default", platform: service.PlatformAntigravity, accountType: service.AccountTypeAPIKey,
			credentials: `{}`, wantModel: "gemini-3-flash",
		},
		{
			name: "grok default", platform: service.PlatformGrok, accountType: service.AccountTypeAPIKey,
			credentials: `{}`, wantModel: "grok-4.5",
		},
		{
			name: "openai compact only", platform: service.PlatformOpenAI, accountType: service.AccountTypeAPIKey,
			credentials: `{"compact_model_mapping":{"gpt-5.4":"gpt-5.4-compact-integration"}}`, wantModel: "gpt-5.4-compact-integration",
		},
		{
			name: "bedrock regional default", platform: service.PlatformAnthropic, accountType: service.AccountTypeBedrock,
			credentials: `{"aws_region":"eu-west-1"}`, wantModel: "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accountID := insertInventoryAccountWithType(t, test.platform, test.accountType, test.credentials)
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			before := inventoryAccountState(t, accountID)
			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			require.NoError(t, err)
			require.Equal(t, before, inventoryAccountState(t, accountID), "inventory must remain read-only")
			require.Contains(t, inventoryItemsForAccount(items, accountID), test.wantModel)
		})
	}
}

func TestModelGovernanceInventoryCompactMappingsFollowRuntimeEligibilityFromAccountExtra(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		extra      string
		wantMapped bool
	}{
		{name: "supported", extra: `{"openai_compact_supported":true}`, wantMapped: true},
		{name: "force off", extra: `{"openai_compact_mode":"force_off"}`},
		{name: "unsupported", extra: `{"openai_compact_supported":false}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accountID := insertInventoryAccount(t, `{"compact_model_mapping":{"gpt":"compact-eligibility-target"}}`)
			_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = $2::jsonb WHERE id = $1`, accountID, test.extra)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			require.NoError(t, err)
			_, mapped := inventoryItemsForAccount(items, accountID)["compact-eligibility-target"]
			require.Equal(t, test.wantMapped, mapped)
		})
	}
}

func TestModelGovernanceInventoryEffectiveMappingsUseStableConfigurationScope(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	for _, state := range []struct {
		name        string
		status      string
		schedulable bool
	}{
		{name: "disabled", status: "disabled", schedulable: true},
		{name: "unschedulable", status: "active", schedulable: false},
	} {
		t.Run(state.name, func(t *testing.T) {
			accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey, `{}`)
			_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET status = $2, schedulable = $3 WHERE id = $1`, accountID, state.status, state.schedulable)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			require.NoError(t, err)
			require.Empty(t, inventoryItemsForAccount(items, accountID))
		})
	}
}

func TestModelGovernanceInventoryEffectiveMappingsUseEnabledRouteDimensionsAndIgnoreTransientExclusions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey, `{}`)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', $2) RETURNING id
	`, "inventory-effective-group-"+suffix, service.PlatformAntigravity).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', '{}'::jsonb) RETURNING id
	`, "inventory-effective-channel-"+suffix).Scan(&channelID))
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE accounts
		SET rate_limit_reset_at = $2, overload_until = $2, temp_unschedulable_until = $2
		WHERE id = $1
	`, accountID, now.Add(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	var found bool
	for _, item := range items {
		if item.AccountID == accountID && item.UpstreamModelID == "gemini-3-flash" {
			require.Equal(t, &groupID, item.GroupID)
			require.Equal(t, &channelID, item.ChannelID)
			found = true
		}
	}
	require.True(t, found, "configuration-enabled account must remain inventoried during transient scheduler exclusion")
}

func TestModelGovernanceInventoryProjectsMixedAndForcedRuntimeTargetDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"native-public":"native-account-model","mixed-channel-model":"mixed-channel-model","forced-channel-model":"forced-channel-model"}}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'anthropic') RETURNING id
	`, "inventory-mixed-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', $2::jsonb) RETURNING id
	`, "inventory-mixed-channel-"+suffix, `{
		"anthropic":{"mixed-public":"mixed-channel-model"},
		"antigravity":{"forced-public":"forced-channel-model"}
	}`).Scan(&channelID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO channel_model_pricing (channel_id, platform, models) VALUES
			($1, 'anthropic', '["mixed-priced-model"]'::jsonb),
			($1, 'antigravity', '["forced-priced-model"]'::jsonb)
	`, channelID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	for _, model := range []string{"native-account-model", "mixed-channel-model", "forced-channel-model"} {
		item, ok := inventoryItemForTarget(items, accountID, model, map[string]string{
			"native-account-model": service.PlatformAntigravity,
			"mixed-channel-model":  service.PlatformAnthropic,
			"forced-channel-model": service.PlatformAntigravity,
		}[model])
		require.Truef(t, ok, "missing runtime target model %q", model)
		require.Equal(t, &groupID, item.GroupID)
		require.Equal(t, &channelID, item.ChannelID)
	}
	mixed, _ := inventoryItemForTarget(items, accountID, "mixed-channel-model", service.PlatformAnthropic)
	forced, _ := inventoryItemForTarget(items, accountID, "forced-channel-model", service.PlatformAntigravity)
	require.Equal(t, "unknown", mixed.Classification)
	require.Equal(t, "ignored", forced.Classification)
	require.NotContains(t, byModel, "mixed-priced-model")
	require.NotContains(t, byModel, "forced-priced-model")
	require.NotEmpty(t, byModel)
}

func TestModelGovernanceInventoryUsesGeminiTargetForMixedAntigravityChannelCapabilities(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"mixed-gemini-channel":"mixed-gemini-channel","wrong-native-channel":"wrong-native-channel"}}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'gemini') RETURNING id
	`, "inventory-mixed-gemini-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active',
			'{"gemini":{"public":"mixed-gemini-channel"},"antigravity":{"wrong":"wrong-native-channel"}}'::jsonb)
		RETURNING id
	`, "inventory-mixed-gemini-channel-"+suffix).Scan(&channelID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO channel_model_pricing (channel_id, platform, models) VALUES
			($1, 'gemini', '["mixed-gemini-priced"]'::jsonb),
			($1, 'antigravity', '["forced-gemini-group-native-priced"]'::jsonb)
	`, channelID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	require.Contains(t, byModel, "mixed-gemini-channel")
	require.Contains(t, byModel, "wrong-native-channel", "forced native path remains a separate enabled dimension")
	require.NotContains(t, byModel, "mixed-gemini-priced")
	require.NotContains(t, byModel, "forced-gemini-group-native-priced")
	require.Equal(t, "unknown", byModel["mixed-gemini-channel"].Classification)
	mixedWrong, mixedOK := inventoryItemForTarget(items, accountID, "wrong-native-channel", service.PlatformGemini)
	forcedWrong, forcedOK := inventoryItemForTarget(items, accountID, "wrong-native-channel", service.PlatformAntigravity)
	require.True(t, mixedOK)
	require.True(t, forcedOK)
	require.Equal(t, "unknown", mixedWrong.Classification)
	require.Equal(t, "ignored", forcedWrong.Classification)
}

func TestModelGovernanceInventoryExcludesMixedAntigravityDimensionWhenDisabled(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"enabled-forced-channel":"enabled-forced-channel"}}`)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'anthropic') RETURNING id
	`, "inventory-mixed-disabled-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active',
			'{"anthropic":{"public":"disabled-mixed-channel"},"antigravity":{"native":"enabled-forced-channel"}}'::jsonb)
		RETURNING id
	`, "inventory-mixed-disabled-channel-"+suffix).Scan(&channelID))
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	require.NotContains(t, byModel, "disabled-mixed-channel")
	require.Contains(t, byModel, "enabled-forced-channel")
}

func TestModelGovernanceInventoryHistoricalMixedUsageUsesRecordedGroupTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	_, err := integrationDB.ExecContext(ctx, `
		UPDATE accounts SET platform = 'antigravity', extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1
	`, fixture.accountID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `UPDATE groups SET platform = 'anthropic' WHERE id = $1`, fixture.groupID)
	require.NoError(t, err)
	insertInventoryUsage(t, fixture, "historical-mixed-model", "USD", "inventory-historical-mixed", fixture.apiKey1, "1", now.Add(-time.Hour))

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	item, ok := inventoryItemForTarget(items, fixture.accountID, "historical-mixed-model", service.PlatformAnthropic)
	require.True(t, ok)
	require.Equal(t, int64(1), item.Requests30d)
	require.Equal(t, "unknown", item.Classification)
}

func TestModelGovernanceInventoryExcludesUnsupportedCurrentGroupDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{"model_mapping":{"public":"unsupported-account-model"}}`)
	insertInventoryObservation(t, accountID, "unsupported-observation", "approved", now.Add(-time.Hour))
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'anthropic') RETURNING id
	`, "inventory-unsupported-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active',
			'{"anthropic":{"public":"unsupported-channel-model"}}'::jsonb) RETURNING id
	`, "inventory-unsupported-channel-"+suffix).Scan(&channelID))
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	require.Empty(t, inventoryItemsForAccount(items, accountID))
}

func TestModelGovernanceInventoryCompositeTargetsUseNativeAndMixedEligibility(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccountWithType(t, service.PlatformAntigravity, service.AccountTypeAPIKey,
		`{"model_mapping":{"anthropic-exact-pass":"scheduler-anthropic","antigravity-exact-pass":"scheduler-antigravity","composite-anthropic-channel":"composite-anthropic-channel","composite-antigravity-channel":"composite-antigravity-channel"}}`)
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET extra = '{"mixed_scheduling":true}'::jsonb WHERE id = $1`, accountID)
	require.NoError(t, err)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'composite') RETURNING id
	`, "inventory-composite-eligibility-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', $2::jsonb) RETURNING id
	`, "inventory-composite-eligibility-channel-"+suffix, `{
		"anthropic":{"anthropic-exact-pass":"composite-anthropic-channel"},
		"antigravity":{"antigravity-exact-pass":"composite-antigravity-channel"},
		"openai":{"o":"composite-openai-ineligible"}
	}`).Scan(&channelID))
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO composite_model_routes
			(group_id, public_model, match_type, target_platform, upstream_model, enabled)
		VALUES
			($1, 'anthropic-exact-pass', 'exact', 'anthropic', '', TRUE),
			($1, 'anthropic-prefix-', 'prefix', 'anthropic', 'anthropic-prefix-upstream', TRUE),
			($1, 'antigravity-exact-pass', 'exact', 'antigravity', '', TRUE),
			($1, 'openai-exact-pass', 'exact', 'openai', '', TRUE),
			($1, 'dynamic-prefix-', 'prefix', 'gemini', '', TRUE)
	`, groupID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	for _, model := range []string{"composite-anthropic-channel", "composite-antigravity-channel"} {
		require.Contains(t, byModel, model)
	}
	for _, model := range []string{"anthropic-exact-pass", "anthropic-prefix-upstream", "antigravity-exact-pass"} {
		require.NotContainsf(t, byModel, model, "account forwarding does not support arbitrary route model %q", model)
	}
	require.NotContains(t, byModel, "openai-exact-pass")
	require.NotContains(t, byModel, "composite-openai-ineligible")
	require.NotContains(t, byModel, "dynamic-prefix-")
}

func TestModelGovernanceInventoryUsesOneRepeatableReadSnapshotAcrossProjectionAndAggregation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{"model_mapping":{"public":"snapshot-old-model"}}`)
	var groupID, channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'openai') RETURNING id
	`, "inventory-snapshot-group-"+suffix).Scan(&groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active',
			'{"openai":{"old":"snapshot-old-channel-model"}}'::jsonb) RETURNING id
	`, "inventory-snapshot-channel-"+suffix).Scan(&channelID))
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	repo := &modelGovernanceInventoryRepository{db: integrationDB}
	repo.afterAccountsLoaded = func() error {
		if _, err := integrationDB.ExecContext(ctx, `
			UPDATE accounts
			SET credentials = '{"model_mapping":{"public":"snapshot-new-model"}}'::jsonb,
			    status = 'disabled'
			WHERE id = $1
		`, accountID); err != nil {
			return err
		}
		if _, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, accountID, groupID); err != nil {
			return err
		}
		if _, err := integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, channelID, groupID); err != nil {
			return err
		}
		_, err := integrationDB.ExecContext(ctx, `
			INSERT INTO composite_model_routes
				(group_id, public_model, match_type, target_platform, upstream_model, enabled)
			VALUES ($1, 'snapshot-new-route', 'exact', 'openai', '', TRUE)
		`, groupID)
		return err
	}

	items, err := repo.List(
		ctx, func(input service.InventoryProjectionInput) []service.InventoryRuntimeDimensionProjection {
			return service.ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)
		},
		now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now,
	)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, accountID)
	require.Contains(t, byModel, "snapshot-old-model")
	require.Nil(t, byModel["snapshot-old-model"].GroupID)
	require.NotContains(t, byModel, "snapshot-new-model")
	require.NotContains(t, byModel, "snapshot-old-channel-model")
	require.NotContains(t, byModel, "snapshot-new-route")
}

func TestModelGovernanceInventoryUsesExactPlatformClassificationAllowlistUnlessObserved(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		platform string
		want     string
	}{
		{platform: "anthropic", want: "unknown"},
		{platform: "openai", want: "unknown"},
		{platform: "gemini", want: "unknown"},
		{platform: "grok", want: "unknown"},
		{platform: "antigravity", want: "ignored"},
		{platform: "composite", want: "ignored"},
	} {
		t.Run(testCase.platform, func(t *testing.T) {
			accountID := insertInventoryAccountForPlatform(t, testCase.platform, `{"model_mapping":{"public":"mapped-model","observed-model":"observed-model"}}`)
			insertInventoryObservation(t, accountID, "observed-model", "approved", now.Add(-time.Hour))
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			require.NoError(t, err)
			byModel := inventoryItemsForAccount(items, accountID)
			require.Equal(t, testCase.want, byModel["mapped-model"].Classification)
			require.Equal(t, "approved", byModel["observed-model"].Classification)
		})
	}
}

func TestModelGovernanceInventoryExcludesCurrentEvidenceForInactiveOrUnschedulableAccounts(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	for _, state := range []struct {
		name        string
		status      string
		schedulable bool
	}{
		{name: "disabled", status: "disabled", schedulable: true},
		{name: "unschedulable", status: "active", schedulable: false},
	} {
		t.Run(state.name, func(t *testing.T) {
			accountID := insertInventoryAccount(t, `{"model_mapping":{"public":"current-model"}}`)
			insertInventoryObservation(t, accountID, "observed-model", "approved", now.Add(-time.Hour))
			_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET status = $2, schedulable = $3 WHERE id = $1`, accountID, state.status, state.schedulable)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			require.NoError(t, err)
			require.Empty(t, inventoryItemsForAccount(items, accountID))
		})
	}
}

func TestModelGovernanceInventoryRetainsHistoricalUsageForDisabledAccount(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryObservation(t, fixture.accountID, "current-observation", "approved", now.Add(-time.Hour))
	insertInventoryObservation(t, fixture.accountID, "historical-model", "approved", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "historical-model", "USD", "inventory-disabled-history", fixture.apiKey1, "1.25", now.Add(-20*24*time.Hour))
	_, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET status = 'disabled' WHERE id = $1`, fixture.accountID)
	require.NoError(t, err)

	items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, fixture.accountID)
	require.Equal(t, service.InventoryItem{
		AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
		TargetPlatform: service.PlatformOpenAI, UpstreamModelID: "historical-model", Classification: "unknown", Requests30d: 1,
		Revenue30dBillingMicros: 1_250_000, BillingCurrency: "USD", AffectedAPIKeys30d: 1,
	}, byModel["historical-model"])
	require.NotContains(t, byModel, "account-mapped-model")
	require.NotContains(t, byModel, "channel-mapped-model")
	require.NotContains(t, byModel, "priced-model")
	require.NotContains(t, byModel, "routed-model")
	require.NotContains(t, byModel, "current-observation")
}

func TestModelGovernanceInventoryRejectsMixedCurrencyBeforeRevenueConversion(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsageSeries(t, fixture, "mixed-model", "USD", fixture.apiKey1, "9999999999.9999999999", now.Add(-time.Hour), 1000)
	insertInventoryUsage(t, fixture, "mixed-model", "CNY", "inventory-mixed-cny", fixture.apiKey2, "-1", now.Add(-2*time.Hour))

	_, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_MIXED_CURRENCY", fixture, "mixed-model")
}

func TestModelGovernanceInventoryRejectsNegativeRevenueWithTypedDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "negative-model", "USD", "inventory-negative", fixture.apiKey1, "-0.000001", now.Add(-time.Hour))

	_, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", fixture, "negative-model")
}

func TestModelGovernanceInventoryRejectsNegativeSourceRevenueWhenDimensionNetsPositive(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "offset-model", "USD", "inventory-offset-negative", fixture.apiKey1, "-1", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "offset-model", "USD", "inventory-offset-positive", fixture.apiKey2, "2", now.Add(-2*time.Hour))

	_, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", fixture, "offset-model")
}

func TestModelGovernanceInventoryRejectsTinyNegativeSourceThatRoundsToZero(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "tiny-negative-model", "USD", "inventory-tiny-negative", fixture.apiKey1, "-0.0000000001", now.Add(-time.Hour))

	_, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", fixture, "tiny-negative-model")
}

func TestModelGovernanceInventoryRoundsAggregateRevenueAtExactMicrosBoundaries(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name        string
		costs       []string
		wantMicros  int64
		wantError   string
		seriesCost  string
		seriesCount int
	}{
		{name: "below-half-micro", costs: []string{"0.0000004999"}, wantMicros: 0},
		{name: "half-micro-rounds-up", costs: []string{"0.0000005000"}, wantMicros: 1},
		{name: "split-fractions-round-after-aggregate", costs: []string{"0.0000003000", "0.0000003000"}, wantMicros: 1},
		{
			name: "exact-rounded-max-int64", seriesCost: "9999999999.0000000000", seriesCount: 922,
			costs: []string{"3372037776.7758070000"}, wantMicros: int64(9223372036854775807),
		},
		{
			name: "raw-above-max-but-rounded-max-int64", seriesCost: "9999999999.0000000000", seriesCount: 922,
			costs: []string{"3372037776.7758074999"}, wantMicros: int64(9223372036854775807),
		},
		{
			name: "first-rounded-overflow-at-positive-half-micro", seriesCost: "9999999999.0000000000", seriesCount: 922,
			costs: []string{"3372037776.7758075000"}, wantError: "MODEL_GOVERNANCE_INVENTORY_REVENUE_OVERFLOW",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := createModelGovernanceInventoryFixture(t, "USD")
			model := "boundary-" + testCase.name
			if testCase.seriesCount > 0 {
				insertInventoryUsageSeries(t, fixture, model, "USD", fixture.apiKey1, testCase.seriesCost, now.Add(-time.Hour), testCase.seriesCount)
			}
			for index, cost := range testCase.costs {
				insertInventoryUsage(t, fixture, model, "USD", fmt.Sprintf("inventory-boundary-%d", index), fixture.apiKey2, cost, now.Add(-time.Hour))
			}

			items, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
			if testCase.wantError != "" {
				requireInventoryError(t, err, testCase.wantError, fixture, model)
				return
			}
			require.NoError(t, err)
			item, ok := inventoryItemsForAccount(items, fixture.accountID)[model]
			require.True(t, ok, "boundary inventory row must be present")
			require.Equal(t, testCase.wantMicros, item.Revenue7dBillingMicros)
			require.Equal(t, testCase.wantMicros, item.Revenue30dBillingMicros)
		})
	}
}

func TestModelGovernanceInventoryTypedErrorPreservesNullDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	suffix := fmt.Sprint(time.Now().UnixNano())
	accountID := insertInventoryAccount(t, `{}`)
	var userID, apiKeyID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO users (email, password_hash, billing_currency) VALUES ($1, 'hash', 'USD') RETURNING id
	`, "inventory-null-dimensions-"+suffix+"@example.invalid").Scan(&userID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO api_keys (user_id, key, name) VALUES ($1, $2, 'inventory-null-dimensions') RETURNING id
	`, userID, "sk-inventory-null-dimensions-"+suffix).Scan(&apiKeyID))
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO usage_logs (
			user_id, api_key_id, account_id, request_id, model, upstream_model,
			group_id, channel_id, actual_cost, settlement_currency, created_at
		) VALUES ($1, $2, $3, $4, 'null-dimensions-model', 'null-dimensions-model',
			NULL, NULL, -0.000001, 'USD', $5)
	`, userID, apiKeyID, accountID, "inventory-null-dimensions-"+suffix, now.Add(-time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, userID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	})

	_, err = listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	require.Equal(t, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", infraerrors.Reason(err))
	metadata := infraerrors.FromError(err).Metadata
	require.Equal(t, strconv.FormatInt(accountID, 10), metadata["account_id"])
	require.Equal(t, "", metadata["group_id"])
	require.Equal(t, "", metadata["channel_id"])
	require.Equal(t, "null-dimensions-model", metadata["upstream_model_id"])
}

func TestModelGovernanceInventoryRejectsRevenueOverflowWithTypedDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsageSeries(t, fixture, "overflow-model", "USD", fixture.apiKey1, "9999999999.9999999999", now.Add(-time.Hour), 1000)

	_, err := listModelGovernanceInventory(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour), now)
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_REVENUE_OVERFLOW", fixture, "overflow-model")
}

type modelGovernanceInventoryFixture struct {
	accountID int64
	groupID   int64
	channelID int64
	userID    int64
	apiKey1   int64
	apiKey2   int64
}

func createModelGovernanceInventoryFixture(t *testing.T, billingCurrency string) modelGovernanceInventoryFixture {
	t.Helper()
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	fixture := modelGovernanceInventoryFixture{}
	fixture.accountID = insertInventoryAccount(t, `{"model_mapping":{"public-model":"account-mapped-model","channel-mapped-model":"channel-mapped-model","observed-model":"observed-model","concrete-from-wildcard":"concrete-from-wildcard","exact channel target":"exact channel target","priced-model":"priced-model"}}`)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO groups (name, status, platform) VALUES ($1, 'active', 'openai') RETURNING id
	`, "inventory-group-"+suffix).Scan(&fixture.groupID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, status, model_mapping) VALUES ($1, 'active', $2::jsonb) RETURNING id
	`, "inventory-channel-"+suffix, `{"openai":{"channel-public":"channel-mapped-model"}}`).Scan(&fixture.channelID))
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO account_groups (account_id, group_id) VALUES ($1, $2)`, fixture.accountID, fixture.groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_groups (channel_id, group_id) VALUES ($1, $2)`, fixture.channelID, fixture.groupID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO channel_model_pricing (channel_id, platform, models) VALUES ($1, 'openai', '["priced-model"]'::jsonb)`, fixture.channelID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO composite_model_routes (group_id, public_model, target_platform, upstream_model, enabled)
		VALUES ($1, 'routed-public', 'openai', 'routed-model', TRUE)
	`, fixture.groupID)
	require.NoError(t, err)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO users (email, password_hash, billing_currency) VALUES ($1, 'hash', $2) RETURNING id
	`, "inventory-"+suffix+"@example.invalid", billingCurrency).Scan(&fixture.userID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO api_keys (user_id, key, name, group_id) VALUES ($1, $2, 'inventory-key-1', $3) RETURNING id
	`, fixture.userID, "sk-inventory-1-"+suffix, fixture.groupID).Scan(&fixture.apiKey1))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO api_keys (user_id, key, name, group_id) VALUES ($1, $2, 'inventory-key-2', $3) RETURNING id
	`, fixture.userID, "sk-inventory-2-"+suffix, fixture.groupID).Scan(&fixture.apiKey2))
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, fixture.userID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, fixture.channelID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, fixture.groupID)
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, fixture.accountID)
	})
	return fixture
}

func insertInventoryAccount(t *testing.T, credentials string) int64 {
	return insertInventoryAccountForPlatform(t, "openai", credentials)
}

func insertInventoryAccountForPlatform(t *testing.T, platform, credentials string) int64 {
	return insertInventoryAccountWithType(t, platform, service.AccountTypeAPIKey, credentials)
}

func insertInventoryAccountWithType(t *testing.T, platform, accountType, credentials string) int64 {
	t.Helper()
	var id int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO accounts (name, platform, type, credentials, status, schedulable)
		VALUES ($1, $2, $3, $4::jsonb, 'active', TRUE) RETURNING id
	`, fmt.Sprintf("inventory-account-%d", time.Now().UnixNano()), platform, accountType, credentials).Scan(&id))
	return id
}

func listModelGovernanceInventory(ctx context.Context, cutoff7d, cutoff30d, windowEnd time.Time) ([]service.InventoryItem, error) {
	return listModelGovernanceInventoryForRunMode(ctx, config.RunModeStandard, cutoff7d, cutoff30d, windowEnd)
}

func listModelGovernanceInventoryForRunMode(ctx context.Context, runMode string, cutoff7d, cutoff30d, windowEnd time.Time) ([]service.InventoryItem, error) {
	return NewModelGovernanceInventoryRepository(integrationDB).List(
		ctx, func(input service.InventoryProjectionInput) []service.InventoryRuntimeDimensionProjection {
			return service.ProjectInventoryRuntimeDimensions(input, runMode)
		}, cutoff7d, cutoff30d, windowEnd,
	)
}

func inventoryAccountState(t *testing.T, accountID int64) string {
	t.Helper()
	var state string
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT status || ':' || schedulable::text || ':' || credentials::text
		FROM accounts WHERE id = $1
	`, accountID).Scan(&state))
	return state
}

func inventoryChannelCapabilityState(t *testing.T, channelID int64) string {
	t.Helper()
	var state string
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT c.model_mapping::text || ':' || cmp.models::text
		FROM channels c
		JOIN channel_model_pricing cmp ON cmp.channel_id = c.id AND cmp.platform = 'openai'
		WHERE c.id = $1
	`, channelID).Scan(&state))
	return state
}

func insertInventoryObservation(t *testing.T, accountID int64, model, classification string, observedAt time.Time) {
	t.Helper()
	batchID := fmt.Sprintf("inventory-observation-%d", time.Now().UnixNano())
	_, err := integrationDB.ExecContext(context.Background(), `
		INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, raw_snapshot, observed_at)
		VALUES ($1, $1, $2, '{}'::jsonb, $3)
	`, batchID, accountID, observedAt)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(context.Background(), `
		INSERT INTO model_observations (
			account_id, upstream_model_id, snapshot_batch_id, classification, classification_reason,
			upstream_presence, first_seen_at, last_seen_at
		) VALUES ($1, $2, $3, $4, 'inventory_fixture', 'present', $5, $5)
	`, accountID, model, batchID, classification, observedAt)
	require.NoError(t, err)
}

func insertInventoryUsage(t *testing.T, fixture modelGovernanceInventoryFixture, model, currency, requestID string, apiKeyID int64, actualCost string, createdAt time.Time) {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		INSERT INTO usage_logs (
			user_id, api_key_id, account_id, request_id, model, upstream_model, group_id, channel_id,
			actual_cost, settlement_currency, created_at
		) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8::numeric, $9, $10)
	`, fixture.userID, apiKeyID, fixture.accountID, requestID, model, fixture.groupID, fixture.channelID, actualCost, currency, createdAt)
	require.NoError(t, err)
}

func insertInventoryUsageWithTarget(t *testing.T, fixture modelGovernanceInventoryFixture, model, targetPlatform, currency, requestID string, apiKeyID int64, actualCost string, createdAt time.Time) {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		INSERT INTO usage_logs (
			user_id, api_key_id, account_id, request_id, model, upstream_model, governance_target_platform,
			group_id, channel_id, actual_cost, settlement_currency, created_at
		) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9::numeric, $10, $11)
	`, fixture.userID, apiKeyID, fixture.accountID, requestID, model, targetPlatform, fixture.groupID, fixture.channelID, actualCost, currency, createdAt)
	require.NoError(t, err)
}

func insertInventoryUsageSeries(t *testing.T, fixture modelGovernanceInventoryFixture, model, currency string, apiKeyID int64, actualCost string, createdAt time.Time, count int) {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		INSERT INTO usage_logs (
			user_id, api_key_id, account_id, request_id, model, upstream_model, group_id, channel_id,
			actual_cost, settlement_currency, created_at
		)
		SELECT $1, $2, $3, 'inventory-series-' || sequence, $4, $4, $5, $6, $7::numeric, $8, $9
		FROM generate_series(1, $10) AS sequence
	`, fixture.userID, apiKeyID, fixture.accountID, model, fixture.groupID, fixture.channelID, actualCost, currency, createdAt, count)
	require.NoError(t, err)
}

func inventoryItemsForAccount(items []service.InventoryItem, accountID int64) map[string]service.InventoryItem {
	result := make(map[string]service.InventoryItem)
	for _, item := range items {
		if item.AccountID == accountID {
			result[item.UpstreamModelID] = item
		}
	}
	return result
}

func inventoryItemForTarget(items []service.InventoryItem, accountID int64, model, targetPlatform string) (service.InventoryItem, bool) {
	for _, item := range items {
		if item.AccountID == accountID && item.UpstreamModelID == model && item.TargetPlatform == targetPlatform {
			return item, true
		}
	}
	return service.InventoryItem{}, false
}

func inventoryUngroupedTargets(items []service.InventoryItem, accountID int64, model string) map[string]struct{} {
	targets := make(map[string]struct{})
	for _, item := range items {
		if item.AccountID == accountID && item.UpstreamModelID == model && item.GroupID == nil && item.ChannelID == nil {
			targets[item.TargetPlatform] = struct{}{}
		}
	}
	return targets
}

func requireInventoryError(t *testing.T, err error, reason string, fixture modelGovernanceInventoryFixture, model string) {
	t.Helper()
	require.Equal(t, reason, infraerrors.Reason(err))
	metadata := infraerrors.FromError(err).Metadata
	require.Equal(t, strconv.FormatInt(fixture.accountID, 10), metadata["account_id"])
	require.Equal(t, strconv.FormatInt(fixture.groupID, 10), metadata["group_id"])
	require.Equal(t, strconv.FormatInt(fixture.channelID, 10), metadata["channel_id"])
	require.Equal(t, model, metadata["upstream_model_id"])
}

func inventoryBusinessState(t *testing.T, fixture modelGovernanceInventoryFixture) map[string]int64 {
	t.Helper()
	queries := map[string]struct {
		query string
		args  []any
	}{
		"accounts":        {`SELECT COUNT(*) FROM accounts WHERE id = $1`, []any{fixture.accountID}},
		"groups":          {`SELECT COUNT(*) FROM groups WHERE id = $1`, []any{fixture.groupID}},
		"channels":        {`SELECT COUNT(*) FROM channels WHERE id = $1`, []any{fixture.channelID}},
		"observations":    {`SELECT COUNT(*) FROM model_observations WHERE account_id = $1`, []any{fixture.accountID}},
		"inventory_runs":  {`SELECT COUNT(*) FROM model_inventory_runs`, nil},
		"inventory_items": {`SELECT COUNT(*) FROM model_inventory_items`, nil},
	}
	result := make(map[string]int64, len(queries))
	for name, spec := range queries {
		var count int64
		err := integrationDB.QueryRowContext(context.Background(), spec.query, spec.args...).Scan(&count)
		require.NoError(t, err)
		result[name] = count
	}
	return result
}
