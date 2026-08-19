//go:build integration

package repository

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

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
	items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, before, inventoryBusinessState(t, fixture), "inventory query must not mutate business or governance state")

	byModel := make(map[string]service.InventoryItem, len(items))
	for _, item := range items {
		if item.AccountID == fixture.accountID && item.GroupID != nil && *item.GroupID == fixture.groupID &&
			item.ChannelID != nil && *item.ChannelID == fixture.channelID {
			byModel[item.UpstreamModelID] = item
		}
	}

	for _, model := range []string{"observed-model", "account-mapped-model", "channel-mapped-model", "priced-model", "routed-model", "used-model"} {
		_, ok := byModel[model]
		require.Truef(t, ok, "missing inventory source model %q", model)
	}
	require.NotContains(t, byModel, "outside-window")
	require.Equal(t, "approved", byModel["observed-model"].Classification)
	require.Equal(t, "unknown", byModel["priced-model"].Classification)
	require.Equal(t, service.InventoryItem{
		AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
		UpstreamModelID: "used-model", Classification: "unknown",
		Requests7d: 2, Requests30d: 3,
		Revenue7dBillingMicros: 5_000_002, Revenue30dBillingMicros: 6_250_003,
		BillingCurrency: "USD", AffectedAPIKeys7d: 2, AffectedAPIKeys30d: 2,
	}, byModel["used-model"])
}

func TestModelGovernanceInventoryIncludesEnabledAccountWithoutGroup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	accountID := insertInventoryAccount(t, `{"model_mapping":{"public":"ungrouped-model"}}`)
	t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

	items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	require.NoError(t, err)
	require.Contains(t, items, service.InventoryItem{
		AccountID: accountID, UpstreamModelID: "ungrouped-model", Classification: "unknown", BillingCurrency: "CNY",
	})
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
			accountID := insertInventoryAccountForPlatform(t, testCase.platform, `{"model_mapping":{"public":"mapped-model"}}`)
			insertInventoryObservation(t, accountID, "observed-model", "approved", now.Add(-time.Hour))
			t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) })

			items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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

			items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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

	items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	require.NoError(t, err)
	byModel := inventoryItemsForAccount(items, fixture.accountID)
	require.Equal(t, service.InventoryItem{
		AccountID: fixture.accountID, GroupID: &fixture.groupID, ChannelID: &fixture.channelID,
		UpstreamModelID: "historical-model", Classification: "unknown", Requests30d: 1,
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

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_MIXED_CURRENCY", fixture, "mixed-model")
}

func TestModelGovernanceInventoryRejectsNegativeRevenueWithTypedDimensions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "negative-model", "USD", "inventory-negative", fixture.apiKey1, "-0.000001", now.Add(-time.Hour))

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", fixture, "negative-model")
}

func TestModelGovernanceInventoryRejectsNegativeSourceRevenueWhenDimensionNetsPositive(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "offset-model", "USD", "inventory-offset-negative", fixture.apiKey1, "-1", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "offset-model", "USD", "inventory-offset-positive", fixture.apiKey2, "2", now.Add(-2*time.Hour))

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	requireInventoryError(t, err, "MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", fixture, "offset-model")
}

func TestModelGovernanceInventoryRejectsTinyNegativeSourceThatRoundsToZero(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "tiny-negative-model", "USD", "inventory-tiny-negative", fixture.apiKey1, "-0.0000000001", now.Add(-time.Hour))

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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

			items, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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

	_, err = NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
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
	fixture.accountID = insertInventoryAccount(t, `{"model_mapping":{"public-model":"account-mapped-model"}}`)
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
	t.Helper()
	var id int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO accounts (name, platform, type, credentials, status, schedulable)
		VALUES ($1, $2, 'apikey', $3::jsonb, 'active', TRUE) RETURNING id
	`, fmt.Sprintf("inventory-account-%d", time.Now().UnixNano()), platform, credentials).Scan(&id))
	return id
}

func insertInventoryObservation(t *testing.T, accountID int64, model, classification string, observedAt time.Time) {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		INSERT INTO model_observations (
			account_id, upstream_model_id, classification, classification_reason,
			upstream_presence, first_seen_at, last_seen_at, raw_snapshot
		) VALUES ($1, $2, $3, 'inventory_fixture', 'present', $4, $4, '{}'::jsonb)
	`, accountID, model, classification, observedAt)
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
