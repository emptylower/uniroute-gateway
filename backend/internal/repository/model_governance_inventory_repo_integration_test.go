//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

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
		AccountID: accountID, UpstreamModelID: "ungrouped-model", Classification: "unknown", BillingCurrency: "USD",
	})
}

func TestModelGovernanceInventoryRejectsMixedCurrencyAggregation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	fixture := createModelGovernanceInventoryFixture(t, "USD")
	insertInventoryUsage(t, fixture, "mixed-model", "USD", "inventory-mixed-usd", fixture.apiKey1, "1", now.Add(-time.Hour))
	insertInventoryUsage(t, fixture, "mixed-model", "CNY", "inventory-mixed-cny", fixture.apiKey2, "7", now.Add(-2*time.Hour))

	_, err := NewModelGovernanceInventoryRepository(integrationDB).List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
	require.ErrorContains(t, err, "mixed settlement currencies")
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
	t.Helper()
	var id int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO accounts (name, platform, type, credentials, status, schedulable)
		VALUES ($1, 'openai', 'apikey', $2::jsonb, 'active', TRUE) RETURNING id
	`, fmt.Sprintf("inventory-account-%d", time.Now().UnixNano()), credentials).Scan(&id))
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
