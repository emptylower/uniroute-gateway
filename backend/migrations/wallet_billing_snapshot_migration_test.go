package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration209CreatesWalletBillingSnapshot(t *testing.T) {
	content, err := FS.ReadFile("209_wallet_billing_snapshot.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS wallet_billing_snapshot")
	require.Contains(t, sql, "id TEXT PRIMARY KEY")
	require.Contains(t, sql, "payload JSONB NOT NULL")
	require.Contains(t, sql, "idx_wallet_billing_snapshot_user_created")
	require.NotContains(t, sql, "DROP TABLE")
}

func TestMigration210AddsUsageLogsBillingSnapshotID(t *testing.T) {
	content, err := FS.ReadFile("210_usage_logs_billing_snapshot_id.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS billing_snapshot_id TEXT")
	require.Contains(t, sql, "idx_usage_logs_billing_snapshot_id")
}
