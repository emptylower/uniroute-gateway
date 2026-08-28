package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Phase 3.5 (redesign §11.9): migration 214 carries the durable join
// (authorization_id), the split columns (parent_event_id, split_depth) and
// pending_release_units — the record of what the dispatcher still owes the
// bound lease after a split (§11.3). Text assertions only, like 213's.
func TestMigration214AddsOutboxSplitAndAuthorizationColumns(t *testing.T) {
	content, err := FS.ReadFile("214_wallet_outbox_split_and_authorization.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS authorization_id TEXT")
	require.Contains(t, sql, "ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS parent_event_id TEXT")
	require.Contains(t, sql, "ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS split_depth INTEGER NOT NULL DEFAULT 0")
	require.Contains(t, sql, "ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS pending_release_units BIGINT")
	require.Contains(t, sql, "CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_parent ON wallet_settlement_outbox (parent_event_id)")
	require.Contains(t, sql, "COMMENT ON COLUMN wallet_settlement_outbox.dead_letter_reason", "the extended dead-letter comment names 3.5's terminal reasons")
	require.Contains(t, sql, "contract_violation | payload_conflict | split_exhausted")
	require.Contains(t, sql, "COMMENT ON COLUMN wallet_settlement_outbox.pending_release_units", "the pending-release column carries its own §11.3 comment")
	require.NotContains(t, sql, "DROP")
}
