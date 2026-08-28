package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration212AddsWalletOutboxDeadLetterReason(t *testing.T) {
	content, err := FS.ReadFile("212_wallet_outbox_dead_letter_reason.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS dead_letter_reason TEXT")
	require.Contains(t, sql, "COMMENT ON COLUMN wallet_settlement_outbox.dead_letter_reason IS 'NULL = predates 212 (unknown); balance_shortfall | attempts_exhausted'")
	require.NotContains(t, sql, "DROP")
}
