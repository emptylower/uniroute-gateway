package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration213CreatesWalletHoldOutcome(t *testing.T) {
	content, err := FS.ReadFile("213_wallet_hold_outcome.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS wallet_hold_outcome")
	for _, column := range []string{
		"authorization_id    TEXT PRIMARY KEY",
		"platform_user_id    TEXT NOT NULL",
		"lease_id            TEXT NOT NULL",
		"held_units          BIGINT NOT NULL",
		"class               TEXT NOT NULL",
		"armed_at            TIMESTAMPTZ NOT NULL",
		"classified_at       TIMESTAMPTZ NOT NULL",
		"resolution          TEXT NULL",
		"resolved_at         TIMESTAMPTZ NULL",
		"settlement_event_id TEXT NULL",
	} {
		require.Contains(t, sql, column)
	}
	require.Contains(t, sql, "CREATE INDEX IF NOT EXISTS wallet_hold_outcome_resolution_idx ON wallet_hold_outcome (resolution, classified_at)")
	require.NotContains(t, sql, "DROP")
}
