package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration211CreatesWalletLiveProvisional(t *testing.T) {
	content, err := FS.ReadFile("211_wallet_live_provisional.sql")
	require.NoError(t, err)
	sql := string(content)
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS wallet_live_provisional")
	require.Contains(t, sql, "token               TEXT PRIMARY KEY")
	require.Contains(t, sql, "uq_wallet_live_provisional_call_hash")
	require.Contains(t, sql, "WHERE call_hash <> ''")
	require.Contains(t, sql, "idx_wallet_live_provisional_user_status")
	require.Contains(t, sql, "idx_wallet_live_provisional_status_created")
	require.NotContains(t, sql, "DROP TABLE")
}
