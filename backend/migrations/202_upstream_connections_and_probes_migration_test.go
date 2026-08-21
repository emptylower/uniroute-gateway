package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamConnectionsAndProbesMigrationContract(t *testing.T) {
	raw, err := os.ReadFile("202_upstream_connections_and_probes.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	// Required tables
	for _, table := range []string{
		"UPSTREAM_CONNECTIONS",
		"ACCOUNT_ENDPOINT_PROBES",
		"UPSTREAM_CONNECTION_EVENTS",
		"AGGREGATOR_REUSE_REQUESTS",
	} {
		require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+table)
	}

	// Kind/provider constraints
	require.Contains(t, sql, "CHK_UPSTREAM_CONNECTIONS_KIND")
	require.Contains(t, sql, "CHK_UPSTREAM_CONNECTIONS_PROVIDER")
	require.Contains(t, sql, "CHK_UPSTREAM_CONNECTIONS_KIND_PROVIDER")
	require.Contains(t, sql, "'FIRST_PARTY'")
	require.Contains(t, sql, "'AGGREGATOR'")
	for _, p := range []string{"'ANTHROPIC'", "'OPENAI'", "'GEMINI'", "'GROK'"} {
		require.Contains(t, sql, p)
	}
	require.Contains(t, sql, "PROVIDER IS NULL")
	require.Contains(t, sql, "KIND = 'AGGREGATOR' AND PROVIDER IS NULL")
	require.Contains(t, sql, "KIND = 'FIRST_PARTY'")

	// Protocol constraints
	require.Contains(t, sql, "CHK_ACCOUNT_ENDPOINT_PROBES_PROTOCOL")
	require.Contains(t, sql, "'ANTHROPIC'")
	require.Contains(t, sql, "'OPENAI'")
	require.Contains(t, sql, "'GEMINI'")
	require.NotContains(t, sql, "'GROK' AS PROTOCOL") // grok is provider, not protocol

	// Nullable account connection during backfill (accounts side only)
	require.Contains(t, sql, "ALTER TABLE ACCOUNTS")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS CONNECTION_ID BIGINT REFERENCES UPSTREAM_CONNECTIONS(ID)")
	// accounts.connection_id must be nullable for backfill; other tables (probes, events) legitimately use NOT NULL FKs
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS CONNECTION_ID BIGINT REFERENCES UPSTREAM_CONNECTIONS(ID) ON DELETE SET NULL")
	require.Contains(t, sql, "IDX_ACCOUNTS_CONNECTION_ID")

	// Probe uniqueness key
	require.Contains(t, sql, "ENFORCE_ACCOUNT_ENDPOINT_PROBE_EXACT_UNIQUE")
	require.Contains(t, sql, "PG_ADVISORY_XACT_LOCK")
	require.Contains(t, sql, "UQ_ACCOUNT_ENDPOINT_PROBES_IDENTITY_EXACT")
	require.Contains(t, sql, "CURRENT_SETTING('TRANSACTION_ISOLATION') <> 'READ COMMITTED'")
	require.Contains(t, sql, "ERRCODE = '0A000'")
	require.Contains(t, sql, "ERRCODE = '23505'")

	// Probe validity 24 hours
	require.Contains(t, sql, "CHK_ACCOUNT_ENDPOINT_PROBES_VALIDITY")
	require.Contains(t, sql, "INTERVAL '24 HOURS'")
	require.Contains(t, sql, "EXPIRES_AT > PROBED_AT")
	require.Contains(t, sql, "EXPIRES_AT = PROBED_AT + INTERVAL '24 HOURS'")

	// Credential version checks
	require.Contains(t, sql, "CREDENTIAL_VERSION > 0")
	require.Contains(t, sql, "CONFIG_VERSION > 0")

	// Aggregator reuse uniqueness scope
	require.Contains(t, sql, "UQ_AGGREGATOR_REUSE_REQUESTS_SCOPE")
	require.Contains(t, sql, "CONNECTION_ID, PROVIDER, PROTOCOL, NORMALIZED_ENDPOINT_PATH, CLIENT_REQUEST_ID")

	// Encrypted credential not empty
	require.Contains(t, sql, "ENCRYPTED_CREDENTIAL <> ''")
	require.Contains(t, sql, "BASE_URL <> ''")

	// Append-only protections where required
	require.Contains(t, sql, "TRG_ACCOUNT_ENDPOINT_PROBES_APPEND_ONLY")
	require.Contains(t, sql, "TRG_UPSTREAM_CONNECTION_EVENTS_APPEND_ONLY")
	require.Contains(t, sql, "REJECT_APPEND_ONLY_EVENT_MUTATION")

	// No destructive operations
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")

	// Ensure governed provider check appears for probes
	require.Contains(t, sql, "CHK_ACCOUNT_ENDPOINT_PROBES_PROVIDER")
	require.Contains(t, sql, "CHK_AGGREGATOR_REUSE_REQUESTS_PROVIDER")
}

func TestUpstreamConnectionKindProviderAffinity(t *testing.T) {
	raw, err := os.ReadFile("202_upstream_connections_and_probes.sql")
	require.NoError(t, err)
	sql := string(raw)
	// Ensure constraint text exact match for affinity
	require.Contains(t, sql, "(kind = 'aggregator' AND provider IS NULL) OR")
	require.Contains(t, sql, "(kind = 'first_party' AND provider IN ('anthropic', 'openai', 'gemini', 'grok'))")
}

func TestProbeValidityKeyCoverage(t *testing.T) {
	raw, err := os.ReadFile("202_upstream_connections_and_probes.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))
	// Key components must all appear in uniqueness function
	for _, col := range []string{"ACCOUNT_ID", "CONNECTION_ID", "PROVIDER", "PROTOCOL", "NORMALIZED_ENDPOINT_PATH", "CREDENTIAL_VERSION", "CONFIG_VERSION"} {
		require.Contains(t, sql, col)
	}
}
