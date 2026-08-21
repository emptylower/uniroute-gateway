package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelPublicationEnforcementMigrationContract(t *testing.T) {
	raw, err := os.ReadFile("203_model_publication_enforcement.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	// Required tables
	for _, table := range []string{
		"MODEL_PUBLICATION_ELIGIBILITY",
		"MODEL_PUBLICATION_EVENTS",
		"MODEL_AUTHORIZATION_ACTIVATIONS",
		"GOVERNANCE_IDEMPOTENCY_RECORDS",
	} {
		require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+table, "missing table %s", table)
	}

	// Unique (account_id, canonical_model_id, channel_id)
	require.Contains(t, sql, "UQ_MODEL_PUBLICATION_ELIGIBILITY_IDENTITY")
	require.Contains(t, sql, "UNIQUE (ACCOUNT_ID, CANONICAL_MODEL_ID, CHANNEL_ID)")

	// Eligibility check
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_ELIGIBILITY_ELIGIBILITY")
	require.Contains(t, sql, "'ELIGIBLE'")
	require.Contains(t, sql, "'BLOCKED'")
	require.Contains(t, sql, "'QUARANTINED'")
	require.Contains(t, sql, "'RETIRED'")
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_EVENTS_ELIGIBILITY")

	// Registry/channel versions
	require.Contains(t, sql, "REGISTRY_VERSION BIGINT NOT NULL")
	require.Contains(t, sql, "CHANNEL_VERSION BIGINT NOT NULL")
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_ELIGIBILITY_REGISTRY_VERSION")
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_ELIGIBILITY_CHANNEL_VERSION")
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_EVENTS_REGISTRY_VERSION")
	require.Contains(t, sql, "CHK_MODEL_PUBLICATION_EVENTS_CHANNEL_VERSION")
	require.Contains(t, sql, "REGISTRY_VERSION > 0")
	require.Contains(t, sql, "CHANNEL_VERSION > 0")

	// Quarantine batch ID
	require.Contains(t, sql, "QUARANTINE_BATCH_ID")
	require.Contains(t, sql, "BATCH_ID")

	// Append-only event trigger
	require.Contains(t, sql, "TRG_MODEL_PUBLICATION_EVENTS_APPEND_ONLY")
	require.Contains(t, sql, "REJECT_APPEND_ONLY_EVENT_MUTATION")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_PUBLICATION_EVENTS")
	require.Contains(t, sql, "BEFORE TRUNCATE ON MODEL_PUBLICATION_EVENTS")
	require.Contains(t, sql, "TRG_MODEL_AUTHORIZATION_ACTIVATIONS_APPEND_ONLY")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_AUTHORIZATION_ACTIVATIONS")

	// Activation inventory hash/version fields
	require.Contains(t, sql, "INVENTORY_HASH")
	require.Contains(t, sql, "CHK_MODEL_AUTHORIZATION_ACTIVATIONS_INVENTORY_HASH")
	require.Contains(t, sql, "REGISTRY_VERSION BIGINT NOT NULL")
	require.Contains(t, sql, "PROJECTED_BATCH_ID")
	require.Contains(t, sql, "CHANNEL_VERSIONS JSONB")
	require.Contains(t, sql, "IDEMPOTENCY_KEY VARCHAR(255) NOT NULL UNIQUE")
	require.Contains(t, sql, "ACKNOWLEDGED_BY")

	// Channel version support
	require.Contains(t, sql, "ALTER TABLE CHANNELS ADD COLUMN IF NOT EXISTS GOVERNANCE_VERSION")
	require.Contains(t, sql, "CHK_CHANNELS_GOVERNANCE_VERSION")
	require.Contains(t, sql, "GOVERNANCE_VERSION > 0")

	// Governance idempotency records
	require.Contains(t, sql, "GOVERNANCE_IDEMPOTENCY_RECORDS")
	require.Contains(t, sql, "IDEMPOTENCY_KEY VARCHAR(255) NOT NULL UNIQUE")
	require.Contains(t, sql, "REQUEST_HASH")
	require.Contains(t, sql, "OPERATION")

	// Exact-identity trigger for eligibility (READ COMMITTED)
	require.Contains(t, sql, "ENFORCE_MODEL_PUBLICATION_ELIGIBILITY_EXACT_UNIQUE")
	require.Contains(t, sql, "PG_ADVISORY_XACT_LOCK")
	require.Contains(t, sql, "CURRENT_SETTING('TRANSACTION_ISOLATION') <> 'READ COMMITTED'")
	require.Contains(t, sql, "ERRCODE = '0A000'")
	require.Contains(t, sql, "ERRCODE = '23505'")

	// No destructive operations
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM CHANNEL_PRICING")
	require.NotContains(t, sql, "DELETE FROM MODEL_OBSERVATIONS")
}

func TestModelPublicationEnforcementMigrationNoPriceMutation(t *testing.T) {
	raw, err := os.ReadFile("203_model_publication_enforcement.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))
	// Must not delete or rewrite channel prices, mappings, usage, route policy, or observations.
	require.NotContains(t, sql, "DROP COLUMN")
	// Ensure no direct mutation of pricing/mapping tables beyond additive channel version.
	require.NotContains(t, sql, "UPDATE CHANNEL_MODEL_PRICING")
	require.NotContains(t, sql, "DELETE FROM CHANNEL_MODEL_PRICING")
	require.NotContains(t, sql, "DROP TABLE")
}
