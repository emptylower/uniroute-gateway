package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelGovernanceFoundationMigrationContract(t *testing.T) {
	raw, err := os.ReadFile("200_model_governance_foundation.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	for _, table := range []string{
		"MODEL_REGISTRY",
		"MODEL_REGISTRY_ALIASES",
		"MODEL_REGISTRY_EVENTS",
		"MODEL_CLASSIFICATION_BATCHES",
		"MODEL_OBSERVATIONS",
		"MODEL_OBSERVATION_EVENTS",
		"MODEL_INVENTORY_RUNS",
		"MODEL_INVENTORY_ITEMS",
	} {
		require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+table)
	}

	for _, value := range []string{"'ANTHROPIC'", "'OPENAI'", "'GEMINI'", "'GROK'"} {
		require.Contains(t, sql, value)
	}
	for _, value := range []string{"'TEXT'", "'IMAGE'", "'AUDIO'", "'VIDEO'", "'EMBEDDING'", "'OTHER'"} {
		require.Contains(t, sql, value)
	}
	for _, value := range []string{"'ACTIVE'", "'DEPRECATED'", "'RETIRED'"} {
		require.Contains(t, sql, value)
	}
	for _, value := range []string{"'DISCOVERED'", "'APPROVED'", "'CROSS_PROVIDER'", "'UNKNOWN'", "'IGNORED'"} {
		require.Contains(t, sql, value)
	}
	for _, value := range []string{"'PRESENT'", "'MISSING'"} {
		require.Contains(t, sql, value)
	}

	require.Contains(t, sql, "UNIQUE (ACCOUNT_ID, UPSTREAM_MODEL_ID)")
	require.Contains(t, sql, "CONNECTION_ID BIGINT")
	require.NotContains(t, sql, "CONNECTION_ID BIGINT NOT NULL")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_REGISTRY_EVENTS")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_OBSERVATION_EVENTS")
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")
}
