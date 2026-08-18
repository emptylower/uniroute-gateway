package migrations

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelGovernanceFoundationMigrationContract(t *testing.T) {
	raw, err := os.ReadFile("200_model_governance_foundation.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	expectedTables := []string{
		"MODEL_REGISTRY",
		"MODEL_REGISTRY_ALIASES",
		"MODEL_REGISTRY_EVENTS",
		"MODEL_CLASSIFICATION_BATCHES",
		"MODEL_OBSERVATIONS",
		"MODEL_OBSERVATION_EVENTS",
		"MODEL_INVENTORY_RUNS",
		"MODEL_INVENTORY_ITEMS",
	}
	for _, table := range expectedTables {
		require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+table)
	}
	tablePattern := regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (MODEL_[A-Z0-9_]+)`)
	tableMatches := tablePattern.FindAllStringSubmatch(sql, -1)
	createdTables := make([]string, 0, len(tableMatches))
	for _, match := range tableMatches {
		createdTables = append(createdTables, match[1])
	}
	require.ElementsMatch(t, expectedTables, createdTables, "migration 200 must create exactly the eight approved model tables")

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
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_REGISTRY_EVENTS\s*\([^;]*REGISTRY_ID\s+BIGINT\s+REFERENCES`), sql)
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*OBSERVATION_ID\s+BIGINT[^,]*REFERENCES`), sql)
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*BATCH_ID\s+VARCHAR\([^)]*\)[^,]*REFERENCES`), sql)
	require.Contains(t, sql, "UNIQUE NULLS NOT DISTINCT (RUN_ID, ACCOUNT_ID, GROUP_ID, CHANNEL_ID, UPSTREAM_MODEL_ID)")
	require.NotRegexp(t, regexp.MustCompile(`CREATE TABLE IF NOT EXISTS [A-Z0-9_]*ACTIVATION[A-Z0-9_]*`), sql)
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")
}
