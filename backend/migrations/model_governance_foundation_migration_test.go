package migrations

import (
	"os"
	"regexp"
	"slices"
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
	require.True(t, hasExactCreateTables(sql, expectedTables), "migration 200 must create exactly the eight approved tables")

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
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*ACCOUNT_ID\s+BIGINT\s+NOT NULL`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*UPSTREAM_MODEL_ID\s+VARCHAR\(255\)\s+NOT NULL`), sql)
	require.NotContains(t, sql, "UNIQUE NULLS NOT DISTINCT")
	require.Contains(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS UQ_MODEL_INVENTORY_ITEMS_DIMENSIONS")
	require.Contains(t, sql, "(GROUP_ID IS NULL)")
	require.Contains(t, sql, "COALESCE(GROUP_ID, 0)")
	require.Contains(t, sql, "(CHANNEL_ID IS NULL)")
	require.Contains(t, sql, "COALESCE(CHANNEL_ID, 0)")
	require.NotRegexp(t, regexp.MustCompile(`CREATE TABLE IF NOT EXISTS [A-Z0-9_]*ACTIVATION[A-Z0-9_]*`), sql)
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")
}

func TestModelGovernanceFoundationMigrationRejectsNinthNonModelTable(t *testing.T) {
	raw, err := os.ReadFile("200_model_governance_foundation.sql")
	require.NoError(t, err)
	mutatedSQL := strings.ToUpper(string(raw)) + "\nCREATE TABLE IF NOT EXISTS GOVERNANCE_AUDIT_SHADOW (ID BIGSERIAL PRIMARY KEY);"

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
	require.False(t, hasExactCreateTables(mutatedSQL, expectedTables), "a ninth table with any name must violate migration 200 scope")
}

func hasExactCreateTables(sql string, expected []string) bool {
	tablePattern := regexp.MustCompile(`CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Z_][A-Z0-9_]*)\s*\(`)
	tableMatches := tablePattern.FindAllStringSubmatch(sql, -1)
	created := make([]string, 0, len(tableMatches))
	for _, match := range tableMatches {
		created = append(created, match[1])
	}

	return slices.Equal(expected, created)
}
