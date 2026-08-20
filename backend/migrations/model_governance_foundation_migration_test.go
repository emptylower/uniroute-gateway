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
		"ACCOUNT_INCARNATION_IDS",
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
	require.True(t, hasExactCreateTables(sql, expectedTables), "migration 200 must create exactly the nine approved tables")

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

	require.NotContains(t, sql, "CANONICAL_ID TEXT NOT NULL UNIQUE")
	require.NotContains(t, sql, "ALIAS TEXT NOT NULL UNIQUE")
	require.NotContains(t, sql, "UNIQUE (ACCOUNT_ID, UPSTREAM_MODEL_ID)")
	for _, index := range []string{
		"IDX_MODEL_REGISTRY_CANONICAL_BUCKET",
		"IDX_MODEL_REGISTRY_ALIASES_ALIAS_BUCKET",
		"IDX_MODEL_OBSERVATIONS_IDENTITY_BUCKET",
		"IDX_MODEL_INVENTORY_ITEMS_IDENTITY_BUCKET",
	} {
		require.Contains(t, sql, "CREATE INDEX IF NOT EXISTS "+index)
	}
	for _, table := range []string{"MODEL_REGISTRY", "MODEL_REGISTRY_ALIASES", "MODEL_OBSERVATIONS", "MODEL_INVENTORY_ITEMS"} {
		require.Regexp(t, regexp.MustCompile(`BEFORE INSERT OR UPDATE[^;]* ON `+table), sql)
	}
	require.Contains(t, sql, "PG_ADVISORY_XACT_LOCK")
	require.Contains(t, sql, "ERRCODE = '23505'")
	require.Equal(t, 4, strings.Count(sql, "CURRENT_SETTING('TRANSACTION_ISOLATION') <> 'READ COMMITTED'"),
		"every exact-identity trigger must reject stale-snapshot write isolation before locking")
	require.Equal(t, 4, strings.Count(sql, "ERRCODE = '0A000'"),
		"every exact-identity trigger must report unsupported write isolation clearly")
	firstIsolationGuard := strings.Index(sql, "CURRENT_SETTING('TRANSACTION_ISOLATION') <> 'READ COMMITTED'")
	firstAdvisoryLock := strings.Index(sql, "PG_ADVISORY_XACT_LOCK")
	require.NotEqual(t, -1, firstIsolationGuard)
	require.Less(t, firstIsolationGuard, firstAdvisoryLock,
		"isolation must be rejected before advisory locking or exact-identity queries")
	require.Contains(t, sql, "CONNECTION_ID BIGINT")
	require.NotContains(t, sql, "CONNECTION_ID BIGINT NOT NULL")
	for _, column := range []string{
		"CANONICAL_ID TEXT NOT NULL",
		"ALIAS TEXT NOT NULL",
		"UPSTREAM_MODEL_ID TEXT NOT NULL",
	} {
		require.Contains(t, sql, column)
	}
	require.NotRegexp(t, regexp.MustCompile(`(?:CANONICAL_ID|ALIAS|UPSTREAM_MODEL_ID)\s+VARCHAR\(255\)`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_CLASSIFICATION_BATCHES\s*\([^;]*ACCOUNT_ID\s+BIGINT\s+NOT NULL\s*,`), sql)
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_CLASSIFICATION_BATCHES\s*\([^;]*ACCOUNT_ID\s+BIGINT[^,]*REFERENCES\s+ACCOUNTS`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATIONS\s*\([^;]*SNAPSHOT_BATCH_ID\s+VARCHAR\(64\)\s+NOT NULL\s+REFERENCES\s+MODEL_CLASSIFICATION_BATCHES\(BATCH_ID\)(?:\s+ON DELETE (?:RESTRICT|NO ACTION))?`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*BATCH_ID\s+VARCHAR\(64\)\s+NOT NULL\s+REFERENCES\s+MODEL_CLASSIFICATION_BATCHES\(BATCH_ID\)(?:\s+ON DELETE (?:RESTRICT|NO ACTION))?`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*SNAPSHOT_BATCH_ID\s+VARCHAR\(64\)\s+NOT NULL\s+REFERENCES\s+MODEL_CLASSIFICATION_BATCHES\(BATCH_ID\)(?:\s+ON DELETE (?:RESTRICT|NO ACTION))?`), sql)
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATIONS\s*\([^;]*RAW_SNAPSHOT`), sql)
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_REGISTRY_EVENTS")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_CLASSIFICATION_BATCHES")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON MODEL_OBSERVATION_EVENTS")
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_REGISTRY_EVENTS\s*\([^;]*REGISTRY_ID\s+BIGINT\s+REFERENCES`), sql)
	require.NotRegexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*OBSERVATION_ID\s+BIGINT[^,]*REFERENCES`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*ACCOUNT_ID\s+BIGINT\s+NOT NULL`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_OBSERVATION_EVENTS\s*\([^;]*UPSTREAM_MODEL_ID\s+TEXT\s+NOT NULL`), sql)
	require.NotContains(t, sql, "UNIQUE NULLS NOT DISTINCT")
	require.NotContains(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS UQ_MODEL_INVENTORY_ITEMS_DIMENSIONS")
	require.Contains(t, sql, "(GROUP_ID IS NULL)")
	require.Contains(t, sql, "COALESCE(GROUP_ID, 0)")
	require.Contains(t, sql, "(CHANNEL_ID IS NULL)")
	require.Contains(t, sql, "COALESCE(CHANNEL_ID, 0)")
	require.Contains(t, sql, "ALTER TABLE USAGE_LOGS")
	require.Regexp(t, regexp.MustCompile(`ADD COLUMN IF NOT EXISTS GOVERNANCE_TARGET_PLATFORM\s+VARCHAR\(32\)`), sql)
	require.Regexp(t, regexp.MustCompile(`(?s)MODEL_INVENTORY_ITEMS\s*\([^;]*TARGET_PLATFORM\s+VARCHAR\(32\)\s+NOT NULL`), sql)
	require.Contains(t, sql, "NEW.TARGET_PLATFORM")
	require.Contains(t, sql, "EXISTING.TARGET_PLATFORM = NEW.TARGET_PLATFORM")
	require.NotRegexp(t, regexp.MustCompile(`CREATE TABLE IF NOT EXISTS [A-Z0-9_]*ACTIVATION[A-Z0-9_]*`), sql)
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")
	require.Contains(t, sql, "ENFORCE_ACCOUNT_INCARNATION_ID_NOT_REUSED")
	require.Contains(t, sql, "AFTER INSERT ON ACCOUNTS")
	require.NotContains(t, sql, "BEFORE INSERT ON ACCOUNTS")
	require.Contains(t, sql, "BEFORE UPDATE OF ID ON ACCOUNTS")
	require.Contains(t, sql, "INSERT INTO ACCOUNT_INCARNATION_IDS (ACCOUNT_ID)")
	require.Contains(t, sql, "SELECT ID FROM ACCOUNTS")
	require.Contains(t, sql, "LOCK TABLE ACCOUNTS IN SHARE ROW EXCLUSIVE MODE")
	require.Less(t, strings.Index(sql, "LOCK TABLE ACCOUNTS IN SHARE ROW EXCLUSIVE MODE"), strings.Index(sql, "SELECT ID FROM ACCOUNTS"),
		"accounts writes must be locked before the incarnation ledger backfill")
	require.Less(t, strings.Index(sql, "SELECT ID FROM ACCOUNTS"), strings.Index(sql, "AFTER INSERT ON ACCOUNTS"),
		"existing account IDs must be backfilled before the insert trigger is created")
	require.Contains(t, sql, "BEFORE UPDATE OR DELETE ON ACCOUNT_INCARNATION_IDS")
	for _, table := range []string{
		"ACCOUNT_INCARNATION_IDS",
		"MODEL_REGISTRY_EVENTS",
		"MODEL_CLASSIFICATION_BATCHES",
		"MODEL_OBSERVATION_EVENTS",
	} {
		require.Contains(t, sql, "BEFORE TRUNCATE ON "+table)
	}
	require.Equal(t, 4, strings.Count(sql, "BEFORE TRUNCATE ON"))
	require.GreaterOrEqual(t, strings.Count(sql, "FOR EACH STATEMENT"), 4)
	require.Contains(t, sql, "CONSTRAINT = 'ACCOUNT_ID_INCARNATION_NOT_REUSED'")
	require.Contains(t, sql, "ONLY SUCCESSFULLY INSERTED ACCOUNT INCARNATIONS CONSUME IDS")
	require.Contains(t, sql, "ROLLS BACK")
}

func TestModelGovernanceFoundationMigrationRejectsTenthTable(t *testing.T) {
	raw, err := os.ReadFile("200_model_governance_foundation.sql")
	require.NoError(t, err)
	mutatedSQL := strings.ToUpper(string(raw)) + "\nCREATE TABLE IF NOT EXISTS GOVERNANCE_AUDIT_SHADOW (ID BIGSERIAL PRIMARY KEY);"

	expectedTables := []string{
		"ACCOUNT_INCARNATION_IDS",
		"MODEL_REGISTRY",
		"MODEL_REGISTRY_ALIASES",
		"MODEL_REGISTRY_EVENTS",
		"MODEL_CLASSIFICATION_BATCHES",
		"MODEL_OBSERVATIONS",
		"MODEL_OBSERVATION_EVENTS",
		"MODEL_INVENTORY_RUNS",
		"MODEL_INVENTORY_ITEMS",
	}
	require.False(t, hasExactCreateTables(mutatedSQL, expectedTables), "a tenth table with any name must violate migration 200 scope")
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
