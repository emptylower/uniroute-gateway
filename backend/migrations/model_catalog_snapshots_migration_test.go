package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestModelCatalogSnapshotMigrationContract verifies the phase 6 immutable
// catalog evidence schema: source/status checks, immutable references,
// append-only protection, and defaults.
func TestModelCatalogSnapshotMigrationContract(t *testing.T) {
	raw, err := os.ReadFile("205_model_catalog_snapshots.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	// Required tables
	for _, table := range []string{
		"MODEL_CATALOG_SYNC_RUNS",
		"MODEL_CATALOG_SNAPSHOTS",
		"MODEL_CATALOG_CANDIDATE_EVIDENCE",
		"MODEL_CATALOG_MISSING_EVIDENCE",
		"MODEL_CATALOG_SOURCE_SETTINGS",
	} {
		require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+table)
	}

	// Source IDs are exactly openrouter|modelsdev|litellm on every evidence table.
	for _, chk := range []string{
		"CHK_MODEL_CATALOG_SYNC_RUNS_SOURCE",
		"CHK_MODEL_CATALOG_SNAPSHOTS_SOURCE",
		"CHK_MODEL_CATALOG_CANDIDATE_EVIDENCE_SOURCE",
		"CHK_MODEL_CATALOG_MISSING_EVIDENCE_SOURCE",
		"CHK_MODEL_CATALOG_SOURCE_SETTINGS_SOURCE",
	} {
		require.Contains(t, sql, chk)
	}
	require.Equal(t, 5, strings.Count(sql, "'OPENROUTER', 'MODELSDEV', 'LITELLM'"))

	// Sync run status checks: a run is running until it reaches a terminal state.
	require.Contains(t, sql, "CHK_MODEL_CATALOG_SYNC_RUNS_STATUS")
	for _, s := range []string{"'RUNNING'", "'SUCCEEDED'", "'FAILED'"} {
		require.Contains(t, sql, s)
	}

	// Immutable references: snapshots reference sync runs; evidence references snapshots.
	require.Contains(t, sql, "SYNC_RUN_ID BIGINT NOT NULL REFERENCES MODEL_CATALOG_SYNC_RUNS(ID)")
	require.Contains(t, sql, "SNAPSHOT_ID BIGINT NOT NULL REFERENCES MODEL_CATALOG_SNAPSHOTS(ID)")

	// Snapshot identity is immutable per (source, external_version).
	require.Contains(t, sql, "UQ_MODEL_CATALOG_SNAPSHOTS_IDENTITY")
	require.Contains(t, sql, "SOURCE, EXTERNAL_VERSION")

	// Append-only protection on snapshot payload and both evidence tables.
	require.Contains(t, sql, "TRG_MODEL_CATALOG_SNAPSHOTS_APPEND_ONLY")
	require.Contains(t, sql, "TRG_MODEL_CATALOG_CANDIDATE_EVIDENCE_APPEND_ONLY")
	require.Contains(t, sql, "TRG_MODEL_CATALOG_MISSING_EVIDENCE_APPEND_ONLY")
	require.Contains(t, sql, "REJECT_APPEND_ONLY_EVENT_MUTATION")

	// Defaults: 20% count-drop threshold; stale warnings 24h (openrouter) vs 72h.
	require.Contains(t, sql, "COUNT_DROP_THRESHOLD_PERCENT INTEGER NOT NULL DEFAULT 20")
	require.Contains(t, sql, "('OPENROUTER', 24)")
	require.Contains(t, sql, "('MODELSDEV', 72)")
	require.Contains(t, sql, "('LITELLM', 72)")

	// Payload integrity columns.
	require.Contains(t, sql, "PAYLOAD_ZSTD BYTEA NOT NULL")
	require.Contains(t, sql, "PAYLOAD_SHA256 VARCHAR(80) NOT NULL")

	// No destructive operations.
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DELETE FROM")
}

// TestModelCatalogSnapshotSyncRunTerminalOnce verifies sync runs cannot be
// rewritten after reaching a terminal state.
func TestModelCatalogSnapshotSyncRunTerminalOnce(t *testing.T) {
	raw, err := os.ReadFile("205_model_catalog_snapshots.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	require.Contains(t, sql, "ENFORCE_MODEL_CATALOG_SYNC_RUN_TERMINAL_ONCE")
	require.Contains(t, sql, "OLD.STATUS <> 'RUNNING'")
}
