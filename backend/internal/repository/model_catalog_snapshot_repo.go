package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/klauspost/compress/zstd"
)

// modelCatalogSnapshotRepository is the append-only store for external catalog
// evidence. Snapshots are written with zstd-compressed exact raw bytes; evidence
// rows are written in the same transaction as their snapshot.
type modelCatalogSnapshotRepository struct {
	db *sql.DB
}

// NewModelCatalogSnapshotRepository builds the append-only catalog evidence store.
func NewModelCatalogSnapshotRepository(db *sql.DB) service.ModelCatalogSnapshotStore {
	return &modelCatalogSnapshotRepository{db: db}
}

func (r *modelCatalogSnapshotRepository) StartSyncRun(ctx context.Context, input service.StartCatalogSyncRunInput) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errors.New("model catalog snapshot repository is not configured")
	}
	if err := input.Validate(); err != nil {
		return 0, err
	}
	var id int64
	err := r.db.QueryRowContext(ctx, `
INSERT INTO model_catalog_sync_runs (source, triggered_by, request_url)
VALUES ($1, $2, $3)
RETURNING id
`, input.Source, input.TriggeredBy, strings.TrimSpace(input.RequestURL)).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// FailStaleRunningSyncRuns closes orphaned "running" rows left by a process
// that died or restarted mid-run. The ingestion loop is single-process: at
// startup every running row is orphaned by definition. Same sanctioned
// mutation channel as FinishSyncRun — a status transition, never an edit of
// recorded evidence.
func (r *modelCatalogSnapshotRepository) FailStaleRunningSyncRuns(ctx context.Context, reason string) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errors.New("model catalog snapshot repository is not configured")
	}
	res, err := r.db.ExecContext(ctx, `
UPDATE model_catalog_sync_runs
SET status = 'failed', error_message = $1, finished_at = NOW()
WHERE status = 'running'
`, reason)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (r *modelCatalogSnapshotRepository) FinishSyncRun(ctx context.Context, runID int64, status string, itemCount int, resolvedCommit string, errorMessage string) error {
	if r == nil || r.db == nil {
		return errors.New("model catalog snapshot repository is not configured")
	}
	if runID <= 0 {
		return fmt.Errorf("invalid sync run id: %d", runID)
	}
	if status != service.CatalogSyncStatusSucceeded && status != service.CatalogSyncStatusFailed {
		return fmt.Errorf("invalid terminal sync status: %q", status)
	}
	if itemCount < 0 {
		return fmt.Errorf("item count must be >= 0: %d", itemCount)
	}
	var commit any
	if strings.TrimSpace(resolvedCommit) != "" {
		commit = strings.TrimSpace(resolvedCommit)
	}
	var errMsg any
	if errorMessage != "" {
		errMsg = errorMessage
	}
	res, err := r.db.ExecContext(ctx, `
UPDATE model_catalog_sync_runs
SET status = $2, item_count = $3, resolved_commit = $4, error_message = $5,
    finished_at = NOW()
WHERE id = $1 AND status = 'running'
`, runID, status, itemCount, commit, errMsg)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return infraerrors.Conflict("CATALOG_SYNC_RUN_TERMINAL",
			fmt.Sprintf("sync run %d is already finished or missing", runID))
	}
	return nil
}

// InsertSnapshotWithEvidence writes the compressed snapshot and its normalized
// evidence atomically. Re-ingesting an identical (source, external_version) pair
// returns the existing snapshot id without duplicating evidence.
func (r *modelCatalogSnapshotRepository) InsertSnapshotWithEvidence(
	ctx context.Context,
	input service.CatalogSnapshotInput,
	evidence []service.CatalogCandidateEvidenceInput,
	missing []service.CatalogMissingEvidenceInput,
) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errors.New("model catalog snapshot repository is not configured")
	}
	if !service.ValidCatalogSource(input.Source) {
		return 0, fmt.Errorf("invalid catalog source: %q", input.Source)
	}
	version := strings.TrimSpace(input.ExternalVersion)
	if version == "" {
		return 0, errors.New("external version is required")
	}
	if len(input.RawPayload) == 0 {
		return 0, errors.New("raw payload is required")
	}
	if input.SyncRunID <= 0 {
		return 0, fmt.Errorf("invalid sync run id: %d", input.SyncRunID)
	}

	compressed, sha, size, err := compressSnapshotPayload(input.RawPayload)
	if err != nil {
		return 0, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	snapshotID, err := insertSnapshotRow(ctx, tx, input.SyncRunID, input.Source, version, compressed, sha, size)
	if err != nil {
		return 0, err
	}

	// Idempotent replay: same source+version already stored. Verify content hash
	// matches so immutable identity cannot be hijacked by different bytes.
	if snapshotID == 0 {
		existingID, existingSha, fetchErr := fetchSnapshotIdentity(ctx, tx, input.Source, version)
		if fetchErr != nil {
			return 0, fetchErr
		}
		if existingSha != sha {
			return 0, infraerrors.Conflict("CATALOG_SNAPSHOT_CONTENT_CONFLICT",
				fmt.Sprintf("snapshot %s/%s already exists with different content", input.Source, version))
		}
		if err = tx.Commit(); err != nil {
			return 0, err
		}
		return existingID, nil
	}

	for _, ev := range evidence {
		if err = insertCandidateEvidence(ctx, tx, snapshotID, input.Source, ev); err != nil {
			return 0, err
		}
	}
	for _, m := range missing {
		if err = insertMissingEvidence(ctx, tx, input.SyncRunID, input.Source, m); err != nil {
			return 0, err
		}
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return snapshotID, nil
}

func compressSnapshotPayload(raw []byte) ([]byte, string, int, error) {
	encoder, err := zstd.NewWriter(io.Discard)
	if err != nil {
		return nil, "", 0, err
	}
	defer encoder.Close()
	compressed := encoder.EncodeAll(raw, nil)
	sum := sha256.Sum256(raw)
	return compressed, fmt.Sprintf("sha256:%x", sum), len(raw), nil
}

func insertSnapshotRow(
	ctx context.Context,
	tx *sql.Tx,
	syncRunID int64,
	source string,
	version string,
	compressed []byte,
	sha string,
	size int,
) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
INSERT INTO model_catalog_snapshots (sync_run_id, source, external_version, payload_zstd, payload_sha256, payload_size)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (source, external_version) DO NOTHING
RETURNING id
`, syncRunID, source, version, compressed, sha, size).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		// Conflict happened; caller resolves via fetchSnapshotIdentity.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

func fetchSnapshotIdentity(ctx context.Context, tx *sql.Tx, source, version string) (int64, string, error) {
	var id int64
	var sha string
	err := tx.QueryRowContext(ctx, `
SELECT id, payload_sha256 FROM model_catalog_snapshots
WHERE source = $1 AND external_version = $2
`, source, version).Scan(&id, &sha)
	if err != nil {
		return 0, "", err
	}
	return id, sha, nil
}

func insertCandidateEvidence(ctx context.Context, tx *sql.Tx, snapshotID int64, source string, ev service.CatalogCandidateEvidenceInput) error {
	canonical := strings.TrimSpace(ev.CanonicalModelID)
	if canonical == "" {
		return errors.New("candidate evidence canonical model id is required")
	}
	capabilities := []string{}
	for _, c := range ev.Capabilities {
		if trimmed := strings.TrimSpace(c); trimmed != "" {
			capabilities = append(capabilities, trimmed)
		}
	}
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		return err
	}
	aliases := []string{}
	for _, a := range ev.Aliases {
		if trimmed := strings.TrimSpace(a); trimmed != "" {
			aliases = append(aliases, trimmed)
		}
	}
	aliasesJSON, err := json.Marshal(aliases)
	if err != nil {
		return err
	}
	var hint any
	if trimmed := strings.TrimSpace(ev.ProviderHint); trimmed != "" {
		hint = trimmed
	}
	var price any
	if strings.TrimSpace(ev.PriceJSON) != "" {
		price = ev.PriceJSON
	}
	var contextWindow any
	if ev.ContextWindow > 0 {
		contextWindow = ev.ContextWindow
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO model_catalog_candidate_evidence
    (snapshot_id, source, canonical_model_id, provider_hint, display_name, context_window, capabilities, aliases, price, raw_ref)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10)
ON CONFLICT (snapshot_id, canonical_model_id) DO NOTHING
`, snapshotID, source, canonical, hint, strings.TrimSpace(ev.DisplayName), contextWindow, string(capabilitiesJSON), string(aliasesJSON), price, strings.TrimSpace(ev.RawRef))
	return err
}

func insertMissingEvidence(ctx context.Context, tx *sql.Tx, syncRunID int64, source string, m service.CatalogMissingEvidenceInput) error {
	canonical := strings.TrimSpace(m.CanonicalModelID)
	if canonical == "" {
		return errors.New("missing evidence canonical model id is required")
	}
	detail := strings.TrimSpace(m.DetailJSON)
	if detail == "" {
		detail = "{}"
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO model_catalog_missing_evidence (sync_run_id, source, canonical_model_id, detail)
VALUES ($1, $2, $3, $4::jsonb)
ON CONFLICT (sync_run_id, canonical_model_id) DO NOTHING
`, syncRunID, source, canonical, detail)
	return err
}

// ---------- read side (phase 6 aggregation views) ----------

func (r *modelCatalogSnapshotRepository) SourceSettings(ctx context.Context) ([]service.CatalogSourceSetting, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("model catalog snapshot repository is not configured")
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT source, enabled, count_drop_threshold_percent, stale_after_hours
FROM model_catalog_source_settings
ORDER BY source
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	settings := []service.CatalogSourceSetting{}
	for rows.Next() {
		var setting service.CatalogSourceSetting
		if err := rows.Scan(&setting.Source, &setting.Enabled, &setting.CountDropThresholdPercent, &setting.StaleAfterHours); err != nil {
			return nil, err
		}
		settings = append(settings, setting)
	}
	return settings, rows.Err()
}

func (r *modelCatalogSnapshotRepository) UpdateSourceSetting(ctx context.Context, source string, enabled bool, thresholdPercent int) error {
	if r == nil || r.db == nil {
		return errors.New("model catalog snapshot repository is not configured")
	}
	if !service.ValidCatalogSource(source) {
		return fmt.Errorf("invalid catalog source: %q", source)
	}
	if thresholdPercent < 1 || thresholdPercent > 100 {
		return fmt.Errorf("count drop threshold must be within 1..100: %d", thresholdPercent)
	}
	res, err := r.db.ExecContext(ctx, `
UPDATE model_catalog_source_settings
SET enabled = $2, count_drop_threshold_percent = $3
WHERE source = $1
`, source, enabled, thresholdPercent)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return infraerrors.NotFound("CATALOG_SOURCE_NOT_FOUND", fmt.Sprintf("catalog source %q not found", source))
	}
	return nil
}

func (r *modelCatalogSnapshotRepository) LatestAcceptedSnapshotPerSource(ctx context.Context) (map[string]service.CatalogSnapshotRef, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("model catalog snapshot repository is not configured")
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT DISTINCT ON (run.source)
    run.source, snap.id, snap.external_version, run.finished_at, run.item_count, COALESCE(run.resolved_commit, '')
FROM model_catalog_snapshots snap
JOIN model_catalog_sync_runs run ON run.id = snap.sync_run_id
WHERE run.status = 'succeeded' AND run.finished_at IS NOT NULL
ORDER BY run.source, run.finished_at DESC, snap.id DESC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make(map[string]service.CatalogSnapshotRef)
	for rows.Next() {
		var ref service.CatalogSnapshotRef
		var source string
		if err := rows.Scan(&source, &ref.SnapshotID, &ref.ExternalVersion, &ref.AcceptedAt, &ref.ItemCount, &ref.ResolvedCommit); err != nil {
			return nil, err
		}
		refs[source] = ref
	}
	return refs, rows.Err()
}

func (r *modelCatalogSnapshotRepository) ListEvidence(ctx context.Context, snapshotID int64) ([]service.CatalogCandidateEvidenceRow, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("model catalog snapshot repository is not configured")
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT source, canonical_model_id, COALESCE(provider_hint, ''), COALESCE(display_name, ''),
       COALESCE(context_window, 0), capabilities::text, aliases::text, COALESCE(price::text, '')
FROM model_catalog_candidate_evidence
WHERE snapshot_id = $1
ORDER BY canonical_model_id
`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []service.CatalogCandidateEvidenceRow{}
	for rows.Next() {
		var row service.CatalogCandidateEvidenceRow
		var capabilities, aliases string
		if err := rows.Scan(&row.Source, &row.CanonicalModelID, &row.ProviderHint, &row.DisplayName,
			&row.ContextWindow, &capabilities, &aliases, &row.PriceJSON); err != nil {
			return nil, err
		}
		if row.Capabilities, err = decodeStringArray(capabilities); err != nil {
			return nil, err
		}
		if row.Aliases, err = decodeStringArray(aliases); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *modelCatalogSnapshotRepository) MissingEvidenceSummary(ctx context.Context) ([]service.CatalogMissingSummaryRow, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("model catalog snapshot repository is not configured")
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT m.canonical_model_id, COUNT(DISTINCT m.sync_run_id), MIN(run.started_at), MAX(run.started_at)
FROM model_catalog_missing_evidence m
JOIN model_catalog_sync_runs run ON run.id = m.sync_run_id AND run.status = 'succeeded'
GROUP BY m.canonical_model_id
ORDER BY m.canonical_model_id
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []service.CatalogMissingSummaryRow{}
	for rows.Next() {
		var row service.CatalogMissingSummaryRow
		if err := rows.Scan(&row.CanonicalModelID, &row.AcceptedRunCount, &row.FirstMissingAt, &row.LastMissingAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func decodeStringArray(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
		return nil, fmt.Errorf("decode json string array: %w", err)
	}
	return out, nil
}

// NewModelCatalogEvidenceRepository exposes the read side of the catalog
// evidence store as its own injectable interface.
func NewModelCatalogEvidenceRepository(db *sql.DB) service.ModelCatalogEvidenceReader {
	return &modelCatalogSnapshotRepository{db: db}
}
