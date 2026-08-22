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

	"github.com/Wei-Shaw/sub2api/internal/service"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
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
    (snapshot_id, source, canonical_model_id, provider_hint, display_name, context_window, capabilities, price, raw_ref)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
ON CONFLICT (snapshot_id, canonical_model_id) DO NOTHING
`, snapshotID, source, canonical, hint, strings.TrimSpace(ev.DisplayName), contextWindow, string(capabilitiesJSON), price, strings.TrimSpace(ev.RawRef))
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
