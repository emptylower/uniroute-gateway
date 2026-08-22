//go:build integration

package repository

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

var catalogSnapshotMu sync.Mutex

func newCatalogSnapshotTestInput(t *testing.T) (service.CatalogSnapshotInput, []byte) {
	t.Helper()
	raw := make([]byte, 4096)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return service.CatalogSnapshotInput{
		SyncRunID:       0,
		Source:          service.CatalogSourceOpenRouter,
		ExternalVersion: fmt.Sprintf("snap-%d", time.Now().UnixNano()),
		RawPayload:      raw,
	}, raw
}

func TestModelCatalogSnapshotStore_RoundTripAndAtomicity(t *testing.T) {
	catalogSnapshotMu.Lock()
	defer catalogSnapshotMu.Unlock()

	ctx := context.Background()
	repo := NewModelCatalogSnapshotRepository(integrationDB)

	runID, err := repo.StartSyncRun(ctx, service.StartCatalogSyncRunInput{
		Source:      service.CatalogSourceOpenRouter,
		TriggeredBy: service.CatalogSyncTriggerManual,
		RequestURL:  "https://openrouter.ai/api/v1/models",
	})
	require.NoError(t, err)
	require.Greater(t, runID, int64(0))

	input, raw := newCatalogSnapshotTestInput(t)
	input.SyncRunID = runID
	evidence := []service.CatalogCandidateEvidenceInput{
		{
			CanonicalModelID: "claude-test-4",
			ProviderHint:     "anthropic",
			DisplayName:      "Claude Test 4",
			ContextWindow:    200000,
			Capabilities:     []string{"text", "reasoning"},
			PriceJSON:        `{"input":"0.000003","output":"0.000015"}`,
			RawRef:           "data[0]",
		},
		{
			CanonicalModelID: "unknown-external-model",
			ProviderHint:     "mistral",
			DisplayName:      "External Unknown",
			Capabilities:     []string{"text"},
			RawRef:           "data[1]",
		},
	}
	missing := []service.CatalogMissingEvidenceInput{
		{CanonicalModelID: "vanished-model", DetailJSON: `{"expected_by":"prior_snapshot"}`},
	}

	snapshotID, err := repo.InsertSnapshotWithEvidence(ctx, input, evidence, missing)
	require.NoError(t, err)
	require.Greater(t, snapshotID, int64(0))

	// Exact zstd round-trip: stored bytes decompress to the original payload.
	var stored []byte
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT payload_zstd FROM model_catalog_snapshots WHERE id = $1`, snapshotID).Scan(&stored))
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	decompressed, err := decoder.DecodeAll(stored, nil)
	require.NoError(t, err)
	require.Equal(t, raw, decompressed, "round-trip must preserve exact raw bytes")

	var sha string
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT payload_sha256 FROM model_catalog_snapshots WHERE id = $1`, snapshotID).Scan(&sha))
	require.Len(t, sha, 71) // sha256:<64 hex>

	// Evidence and missing rows were written in the same transaction.
	var evidenceCount, missingCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM model_catalog_candidate_evidence WHERE snapshot_id = $1`, snapshotID).Scan(&evidenceCount))
	require.Equal(t, 2, evidenceCount)
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM model_catalog_missing_evidence WHERE sync_run_id = $1 AND canonical_model_id = 'vanished-model'`, runID).Scan(&missingCount))
	require.Equal(t, 1, missingCount)

	// Finish the run successfully with item count.
	require.NoError(t, repo.FinishSyncRun(ctx, runID, service.CatalogSyncStatusSucceeded, 2, "", ""))

	// Terminal runs cannot be finished again.
	err = repo.FinishSyncRun(ctx, runID, service.CatalogSyncStatusFailed, 0, "", "late failure")
	require.Error(t, err)
	var conflict *infraerrors.ApplicationError
	require.ErrorAs(t, err, &conflict)
}

func TestModelCatalogSnapshotStore_AppendOnlyRejected(t *testing.T) {
	catalogSnapshotMu.Lock()
	defer catalogSnapshotMu.Unlock()

	ctx := context.Background()
	repo := NewModelCatalogSnapshotRepository(integrationDB)
	runID, err := repo.StartSyncRun(ctx, service.StartCatalogSyncRunInput{
		Source:      service.CatalogSourceModelsDev,
		TriggeredBy: service.CatalogSyncTriggerScheduled,
	})
	require.NoError(t, err)
	input, _ := newCatalogSnapshotTestInput(t)
	input.SyncRunID = runID
	input.Source = service.CatalogSourceModelsDev
	snapshotID, err := repo.InsertSnapshotWithEvidence(ctx, input, nil, nil)
	require.NoError(t, err)

	// UPDATE is rejected by the append-only trigger.
	_, err = integrationDB.ExecContext(ctx,
		`UPDATE model_catalog_snapshots SET external_version = 'rewritten' WHERE id = $1`, snapshotID)
	require.Error(t, err, "snapshot updates must be rejected")

	// DELETE is rejected.
	_, err = integrationDB.ExecContext(ctx,
		`DELETE FROM model_catalog_snapshots WHERE id = $1`, snapshotID)
	require.Error(t, err, "snapshot deletes must be rejected")
}

func TestModelCatalogSnapshotStore_IdempotentReplayAndContentConflict(t *testing.T) {
	catalogSnapshotMu.Lock()
	defer catalogSnapshotMu.Unlock()

	ctx := context.Background()
	repo := NewModelCatalogSnapshotRepository(integrationDB)
	runID, err := repo.StartSyncRun(ctx, service.StartCatalogSyncRunInput{
		Source:      service.CatalogSourceLiteLLM,
		TriggeredBy: service.CatalogSyncTriggerScheduled,
	})
	require.NoError(t, err)

	raw := []byte(`{"litellm":"fixed-version-payload","items":[1,2,3]}`)
	input := service.CatalogSnapshotInput{
		SyncRunID:       runID,
		Source:          service.CatalogSourceLiteLLM,
		ExternalVersion: fmt.Sprintf("commit-%d", time.Now().UnixNano()),
		RawPayload:      raw,
	}
	firstID, err := repo.InsertSnapshotWithEvidence(ctx, input, nil, nil)
	require.NoError(t, err)

	// Same source+version+content: idempotent replay returns the existing id.
	input.SyncRunID = runID
	replayID, err := repo.InsertSnapshotWithEvidence(ctx, input, nil, nil)
	require.NoError(t, err)
	require.Equal(t, firstID, replayID)

	// Same source+version but different content: rejected.
	input.RawPayload = []byte(`{"litellm":"tampered"}`)
	_, err = repo.InsertSnapshotWithEvidence(ctx, input, nil, nil)
	require.Error(t, err)
	var conflict *infraerrors.ApplicationError
	require.ErrorAs(t, err, &conflict)
}

func TestModelCatalogSnapshotStore_RejectsInvalidInputs(t *testing.T) {
	catalogSnapshotMu.Lock()
	defer catalogSnapshotMu.Unlock()

	ctx := context.Background()
	repo := NewModelCatalogSnapshotRepository(integrationDB)

	// Unknown source rejected at both run and snapshot level.
	_, err := repo.StartSyncRun(ctx, service.StartCatalogSyncRunInput{Source: "huggingface"})
	require.Error(t, err)

	runID, err := repo.StartSyncRun(ctx, service.StartCatalogSyncRunInput{
		Source:      service.CatalogSourceOpenRouter,
		TriggeredBy: service.CatalogSyncTriggerManual,
	})
	require.NoError(t, err)

	badSource := service.CatalogSnapshotInput{SyncRunID: runID, Source: "huggingface", ExternalVersion: "v1", RawPayload: []byte("x")}
	_, err = repo.InsertSnapshotWithEvidence(ctx, badSource, nil, nil)
	require.Error(t, err)

	emptyPayload := service.CatalogSnapshotInput{SyncRunID: runID, Source: service.CatalogSourceOpenRouter, ExternalVersion: "v2"}
	_, err = repo.InsertSnapshotWithEvidence(ctx, emptyPayload, nil, nil)
	require.Error(t, err)

	emptyVersion := service.CatalogSnapshotInput{SyncRunID: runID, Source: service.CatalogSourceOpenRouter, RawPayload: []byte("x")}
	_, err = repo.InsertSnapshotWithEvidence(ctx, emptyVersion, nil, nil)
	require.Error(t, err)
}
