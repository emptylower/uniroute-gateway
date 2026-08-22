package service

import (
	"context"
	"fmt"
)

// Catalog source IDs are exactly these three; no other source may be ingested.
const (
	CatalogSourceOpenRouter = "openrouter"
	CatalogSourceModelsDev  = "modelsdev"
	CatalogSourceLiteLLM    = "litellm"
)

// Sync run statuses. A run is running until it reaches a terminal state exactly once.
const (
	CatalogSyncStatusRunning   = "running"
	CatalogSyncStatusSucceeded = "succeeded"
	CatalogSyncStatusFailed    = "failed"
)

// Sync run triggers.
const (
	CatalogSyncTriggerScheduled = "scheduled"
	CatalogSyncTriggerManual    = "manual"
)

// ValidCatalogSource reports whether s is one of the three governed source IDs.
func ValidCatalogSource(s string) bool {
	switch s {
	case CatalogSourceOpenRouter, CatalogSourceModelsDev, CatalogSourceLiteLLM:
		return true
	default:
		return false
	}
}

// ValidCatalogSyncTrigger reports whether t is a valid run trigger.
func ValidCatalogSyncTrigger(t string) bool {
	switch t {
	case CatalogSyncTriggerScheduled, CatalogSyncTriggerManual:
		return true
	default:
		return false
	}
}

// StartCatalogSyncRunInput opens a new ingestion run for one source.
type StartCatalogSyncRunInput struct {
	Source      string
	TriggeredBy string
	RequestURL  string
}

// CatalogSnapshotInput carries the exact raw payload bytes of an external catalog
// fetch. RawPayload is stored zstd-compressed but must round-trip byte-exact.
type CatalogSnapshotInput struct {
	SyncRunID       int64
	Source          string
	ExternalVersion string
	RawPayload      []byte
}

// CatalogCandidateEvidenceInput is one normalized model candidate extracted from a
// snapshot. ProviderHint records the external claim verbatim; it never grants
// provider ownership or production prices.
type CatalogCandidateEvidenceInput struct {
	CanonicalModelID string
	ProviderHint     string
	DisplayName      string
	ContextWindow    int64 // 0 means unknown
	Capabilities     []string
	PriceJSON        string // JSON object string; empty means no price evidence
	RawRef           string
}

// CatalogMissingEvidenceInput records that an expected canonical model was absent
// from one source catalog in one sync run.
type CatalogMissingEvidenceInput struct {
	CanonicalModelID string
	DetailJSON       string
}

// ModelCatalogSnapshotStore is the append-only durable store for external catalog
// evidence. There is deliberately no update or delete API: snapshots and evidence
// are immutable once written, and the database rejects mutations independently.
type ModelCatalogSnapshotStore interface {
	StartSyncRun(ctx context.Context, input StartCatalogSyncRunInput) (int64, error)
	FinishSyncRun(ctx context.Context, runID int64, status string, itemCount int, resolvedCommit string, errorMessage string) error
	InsertSnapshotWithEvidence(ctx context.Context, input CatalogSnapshotInput, evidence []CatalogCandidateEvidenceInput, missing []CatalogMissingEvidenceInput) (int64, error)
}

// Validate trims and checks StartCatalogSyncRunInput.
func (in *StartCatalogSyncRunInput) Validate() error {
	if !ValidCatalogSource(in.Source) {
		return fmt.Errorf("invalid catalog source: %q", in.Source)
	}
	if !ValidCatalogSyncTrigger(in.TriggeredBy) {
		return fmt.Errorf("invalid catalog sync trigger: %q", in.TriggeredBy)
	}
	return nil
}
