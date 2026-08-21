package service

import (
	"context"
	"time"
)

type GovernanceProvider string

func CanonicalGovernanceTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

// UpstreamModelDiscovery keeps governance evidence separate from the legacy
// normalized model list consumed by runtime credential mappings.
type UpstreamModelDiscovery struct {
	Models           []string
	EvidenceModelIDs []string
	RawSnapshot      []byte
}

type DiscoveryBatchInput struct {
	IdempotencyKey  string
	ConnectionID    *int64
	AccountID       int64
	AccountProvider *GovernanceProvider
	RoutingPlatform string
	ModelIDs        []string
	RawSnapshot     []byte
	ObservedAt      time.Time
}

type ModelObservationRepository interface {
	RecordDiscovery(ctx context.Context, input DiscoveryBatchInput) (string, error)
	// DeleteBatch removes a batch and its observations/events. It is NOT
	// viable for shadow-compensation: model_shadow_decisions is append-only
	// (BEFORE DELETE trigger rejects). Production shadow path must use
	// AtomicShadowRecorder for true atomicity; this method is retained only
	// for non-shadow cleanup and test fakes.
	DeleteBatch(ctx context.Context, batchID string) error
}
