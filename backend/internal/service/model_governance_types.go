package service

import (
	"context"
	"time"
)

type GovernanceProvider string

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
}
