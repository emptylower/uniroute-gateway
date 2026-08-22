package service

import (
	"context"
	"time"
)

// QuarantinePoolItem is one quarantined publication-eligibility row as shown
// to administrators. Evidence-safe by construction: identifiers, reasons,
// versions, and timestamps only — never credentials, raw snapshots, or
// upstream authorization material.
type QuarantinePoolItem struct {
	AccountID         int64     `json:"account_id"`
	AccountName       string    `json:"account_name"`
	ChannelID         int64     `json:"channel_id"`
	ChannelName       string    `json:"channel_name"`
	CanonicalModelID  string    `json:"canonical_model_id"`
	Reason            string    `json:"reason"`
	RegistryVersion   int64     `json:"registry_version"`
	ChannelVersion    int64     `json:"channel_version"`
	QuarantineBatchID *string   `json:"quarantine_batch_id"`
	BatchID           *string   `json:"batch_id"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// GovernanceEventStream selects the append-only event source.
// "publication" reads model_publication_events; "registry" reads
// model_registry_events. Both are normalized into GovernanceEventItem.
const (
	GovernanceEventStreamPublication = "publication"
	GovernanceEventStreamRegistry    = "registry"
)

// GovernanceEventItem is one append-only governance evidence row. The payload
// column is intentionally never returned: it may carry arbitrary request
// material, and administrators only need the decision metadata.
type GovernanceEventItem struct {
	ID              int64     `json:"id"`
	Stream          string    `json:"stream"`
	EventType       string    `json:"event_type"`
	ActorID         string    `json:"actor_id"`
	RegistryVersion int64     `json:"registry_version"`
	CreatedAt       time.Time `json:"created_at"`

	// Publication-stream fields (empty for registry rows).
	AccountID         *int64  `json:"account_id,omitempty"`
	CanonicalModelID  string  `json:"canonical_model_id,omitempty"`
	ChannelID         *int64  `json:"channel_id,omitempty"`
	Eligibility       string  `json:"eligibility,omitempty"`
	Reason            string  `json:"reason,omitempty"`
	ChannelVersion    *int64  `json:"channel_version,omitempty"`
	BatchID           string  `json:"batch_id,omitempty"`
	QuarantineBatchID *string `json:"quarantine_batch_id,omitempty"`

	// Registry-stream field (empty for publication rows).
	CanonicalID string `json:"canonical_id,omitempty"`
}

// GovernancePage carries the paginated read result using the same shape as
// the other delegated admin list endpoints (items/total/page/page_size).
type GovernancePage[T any] struct {
	Items    []T   `json:"items"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
}

// ModelGovernanceReadRepository provides read-only access to the quarantine
// pool and the append-only governance event history. No mutation surface.
type ModelGovernanceReadRepository interface {
	ListQuarantinePool(ctx context.Context, page, pageSize int) (*GovernancePage[QuarantinePoolItem], error)
	ListGovernanceEvents(ctx context.Context, stream string, page, pageSize int) (*GovernancePage[GovernanceEventItem], error)
}
