package service

import (
	"time"
)

// OutboxWatermark (Phase 4.1-G, redesign §15.4) is the global high-water
// mark 4.1-S records per reconciliation run: the outbox's max delivered_at
// (replacing the parent plan's "settlement high-water mark"), the max row
// id, and the per-status counts.
type OutboxWatermark struct {
	DeliveredAtMax *time.Time
	OutboxIDMax    int64
	Pending        int64
	InFlight       int64
	DeadLetter     int64
}

// LiveProvisionalSummaryRecord (Phase 4.1-G, redesign §15.3 leg 3) is the
// per-user Live read model: the reconciliation-relevant columns of one
// wallet_live_provisional record with its windows decoded and the billing
// snapshot's fx rate folded in (nil when the snapshot is absent — round-3
// fx fold).
type LiveProvisionalSummaryRecord struct {
	Token             string
	AuthorizationID   string
	CallHash          string
	PlatformUserID    string
	BillingSnapshotID string
	BillingFX         *string
	Status            string
	EstimatedUnits    int64
	SettlementEventID string
	Windows           []LiveWindow
	CreatedAt         time.Time
}
