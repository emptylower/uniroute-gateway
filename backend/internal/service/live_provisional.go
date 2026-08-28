package service

import (
	"context"
	"errors"
	"time"
)

// LiveWindow is one lease window of a Live session (spec §2.3). 3.3c records
// window 1 with an empty lease id; 3.7 binds leases and settles per window.
type LiveWindow struct {
	WindowSeq    int    `json:"window_seq"`
	LeaseID      string `json:"lease_id"`
	Token        string `json:"token"`
	PendingUnits int64  `json:"pending_units"`
	SettledUnits int64  `json:"settled_units"`
}

type LiveProvisionalStatus string

const (
	LiveProvisionalStatusProvisional LiveProvisionalStatus = "provisional" // written before the SDP POST
	LiveProvisionalStatusActive      LiveProvisionalStatus = "active"      // POST returned a call id
	LiveProvisionalStatusAborted     LiveProvisionalStatus = "aborted"     // POST failed definitely
	LiveProvisionalStatusFinalizing  LiveProvisionalStatus = "finalizing"  // claimed by one finalization attempt; the observer runs under this claim
	LiveProvisionalStatusFinalized   LiveProvisionalStatus = "finalized"   // the outbox row is committed
)

// LiveProvisionalRecord is the durable, restart-surviving record of one Live
// POST attempt (spec §2.3, §3.5). Token == AuthorizationID in 3.3c: the row must
// exist before any per-write token does, so it is keyed by the attempt's
// authorization id; 3.7's windows carry the per-write tokens.
type LiveProvisionalRecord struct {
	Token             string
	AuthorizationID   string
	CallHash          string // "" until activated
	PlatformUserID    string
	UserID            int64
	APIKeyID          int64
	AccountID         int64
	BillingCurrency   string
	BillingSnapshotID string
	EstimatedUnits    int64
	Status            LiveProvisionalStatus
	Windows           []LiveWindow
	SettlementEventID string // set at completion, only when the bridge committed the outbox row
	CreatedAt         time.Time
	ActivatedAt       *time.Time
	TerminalAt        *time.Time
}

var ErrLiveProvisionalNotFound = errors.New("live provisional record not found")

// LiveProvisionalStore is the durable port. Save is insert-only; every transition
// is a compare-and-set on status so finalization stays one-shot under retries.
type LiveProvisionalStore interface {
	Save(ctx context.Context, rec *LiveProvisionalRecord) error                                                        // INSERT … ON CONFLICT (token) DO NOTHING
	Activate(ctx context.Context, token, callHash string, at time.Time) error                                          // provisional → active
	Abort(ctx context.Context, token string, at time.Time) error                                                       // provisional → aborted
	ClaimFinalization(ctx context.Context, token string, at time.Time) (bool, error)                                   // active → finalizing; false when not active
	CompleteFinalization(ctx context.Context, token, settlementEventID string, settledUnits int64, at time.Time) error // finalizing → finalized, windows[0].settled_units = settledUnits
	ReleaseFinalizationClaim(ctx context.Context, token string) error                                                  // finalizing → active
	Get(ctx context.Context, token string) (*LiveProvisionalRecord, error)
	GetByCallHash(ctx context.Context, callHash string) (*LiveProvisionalRecord, error)
}
