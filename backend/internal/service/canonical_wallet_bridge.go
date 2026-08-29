package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrCanonicalWalletLeaseMissing          = errors.New("canonical wallet lease is missing")
	ErrCanonicalWalletLeaseExpired          = errors.New("canonical wallet lease is expired")
	ErrCanonicalWalletLeaseExhausted        = errors.New("canonical wallet lease is exhausted")
	ErrCanonicalWalletLeaseCurrencyMismatch = errors.New("canonical wallet lease currency mismatch")
	ErrCanonicalWalletReservationConflict   = errors.New("canonical wallet event was reserved against a different lease")
	ErrCanonicalWalletOutboxClaimLost       = errors.New("canonical wallet outbox row is no longer claimed by this dispatcher")
	ErrCanonicalWalletOutboxPayloadConflict = errors.New("wallet outbox event id already used with a different payload")
	// ErrCanonicalWalletBalanceShortfall (§9.3): the server's
	// insufficient_balance refusal — TERMINAL. The dispatcher dead-letters
	// with reason balance_shortfall; Authorize refuses balance_shortfall.
	// Produced ONLY by EnsureLease's refusal mapping; the local under-grant
	// guard carries its own transient sentinel below.
	ErrCanonicalWalletBalanceShortfall = errors.New("canonical wallet lease granted below the amount being authorized")
	// Phase 3.4 (redesign §3/§4): the ensure route's two other refusals.
	ErrCanonicalWalletLeaseCapReached = errors.New("canonical wallet lease cap reached for this user")                  // terminal on the authorize path
	ErrCanonicalWalletLeaseContention = errors.New("canonical wallet lease issuance lost the per-user race repeatedly") // transient
	// ErrCanonicalWalletLeaseGrantBelowAmount (§9.3): ensureLease's local
	// guard — the grant covers less than the amount being authorized.
	// Unreachable against a §3-conformant control plane (clampBudget refuses
	// rather than issuing below min_headroom_units); TRANSIENT — retried on
	// the outbox backoff and lease_unavailable in Authorize — so a server bug
	// can never turn a retryable condition into permanently uncollected revenue.
	ErrCanonicalWalletLeaseGrantBelowAmount = errors.New("canonical wallet lease granted below the amount being authorized (transient: a conformant control plane never does this)")
	// ErrCanonicalWalletControlPlaneIncompatible (§9.6 item 6): the control
	// plane has no /ensure route (startup probe or a runtime 404/405) — the
	// rollout ordering was violated. Transient at runtime: the outbox retries
	// on the backoff and, if it exhausts, dead-letters with attempts_exhausted.
	ErrCanonicalWalletControlPlaneIncompatible = errors.New("canonical wallet control plane has no ensure route (deploy ShipAny 3.4a-S first)")
	// Phase 3.4b (§10.3): the hold store's two state errors. Missing is a
	// vanished/expired hold hash (idempotent for every caller); NotArmed is
	// the scripts' {7} — the hold exists but its money state is terminal.
	ErrCanonicalWalletHoldMissing = errors.New("canonical wallet hold is missing")
	// ErrCanonicalWalletHoldNotArmed is the sentinel CanonicalWalletHoldNotArmedError wraps.
	ErrCanonicalWalletHoldNotArmed = errors.New("canonical wallet hold is not armed")
)

// CanonicalWalletHoldNotArmedError carries the stored state and event id so
// the caller can branch on the EVENT ID, never on the code alone (§10.5):
// non-empty (settled) is a retried submission; empty (released/abandoned)
// means the money was already given back and the event proceeds unbound.
type CanonicalWalletHoldNotArmedError struct {
	State   string
	EventID string
}

func (e *CanonicalWalletHoldNotArmedError) Error() string {
	return fmt.Sprintf("canonical wallet hold is not armed (state %q, event_id %q)", e.State, e.EventID)
}

func (e *CanonicalWalletHoldNotArmedError) Unwrap() error { return ErrCanonicalWalletHoldNotArmed }

func isHoldNotArmed(err error) bool {
	var notArmed *CanonicalWalletHoldNotArmedError
	return errors.As(err, &notArmed)
}

const (
	canonicalWalletLeaseScope = "wallet:lease"
	// canonicalWalletEnsurePath must be identical at the POST, the 404/405
	// classification and the probe: the classification is a string
	// comparison against the caller's path, so a diverging literal would
	// silently stop matching.
	canonicalWalletEnsurePath      = "/api/internal/v2/wallet/leases/ensure"
	canonicalWalletSettlementScope = "wallet:settlement"
	// canonicalWalletSettlementsPath (Phase 3.5, §11.2): the units-native v2
	// route — the only settlements route this client speaks; the v1 micros
	// path is deleted, not kept behind a switch.
	canonicalWalletSettlementsPath = "/api/internal/v2/wallet/settlements"
)

type CanonicalWalletLease struct {
	LeaseID        string    `json:"lease_id"`
	PlatformUserID string    `json:"platform_user_id"`
	Currency       string    `json:"currency"`
	BudgetUnits    int64     `json:"budget_units"`
	ConsumedUnits  int64     `json:"consumed_units"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (l CanonicalWalletLease) RemainingUnits() int64 {
	remaining, err := SubUnits(l.BudgetUnits, l.ConsumedUnits)
	if err != nil {
		return 0
	}
	return remaining
}

// leaseExpiredAt (Phase 3.4, redesign §4): a lease is usable only while its
// expiry is strictly beyond now + expiry_skew_margin_ms — the margin absorbs
// the round trip to Redis, whose own PEXPIREAT decides the reservation.
func (b *CanonicalWalletBridge) leaseExpiredAt(lease *CanonicalWalletLease, now time.Time) bool {
	margin := time.Duration(b.cfg.ExpirySkewMarginMS) * time.Millisecond
	return lease == nil || !lease.ExpiresAt.After(now.Add(margin))
}

func (b *CanonicalWalletBridge) leaseCovers(lease *CanonicalWalletLease, currency string, amountUnits int64, now time.Time) bool {
	return lease != nil && lease.Currency == currency && !b.leaseExpiredAt(lease, now) && lease.RemainingUnits() >= amountUnits
}

type CanonicalWalletReservation struct {
	Lease     CanonicalWalletLease
	Duplicate bool
}

// CanonicalWalletHold (Phase 3.4b, §10.3) is one authorization's hold hash.
type CanonicalWalletHold struct {
	AuthorizationID string
	LeaseID         string
	HeldUnits       int64
	ArmedAt         time.Time
	Class           string // "" | not_written | indeterminate (no "result" — §10.5)
	State           string // armed | settled | released | abandoned — the money state
	EventID         string // set at conversion
}

// CanonicalWalletHoldConversion is ConvertCanonicalWalletHold's result. The
// caller branches on Code and (for {7}) on EventID, never on an error class.
type CanonicalWalletHoldConversion struct {
	Code    int    // 0 converted; 1 missing; 3 lease gone; 4 overrun released; 7 not armed
	LeaseID string // the {0} answer's lease; also carried on {7} (§13.2.7)
	State   string
	EventID string
}

// CanonicalWalletLeaseStore is implemented by the Redis-backed gateway cache.
// Reserve must be atomic and idempotent for the supplied event ID.
type CanonicalWalletLeaseStore interface {
	InstallCanonicalWalletLease(ctx context.Context, lease CanonicalWalletLease) error
	// GetCanonicalWalletLease resolves the user's CURRENT (most recently
	// issued, non-expired) lease — used when admitting a brand-new request.
	GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*CanonicalWalletLease, error)
	// GetCanonicalWalletLeaseByID resolves one specific lease regardless of
	// whether it is still the user's current lease — used to inspect a
	// lease an in-flight reservation was already anchored to.
	GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*CanonicalWalletLease, error)
	// ReserveCanonicalWalletLease reserves against the EXPLICIT leaseID the
	// caller supplies — a fresh request supplies the ID it just got from
	// GetCanonicalWalletLease; a retry supplies the ID it used the first
	// time (service.CanonicalWalletSettlementEvent.LeaseID), never "whatever
	// is current now".
	ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*CanonicalWalletReservation, error)
	// SealCanonicalWalletLease (Phase 3.4, redesign §3.3) closes the lease to new
	// reservations ahead of a drain and returns the pre-seal consumed units and
	// (3.4b, §10.2) the released units. Returns ErrCanonicalWalletLeaseMissing
	// when the lease hash is absent.
	SealCanonicalWalletLease(ctx context.Context, platformUserID, leaseID string) (preSealConsumed, released int64, err error)
	// ArmCanonicalWalletHold (3.4b, §10.4) raises consumed by units and writes
	// the hold hash {state=armed, class=""} with PEXPIREAT = the lease's
	// expires_at + grace; duplicate=true is the {5} idempotent re-arm. The
	// {1}/{2}/{3} lease checks are the reserve script's own sentinels; {4} is
	// ErrCanonicalWalletLeaseExhausted.
	ArmCanonicalWalletHold(ctx context.Context, platformUserID, leaseID, currency, authorizationID string, units int64, graceMS int64, now time.Time) (leaseIDOut string, held int64, duplicate bool, err error)
	// ReleaseCanonicalWalletHold (§10.5/§10.7) moves an armed hold to
	// stateAfter ('released' | 'abandoned'), HINCRBYs the lease's
	// released_units by the held figure iff the lease hash still exists,
	// SREMs the per-user set and returns the released amount. {7} is a
	// CanonicalWalletHoldNotArmedError; {1} ErrCanonicalWalletHoldMissing.
	ReleaseCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, stateAfter, classAfter string) (released int64, err error)
	// ConvertCanonicalWalletHold (§10.5) settles the hold against actualUnits;
	// every outcome is a CanonicalWalletHoldConversion, never an error for a
	// well-formed reply ({7} carries the stored State/EventID).
	ConvertCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, eventID string, actualUnits int64, now time.Time) (CanonicalWalletHoldConversion, error)
	GetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) (*CanonicalWalletHold, error)
	// ListCanonicalWalletHolds returns the user's set members, capped.
	ListCanonicalWalletHolds(ctx context.Context, platformUserID string, limit int) ([]string, error)
	// ListCanonicalWalletHoldUsers SSCANs hold_users (platform user IDS, not
	// hashes — §10.3's cluster note), cursor-based.
	ListCanonicalWalletHoldUsers(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error)
	PruneCanonicalWalletHoldUser(ctx context.Context, platformUserID string) error
	// TryCanonicalWalletReaperLease is the deployment-wide tick leader
	// (SET canonical_wallet:reaper:tick NX PX ttl).
	TryCanonicalWalletReaperLease(ctx context.Context, ttl time.Duration) (bool, error)
	// ForgetCanonicalWalletHold SREMs the per-user set ONLY — the reaper's
	// absent/not-armed branches; the hash keeps its TTL'd observability copy.
	ForgetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) error
	// MarkCanonicalWalletHoldUserEmpty is §10.3's two-consecutive-empty-ticks
	// state, in Redis so it survives leader rotation: SET holds_empty:{user}
	// 1 NX PX ttl; alreadySeen=true means some reaper observed this user
	// empty within the window.
	MarkCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string, ttl time.Duration) (alreadySeen bool, err error)
	ClearCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string) error
	// MarkCanonicalWalletHoldClass sets class iff state = armed (a script);
	// {7} is a CanonicalWalletHoldNotArmedError, {1} ErrCanonicalWalletHoldMissing.
	MarkCanonicalWalletHoldClass(ctx context.Context, platformUserID, authorizationID, class string) (*CanonicalWalletHold, error)
	// ReleaseCanonicalWalletReservation (Phase 3.5, §11.2) releases a
	// settlement event's reservation on the lease its marker names:
	// dropMarker = true (named_lease_id, the late capture) drops the marker —
	// the full form; dropMarker = false (the split) keeps it and is gated on
	// a per-event release marker — the partial form's idempotency key.
	// Returns true iff the script ran its write path.
	ReleaseCanonicalWalletReservation(ctx context.Context, platformUserID, leaseID, eventID string, units int64, dropMarker bool) (bool, error)
}

type CanonicalWalletSettlementEvent struct {
	EventID                string    `json:"event_id"`
	GatewayRequestID       string    `json:"gateway_request_id"`
	PlatformUserID         string    `json:"platform_user_id"`
	LeaseID                string    `json:"lease_id"`
	Currency               string    `json:"currency"`
	AmountUnits            int64     `json:"amount_units"`
	LocalBalanceAfterUnits *int64    `json:"local_balance_after_units,omitempty"`
	OccurredAt             time.Time `json:"occurred_at"`
	// AuthorizationToken / AuthorizationID (Phase 3.3): json:"-" — the outbox row
	// payload is unchanged in 3.3 (3.5 adds the column and the wire field; changing
	// the payload now would make a redelivered pre-3.3 row a payload conflict).
	AuthorizationToken string `json:"-"`
	AuthorizationID    string `json:"-"`
	// BillingSnapshotID (Phase 4.2-G, 4.1-G's hand-on): the frozen pricing
	// basis of the settlement's usage (wallet_billing_snapshot.id), written
	// to the outbox row's OWN column so leg 4's outbox portion becomes
	// fx-verified. Hash-NEUTRAL by construction: walletOutboxHashPayload's
	// seven fields do not include it (constraint 3, the 3.5 AuthorizationID
	// precedent — test 76 pins it). json:"-" is harmless bookkeeping; the
	// mechanism is the hash struct's fixed shape. Empty = no snapshot
	// (pre-3.2 callers, Count Tokens, off mode) — the column stays NULL.
	BillingSnapshotID string `json:"-"`
}

type CanonicalWalletSettlementResult struct {
	Accepted              bool   `json:"accepted"`
	Duplicate             bool   `json:"duplicate"`
	CanonicalBalanceUnits *int64 `json:"-"`
	// Phase 3.5 (§11.2): the v2 response's own fields.
	NamedLeaseID    string `json:"-"` // the lease a duplicate's redelivery named, when it differs from the event's
	EventLeaseID    string `json:"-"` // event.lease_id — the lease the event actually captured on
	CapturedUnits   int64  `json:"-"` // event.amount, the strict parser
	LeaseCaptureSeq int64  `json:"-"`
}

// CanonicalWalletOutboxEvent carries every field the outbox dispatcher needs
// to reconstruct the original settlement event for delivery — including
// GatewayRequestID/OccurredAt (SubmitSettlement requires them) and
// LocalBalanceAfterUnits (the drift-detection signal the pre-outbox inline
// path already had).
type CanonicalWalletOutboxEvent struct {
	ID                     int64
	EventID                string
	PlatformUserID         string
	LeaseID                string
	GatewayRequestID       string
	Currency               string
	AmountUnits            int64
	LocalBalanceAfterUnits *int64
	OccurredAt             time.Time
	AttemptCount           int
	// Phase 3.5 (redesign §11.9): the split columns and the token's durable
	// join. PendingReleaseUnits != nil means the dispatcher still owes the
	// bound lease a release of exactly that many units BEFORE it may reserve
	// anything new (§11.3) — the release marker in Redis, not this column, is
	// the idempotency gate.
	ParentEventID       string
	SplitDepth          int
	PendingReleaseUnits *int64
	AuthorizationID     string
	// Phase 4.1-G (redesign §15.3): the reconciliation read model's
	// additive columns — only ListOutboxEventsByUser's read query populates
	// them; every existing scan (the dispatcher's claim, the split) is
	// unchanged and leaves them zero-valued.
	Status           string
	DeadLetterReason string
	DeliveredAt      *time.Time
	// Phase 4.2-G (4.1-G's hand-on): the billing snapshot's own column
	// (NULL = pre-4.2/token-less — leg 4's unverified tail, the operator
	// backfill's set) and the receivable collector's own bound — how many
	// times a balance_shortfall dead-letter has been re-driven (Task 2).
	// attempt_count stays the dispatcher's TRANSPORT budget; redrive_count
	// is the re-drive bound and the two are disjoint by construction.
	// BillingFX is the LEFT JOIN fold of wallet_billing_snapshot's
	// payload->fx->rate — nil when the snapshot is absent.
	BillingSnapshotID string
	RedriveCount      int
	BillingFX         *string
}

// CanonicalWalletOutboxStore is implemented by repository.WalletOutboxStore.
// Defined here (same pattern as CanonicalWalletLeaseStore) so the bridge can
// depend on the outbox without a service<->repository import cycle.
type CanonicalWalletOutboxStore interface {
	InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event CanonicalWalletSettlementEvent) error
	// Every claim/resolve method carries an opaque workerID claim token.
	// A dispatcher may only resolve rows it currently owns — see
	// WalletOutboxStore's implementations and runOutboxDispatcher below.
	ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]CanonicalWalletOutboxEvent, error)
	MarkOutboxEventDelivered(ctx context.Context, id int64, workerID string) error
	MarkOutboxEventFailed(ctx context.Context, id int64, workerID string, simulatedNow time.Time) error
	// MarkOutboxEventDeadLetter (Phase 3.4, redesign §4) resolves a row this
	// worker owns straight to dead_letter with a named reason — used for
	// terminal lease errors (balance_shortfall) that no retry can fix. The
	// reason is logged and counted; a persisted dead_letter_reason column is
	// a 3.4a item.
	MarkOutboxEventDeadLetter(ctx context.Context, id int64, workerID, reason string) error
	// BindOutboxEventLease durably records WHICH lease this event's
	// reservation is anchored to, and is called BEFORE the reservation is
	// attempted — never after. The Redis reservation marker is keyed by
	// (platform_user_id, event_id) and remembers the lease the reservation
	// landed on; a retry that reserves against "whatever lease is current
	// now" gets a cross-lease conflict on every attempt until the row
	// dead-letters. Binding first means no crash point can leave a
	// reservation whose lease the row does not know. Passing an empty
	// leaseID releases the binding — only legal when the reservation itself
	// proved no marker exists on that lease (see deliverOutboxEvent).
	// Ownership-guarded like every other resolve method: a row this worker
	// no longer owns returns ErrCanonicalWalletOutboxClaimLost.
	BindOutboxEventLease(ctx context.Context, id int64, workerID, leaseID string) error
	// SplitOutboxEvent (Phase 3.5, §11.3) splits one claimed row: the
	// remainder (amount − capturedUnits) becomes a NEW pending row with its
	// own event id, attempt budget and split depth; the parent's amount
	// becomes capturedUnits and it returns to pending (delivered when
	// capturedUnits = 0). reserved records whether the gateway already
	// reserved the full amount at delivery — when true the parent's
	// pending_release_units records what the dispatcher owes the bound lease.
	// The parent's payload_hash is never touched.
	SplitOutboxEvent(ctx context.Context, id int64, workerID string, capturedUnits int64, remainderEventID string, reserved bool) (remainderUnits int64, err error)
	// ClearPendingRelease nulls pending_release_units after the owed release
	// landed (§11.3).
	ClearPendingRelease(ctx context.Context, id int64) error
	// SumDeadLetterUnits (§11.4) sums amount_units over dead-letter rows
	// carrying the named reason — the receivable figure.
	SumDeadLetterUnits(ctx context.Context, reason string) (int64, error)
	// ListReceivableRedriveCandidates (Phase 4.2-G Task 2): the collector's
	// candidate set — balance_shortfall dead-letters inside the retention
	// window and under the re-drive bound. Read-only.
	ListReceivableRedriveCandidates(ctx context.Context, notBefore time.Time, maxRedrives, limit int) ([]CanonicalWalletOutboxEvent, error)
	// RequeueDeadLetter (Phase 4.2-G Task 2): ONE balance_shortfall
	// dead-letter back to pending — attempt_count reset, redrive_count+1,
	// immediately claimable. One statement, idempotent by its status guard.
	RequeueDeadLetter(ctx context.Context, id int64, workerID string) error
	// ReclaimStaleInFlightEvents recovers rows a crashed dispatcher left
	// stuck in_flight.
	ReclaimStaleInFlightEvents(ctx context.Context, staleAfter time.Duration) (int64, error)
}

type canonicalWalletLeasePurpose string

const (
	canonicalWalletLeasePurposeAuthorize canonicalWalletLeasePurpose = "authorize"
	canonicalWalletLeasePurposeSettle    canonicalWalletLeasePurpose = "settle"
)

type canonicalWalletControlPlane interface {
	EnsureLease(ctx context.Context, request canonicalWalletEnsureRequest) (*canonicalWalletEnsureResult, error)
	SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error)
}

type canonicalWalletHTTPClient struct {
	cfg    config.CanonicalWalletConfig
	client *http.Client
	now    func() time.Time
}

func newCanonicalWalletHTTPClient(cfg config.CanonicalWalletConfig, client *http.Client) *canonicalWalletHTTPClient {
	if client == nil {
		client = &http.Client{Timeout: time.Duration(cfg.RequestTimeoutMS) * time.Millisecond}
	}
	return &canonicalWalletHTTPClient{cfg: cfg, client: client, now: func() time.Time { return time.Now().UTC() }}
}

// canonicalWalletEnsureRequest is redesign §3's request on the v2 wire —
// every amount a Phase 0 amount object (§9.2), no bare integer crosses the
// wire. No idempotency key: ensure is idempotent by transaction.
type canonicalWalletEnsureRequest struct {
	PlatformUserID       string                      `json:"platform_user_id"`
	Currency             string                      `json:"currency"`
	Purpose              string                      `json:"purpose"`
	MinHeadroom          canonicalWalletAmountObject `json:"min_headroom"`
	RequestedBudget      canonicalWalletAmountObject `json:"requested_budget"`
	RequestedTTLSeconds  int                         `json:"requested_ttl_seconds"`
	PreferLeaseID        string                      `json:"prefer_lease_id,omitempty"`
	Drained              []canonicalWalletDrainEntry `json:"drained,omitempty"` // omitempty: a nil slice is OMITTED (not null); ShipAny accepts absent, null and [] alike
	CallerSlotTTLSeconds int                         `json:"caller_slot_ttl_seconds"`
	GatewayAttemptID     string                      `json:"gateway_attempt_id,omitempty"`
}

type canonicalWalletDrainEntry struct {
	LeaseID         string                      `json:"lease_id"`
	GatewayConsumed canonicalWalletAmountObject `json:"gateway_consumed"`
	// GatewayReleased (3.4b): the drain identity becomes
	// consumed == captured + gateway_released once releases exist.
	GatewayReleased *canonicalWalletAmountObject `json:"gateway_released,omitempty"`
}

// canonicalWalletEnsureWireResponse is §9.2's response: leaseWireView plus
// headroom/outcome/clamped_by, every amount an object.
type canonicalWalletEnsureWireResponse struct {
	LeaseID        string                      `json:"lease_id"`
	PlatformUserID string                      `json:"platform_user_id"`
	Currency       string                      `json:"currency"`
	UnitVersion    string                      `json:"unit_version"`
	Scale          int                         `json:"scale"`
	Budget         canonicalWalletAmountObject `json:"budget"`
	Reserved       canonicalWalletAmountObject `json:"reserved"`
	Captured       canonicalWalletAmountObject `json:"captured"`
	Released       canonicalWalletAmountObject `json:"released"`
	Headroom       canonicalWalletAmountObject `json:"headroom"`
	CaptureSeq     int64                       `json:"capture_seq"`
	Status         string                      `json:"status"`
	ExpiresAt      time.Time                   `json:"expires_at"`
	Outcome        string                      `json:"outcome"`
	ClampedBy      string                      `json:"clamped_by"`
}

// canonicalWalletAmountObject is Phase 0's four-field amount (redesign §9.2). The
// decimal string is the ONLY carrier of the value; no bare integer crosses the wire.
type canonicalWalletAmountObject struct {
	AmountUnits string `json:"amount_units"`
	Currency    string `json:"currency"`
	Scale       int    `json:"scale"`
	UnitVersion string `json:"unit_version"`
}

func newCanonicalWalletAmountObject(units int64) canonicalWalletAmountObject {
	return canonicalWalletAmountObject{AmountUnits: strconv.FormatInt(units, 10), Currency: "CNY", Scale: 8, UnitVersion: "cny-e8-v1"}
}

var canonicalWalletDecimalUnitsPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)

// parseCanonicalWalletAmountObject is strict on every field: decimal string only (no sign,
// no leading zero, ≤ 19 digits — ShipAny's units.ts:53-85 twin), CNY, scale 8,
// cny-e8-v1, and a value int64 can hold.
func parseCanonicalWalletAmountObject(field string, a canonicalWalletAmountObject) (int64, error) {
	if a.Currency != "CNY" || a.Scale != 8 || a.UnitVersion != "cny-e8-v1" {
		return 0, fmt.Errorf("%s: not a cny-e8-v1 amount object", field)
	}
	if !canonicalWalletDecimalUnitsPattern.MatchString(a.AmountUnits) {
		return 0, fmt.Errorf("%s.amount_units: not a decimal integer string", field)
	}
	v, err := strconv.ParseInt(a.AmountUnits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s.amount_units: exceeds int64", field)
	}
	return v, nil
}

type canonicalWalletEnsureResult struct {
	Lease     CanonicalWalletLease
	Outcome   string // reused | issued
	ClampedBy string // none | cap | balance
}

// canonicalWalletStatusError (Phase 3.5, §11.5) carries every non-2xx the
// control plane can answer: the status, the path (the classification is
// route-sensitive — a 404 on settlements and a 404 on ensure differ),
// data.reason when the body carries one, and — for the settlements route's
// lease_over_capture — data.headroom parsed to units. The wrapped error is
// set ONLY on the §9.6 item 6 branch (a 404/405 on the ensure route), so
// errors.As(err, &se) and errors.Is(err,
// ErrCanonicalWalletControlPlaneIncompatible) both succeed there.
type canonicalWalletStatusError struct {
	Status        int
	Path          string
	Reason        string
	HeadroomUnits *int64
	wrapped       error
}

func (e *canonicalWalletStatusError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("canonical wallet control plane refused: %s (status %d)", e.Reason, e.Status)
	}
	return fmt.Sprintf("canonical wallet control plane returned status %d", e.Status)
}

func (e *canonicalWalletStatusError) Unwrap() error { return e.wrapped }

// classifyControlPlaneError (§11.5): the terminal rows of the
// classification table. Everything else — 401 (a rotated service secret
// self-heals), 5xx, 503 flag-off, transport errors, the local guards — is
// transient and returns ok=false; lease_over_capture and
// lease_not_capturable are handled BEFORE this (the split and the stale
// binding) and never reach it.
func classifyControlPlaneError(err error) (terminalReason string, ok bool) {
	var se *canonicalWalletStatusError
	if !errors.As(err, &se) {
		return "", false
	}
	switch {
	case se.Status == 400, se.Reason == "lease_owner_mismatch", se.Reason == "lease_not_found":
		return "contract_violation", true
	case se.Reason == "settlement_payload_conflict":
		return "payload_conflict", true
	}
	return "", false
}

func (c *canonicalWalletHTTPClient) EnsureLease(ctx context.Context, request canonicalWalletEnsureRequest) (*canonicalWalletEnsureResult, error) {
	if _, err := RequireCNYBillingCurrency(request.Currency); err != nil {
		return nil, fmt.Errorf("requested an unsupported currency: %w", err)
	}
	var wire canonicalWalletEnsureWireResponse
	if err := c.doJSON(ctx, http.MethodPost, canonicalWalletEnsurePath, canonicalWalletLeaseScope, "", request, &wire); err != nil {
		var refusal *canonicalWalletStatusError
		if errors.As(err, &refusal) {
			switch refusal.Reason {
			case "lease_cap_reached":
				return nil, fmt.Errorf("%w: %s", ErrCanonicalWalletLeaseCapReached, refusal.Error())
			case "insufficient_balance":
				return nil, fmt.Errorf("%w: %s", ErrCanonicalWalletBalanceShortfall, refusal.Error())
			case "lease_contention":
				return nil, fmt.Errorf("%w: %s", ErrCanonicalWalletLeaseContention, refusal.Error())
			}
		}
		return nil, err
	}
	// §9.2: every response amount is parsed by the strict parser — a parse
	// failure is a decode-class error and takes the generic transport path.
	budget, err := parseCanonicalWalletAmountObject("budget", wire.Budget)
	if err != nil {
		return nil, err
	}
	if _, err := parseCanonicalWalletAmountObject("reserved", wire.Reserved); err != nil {
		return nil, err
	}
	if _, err := parseCanonicalWalletAmountObject("captured", wire.Captured); err != nil {
		return nil, err
	}
	if _, err := parseCanonicalWalletAmountObject("released", wire.Released); err != nil {
		return nil, err
	}
	headroom, err := parseCanonicalWalletAmountObject("headroom", wire.Headroom)
	if err != nil {
		return nil, err
	}
	// The §4 invariants, restated over the parsed int64s. The sign clauses are
	// gone — the parser rejects a sign — and §9.2's status is added: only an
	// active lease may be installed.
	if strings.TrimSpace(wire.LeaseID) == "" || strings.TrimSpace(wire.PlatformUserID) != strings.TrimSpace(request.PlatformUserID) || budget <= 0 || headroom > budget || wire.ExpiresAt.IsZero() || wire.Status != "active" || (wire.Outcome != "reused" && wire.Outcome != "issued") {
		return nil, errors.New("control plane returned an invalid canonical wallet lease")
	}
	currency, err := RequireCNYBillingCurrency(wire.Currency)
	if err != nil {
		return nil, fmt.Errorf("control plane returned an unsupported currency: %w", err)
	}
	if currency != strings.ToUpper(strings.TrimSpace(request.Currency)) {
		return nil, ErrCanonicalWalletLeaseCurrencyMismatch
	}
	// redesign §4: consumed := budget − headroom (captured + released as the server sees them)
	consumed, err := SubUnits(budget, headroom)
	if err != nil {
		return nil, err
	}
	return &canonicalWalletEnsureResult{
		Lease: CanonicalWalletLease{
			LeaseID: wire.LeaseID, PlatformUserID: strings.TrimSpace(request.PlatformUserID), Currency: currency,
			BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: wire.ExpiresAt,
		},
		Outcome: wire.Outcome, ClampedBy: wire.ClampedBy,
	}, nil
}

// canonicalWalletLeaseWireView is leaseWireView's twelve fields exactly
// (lease-wire.ts: lease_id, platform_user_id, currency, unit_version, scale,
// budget, reserved, captured, released, capture_seq, status, expires_at) —
// a DEDICATED struct: the settlements route's lease field is this view and
// nothing else, while the ensure response ADDS headroom/outcome/clamped_by;
// reusing the ensure type here would let a zero-valued headroom leak into a
// strict parse that this route never sends.
type canonicalWalletLeaseWireView struct {
	LeaseID        string                      `json:"lease_id"`
	PlatformUserID string                      `json:"platform_user_id"`
	Currency       string                      `json:"currency"`
	UnitVersion    string                      `json:"unit_version"`
	Scale          int                         `json:"scale"`
	Budget         canonicalWalletAmountObject `json:"budget"`
	Reserved       canonicalWalletAmountObject `json:"reserved"`
	Captured       canonicalWalletAmountObject `json:"captured"`
	Released       canonicalWalletAmountObject `json:"released"`
	CaptureSeq     int64                       `json:"capture_seq"`
	Status         string                      `json:"status"`
	ExpiresAt      time.Time                   `json:"expires_at"`
}

// canonicalWalletSettlementWireRequest is Phase 3.5's request (§11.2): the
// event's units exactly, no conversion in either direction; currency is
// sent for symmetry with ensure but the v2 route ignores it;
// local_balance_after is dropped (the drift comparison happens client-side
// against the response).
type canonicalWalletSettlementWireRequest struct {
	PlatformUserID   string                      `json:"platform_user_id"`
	EventID          string                      `json:"event_id"`
	LeaseID          string                      `json:"lease_id"`
	Currency         string                      `json:"currency"`
	Amount           canonicalWalletAmountObject `json:"amount"`
	GatewayRequestID string                      `json:"gateway_request_id,omitempty"`
	OccurredAt       string                      `json:"occurred_at"`
}

type canonicalWalletSettlementWireEvent struct {
	EventID             string                      `json:"event_id"`
	LeaseID             string                      `json:"lease_id"`
	Amount              canonicalWalletAmountObject `json:"amount"`
	LeaseCaptureSeq     int64                       `json:"lease_capture_seq"`
	LeaseCapturedBefore canonicalWalletAmountObject `json:"lease_captured_before"`
	LeaseCapturedAfter  canonicalWalletAmountObject `json:"lease_captured_after"`
	OccurredAt          string                      `json:"occurred_at"`
}

type canonicalWalletSettlementWireResponse struct {
	Accepted         bool                               `json:"accepted"`
	Duplicate        bool                               `json:"duplicate"`
	NamedLeaseID     *string                            `json:"named_lease_id"`
	Event            canonicalWalletSettlementWireEvent `json:"event"`
	Lease            canonicalWalletLeaseWireView       `json:"lease"`
	CanonicalBalance canonicalWalletAmountObject        `json:"canonical_balance"`
}

func (c *canonicalWalletHTTPClient) SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	wireRequest := canonicalWalletSettlementWireRequest{
		PlatformUserID: event.PlatformUserID, EventID: event.EventID, LeaseID: event.LeaseID, Currency: event.Currency,
		Amount:           newCanonicalWalletAmountObject(event.AmountUnits),
		GatewayRequestID: event.GatewayRequestID, OccurredAt: event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	var wireResponse canonicalWalletSettlementWireResponse
	if err := c.doJSON(ctx, http.MethodPost, canonicalWalletSettlementsPath, canonicalWalletSettlementScope, event.EventID, wireRequest, &wireResponse); err != nil {
		return nil, err
	}
	if !wireResponse.Accepted && !wireResponse.Duplicate {
		return nil, errors.New("control plane rejected canonical wallet settlement")
	}
	// §9.2/§11.2: the amounts this client consumes are parsed by the strict
	// parser — a parse failure is a decode-class error.
	capturedUnits, err := parseCanonicalWalletAmountObject("event.amount", wireResponse.Event.Amount)
	if err != nil {
		return nil, err
	}
	balanceUnits, err := parseCanonicalWalletAmountObject("canonical_balance", wireResponse.CanonicalBalance)
	if err != nil {
		return nil, err
	}
	result := &CanonicalWalletSettlementResult{
		Accepted:              wireResponse.Accepted,
		Duplicate:             wireResponse.Duplicate,
		CapturedUnits:         capturedUnits,
		EventLeaseID:          wireResponse.Event.LeaseID,
		LeaseCaptureSeq:       wireResponse.Event.LeaseCaptureSeq,
		CanonicalBalanceUnits: &balanceUnits,
	}
	if wireResponse.NamedLeaseID != nil {
		result.NamedLeaseID = *wireResponse.NamedLeaseID
	}
	return result, nil
}

func (c *canonicalWalletHTTPClient) doJSON(ctx context.Context, method, path, scope, idempotencyKey string, requestBody, responseBody any) error {
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("encode canonical wallet request: %w", err)
	}
	assertion, err := c.serviceAssertion(scope)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.ControlPlaneURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build canonical wallet request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+assertion)
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("call canonical wallet control plane: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read canonical wallet response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// §11.5: EVERY non-2xx comes back as the typed status error — the
		// status, the path, data.reason and (settlements' over-capture)
		// data.headroom, parsed with the strict parser when present.
		var refusal struct {
			Data struct {
				Reason   string                       `json:"reason"`
				Headroom *canonicalWalletAmountObject `json:"headroom"`
			} `json:"data"`
		}
		statusErr := &canonicalWalletStatusError{Status: resp.StatusCode, Path: path}
		if json.Unmarshal(body, &refusal) == nil {
			statusErr.Reason = refusal.Data.Reason
			if refusal.Data.Headroom != nil {
				if units, herr := parseCanonicalWalletAmountObject("data.headroom", *refusal.Data.Headroom); herr == nil {
					statusErr.HeadroomUnits = &units
				}
			}
		}
		// §9.6 item 6 (PRESERVED, not replaced): a 404/405 from the ensure
		// route at runtime means the control plane predates 3.4a-S — the
		// status error WRAPS ErrCanonicalWalletControlPlaneIncompatible so
		// both errors.As(err, &se) and errors.Is(err, …) succeed. Keyed on
		// the PATH — doJSON is shared with the settlements route, whose
		// statuses are its own.
		if path == canonicalWalletEnsurePath && (resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed) {
			statusErr.wrapped = fmt.Errorf("%w: ensure route answered status %d", ErrCanonicalWalletControlPlaneIncompatible, resp.StatusCode)
		}
		return statusErr
	}
	if len(body) == 0 || responseBody == nil {
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) == nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		body = envelope.Data
	}
	if err := json.Unmarshal(body, responseBody); err != nil {
		return fmt.Errorf("decode canonical wallet response: %w", err)
	}
	return nil
}

// probeEnsureRoute (§9.6 item 6) is on the HTTP CLIENT only — never on
// canonicalWalletControlPlane — so the stubs need no change. GET the ensure
// route; the caller keys on the returned status alone (405 = the route exists,
// POST-only; 404 = the control plane predates 3.4a-S).
func (c *canonicalWalletHTTPClient) probeEnsureRoute(ctx context.Context) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.ControlPlaneURL+canonicalWalletEnsurePath, nil)
	if err != nil {
		return 0, fmt.Errorf("build canonical wallet probe request: %w", err)
	}
	assertion, err := c.serviceAssertion(canonicalWalletLeaseScope)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+assertion)
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("probe canonical wallet control plane: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, nil
}

func (c *canonicalWalletHTTPClient) serviceAssertion(scope string) (string, error) {
	now := c.now()
	claims := jwt.MapClaims{
		"iss":   c.cfg.Issuer,
		"aud":   c.cfg.Audience,
		"sub":   "sub2api-gateway",
		"iat":   now.Unix(),
		"exp":   now.Add(30 * time.Second).Unix(),
		"jti":   uuid.NewString(),
		"scope": scope,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = c.cfg.Version
	signed, err := token.SignedString([]byte(c.cfg.Secret))
	if err != nil {
		return "", fmt.Errorf("sign canonical wallet assertion: %w", err)
	}
	return signed, nil
}

type canonicalWalletMetrics struct {
	queued                     atomic.Int64
	queueDropped               atomic.Int64
	outboxPayloadConflict      atomic.Int64
	leaseAcquireOK             atomic.Int64
	leaseIssued                atomic.Int64
	leaseReused                atomic.Int64
	leaseGrantBelowAmount      atomic.Int64
	leaseGrantExpired          atomic.Int64
	leaseAcquireError          atomic.Int64
	leaseBindError             atomic.Int64
	reserveOK                  atomic.Int64
	reserveError               atomic.Int64
	settlementOK               atomic.Int64
	settlementError            atomic.Int64
	balanceMismatch            atomic.Int64
	missingPlatformID          atomic.Int64
	unsupportedCurrency        atomic.Int64
	deadLetterBalanceShortfall atomic.Int64
	// Phase 3.5 (§11.2): the v2 settlements client's counters.
	namedLeaseReleased atomic.Int64
	balanceBehindLocal atomic.Int64
	// Phase 3.5 (§11.5): the terminal classification counters.
	deadLetterContractViolation atomic.Int64
	deadLetterPayloadConflict   atomic.Int64
	// Phase 3.5 (§11.3): the split counters.
	settlementSplit          atomic.Int64
	settlementSplitFull      atomic.Int64
	settlementSplitExhausted atomic.Int64
	pendingReleaseReplayed   atomic.Int64
	// Phase 3.7a (§13.2.8): the reactive split's seal of the refusing lease.
	settlementSplitSealed atomic.Int64
	// Phase 3.5 (§11.4): the late capture and the receivable gauge.
	lateCaptureRetargeted        atomic.Int64
	settlementUncollectableUnits atomic.Int64
	// Phase 3.5 (§11.7): the zero-cost abort points and the token-less alert.
	holdReleasedZeroCost           atomic.Int64
	settlementWithoutAuthorization atomic.Int64
	// §9.6 item 6's rollout-guard counters (global, like every counter here).
	controlPlaneIncompatible atomic.Int64
	controlPlaneProbeFailed  atomic.Int64
	// Phase 3.4b (§10.4/§10.5): the hold counters.
	holdReleasedNotWritten      atomic.Int64
	holdReleaseError            atomic.Int64
	holdConverted               atomic.Int64
	holdConvertError            atomic.Int64
	holdConvertDuplicate        atomic.Int64
	holdSettlementAfterRelease  atomic.Int64
	holdConvertOverrunReleased  atomic.Int64
	holdMissingAtSettlement     atomic.Int64
	holdIndeterminateClassified atomic.Int64
	holdIndeterminateRowFailed  atomic.Int64
	// Phase 3.4b (§10.7): the reaper counters.
	reaperError         atomic.Int64
	holdsAbandoned      atomic.Int64
	holdOutcomesExpired atomic.Int64
	// Phase 3.7a (§13.2.6): one per reaper tick, incremented before
	// reapOnce — the loop's heartbeat for the stop test.
	reaperTicks atomic.Int64
	// Phase 4.2-G Task 2: the receivable collector's counters — re-drives
	// performed, and candidates seen at the bound (terminal, staying in the
	// receivable) so the collector's exhaustion is observable.
	receivableRedriven         atomic.Int64
	receivableRedriveExhausted atomic.Int64
}

var canonicalWalletBridgeMetrics canonicalWalletMetrics

func CanonicalWalletBridgeStats() map[string]int64 {
	m := &canonicalWalletBridgeMetrics
	return map[string]int64{
		"queued": m.queued.Load(), "queue_dropped": m.queueDropped.Load(),
		"outbox_payload_conflict": m.outboxPayloadConflict.Load(),
		"lease_acquire_ok":        m.leaseAcquireOK.Load(), "lease_acquire_error": m.leaseAcquireError.Load(),
		"lease_issued": m.leaseIssued.Load(), "lease_reused": m.leaseReused.Load(),
		"lease_grant_below_amount": m.leaseGrantBelowAmount.Load(), "lease_grant_expired": m.leaseGrantExpired.Load(),
		"lease_bind_error": m.leaseBindError.Load(),
		"reserve_ok":       m.reserveOK.Load(), "reserve_error": m.reserveError.Load(),
		"settlement_ok": m.settlementOK.Load(), "settlement_error": m.settlementError.Load(),
		"balance_mismatch": m.balanceMismatch.Load(), "missing_platform_user_id": m.missingPlatformID.Load(),
		"unsupported_currency":             m.unsupportedCurrency.Load(),
		"dead_letter_balance_shortfall":    m.deadLetterBalanceShortfall.Load(),
		"named_lease_released":             m.namedLeaseReleased.Load(),
		"balance_behind_local":             m.balanceBehindLocal.Load(),
		"dead_letter_contract_violation":   m.deadLetterContractViolation.Load(),
		"dead_letter_payload_conflict":     m.deadLetterPayloadConflict.Load(),
		"settlement_split":                 m.settlementSplit.Load(),
		"settlement_split_full":            m.settlementSplitFull.Load(),
		"settlement_split_exhausted":       m.settlementSplitExhausted.Load(),
		"settlement_split_sealed":          m.settlementSplitSealed.Load(),
		"pending_release_replayed":         m.pendingReleaseReplayed.Load(),
		"late_capture_retargeted":          m.lateCaptureRetargeted.Load(),
		"settlement_uncollectable_units":   m.settlementUncollectableUnits.Load(),
		"hold_released_zero_cost":          m.holdReleasedZeroCost.Load(),
		"settlement_without_authorization": m.settlementWithoutAuthorization.Load(),
		"control_plane_incompatible":       m.controlPlaneIncompatible.Load(),
		"control_plane_probe_failed":       m.controlPlaneProbeFailed.Load(),
		"hold_released_not_written":        m.holdReleasedNotWritten.Load(),
		"hold_release_error":               m.holdReleaseError.Load(),
		"hold_converted":                   m.holdConverted.Load(),
		"hold_convert_error":               m.holdConvertError.Load(),
		"hold_convert_duplicate":           m.holdConvertDuplicate.Load(),
		"hold_settlement_after_release":    m.holdSettlementAfterRelease.Load(),
		"hold_convert_overrun_released":    m.holdConvertOverrunReleased.Load(),
		"hold_missing_at_settlement":       m.holdMissingAtSettlement.Load(),
		"hold_indeterminate_classified":    m.holdIndeterminateClassified.Load(),
		"hold_indeterminate_row_failed":    m.holdIndeterminateRowFailed.Load(),
		"reaper_error":                     m.reaperError.Load(),
		"holds_abandoned":                  m.holdsAbandoned.Load(),
		"hold_outcomes_expired":            m.holdOutcomesExpired.Load(),
		"reaper_ticks":                     m.reaperTicks.Load(),
		"receivable_redriven":              m.receivableRedriven.Load(),
		"receivable_redrive_exhausted":     m.receivableRedriveExhausted.Load(),
	}
}

type CanonicalWalletBridge struct {
	cfg      config.CanonicalWalletConfig
	store    CanonicalWalletLeaseStore
	control  canonicalWalletControlPlane
	outboxDB *sql.DB
	outbox   CanonicalWalletOutboxStore
	workerID string
	// now (Phase 3.4): the injectable clock every lease-expiry decision in
	// this file uses — ensureLease, resolveOutboxEventLease and
	// HasCanonicalWalletHeadroom. Tests set it; production keeps UTC wall time.
	now func() time.Time
	// callerSlotTTLSeconds (Phase 3.4): gateway.concurrency_slot_ttl_minutes × 60,
	// sent on every ensure as caller_slot_ttl_seconds (redesign §3.3).
	callerSlotTTLSeconds int
	// observedForTest (Phase 3.3a): test-only hook invoked at the top of
	// ObserveSettlement so the token's arrival can be asserted in-process.
	observedForTest func(CanonicalWalletSettlementEvent)
	// holdOutcomes (Phase 3.4b, §10.6): the durable outcome-row sink, built
	// from the bridge's own outbox DB (never a new provider — constraint 2);
	// the nil-safe no-op until Task 4 constructs the real store.
	holdOutcomes walletHoldOutcomeSink
	// liveProvisional (Phase 3.4b, §10.7): the reaper's ONLY liveness signal
	// (§5's second signal is deliberately unimplemented — no general
	// per-attempt durable record exists before 3.5's outbox authorization_id
	// column). Built from the same outbox DB, the liveProvisionalStore pattern.
	liveProvisional LiveProvisionalStore
	// stop/stopOnce/loops (Phase 3.7a, §13.2.6): Close closes stop once and
	// waits on loops; both tick loops select on stop. Tests only — a nil
	// stop means the bridge was built as a bare struct literal and started
	// no loops, and Close is a no-op for it.
	stop     chan struct{}
	stopOnce sync.Once
	loops    sync.WaitGroup
}

// walletHoldOutcomeSink is the durable outcome row's write surface (§10.6).
// A nil receiver is a no-op on every method — the bridges built with a nil
// outboxDB (most tests) must never panic on an indeterminate classification.
type walletHoldOutcomeSink interface {
	InsertIndeterminate(ctx context.Context, hold CanonicalWalletHold, platformUserID string, at time.Time) error
	InsertAbandoned(ctx context.Context, hold CanonicalWalletHold, platformUserID string, at time.Time) error
	MarkSettled(ctx context.Context, authorizationID, eventID string, at time.Time) error
	MarkExpiredOlderThan(ctx context.Context, cutoff, at time.Time) (int64, error)
}

// noopHoldOutcomeSink is Task 3's placeholder (commit order matters): the
// indeterminate classification writes the class and defers the row to Task 4.
type noopHoldOutcomeSink struct{}

func (noopHoldOutcomeSink) InsertIndeterminate(context.Context, CanonicalWalletHold, string, time.Time) error {
	return nil
}
func (noopHoldOutcomeSink) InsertAbandoned(context.Context, CanonicalWalletHold, string, time.Time) error {
	return nil
}
func (noopHoldOutcomeSink) MarkSettled(context.Context, string, string, time.Time) error { return nil }
func (noopHoldOutcomeSink) MarkExpiredOlderThan(context.Context, time.Time, time.Time) (int64, error) {
	return 0, nil
}

// HoldsEnabled (§10.1): canonical_wallet.holds == on and a bridge that acts.
// Off is 3.4a byte-for-byte — every hold path in this file is behind this
// predicate.
func (b *CanonicalWalletBridge) HoldsEnabled() bool {
	return b != nil && b.cfg.Holds == "on" && b.cfg.Mode != config.CanonicalWalletModeDisabled
}

// graceMS is the hold's PEXPIREAT margin past its lease (§10.3): the hold
// outlives its lease hash by one orphan grace so the reaper can still record
// an abandoned outcome for an orphan whose lease expired first.
func (b *CanonicalWalletBridge) graceMS() int64 {
	if b == nil || b.cfg.OrphanGraceSeconds <= 0 {
		return 0
	}
	return int64(time.Duration(b.cfg.OrphanGraceSeconds) * time.Second / time.Millisecond)
}

// holdOutcome is installed on the handle by Authorize when a hold was armed
// (§10.4/§10.5). Invoked by RecordOutcome outside the handle's mutex; never
// re-enters the handle — everything it needs is in its closure and params.
func (b *CanonicalWalletBridge) holdOutcome(platformUserID, authorizationID string) func(string, AuthorizationOutcome, error) {
	return func(_ string, outcome AuthorizationOutcome, err error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		defer cancel()
		switch outcome {
		case AuthorizationOutcomeNotWritten:
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// §10.5's cross-check (the 3.3 residual): a cancelled context
				// may have reached the wire before WroteHeaders fired — kept
				// indeterminate, never released.
				b.markHoldIndeterminate(ctx, platformUserID, authorizationID)
				return
			}
			if _, rerr := b.store.ReleaseCanonicalWalletHold(ctx, platformUserID, authorizationID, "released", "not_written"); rerr != nil && !errors.Is(rerr, ErrCanonicalWalletHoldMissing) && !isHoldNotArmed(rerr) {
				canonicalWalletBridgeMetrics.holdReleaseError.Add(1)
				slog.Warn("canonical wallet hold release failed", "authorization_id", authorizationID, "error", rerr)
				return
			}
			canonicalWalletBridgeMetrics.holdReleasedNotWritten.Add(1)
		case AuthorizationOutcomeIndeterminate:
			b.markHoldIndeterminate(ctx, platformUserID, authorizationID)
		default: // result: nothing is written (§10.5)
		}
	}
}

// markHoldIndeterminate sets class=indeterminate iff the hold is still armed
// (an atomic script — a concurrent release must not be overwritten) and then
// writes the durable outcome row (§10.6; the no-op sink until Task 4).
func (b *CanonicalWalletBridge) markHoldIndeterminate(ctx context.Context, platformUserID, authorizationID string) {
	hold, err := b.store.MarkCanonicalWalletHoldClass(ctx, platformUserID, authorizationID, "indeterminate")
	if err != nil {
		if !errors.Is(err, ErrCanonicalWalletHoldMissing) && !isHoldNotArmed(err) {
			slog.Warn("canonical wallet hold indeterminate mark failed", "authorization_id", authorizationID, "error", err)
		}
		return
	}
	canonicalWalletBridgeMetrics.holdIndeterminateClassified.Add(1)
	if b.holdOutcomes == nil {
		return
	}
	if err := b.holdOutcomes.InsertIndeterminate(ctx, *hold, platformUserID, b.clock()); err != nil {
		canonicalWalletBridgeMetrics.holdIndeterminateRowFailed.Add(1)
		slog.Warn("canonical wallet hold outcome row insert failed", "authorization_id", authorizationID, "error", err)
	}
}

// clock is the only way this file reads the injectable clock. Several existing
// tests build a bare &CanonicalWalletBridge{…} literal instead of going through
// newCanonicalWalletBridge — deliberately, because the constructor always starts
// runOutboxDispatcher (see the comment above the literal in
// canonical_wallet_outbox_integration_test.go) — so `now` can be nil in
// canonical_wallet_failure_paths_integration_test.go,
// canonical_wallet_outbox_integration_test.go, canonical_wallet_wire_http_test.go,
// canonical_wallet_lease_rebind_integration_test.go and
// canonical_wallet_authorizer_test.go. Never read b.now directly.
func (b *CanonicalWalletBridge) clock() time.Time {
	if b == nil || b.now == nil {
		return time.Now().UTC()
	}
	return b.now()
}

func NewCanonicalWalletBridge(cfg *config.Config, store CanonicalWalletLeaseStore, outboxDB *sql.DB, outbox CanonicalWalletOutboxStore) *CanonicalWalletBridge {
	if cfg == nil || cfg.CanonicalWallet.Mode == "" || cfg.CanonicalWallet.Mode == config.CanonicalWalletModeDisabled {
		return nil
	}
	slot := 0
	if cfg.Gateway.ConcurrencySlotTTLMinutes > 0 {
		slot = cfg.Gateway.ConcurrencySlotTTLMinutes * 60
	}
	// probeCanonicalWalletEnsureRoute (§9.6 item 6): the rollout guard. Hoisted
	// here — the client is built once, probed BEFORE the bridge (and its
	// dispatcher goroutine) starts. ShipAny 3.4a-S deploys first; this half
	// refuses to enforce against a control plane with no /ensure route.
	client := newCanonicalWalletHTTPClient(cfg.CanonicalWallet, nil)
	probeCanonicalWalletEnsureRoute(cfg, client)
	// §9.5: production keeps UTC wall time — the injection exists for the
	// skew-margin and expiry tests.
	return newCanonicalWalletBridge(cfg.CanonicalWallet, store, client, outboxDB, outbox, slot, nil)
}

// probeCanonicalWalletEnsureRoute GETs the ensure route once per bridge
// construction (the process builds three bridges — Known limits): a body-less
// 405 means the route exists (vinext answers a resolved route with no GET
// export with 405 — the probe keys on the status alone); an exact 404 means
// the control plane predates 3.4a-S — enforce refuses to start, shadow counts
// control_plane_incompatible and continues. Any other status or a transport
// error is control_plane_probe_failed and continues: a WAF, a misrouted URL
// or a transient must never crash-loop a healthy deployment. The panic is
// deviation 1: this codebase's fatal startup path is log.Fatalf in
// cmd/server/main.go, but constraint 2 forbids changing the provider
// signatures that would have to carry an error; a production enforce
// deployment recovers by setting canonical_wallet.mode to shadow/disabled
// and restarting.
func probeCanonicalWalletEnsureRoute(cfg *config.Config, client *canonicalWalletHTTPClient) {
	mode := cfg.CanonicalWallet.Mode
	if mode != config.CanonicalWalletModeShadow && mode != config.CanonicalWalletModeEnforce {
		return // disabled never probes
	}
	timeout := time.Duration(cfg.CanonicalWallet.RequestTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	status, err := client.probeEnsureRoute(ctx)
	if err != nil {
		canonicalWalletBridgeMetrics.controlPlaneProbeFailed.Add(1)
		slog.Warn("canonical wallet control plane probe failed", "error", err)
		return
	}
	switch status {
	case http.StatusMethodNotAllowed:
		// The route exists (POST-only) — ok.
	case http.StatusNotFound:
		if mode == config.CanonicalWalletModeEnforce {
			panic("canonical_wallet: control plane has no ensure route — deploy ShipAny 3.4a-S first (redesign §9.6 item 6)")
		}
		canonicalWalletBridgeMetrics.controlPlaneIncompatible.Add(1)
		slog.Error("canonical wallet: control plane has no ensure route — shadow continues; deploy ShipAny 3.4a-S before enforcing (redesign §9.6 item 6)")
	default:
		canonicalWalletBridgeMetrics.controlPlaneProbeFailed.Add(1)
		slog.Warn("canonical wallet control plane probe returned an unexpected status", "status", status)
	}
}

// newCanonicalWalletBridge takes the injectable clock as its LAST parameter
// (§9.5): every lease-expiry decision and every reserve-script `now` in this
// file reads b.clock(). The HTTP client's own clock is deliberately NOT this
// one — it mints the service assertion (iat/exp), which the verifier checks
// against wall-clock. A nil clock means UTC wall time. Assigning b.now after
// construction is forbidden: the dispatcher goroutine starts inside this
// constructor and would race the assignment.
func newCanonicalWalletBridge(cfg config.CanonicalWalletConfig, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane, outboxDB *sql.DB, outbox CanonicalWalletOutboxStore, callerSlotTTLSeconds int, clock func() time.Time) *CanonicalWalletBridge {
	if callerSlotTTLSeconds <= 0 {
		callerSlotTTLSeconds = 1800 // gateway.concurrency_slot_ttl_minutes' default (30) × 60
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	b := &CanonicalWalletBridge{
		cfg:      cfg,
		store:    store,
		control:  control,
		outboxDB: outboxDB,
		outbox:   outbox,
		now:      clock,
		// This dispatcher instance's opaque claim token — generated once
		// per bridge, never per tick, so every row this instance claims is
		// resolvable only by this same instance.
		workerID:             "sub2api-wallet-dispatcher-" + uuid.NewString(),
		callerSlotTTLSeconds: callerSlotTTLSeconds,
		// Phase 3.4b (§10.6): the outcome-row store on the bridge's OWN db —
		// never a new provider (constraint 2); nil (a no-op sink) without one.
		holdOutcomes: newWalletHoldOutcomeStore(outboxDB),
		liveProvisional: func() LiveProvisionalStore {
			if outboxDB == nil {
				return nil
			}
			return newLiveProvisionalStore(outboxDB)
		}(),
	}
	b.stop = make(chan struct{})
	b.loops.Add(1)
	go func() {
		defer b.loops.Done()
		b.runOutboxDispatcher()
	}()
	// Phase 4.2-G Task 2: the receivable collector, started with the
	// dispatcher (it re-queues balance_shortfall dead-letters so a funded
	// user's debt is collected; the SETNX leader makes the deployment-wide
	// pass singular despite the three bridges per process).
	b.loops.Add(1)
	go func() {
		defer b.loops.Done()
		b.runReceivableCollector()
	}()
	// §10.7: a reaper goroutine per bridge, started with the dispatcher —
	// only when holds are on AND the outcome row's DB exists (the reaper's
	// abandoned/expired passes have nowhere to write without it). Each tick
	// is idempotent and the SETNX leader serialises sweeps deployment-wide.
	if b.HoldsEnabled() && b.outboxDB != nil {
		b.loops.Add(1)
		go func() {
			defer b.loops.Done()
			b.runHoldReaper()
		}()
	}
	return b
}

// Close (Phase 3.7a, redesign §13.2.6) stops every tick loop (the
// dispatcher, the reaper and the receivable collector) and waits for them
// to exit; it is idempotent (sync.Once) and nil-safe. It is a TEST
// facility with no production caller: the three bridges are constructed
// inside NewGatewayService, NewOpenAIGatewayService and
// ProvideBillingCacheService and are never returned to the DI graph, so in
// production the loops end with the process exactly as before. A bridge
// built as a bare &CanonicalWalletBridge{…} literal has stop == nil and
// never started a loop — Close on it is a no-op, not a nil-channel panic.
// Close is never called from inside a tick (neither deliverOutboxEvent nor
// reapOnce reaches it), so loops.Wait cannot deadlock; for a nil-outbox
// bridge Add(1) precedes the goroutine, the loop returns on its guard,
// Done() fires, and Wait() returns at once.
func (b *CanonicalWalletBridge) Close() {
	if b == nil || b.stop == nil {
		return
	}
	b.stopOnce.Do(func() { close(b.stop) })
	b.loops.Wait()
}

// ObserveSettlement durably records the settlement event in the Postgres
// outbox (its own transaction, committed synchronously before returning) —
// replacing the previous in-memory bounded channel, which silently dropped
// events when full or on crash. Once this returns, the event WILL
// eventually be delivered (at-least-once, surviving a crash). It never
// returns an error to the existing billing path.
//
// Disclosed limit: a BeginTx/Insert/Commit failure here still loses the
// event — logged and counted in queueDropped, but not retried, because the
// durability store itself is what failed. Same class of gap the in-memory
// channel had, now bounded to "Postgres itself is down or the write
// genuinely failed" instead of "an ordinary burst filled a fixed buffer."
// The one Insert error that loses nothing — the payload-hash conflict
// rejecting a repriced resubmission (the stored row stands and will
// deliver) — is counted separately as outboxPayloadConflict (round-1
// MINOR-1), keeping queueDropped a pure durability signal.
func (b *CanonicalWalletBridge) ObserveSettlement(event CanonicalWalletSettlementEvent) bool {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return false
	}
	if event.AmountUnits <= 0 {
		// §11.7: the zero-amount return is an abort point too — an armed
		// hold whose settlement carried nothing must not wait for the reaper.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		b.releaseHoldZeroCost(ctx, event.PlatformUserID, event.AuthorizationID)
		cancel()
		return false
	}
	if b.observedForTest != nil {
		b.observedForTest(event)
	}
	if b.outbox == nil || b.outboxDB == nil {
		// Several of this file's own tests construct a bridge with a nil
		// outbox because they don't exercise ObserveSettlement — guard BOTH
		// fields here, since the very next statement dereferences outboxDB.
		return false
	}
	event.PlatformUserID = strings.TrimSpace(event.PlatformUserID)
	if event.PlatformUserID == "" {
		canonicalWalletBridgeMetrics.missingPlatformID.Add(1)
		return false
	}
	currency, err := RequireCNYBillingCurrency(event.Currency)
	if err != nil {
		canonicalWalletBridgeMetrics.unsupportedCurrency.Add(1)
		return false
	}
	event.Currency = currency
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency)
	}
	// Phase 3.3a: in shadow mode the bridge logs the token with the event id
	// when the settlement is enqueued — the durable join between a wallet
	// event and the write that produced it (debug-level, token non-empty only).
	if event.AuthorizationToken != "" {
		logger.LegacyPrintf("service.authorization", "shadow: settlement %s carries authorization token %s (authorization %s)", event.EventID, event.AuthorizationToken, event.AuthorizationID)
	}
	// Phase 3.4b (§10.5): the settlement converts the hold IN-PROCESS, before
	// the outbox insert — the event's AuthorizationID is in memory here and
	// nowhere else (the outbox payload omits it until 3.5). The conversion
	// runs under its OWN budget so the Lua round trip never eats the outbox
	// transaction's.
	var holdConv CanonicalWalletHoldConversion
	if b.HoldsEnabled() && event.AuthorizationID != "" {
		convCtx, convCancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		conv, cerr := b.store.ConvertCanonicalWalletHold(convCtx, event.PlatformUserID, event.AuthorizationID, event.EventID, event.AmountUnits, b.clock())
		convCancel()
		holdConv = conv
		switch {
		case cerr != nil:
			canonicalWalletBridgeMetrics.holdConvertError.Add(1) // proceed unbound: money-safe (the hold stays armed; the reaper resolves it)
		case conv.Code == 0:
			event.LeaseID = conv.LeaseID
			canonicalWalletBridgeMetrics.holdConverted.Add(1)
		case conv.Code == 7 && conv.EventID != "":
			event.LeaseID = conv.LeaseID
			canonicalWalletBridgeMetrics.holdConvertDuplicate.Add(1) // a retried submission: the lease is restored so the payload hashes identically and the outbox dedups the row (§13.2.7)
		case conv.Code == 7: // released / abandoned: real usage after the money went back (§10.5)
			canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Add(1)
			event.LeaseID = ""
		case conv.Code == 4:
			canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Add(1)
			event.LeaseID = ""
		default: // 1 missing, 3 lease gone: the event proceeds untouched — a caller-supplied LeaseID deliberately stays (3.4a's test 19b depends on it)
			canonicalWalletBridgeMetrics.holdMissingAtSettlement.Add(1)
		}
	}
	// §11.7's token-less alert: a settlement that reaches this point under
	// holds carrying no authorization id — a path that minted no handle (a
	// missed freeze point) or a pre-token row being redelivered. Counted and
	// logged with the gateway request id for Phase 4's reconciliation; NO
	// behaviour change — the row is inserted below either way.
	if b.HoldsEnabled() && event.AuthorizationID == "" {
		canonicalWalletBridgeMetrics.settlementWithoutAuthorization.Add(1)
		slog.Warn("canonical wallet settlement without an authorization id (holds on)", "gateway_request_id", event.GatewayRequestID, "event_id", event.EventID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox begin failed", "event_id", event.EventID, "error", err)
		return false
	}
	if err := b.outbox.InsertOutboxEventTx(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		// Round-1 MINOR-1: a rejected repriced resubmission is a SUCCESSFUL
		// dedup — the stored row stands and will deliver — not a durability
		// drop, so it gets its own counter and an info line; queueDropped
		// stays reserved for real write failures.
		if errors.Is(err, ErrCanonicalWalletOutboxPayloadConflict) {
			canonicalWalletBridgeMetrics.outboxPayloadConflict.Add(1)
			slog.Info("canonical wallet outbox insert rejected a repriced resubmission (payload conflict)", "event_id", event.EventID, "error", err)
			return false
		}
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox insert failed", "event_id", event.EventID, "error", err)
		return false
	}
	if err := tx.Commit(); err != nil {
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox commit failed", "event_id", event.EventID, "error", err)
		return false
	}
	canonicalWalletBridgeMetrics.queued.Add(1)
	// §10.6's MarkSettled runs AFTER the commit, and only then — a rolled-back
	// settlement must never leave a settled row. Best effort.
	if b.HoldsEnabled() && event.AuthorizationID != "" && b.holdOutcomes != nil &&
		(holdConv.Code == 0 || (holdConv.Code == 7 && holdConv.EventID != "")) {
		markCtx, markCancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		_ = b.holdOutcomes.MarkSettled(markCtx, event.AuthorizationID, event.EventID, b.clock())
		markCancel()
	}
	return true
}

// CheckAndReserve is the real request-preflight state machine. Shadow mode
// observes a denial but always allows the request; enforce mode fails closed.
// A retry carrying an explicit event.LeaseID resolves against THAT lease —
// never silently against whatever lease is current now.
func (b *CanonicalWalletBridge) CheckAndReserve(ctx context.Context, event CanonicalWalletSettlementEvent) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency)
	}
	var lease *CanonicalWalletLease
	var err error
	// Phase 3.4: the explicit-id branch now lives inside ensureLease — on a
	// miss the retry recovers through prefer_lease_id (redesign §4) instead
	// of failing with ErrCanonicalWalletLeaseMissing.
	lease, err = b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountUnits, canonicalWalletLeasePurposeAuthorize, strings.TrimSpace(event.LeaseID))
	if err != nil {
		if b.cfg.Mode == config.CanonicalWalletModeShadow {
			return true, nil
		}
		return false, err
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, b.clock())
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	event.LeaseID = reservation.Lease.LeaseID
	return true, nil
}

// Mode reports the bridge's configured mode — exported so callers outside
// this package's own methods (billing_cache_service.go, same `service`
// package but a different file) have a stable accessor rather than reaching
// into the unexported cfg field directly.
func (b *CanonicalWalletBridge) Mode() string {
	if b == nil {
		return config.CanonicalWalletModeDisabled
	}
	return b.cfg.Mode
}

// HasCanonicalWalletHeadroom is a READ-ONLY admission gate — it does not
// reserve anything and does not touch the Redis reservation script at all.
// It deliberately does NOT call ensureLease: ensureLease can synchronously
// call ShipAny's EnsureLease and INSTALL a brand-new lease when none
// exists or the current one is exhausted, and ShipAny's issueShadowLease
// only reads available credits and inserts a lease row (it does not
// atomically transfer credits into a leased ledger) — so repeatedly calling
// such a check against an exhausted lease could mint overlapping shadow
// budgets across concurrent requests. A pure GetCanonicalWalletLease read
// never writes anything. If no lease has ever been issued for this user
// (or Redis evicted it), there is no data to check and this method fails
// CLOSED in enforce mode rather than fabricating one — pre-warming leases
// at purchase/recharge/key-creation is later-phase work.
func (b *CanonicalWalletBridge) HasCanonicalWalletHeadroom(ctx context.Context, platformUserID, currency string) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	lease, err := b.store.GetCanonicalWalletLease(ctx, platformUserID)
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if lease.Currency != currency || b.leaseExpiredAt(lease, b.clock()) {
		return false, nil
	}
	return lease.RemainingUnits() > 0, nil
}

// runHoldReaper (§10.7) is the per-bridge tick loop. Every decision reads
// b.clock() through reapOnce; tests never sleep — they drive reapOnce with
// the injected clock directly.
func (b *CanonicalWalletBridge) runHoldReaper() {
	interval := time.Duration(b.cfg.OrphanSweepIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			canonicalWalletBridgeMetrics.reaperTicks.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), interval/2)
			b.reapOnce(ctx, b.clock())
			cancel()
		}
	}
}

// reapOnce is one tick (§10.7). Exported to tests through the package;
// production only calls it from runHoldReaper. The deployment-wide SETNX
// leader serialises sweeps; the two-consecutive-empty-ticks prune state
// lives in REDIS (not per-bridge memory) so it survives leader rotation.
func (b *CanonicalWalletBridge) reapOnce(ctx context.Context, now time.Time) {
	if b == nil || b.store == nil {
		return
	}
	leader, err := b.store.TryCanonicalWalletReaperLease(ctx, 2*time.Duration(b.cfg.OrphanSweepIntervalSeconds)*time.Second)
	if err != nil || !leader {
		return
	}
	grace := time.Duration(b.cfg.OrphanGraceSeconds) * time.Second
	var cursor uint64
	for {
		users, next, err := b.store.ListCanonicalWalletHoldUsers(ctx, cursor, 100)
		if err != nil {
			canonicalWalletBridgeMetrics.reaperError.Add(1)
			return
		}
		for _, user := range users {
			ids, err := b.store.ListCanonicalWalletHolds(ctx, user, b.cfg.OrphanSweepBatch)
			if err != nil {
				continue
			}
			if len(ids) == 0 {
				// §10.3's two-consecutive-empty-ticks prune, with the state in
				// REDIS so it survives leader rotation (the SETNX leader is
				// deployment-wide; per-bridge memory never sees two ticks).
				if alreadySeen, err := b.store.MarkCanonicalWalletHoldUserEmpty(ctx, user, 2*time.Duration(b.cfg.OrphanSweepIntervalSeconds)*time.Second); err == nil && alreadySeen {
					_ = b.store.PruneCanonicalWalletHoldUser(ctx, user)
					_ = b.store.ClearCanonicalWalletHoldUserEmpty(ctx, user)
				}
				continue
			}
			_ = b.store.ClearCanonicalWalletHoldUserEmpty(ctx, user)
			for _, id := range ids {
				h, err := b.store.GetCanonicalWalletHold(ctx, user, id)
				if errors.Is(err, ErrCanonicalWalletHoldMissing) || (err == nil && h.State != "armed") {
					_ = b.store.ForgetCanonicalWalletHold(ctx, user, id) // SREM only
					continue
				}
				if err != nil || h.Class == "indeterminate" || now.Sub(h.ArmedAt) < grace {
					continue
				}
				if b.liveProvisionalActive(ctx, id) {
					continue
				}
				if _, err := b.store.ReleaseCanonicalWalletHold(ctx, user, id, "abandoned", ""); err == nil {
					canonicalWalletBridgeMetrics.holdsAbandoned.Add(1)
					if b.holdOutcomes != nil {
						_ = b.holdOutcomes.InsertAbandoned(ctx, *h, user, now)
					}
				}
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	// §10.6's expired pass: rows still open whose armed_at is older than
	// lease_ttl + grace — the hold hash is gone by then; Phase 4 reconciles
	// these against wallet_lease_close. §10.6 says lease_ttl + the SETTLE-side
	// grace (ShipAny's number, unknown on this side of the wire); orphan_grace
	// is the only figure available here — recorded as deviation 1.
	if b.holdOutcomes != nil {
		cutoff := now.Add(-(time.Duration(b.cfg.LeaseTTLSeconds)*time.Second + grace))
		if n, err := b.holdOutcomes.MarkExpiredOlderThan(ctx, cutoff, now); err == nil && n > 0 {
			canonicalWalletBridgeMetrics.holdOutcomesExpired.Add(n)
		}
	}
}

// liveProvisionalActive is §10.7's liveness signal: an active Live record for
// the authorization id (Live's token is its authorization id, openai_live.go)
// keeps the hold. Not-found → false; ANY OTHER ERROR → true (fail safe: never
// reap on a DB error).
func (b *CanonicalWalletBridge) liveProvisionalActive(ctx context.Context, authorizationID string) bool {
	if b.liveProvisional == nil {
		return false
	}
	rec, err := b.liveProvisional.Get(ctx, authorizationID)
	if err != nil {
		return !errors.Is(err, ErrLiveProvisionalNotFound)
	}
	switch rec.Status {
	case LiveProvisionalStatusProvisional, LiveProvisionalStatusActive, LiveProvisionalStatusFinalizing:
		return true
	default:
		return false
	}
}

// runOutboxDispatcher polls for durably-persisted pending events instead of
// draining an in-memory channel, so a crash mid-delivery leaves the event
// claimable again on restart instead of gone.
// walletOutboxDispatchBatch bounds how many events one tick claims. It is
// deliberately small: staleAfter (below) must provably exceed the worst-case
// time the LAST event in a batch waits behind its predecessors, and that
// worst case grows linearly with this number.
const walletOutboxDispatchBatch = 10

// refreshReceivableGauge (§11.4) recomputes settlementUncollectableUnits —
// the sum of amount_units over balance_shortfall dead-letters, the
// receivable — from the durable table. Production refreshes it every 100th
// dispatcher tick (the tick is RequestTimeoutMS — per-tick would be ~3
// aggregates/s); tests call it directly. A store error keeps the last value.
func (b *CanonicalWalletBridge) refreshReceivableGauge(ctx context.Context) {
	if b == nil || b.outbox == nil {
		return
	}
	units, err := b.outbox.SumDeadLetterUnits(ctx, "balance_shortfall")
	if err != nil {
		slog.Warn("canonical wallet receivable gauge refresh failed", "error", err)
		return
	}
	canonicalWalletBridgeMetrics.settlementUncollectableUnits.Store(units)
}

// canonicalWalletCollectorLease is the receivable collector's
// deployment-wide leader: the reaper's TryCanonicalWalletReaperLease
// pattern with its OWN key (the process builds THREE bridges — a per-
// process loop would re-drive three times). Exposed as a narrow OPTIONAL
// interface (the NewWalletReconciliationReadService type-assertion
// pattern): a store that does not implement it never collects, so the
// in-memory test stubs are not forced to carry Redis leadership.
type canonicalWalletCollectorLease interface {
	TryCanonicalWalletReceivableCollectorLease(ctx context.Context, ttl time.Duration) (bool, error)
}

// walletReceivableRedriveBatch bounds one collector pass.
const walletReceivableRedriveBatch = 100

// runReceivableCollector (Phase 4.2-G Task 2) is the collector's tick loop
// — the runHoldReaper shape (stop chan + ticker, tests drive
// collectReceivableOnce directly with the injected clock).
func (b *CanonicalWalletBridge) runReceivableCollector() {
	if b.outbox == nil || b.outboxDB == nil {
		return
	}
	interval := time.Duration(b.receivableRedriveIntervalSeconds()) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), interval/2)
			b.collectReceivableOnce(ctx)
			cancel()
		}
	}
}

// collectReceivableOnce is one collector pass (§11.3/§11.4's receivable,
// made collectable): balance_shortfall dead-letters younger than the
// retention window are re-queued to pending under the SETNX leader, bounded
// by redrive_count — a funded user's debt is then collected by the ordinary
// dispatcher. The collector owns ONLY the re-queue and the bound: it never
// reclassifies (a re-driven delivery refused again re-acquires
// balance_shortfall through the existing terminal path; one that fails on
// transport correctly dead-letters attempts_exhausted — a fault is what it
// is) and it never deletes. Candidates AT the bound are terminal: counted
// (receivable_redrive_exhausted) and left in the receivable.
func (b *CanonicalWalletBridge) collectReceivableOnce(ctx context.Context) {
	if b == nil || b.outbox == nil || b.outboxDB == nil {
		return
	}
	leader, ok := b.store.(canonicalWalletCollectorLease)
	if !ok {
		return // a store without the leader surface never collects
	}
	interval := time.Duration(b.receivableRedriveIntervalSeconds()) * time.Second
	held, err := leader.TryCanonicalWalletReceivableCollectorLease(ctx, 2*interval)
	if err != nil || !held {
		return
	}
	notBefore := b.clock().Add(-time.Duration(b.retentionDays()) * 24 * time.Hour)
	maxRedrives := b.receivableRedriveMaxAttempts()
	candidates, err := b.outbox.ListReceivableRedriveCandidates(ctx, notBefore, maxRedrives, walletReceivableRedriveBatch)
	if err != nil {
		slog.Warn("canonical wallet receivable redrive candidate listing failed", "error", err)
		return
	}
	for _, c := range candidates {
		if c.RedriveCount >= maxRedrives {
			canonicalWalletBridgeMetrics.receivableRedriveExhausted.Add(1)
			continue
		}
		if err := b.outbox.RequeueDeadLetter(ctx, c.ID, b.workerID); err != nil {
			slog.Warn("canonical wallet receivable redrive requeue failed", "event_id", c.EventID, "error", err)
			continue
		}
		canonicalWalletBridgeMetrics.receivableRedriven.Add(1)
		slog.Info("canonical wallet receivable re-driven (the user may have funded)",
			"event_id", c.EventID, "platform_user_id", c.PlatformUserID, "amount_units", c.AmountUnits, "redrive_count", c.RedriveCount+1)
	}
}

// receivableRedriveIntervalSeconds defaults the cadence for the struct
// literals the tests build (viper sets the default in production).
func (b *CanonicalWalletBridge) receivableRedriveIntervalSeconds() int {
	if b == nil || b.cfg.ReceivableRedriveIntervalSeconds <= 0 {
		return 300
	}
	return b.cfg.ReceivableRedriveIntervalSeconds
}

// receivableRedriveMaxAttempts defaults the re-drive bound the same way.
func (b *CanonicalWalletBridge) receivableRedriveMaxAttempts() int {
	if b == nil || b.cfg.ReceivableRedriveMaxAttempts <= 0 {
		return 30
	}
	return b.cfg.ReceivableRedriveMaxAttempts
}

// retentionDays defaults the retention floor (Task 3's pruners share it).
func (b *CanonicalWalletBridge) retentionDays() int {
	if b == nil || b.cfg.RetentionDays <= 0 {
		return 45
	}
	return b.cfg.RetentionDays
}

func (b *CanonicalWalletBridge) runOutboxDispatcher() {
	if b.outbox == nil || b.outboxDB == nil {
		return // several of this file's own tests construct a bridge with a nil outbox (they don't exercise ObserveSettlement) — starting the ticker loop unconditionally would eventually dereference it. "Start only when BOTH are non-nil" is enforced in this one place.
	}
	perAttempt := time.Duration(b.cfg.RequestTimeoutMS) * time.Millisecond
	// staleAfter must exceed the worst case for the LAST event in a batch:
	// it waits behind (batch-1) predecessors, each bounded by perAttempt,
	// then takes up to perAttempt itself — i.e. batch*perAttempt — plus
	// margin for the claim/reclaim round-trips themselves. 2x that bound is
	// the margin used here.
	staleAfter := 2 * time.Duration(walletOutboxDispatchBatch) * perAttempt
	ticker := time.NewTicker(perAttempt)
	defer ticker.Stop()
	ticks := int64(0)
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
		}
		ticks++
		// §11.4: the receivable gauge refreshes every 100th tick (~30 s at
		// the default 300 ms RequestTimeoutMS) — per-tick would be ~3
		// aggregates/s. The lag is a Phase 4 reconciliation input, not an
		// alert source.
		if ticks%100 == 0 {
			gaugeCtx, cancelGauge := context.WithTimeout(context.Background(), perAttempt)
			b.refreshReceivableGauge(gaugeCtx)
			cancelGauge()
		}
		// Reclaim stale in_flight rows before claiming fresh pending ones,
		// so a row a crashed dispatcher instance abandoned mid-delivery
		// becomes eligible for ClaimPendingOutboxEvents again in this same
		// tick rather than staying stuck until some future tick happens to
		// run this first.
		reclaimCtx, cancelReclaim := context.WithTimeout(context.Background(), perAttempt)
		if reclaimed, err := b.outbox.ReclaimStaleInFlightEvents(reclaimCtx, staleAfter); err != nil {
			slog.Warn("canonical wallet outbox stale-claim reclaim failed", "error", err)
		} else if reclaimed > 0 {
			slog.Warn("canonical wallet outbox reclaimed stale in_flight events", "count", reclaimed)
		}
		cancelReclaim()

		claimCtx, cancelClaim := context.WithTimeout(context.Background(), perAttempt)
		events, err := b.outbox.ClaimPendingOutboxEvents(claimCtx, b.workerID, walletOutboxDispatchBatch)
		cancelClaim()
		if err != nil {
			slog.Warn("canonical wallet outbox claim failed", "error", err)
			continue
		}
		for _, e := range events {
			// One fresh context PER EVENT — a slow delivery must not consume
			// the time budget of the events queued behind it.
			eventCtx, cancelEvent := context.WithTimeout(context.Background(), perAttempt)
			b.deliverOutboxEvent(eventCtx, e)
			cancelEvent()
		}
	}
}

func (b *CanonicalWalletBridge) deliverOutboxEvent(ctx context.Context, e CanonicalWalletOutboxEvent) {
	event := CanonicalWalletSettlementEvent{
		EventID: e.EventID, GatewayRequestID: e.GatewayRequestID, PlatformUserID: e.PlatformUserID,
		LeaseID: e.LeaseID, Currency: e.Currency, AmountUnits: e.AmountUnits,
		LocalBalanceAfterUnits: e.LocalBalanceAfterUnits, OccurredAt: e.OccurredAt,
	}
	// (0) §11.3: a pending release is owed to the bound lease — pay it
	// BEFORE anything else reserves again. The partial form is gated on its
	// own release marker, so a replay answers {7} (counted) and writes
	// nothing; only a transport/store ERROR defers the whole delivery
	// (never reserve with a release owed).
	if e.PendingReleaseUnits != nil {
		released, rerr := b.store.ReleaseCanonicalWalletReservation(ctx, e.PlatformUserID, e.LeaseID, e.EventID, *e.PendingReleaseUnits, false)
		if rerr != nil {
			slog.Warn("canonical wallet pending release failed", "event_id", e.EventID, "lease_id", e.LeaseID, "error", rerr)
			_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
			return
		}
		if !released {
			canonicalWalletBridgeMetrics.pendingReleaseReplayed.Add(1)
		}
		if err := b.outbox.ClearPendingRelease(ctx, e.ID); err != nil {
			slog.Warn("canonical wallet pending release clear failed", "event_id", e.EventID, "error", err)
			_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
			return
		}
	}
	lease, err := b.resolveOutboxEventLease(ctx, e)
	if err != nil {
		canonicalWalletBridgeMetrics.leaseAcquireError.Add(1)
		if errors.Is(err, ErrCanonicalWalletBalanceShortfall) {
			// Terminal (§9.3): the control plane clamped below the amount;
			// no backoff changes the balance. Dead-letter now, reason
			// balance_shortfall (persisted by migration 212). §11.4: that
			// row is the receivable — parent_event_id/split_depth tell
			// Phase 4 which usage it belongs to.
			canonicalWalletBridgeMetrics.deadLetterBalanceShortfall.Add(1)
			slog.Warn("canonical wallet outbox dead-lettered balance_shortfall (the receivable)",
				"event_id", e.EventID, "parent_event_id", e.ParentEventID, "split_depth", e.SplitDepth, "amount_units", e.AmountUnits)
			_ = b.outbox.MarkOutboxEventDeadLetter(ctx, e.ID, b.workerID, "balance_shortfall")
			return
		}
		// §11.5 classification: a terminal control-plane answer (a wire bug
		// the backoff cannot fix) dead-letters immediately with its named
		// reason; everything else below stays transient.
		if reason, terminal := classifyControlPlaneError(err); terminal {
			b.markTerminalClassification(ctx, e.ID, reason)
			return
		}
		// Transport/store errors, the transient lease_contention, the local
		// under-grant guard (ErrCanonicalWalletLeaseGrantBelowAmount) and a
		// control plane without the ensure route
		// (ErrCanonicalWalletControlPlaneIncompatible) all retry on the
		// backoff as today (§9.3). ErrCanonicalWalletLeaseCapReached cannot
		// reach here (the dispatcher ensures with purpose = settle); if it
		// ever did, it would retry as today.
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
		return
	}
	// (2) §11.3 proactive split: a FRESH settle-purpose lease (unbound row)
	// granted below the amount — the settle purpose installs it (above) —
	// splits before reserving: H = the lease's remaining, the remainder row
	// carries A − H. A bound retry is NOT split here: its reservation
	// already exists and ShipAny decides; a refusal over-capture is handled
	// reactively below.
	if e.LeaseID == "" && lease.RemainingUnits() > 0 && lease.RemainingUnits() < e.AmountUnits {
		b.splitOutboxEvent(ctx, e, lease, lease.RemainingUnits(), false /* reserved: nothing reserved yet */)
		return
	}
	// Bind BEFORE reserving, never after. Binding afterwards leaves a crash
	// window in which a reservation exists in Redis but the row does not know
	// its lease — the retry then reserves against a different lease and
	// conflicts forever.
	if lease.LeaseID != e.LeaseID {
		if err := b.outbox.BindOutboxEventLease(ctx, e.ID, b.workerID, lease.LeaseID); err != nil {
			if errors.Is(err, ErrCanonicalWalletOutboxClaimLost) {
				// Another dispatcher owns this row now and will deliver it.
				// Not this call's outcome to report — do NOT mark it failed,
				// that would burn an attempt against the other worker's claim.
				return
			}
			canonicalWalletBridgeMetrics.leaseBindError.Add(1)
			_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
			return
		}
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, b.clock())
	if err != nil {
		// A bound lease that has since gone missing, expired, or run out can
		// never accept this event. The Lua script checks the event's
		// reservation marker BEFORE it checks the budget, so any of these
		// three outcomes proves no marker for this event exists on that lease
		// — releasing the binding is safe, and lets the next attempt acquire
		// a fresh lease instead of retrying the same dead one until
		// dead-letter. A cross-lease CONFLICT is deliberately excluded: there
		// the marker does exist and points elsewhere, so the binding must
		// stand.
		if canonicalWalletLeaseBindingIsStale(err) {
			_ = b.outbox.BindOutboxEventLease(ctx, e.ID, b.workerID, "")
		}
		canonicalWalletBridgeMetrics.reserveError.Add(1)
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
		return
	}
	canonicalWalletBridgeMetrics.reserveOK.Add(1)
	event.LeaseID = reservation.Lease.LeaseID
	result, err := b.control.SubmitSettlement(ctx, event)
	if err != nil {
		canonicalWalletBridgeMetrics.settlementError.Add(1)
		// §11.3 reactive split: the bound lease's headroom is below the
		// amount — the refusal carries the headroom (absent → H = 0). The
		// split happens BEFORE the generic classification: over-capture is a
		// mechanism, not a dead-letter.
		var se *canonicalWalletStatusError
		if errors.As(err, &se) && se.Reason == "lease_over_capture" {
			headroomUnits := int64(0)
			if se.HeadroomUnits != nil {
				headroomUnits = *se.HeadroomUnits
			}
			b.splitOutboxEvent(ctx, e, lease, headroomUnits, true /* reserved: A is on the lease */)
			return
		}
		// §11.4 late capture: ShipAny closed the lease — the same disposition
		// canonicalWalletLeaseBindingIsStale gives Missing/Expired/Exhausted,
		// under its own check because the 409 arrives from SubmitSettlement.
		// Release the reservation IN FULL (the full form: the row is
		// unbound, nothing more will be captured there, and the marker goes
		// with the release so a later drain reads an exact identity), unbind,
		// and SEAL the closed lease — the cached hash still covers the amount
		// by the gateway's own figures (the server's refusal is invisible
		// here), and without the seal the next ensure would answer from the
		// cache and loop until attempts_exhausted. The next delivery then
		// ensures a settle-purpose lease from the balance and captures there;
		// if the balance cannot cover it the settle ensure refuses
		// insufficient_balance → the receivable above.
		if errors.As(err, &se) && se.Reason == "lease_not_capturable" {
			if _, rerr := b.store.ReleaseCanonicalWalletReservation(ctx, event.PlatformUserID, event.LeaseID, event.EventID, event.AmountUnits, true /* dropMarker */); rerr != nil {
				slog.Warn("canonical wallet late-capture release failed", "event_id", event.EventID, "lease_id", event.LeaseID, "error", rerr)
			}
			if bindErr := b.outbox.BindOutboxEventLease(ctx, e.ID, b.workerID, ""); bindErr != nil && !errors.Is(bindErr, ErrCanonicalWalletOutboxClaimLost) {
				slog.Warn("canonical wallet late-capture unbind failed", "event_id", event.EventID, "error", bindErr)
			}
			if _, _, sealErr := b.store.SealCanonicalWalletLease(ctx, event.PlatformUserID, event.LeaseID); sealErr != nil && !errors.Is(sealErr, ErrCanonicalWalletLeaseMissing) {
				slog.Warn("canonical wallet late-capture seal failed", "event_id", event.EventID, "lease_id", event.LeaseID, "error", sealErr)
			}
			canonicalWalletBridgeMetrics.lateCaptureRetargeted.Add(1)
			_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
			return
		}
		// §11.5 classification on the settlements branch too: a terminal
		// answer dead-letters with its named reason (lease_not_capturable is
		// handled by its own mechanism in Task 6 and never reaches this
		// classifier); 401/5xx/transport stay on the backoff.
		if reason, terminal := classifyControlPlaneError(err); terminal {
			b.markTerminalClassification(ctx, e.ID, reason)
			return
		}
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
		return
	}
	canonicalWalletBridgeMetrics.settlementOK.Add(1)
	// §11.2 named_lease_id: a duplicate whose redelivery named a lease other
	// than the lease the EVENT captured on means this retry reserved on the
	// named lease at delivery while ShipAny captured on the original —
	// release that reservation in the FULL form (dropMarker: the event was
	// captured elsewhere, nothing more will be captured here, and the DEL is
	// the idempotency). A null named_lease_id releases nothing.
	if result.Duplicate && result.NamedLeaseID != "" && result.NamedLeaseID != result.EventLeaseID {
		if _, rerr := b.store.ReleaseCanonicalWalletReservation(ctx, event.PlatformUserID, result.NamedLeaseID, event.EventID, event.AmountUnits, true); rerr != nil {
			slog.Warn("canonical wallet named-lease release failed", "event_id", event.EventID, "named_lease_id", result.NamedLeaseID, "error", rerr)
		} else {
			canonicalWalletBridgeMetrics.namedLeaseReleased.Add(1)
		}
	}
	// Balance-mismatch (drift) detection, Phase 3.5's quantum (§11.2): a
	// delta within one credit (1,000,000 units — the v2 route's own
	// units→credits ceiling) is the float-derived local balance's rounding,
	// not a mismatch; beyond it both figures are logged. A canonical figure
	// BELOW the local one by more than the quantum is additionally counted
	// under its own key — grant-batch expiry steps the canonical figure down
	// with no gateway counterpart.
	attrs := []any{"event_id", event.EventID, "lease_id", event.LeaseID, "duplicate", result.Duplicate, "amount_units", event.AmountUnits}
	if result.CanonicalBalanceUnits != nil && event.LocalBalanceAfterUnits != nil {
		delta := *result.CanonicalBalanceUnits - *event.LocalBalanceAfterUnits
		if delta > canonicalWalletDriftQuantumUnits || delta < -canonicalWalletDriftQuantumUnits {
			canonicalWalletBridgeMetrics.balanceMismatch.Add(1)
			attrs = append(attrs, "balance_delta_units", delta, "canonical_balance_units", *result.CanonicalBalanceUnits, "local_balance_after_units", *event.LocalBalanceAfterUnits)
			if delta < -canonicalWalletDriftQuantumUnits {
				canonicalWalletBridgeMetrics.balanceBehindLocal.Add(1)
			}
		}
	}
	slog.Info("canonical wallet settlement delivered", attrs...)
	_ = b.outbox.MarkOutboxEventDelivered(ctx, e.ID, b.workerID)
}

// canonicalWalletDriftQuantumUnits (§11.2): one credit — the v2 route's own
// units→credits rounding quantum. Deltas within it are the local balance's
// float rounding; beyond it the two ledgers genuinely disagree.
const canonicalWalletDriftQuantumUnits = 1_000_000

// markTerminalClassification (§11.5) resolves a row this worker owns
// straight to dead_letter with the classifier's named reason and counts it.
func (b *CanonicalWalletBridge) markTerminalClassification(ctx context.Context, id int64, reason string) {
	if err := b.outbox.MarkOutboxEventDeadLetter(ctx, id, b.workerID, reason); err != nil {
		slog.Warn("canonical wallet outbox terminal classification failed", "id", id, "reason", reason, "error", err)
		return
	}
	switch reason {
	case "payload_conflict":
		canonicalWalletBridgeMetrics.deadLetterPayloadConflict.Add(1)
	default:
		canonicalWalletBridgeMetrics.deadLetterContractViolation.Add(1)
	}
}

// splitOutboxEvent (§11.3) durably splits row e at headroomUnits: the
// remainder (amount − headroom) becomes its own pending row; the parent's
// amount becomes headroomUnits and returns to pending (delivered on
// split_full when headroomUnits = 0). reserved records whether the gateway
// already reserved the full amount on the lease — then the remainder is
// OWED back and the partial release runs immediately (the marker survives
// for the parent's {5} redelivery); a failed release leaves
// pending_release_units set and the next delivery pays it first. Depth is
// bounded at 8: a ninth split dead-letters split_exhausted.
func (b *CanonicalWalletBridge) splitOutboxEvent(ctx context.Context, e CanonicalWalletOutboxEvent, lease *CanonicalWalletLease, headroomUnits int64, reserved bool) {
	if e.SplitDepth >= 8 {
		canonicalWalletBridgeMetrics.settlementSplitExhausted.Add(1)
		_ = b.outbox.MarkOutboxEventDeadLetter(ctx, e.ID, b.workerID, "split_exhausted")
		return
	}
	remainderID := e.EventID + ":r" + strconv.Itoa(e.SplitDepth+1)
	remainderUnits, err := b.outbox.SplitOutboxEvent(ctx, e.ID, b.workerID, headroomUnits, remainderID, reserved)
	if err != nil {
		slog.Warn("canonical wallet outbox split failed", "event_id", e.EventID, "error", err)
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, b.clock())
		return
	}
	if reserved {
		// The partial form: the parent's marker must survive (its redelivery
		// answers {5} and captures H), so only the remainder goes back. A
		// failure leaves the debt recorded on the row — §11.3's repair.
		if _, rerr := b.store.ReleaseCanonicalWalletReservation(ctx, e.PlatformUserID, lease.LeaseID, e.EventID, remainderUnits, false); rerr != nil {
			slog.Warn("canonical wallet split release failed — the next delivery pays it first", "event_id", e.EventID, "lease_id", lease.LeaseID, "error", rerr)
			return
		}
		if cerr := b.outbox.ClearPendingRelease(ctx, e.ID); cerr != nil {
			slog.Warn("canonical wallet split release clear failed — the next delivery replays it through the release marker", "event_id", e.EventID, "error", cerr)
			return
		}
		// §13.2.8 — the same seal as §11.4's late capture: consumed = budget
		// so the hash never covers again, the current pointer dropped if it
		// names this lease, the hash kept so the parent's marker still
		// resolves. Without it the remainder's ensureLease(settle, "") would
		// return this lease as covering (RemainingUnits() ignores
		// released_units) and burn split depth on the one that refused. A
		// seal failure is logged, not fatal — it degrades to the pre-existing
		// depth-burn behaviour.
		if _, _, sealErr := b.store.SealCanonicalWalletLease(ctx, e.PlatformUserID, lease.LeaseID); sealErr != nil && !errors.Is(sealErr, ErrCanonicalWalletLeaseMissing) {
			slog.Warn("canonical wallet split seal failed — the remainder may rebind to the refusing lease", "event_id", e.EventID, "lease_id", lease.LeaseID, "error", sealErr)
		} else {
			canonicalWalletBridgeMetrics.settlementSplitSealed.Add(1)
		}
	}
	if headroomUnits == 0 {
		canonicalWalletBridgeMetrics.settlementSplitFull.Add(1)
		slog.Info("canonical wallet settlement split_full", "event_id", e.EventID, "remainder_event_id", remainderID, "remainder_units", remainderUnits)
	} else {
		canonicalWalletBridgeMetrics.settlementSplit.Add(1)
		slog.Info("canonical wallet settlement split", "event_id", e.EventID, "headroom_units", headroomUnits, "remainder_event_id", remainderID, "remainder_units", remainderUnits)
	}
}

// canonicalWalletLeaseBindingIsStale reports whether a reservation failure
// against an ALREADY-BOUND lease proves that binding can never succeed, so it
// must be released. See the call site for why conflict is excluded.
func canonicalWalletLeaseBindingIsStale(err error) bool {
	return errors.Is(err, ErrCanonicalWalletLeaseMissing) ||
		errors.Is(err, ErrCanonicalWalletLeaseExpired) ||
		errors.Is(err, ErrCanonicalWalletLeaseExhausted)
}

// resolveOutboxEventLease returns the lease this event's reservation must be
// anchored to: the lease already bound to the row if it is still usable,
// otherwise a freshly acquired one.
//
// Falling back to a fresh lease when the bound one is gone or expired is
// safe, and this is the reason: the reservation marker is written with
// `SET KEYS[2] <lease_id>` followed by `PEXPIREAT KEYS[2] expires_at` — the
// SAME absolute deadline the lease hash itself carries. Both keys expire
// against one clock, Redis's, so a dead lease implies a dead marker and
// there is nothing left for a fresh lease's reservation to collide with.
//
// This was not always true, and the history is worth keeping: the marker
// used to get a RELATIVE `PX expires_at - gateway_now`, which Redis applied
// at its own execution instant. That shifted the marker's deadline by
// `redis_now - gateway_now` — round-trip latency plus clock offset — so a
// marker COULD outlive its lease, and did whenever that shift was positive,
// which is the ordinary case. Inside such a window this fallback rebound the
// event to a fresh lease while the old marker still pointed at the previous
// one, so every retry hit a cross-lease conflict until the marker finally
// expired. Retries recovered on their own after that; the row dead-lettered
// only when the attempt budget ran out first — which is exactly what made it
// a money bug rather than a delay. Do not reintroduce a relative TTL here.
func (b *CanonicalWalletBridge) resolveOutboxEventLease(ctx context.Context, e CanonicalWalletOutboxEvent) (*CanonicalWalletLease, error) {
	if e.LeaseID == "" {
		return b.ensureLease(ctx, e.PlatformUserID, e.Currency, e.AmountUnits, canonicalWalletLeasePurposeSettle, "")
	}
	if b.store == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, e.PlatformUserID, e.LeaseID)
	if err != nil && !errors.Is(err, ErrCanonicalWalletLeaseMissing) {
		// A real transport failure — do NOT silently fall through to a new
		// lease, or a Redis blip would rebind an event that still has a live
		// marker somewhere.
		return nil, err
	}
	if errors.Is(err, ErrCanonicalWalletLeaseMissing) {
		// Phase 3.4 (redesign §4): a MISSING bound lease recovers through
		// prefer_lease_id instead of failing the delivery. Inside ensureLease
		// this tries GetByID once more — a second harmless miss — then sends
		// prefer_lease_id with no drain.
		return b.ensureLease(ctx, e.PlatformUserID, e.Currency, e.AmountUnits, canonicalWalletLeasePurposeSettle, e.LeaseID)
	}
	if lease != nil && lease.Currency == e.Currency && !b.leaseExpiredAt(lease, b.clock()) {
		return lease, nil
	}
	return b.ensureLease(ctx, e.PlatformUserID, e.Currency, e.AmountUnits, canonicalWalletLeasePurposeSettle, "")
}

// ensureLease (Phase 3.4, redesign §4) is cache-then-ensure. It reads the
// cached lease — the user's current lease, or on the explicit-id branch the
// lease named by preferLeaseID — and returns it when it covers the ask
// (currency, expiry beyond the skew margin, remaining ≥ amount). On the
// explicit-id branch a lease that EXISTS is returned as-is regardless of
// cover: the caller is a retry whose reservation marker lives on that lease,
// and the reserve script's duplicate check (which runs before its budget
// check) resolves it; sealing or replacing it here would send the retry to a
// different lease and manufacture a cross-lease conflict. §4's only change to
// that branch is "on a miss it calls ensure with prefer_lease_id". Otherwise
// the non-covering lease in hand is SEALED (§3.3) and sent as the drained
// entry with the pre-seal consumed, and the control plane's ensure answers
// reused or issued under its own transaction order — no window idempotency
// key, no epoch. Refusals arrive as the named errors; the grant is installed
// under the existing monotone rule with consumed = budget − headroom_units.
// No hold is armed here.
func (b *CanonicalWalletBridge) ensureLease(ctx context.Context, platformUserID, currency string, amountUnits int64, purpose canonicalWalletLeasePurpose, preferLeaseID string) (*CanonicalWalletLease, error) {
	if b.store == nil || b.control == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	now := b.clock()
	var cached *CanonicalWalletLease
	var err error
	if preferLeaseID != "" {
		cached, err = b.store.GetCanonicalWalletLeaseByID(ctx, platformUserID, preferLeaseID)
		if err == nil && cached != nil {
			return cached, nil // explicit-id branch: the lease exists → the reserve script decides
		}
	} else {
		cached, err = b.store.GetCanonicalWalletLease(ctx, platformUserID)
	}
	if err != nil && !errors.Is(err, ErrCanonicalWalletLeaseMissing) {
		return nil, err // a Redis transport failure is not a miss
	}
	if err != nil {
		cached = nil
	}
	if b.leaseCovers(cached, currency, amountUnits, now) {
		return cached, nil
	}
	request := canonicalWalletEnsureRequest{
		PlatformUserID: strings.TrimSpace(platformUserID), Currency: currency, Purpose: string(purpose),
		MinHeadroom: newCanonicalWalletAmountObject(amountUnits), RequestedBudget: newCanonicalWalletAmountObject(b.cfg.LeaseBudgetUnits), RequestedTTLSeconds: b.cfg.LeaseTTLSeconds,
		PreferLeaseID: preferLeaseID, CallerSlotTTLSeconds: b.callerSlotTTLSeconds,
	}
	if cached != nil {
		preSealConsumed, preSealReleased, sealErr := b.store.SealCanonicalWalletLease(ctx, platformUserID, cached.LeaseID)
		if sealErr != nil && !errors.Is(sealErr, ErrCanonicalWalletLeaseMissing) {
			return nil, sealErr
		}
		if sealErr == nil {
			// §10.2: the drain identity is consumed == captured + released —
			// gateway_released rides the wire only when releases exist, so a
			// 3.4a-S control plane that ignores the field sees the 3.4a wire.
			entry := canonicalWalletDrainEntry{LeaseID: cached.LeaseID, GatewayConsumed: newCanonicalWalletAmountObject(preSealConsumed)}
			if preSealReleased > 0 {
				released := newCanonicalWalletAmountObject(preSealReleased)
				entry.GatewayReleased = &released
			}
			request.Drained = []canonicalWalletDrainEntry{entry}
		}
	}
	result, err := b.control.EnsureLease(ctx, request)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	lease := result.Lease
	if b.leaseExpiredAt(&lease, now) {
		canonicalWalletBridgeMetrics.leaseGrantExpired.Add(1)
		return nil, fmt.Errorf("%w: lease %s expired at %s", ErrCanonicalWalletLeaseExpired, lease.LeaseID, lease.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	// §11.3 (MAJOR-2 of the §11 review): the under-grant guard is
	// AUTHORIZE-ONLY. On the settle purpose the under-granted lease is
	// installed and returned like any other — the dispatcher, holding the
	// lease and its RemainingUnits, splits before reserving, which is what
	// makes the chain terminate; refusing here would dead-letter a real
	// settlement larger than one lease's maximum budget (§9.3's transient
	// sentinel retried to attempts_exhausted — real usage lost).
	if purpose == canonicalWalletLeasePurposeAuthorize && lease.RemainingUnits() < amountUnits {
		// §9.3: this local guard is TRANSIENT and carries its own sentinel,
		// distinct from the server's terminal insufficient_balance refusal.
		// Unreachable against a §3-conformant server (ensure never answers
		// below min_headroom_units); retried on the outbox backoff.
		canonicalWalletBridgeMetrics.leaseGrantBelowAmount.Add(1)
		return nil, fmt.Errorf("%w: granted %d units, %d required", ErrCanonicalWalletLeaseGrantBelowAmount, lease.RemainingUnits(), amountUnits)
	}
	if err := b.store.InstallCanonicalWalletLease(ctx, lease); err != nil {
		return nil, err
	}
	canonicalWalletBridgeMetrics.leaseAcquireOK.Add(1)
	if result.Outcome == "issued" {
		canonicalWalletBridgeMetrics.leaseIssued.Add(1)
	} else {
		canonicalWalletBridgeMetrics.leaseReused.Add(1)
	}
	return &lease, nil
}

func CanonicalWalletSettlementEventID(requestID, platformUserID, currency string) string {
	raw := fmt.Sprintf("v1|%s|%s|%s", strings.TrimSpace(requestID), strings.TrimSpace(platformUserID), NormalizeUserBillingCurrency(currency))
	sum := sha256.Sum256([]byte(raw))
	return "gwusg_" + hex.EncodeToString(sum[:])
}

// canonicalWalletUnitsFromCNY converts a CNY float64 to cny-e8-v1 units
// (1 CNY = 100,000,000 units — canonicalWalletUnitsPerCNY from
// canonical_wallet_units.go; do NOT redeclare it here). Rejects non-finite
// and negative inputs, and rejects any scaled value at or beyond int64's
// ceiling BEFORE converting — a bare int64(...) cast of an out-of-range
// float64 is implementation-defined behavior in Go, not a panic.
func canonicalWalletUnitsFromCNY(amount float64) (int64, error) {
	if amount < 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, errors.New("canonical wallet amount must be a finite, non-negative number")
	}
	scaled := amount * canonicalWalletUnitsPerCNY
	if scaled >= math.MaxInt64 {
		return 0, ErrCanonicalWalletUnitsOverflow
	}
	return int64(math.Round(scaled)), nil
}

// RequireCNYBillingCurrency is the STRICT counterpart to
// NormalizeUserBillingCurrency, exported so other packages (repository,
// same as CanonicalWalletLeaseStore's other cross-package uses) and other
// call sites in this package can reject an invalid currency outright
// instead of silently treating it as CNY. NormalizeUserBillingCurrency
// coerces ANY unrecognized value to CNY (confirmed by reading currency.go's
// normalizeBillingCurrencyOrDefault), so a check performed AFTER that
// coercion can never observe an invalid currency — every genuine
// admission/reservation/settlement boundary must reject BEFORE coercing.
func RequireCNYBillingCurrency(value string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	if normalized != CurrencyCNY {
		return "", fmt.Errorf("cny-e8-v1 requires CNY, got %q", value)
	}
	return normalized, nil
}

// releaseHoldZeroCost (§11.7) releases an armed hold at a zero-cost abort
// point — a no-op on anything already resolved ({7}) or never armed ({1}),
// and on holds-off / token-less calls. Nil-safe: the observer's first guard
// has no bridge or user to release with.
func (b *CanonicalWalletBridge) releaseHoldZeroCost(ctx context.Context, platformUserID, authorizationID string) {
	if b == nil || !b.HoldsEnabled() || platformUserID == "" || authorizationID == "" {
		return
	}
	if _, err := b.store.ReleaseCanonicalWalletHold(ctx, platformUserID, authorizationID, "released", "zero_cost"); err != nil {
		if !errors.Is(err, ErrCanonicalWalletHoldMissing) && !isHoldNotArmed(err) {
			canonicalWalletBridgeMetrics.holdReleaseError.Add(1)
			slog.Warn("canonical wallet zero-cost hold release failed", "authorization_id", authorizationID, "error", err)
		}
		return
	}
	canonicalWalletBridgeMetrics.holdReleasedZeroCost.Add(1)
}

// observeCanonicalWalletSettlement is the shared write path of the HTTP/WS
// settlement callers (openai_gateway_usage.go / gateway_usage_billing.go).
// billingSnapshotID (Phase 4.2-G) is the frozen snapshot those callers hold
// (input.BillingSnapshot.ID — Task 0's scope finding); "" writes NULL.
func observeCanonicalWalletSettlement(bridge *CanonicalWalletBridge, requestID string, user *User, cost *CostBreakdown, subscriptionBilling, billingApplied bool, billingResult *UsageBillingApplyResult, authorizationToken, authorizationID, billingSnapshotID string) bool {
	if bridge == nil || user == nil || cost == nil {
		return false
	}
	// §11.7's zero-cost abort points: every early return below is an abort
	// point that RELEASES an armed hold (state = released, class =
	// zero_cost) — the reaper was the backstop in 3.4b; now that the release
	// primitive exists the hold does not outlive the request by one grace.
	// Holds off writes nothing (3.4a byte-for-byte, §10.1).
	releaseZeroCost := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(bridge.cfg.RequestTimeoutMS)*time.Millisecond)
		defer cancel()
		bridge.releaseHoldZeroCost(ctx, user.PlatformUserID, authorizationID)
		return false
	}
	if subscriptionBilling || !billingApplied || cost.ActualCost <= 0 {
		return releaseZeroCost()
	}
	amountUnits, err := canonicalWalletUnitsFromCNY(cost.ActualCost)
	if err != nil || amountUnits <= 0 {
		return releaseZeroCost()
	}
	localBalance := user.Balance - cost.ActualCost
	if billingResult != nil && billingResult.NewBalance != nil {
		localBalance = *billingResult.NewBalance
	}
	localBalanceAfterUnits, balanceErr := canonicalWalletUnitsFromCNY(localBalance)
	var localBalanceAfterPtr *int64
	if balanceErr == nil {
		localBalanceAfterPtr = &localBalanceAfterUnits
	}
	// Pass the raw BillingCurrency through and let ObserveSettlement's own
	// RequireCNYBillingCurrency be the single authoritative boundary —
	// coercing here first would force every value to "CNY" before that
	// check ever runs, so it could never reject a real non-CNY user.
	return bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: requestID, PlatformUserID: user.PlatformUserID, Currency: user.BillingCurrency,
		AmountUnits: amountUnits, LocalBalanceAfterUnits: localBalanceAfterPtr, OccurredAt: time.Now().UTC(),
		AuthorizationToken: authorizationToken, AuthorizationID: authorizationID,
		BillingSnapshotID: strings.TrimSpace(billingSnapshotID),
	})
}
