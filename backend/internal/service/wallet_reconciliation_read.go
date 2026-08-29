package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
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
	BillingCurrency   string
	BillingSnapshotID string
	BillingFX         *string
	Status            string
	EstimatedUnits    int64
	SettlementEventID string
	Windows           []LiveWindow
	CreatedAt         time.Time
}

// WalletReconciliationOutboxRowCap is the wire's outbox page size: on
// truncation the server returns rows up to the cap ordered by id and sets
// NextAfterID to the last returned row's id; the caller re-calls with the
// same window and after_id (round-1 MAJOR-4, round-2 MAJOR-2).
const WalletReconciliationOutboxRowCap = 5000

// WalletReconciliationOutboxRead is the outbox's read surface the
// reconciliation summary composes — implemented by
// repository.WalletOutboxStore (the service package cannot import
// repository; the bridge's CanonicalWalletOutboxStore carries the same
// dynamic type).
type WalletReconciliationOutboxRead interface {
	ListOutboxEventsByUser(ctx context.Context, platformUserID string, since, until time.Time, afterID int64, limit int) ([]CanonicalWalletOutboxEvent, bool, error)
	DeliveredWatermark(ctx context.Context) (OutboxWatermark, error)
	SumDeadLetterUnits(ctx context.Context, reason string) (int64, error)
}

// WalletReconciliationLeaseView is the gateway cache's canonical-wallet
// read surface used for the summary's DIAGNOSTIC Redis view — never a leg
// (test 63 proved the loss recoverable). Implemented by the same gateway
// cache that backs CanonicalWalletLeaseStore.
type WalletReconciliationLeaseView interface {
	GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*CanonicalWalletLease, error)
	ListCanonicalWalletHolds(ctx context.Context, platformUserID string, limit int) ([]string, error)
}

type WalletReconciliationWindow struct {
	Since time.Time
	Until time.Time
}

type WalletReconciliationOutboxRow struct {
	EventID             string
	LeaseID             string
	GatewayRequestID    string
	Currency            string
	AmountUnits         int64
	Status              string
	AttemptCount        int
	DeadLetterReason    string
	ParentEventID       string
	SplitDepth          int
	PendingReleaseUnits *int64
	AuthorizationID     string
	OccurredAt          time.Time
	DeliveredAt         *time.Time
	// Phase 4.2-G (4.1-G's hand-on): the row's own billing snapshot and
	// its fx rate (the Live rows' LEFT JOIN, applied to the outbox) —
	// BillingSnapshotID "" and BillingFX nil are the leg-4 unverified
	// tail (pre-4.2, token-less, off-mode), never a dropped record.
	BillingSnapshotID string
	BillingFX         *string
	// RedriveCount is the receivable collector's re-drive bound (Task 2)
	// — 4.1-S's rule: a window is not final while any of its rows is
	// dead_letter balance_shortfall or pending with redrive_count > 0.
	RedriveCount int
}

// WalletReconciliationReceivable is the receivable figure derived from a
// user's OWN dead-lettered outbox rows (§11.3): balance_shortfall (the
// uncollectable-until-funded class) and split_exhausted (explained; an
// early signal the lease budget no longer covers the model set), plus the
// contributing row count.
type WalletReconciliationReceivable struct {
	BalanceShortfallUnits int64
	SplitExhaustedUnits   int64
	Rows                  int
}

type WalletReconciliationHoldRow struct {
	AuthorizationID   string
	LeaseID           string
	HeldUnits         int64
	Class             string
	ArmedAt           time.Time
	ClassifiedAt      time.Time
	Resolution        *string
	ResolvedAt        *time.Time
	SettlementEventID *string
}

type WalletReconciliationLiveWindowRow struct {
	Seq          int
	LeaseID      string
	SettledUnits int64
	PendingUnits int64
	OpenedAtMS   int64
	// EventID is DERIVED (redesign §15.3 leg 3), never stored: window 1's
	// gateway request id is the session's bare call hash, windows ≥ 2
	// append :window:n, then CanonicalWalletSettlementEventID hashes it.
	EventID string
}

type WalletReconciliationLiveRow struct {
	CallHash          string
	AuthorizationID   string
	BillingSnapshotID string
	BillingFX         *string
	Status            string
	EstimatedUnits    int64
	SettlementEventID string
	Windows           []WalletReconciliationLiveWindowRow
}

// WalletReconciliationRedisView is the DIAGNOSTIC Redis view: available
// false means the cache could not be reached (or no store was wired) —
// never a failed request. An EMPTY keyspace is not an outage (test 63's
// recoverable shape): available stays true with a nil current lease and
// zero open holds.
type WalletReconciliationRedisView struct {
	Available      bool
	CurrentLeaseID *string
	Lease          *CanonicalWalletLease
	OpenHolds      int
}

type WalletReconciliationSummary struct {
	PlatformUserID string
	Window         WalletReconciliationWindow
	Outbox         []WalletReconciliationOutboxRow
	Receivable     WalletReconciliationReceivable
	Truncated      bool
	NextAfterID    *int64
	Holds          []WalletReconciliationHoldRow
	Live           []WalletReconciliationLiveRow
	Redis          WalletReconciliationRedisView
}

// WalletReconciliationFaults reports the durability drop fault counter
// (process-metric, resets on restart) and the process start timestamp.
type WalletReconciliationFaults struct {
	QueueDropped         int64
	Since                time.Time
	ProcessUptimeSeconds int64
}

var processStartTime = time.Now().UTC()

type WalletReconciliationWatermark struct {
	DeliveredAtMax *time.Time
	OutboxIDMax    int64
	Pending        int64
	InFlight       int64
	DeadLetter     int64
	// Receivable here is the GLOBAL figure (SumDeadLetterUnits for both
	// reasons) — unlike the summary's, which is derived from the user's
	// own returned rows (round-1 MAJOR-3).
	Receivable WalletReconciliationReceivable
	Faults     WalletReconciliationFaults
}

// WalletReconciliationReadService (Phase 4.1-G) composes the four existing
// read surfaces into the per-user Summary and the global Watermark the
// ShipAny reconciliation pulls. READ-ONLY: no method writes to any wallet
// table or to Redis.
type WalletReconciliationReadService struct {
	outbox WalletReconciliationOutboxRead
	holds  *walletHoldOutcomeStore
	live   *liveProvisionalStore
	leases WalletReconciliationLeaseView
}

// NewWalletReconciliationReadService derives the outbox read surface from
// the shared CanonicalWalletOutboxStore by type assertion (the
// requireCanonicalWalletStore pattern: repository.WalletOutboxStore is the
// only production implementation) and the lease view from the shared
// gateway cache. Under shadow a miss degrades to a logged, serviceless
// construction (every endpoint answers 503); under enforce it panics —
// the same wiring-gate shape the three bridge sites use.
func NewWalletReconciliationReadService(
	cfg *config.Config,
	db *sql.DB,
	cache GatewayCache,
	outbox CanonicalWalletOutboxStore,
) (*WalletReconciliationReadService, error) {
	s := &WalletReconciliationReadService{
		holds: &walletHoldOutcomeStore{db: db},
		live:  &liveProvisionalStore{db: db},
	}
	if reader, ok := outbox.(WalletReconciliationOutboxRead); ok {
		s.outbox = reader
	} else {
		if cfg != nil && cfg.CanonicalWallet.Mode == config.CanonicalWalletModeEnforce {
			panic("canonical_wallet.mode=enforce but the outbox store does not provide the reconciliation read surface (WalletReconciliationOutboxRead) at NewWalletReconciliationReadService — the summary would be silently absent (3.3a hand-on; redesign §14.3)")
		}
		slog.Error("canonical wallet reconciliation read service built without an outbox read surface; the summary and watermark will answer 503", "site", "NewWalletReconciliationReadService")
	}
	if store, ok := requireCanonicalWalletStore(cfg, cache, "NewWalletReconciliationReadService"); ok {
		s.leases = store
	}
	return s, nil
}

// Summary composes the per-user read model for the half-open
// [since, until) window, resuming after afterID (the BIGSERIAL cursor).
// The per-user receivable is derived from the RETURNED rows — never from
// the global SumDeadLetterUnits (round-1 MAJOR-3). A store error fails
// the request (the handler maps it to 503); Redis errors never do.
func (s *WalletReconciliationReadService) Summary(ctx context.Context, platformUserID string, since, until time.Time, afterID int64) (*WalletReconciliationSummary, error) {
	if s == nil || s.outbox == nil {
		return nil, errors.New("wallet reconciliation read service unavailable")
	}
	events, truncated, err := s.outbox.ListOutboxEventsByUser(ctx, platformUserID, since, until, afterID, WalletReconciliationOutboxRowCap)
	if err != nil {
		return nil, err
	}
	summary := &WalletReconciliationSummary{
		PlatformUserID: platformUserID,
		Window:         WalletReconciliationWindow{Since: since, Until: until},
		Outbox:         make([]WalletReconciliationOutboxRow, 0, len(events)),
		Truncated:      truncated,
	}
	for _, e := range events {
		summary.Outbox = append(summary.Outbox, WalletReconciliationOutboxRow{
			EventID: e.EventID, LeaseID: e.LeaseID, GatewayRequestID: e.GatewayRequestID, Currency: e.Currency,
			AmountUnits: e.AmountUnits, Status: e.Status, AttemptCount: e.AttemptCount, DeadLetterReason: e.DeadLetterReason,
			ParentEventID: e.ParentEventID, SplitDepth: e.SplitDepth, PendingReleaseUnits: e.PendingReleaseUnits,
			AuthorizationID: e.AuthorizationID, OccurredAt: e.OccurredAt, DeliveredAt: e.DeliveredAt,
			BillingSnapshotID: e.BillingSnapshotID, BillingFX: e.BillingFX, RedriveCount: e.RedriveCount,
		})
		if e.Status == "dead_letter" {
			switch e.DeadLetterReason {
			case "balance_shortfall":
				summary.Receivable.BalanceShortfallUnits += e.AmountUnits
				summary.Receivable.Rows++
			case "split_exhausted":
				summary.Receivable.SplitExhaustedUnits += e.AmountUnits
				summary.Receivable.Rows++
			}
		}
	}
	if truncated && len(events) > 0 {
		v := events[len(events)-1].ID
		summary.NextAfterID = &v
	}

	holdRows, err := s.holds.ListHoldOutcomesByUser(ctx, platformUserID, since, until)
	if err != nil {
		return nil, err
	}
	summary.Holds = make([]WalletReconciliationHoldRow, 0, len(holdRows))
	for _, h := range holdRows {
		summary.Holds = append(summary.Holds, WalletReconciliationHoldRow{
			AuthorizationID: h.AuthorizationID, LeaseID: h.LeaseID, HeldUnits: h.HeldUnits, Class: h.Class,
			ArmedAt: h.ArmedAt, ClassifiedAt: h.ClassifiedAt, Resolution: h.Resolution,
			ResolvedAt: h.ResolvedAt, SettlementEventID: h.SettlementEventID,
		})
	}

	liveRows, err := s.live.ListLiveProvisionalByUser(ctx, platformUserID, since, until)
	if err != nil {
		return nil, err
	}
	summary.Live = make([]WalletReconciliationLiveRow, 0, len(liveRows))
	for _, rec := range liveRows {
		row := WalletReconciliationLiveRow{
			CallHash: rec.CallHash, AuthorizationID: rec.AuthorizationID, BillingSnapshotID: rec.BillingSnapshotID,
			BillingFX: rec.BillingFX, Status: rec.Status, EstimatedUnits: rec.EstimatedUnits,
			SettlementEventID: rec.SettlementEventID,
			Windows:           make([]WalletReconciliationLiveWindowRow, 0, len(rec.Windows)),
		}
		for _, w := range rec.Windows {
			row.Windows = append(row.Windows, WalletReconciliationLiveWindowRow{
				Seq: w.WindowSeq, LeaseID: w.LeaseID, SettledUnits: w.SettledUnits, PendingUnits: w.PendingUnits,
				OpenedAtMS: w.OpenedAtMS,
				EventID:    LiveWindowSettlementEventID(rec.CallHash, w.WindowSeq, platformUserID, rec.BillingCurrency),
			})
		}
		summary.Live = append(summary.Live, row)
	}

	summary.Redis = s.redisView(ctx, platformUserID)
	return summary, nil
}

// Watermark is the global figure set: the delivered high-water mark, the
// max row id, the per-status counts, and the GLOBAL receivable via
// SumDeadLetterUnits for both reasons (round-1 MAJOR-3).
func (s *WalletReconciliationReadService) Watermark(ctx context.Context) (*WalletReconciliationWatermark, error) {
	if s == nil || s.outbox == nil {
		return nil, errors.New("wallet reconciliation read service unavailable")
	}
	wm, err := s.outbox.DeliveredWatermark(ctx)
	if err != nil {
		return nil, err
	}
	balanceShortfall, err := s.outbox.SumDeadLetterUnits(ctx, "balance_shortfall")
	if err != nil {
		return nil, err
	}
	splitExhausted, err := s.outbox.SumDeadLetterUnits(ctx, "split_exhausted")
	if err != nil {
		return nil, err
	}
	uptime := int64(time.Since(processStartTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}
	return &WalletReconciliationWatermark{
		DeliveredAtMax: wm.DeliveredAtMax, OutboxIDMax: wm.OutboxIDMax,
		Pending: wm.Pending, InFlight: wm.InFlight, DeadLetter: wm.DeadLetter,
		Receivable: WalletReconciliationReceivable{BalanceShortfallUnits: balanceShortfall, SplitExhaustedUnits: splitExhausted},
		Faults: WalletReconciliationFaults{
			QueueDropped:         canonicalWalletBridgeMetrics.queueDropped.Load(),
			Since:                processStartTime,
			ProcessUptimeSeconds: uptime,
		},
	}, nil
}

// redisView is the diagnostic-only Redis view: an unreachable cache sets
// Available=false and never fails the request; an empty keyspace (the
// current pointer absent — test 63's recoverable shape, and the state
// after a FLUSHDB) keeps Available=true with a nil lease and zero holds.
func (s *WalletReconciliationReadService) redisView(ctx context.Context, platformUserID string) WalletReconciliationRedisView {
	view := WalletReconciliationRedisView{Available: true}
	if s == nil || s.leases == nil {
		view.Available = false
		return view
	}
	lease, err := s.leases.GetCanonicalWalletLease(ctx, platformUserID)
	switch {
	case err == nil:
		id := lease.LeaseID
		view.CurrentLeaseID = &id
		view.Lease = lease
	case errors.Is(err, ErrCanonicalWalletLeaseMissing):
		// empty keyspace — not an outage
	default:
		view.Available = false
		return view
	}
	members, err := s.leases.ListCanonicalWalletHolds(ctx, platformUserID, 10000)
	if err != nil {
		view.Available = false
		view.OpenHolds = 0
		view.CurrentLeaseID = nil
		view.Lease = nil
		return view
	}
	view.OpenHolds = len(members)
	return view
}
