package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// AuthorizationOutcome is the decorator's classification of one upstream write
// (spec §2.0.1 "three outcomes, not two"). It is recorded on the handle so the
// settling code can name the write it settles; nothing in 3.3 acts on it.
type AuthorizationOutcome string

const (
	AuthorizationOutcomeUnknown       AuthorizationOutcome = ""
	AuthorizationOutcomeNotWritten    AuthorizationOutcome = "not_written"   // error before any request byte left the process
	AuthorizationOutcomeResult        AuthorizationOutcome = "result"        // a response was obtained
	AuthorizationOutcomeIndeterminate AuthorizationOutcome = "indeterminate" // error after bytes left
	AuthorizationOutcomeRejected      AuthorizationOutcome = "rejected"      // explicit unbillable provider validation/authentication rejection
)

// AuthorizationWrite is one write through the decorated port under one handle.
type AuthorizationWrite struct {
	Token     string
	Outcome   AuthorizationOutcome
	StartedAt time.Time
	EndedAt   time.Time
	Err       error
}

// AuthorizationHandle is minted once per billable attempt at its authorization
// point — after the 3.2 freeze, before the first upstream write — and attached to
// the request context the forwarder builds its upstream request from. Every write
// through the decorated port mints its own token from the handle (spec §4: unique
// per billable write, including each forwarder-level retry). The handle is a
// convenience mirror in context; the settling code receives the token as an
// explicit field on its input struct (spec §3.5).
type AuthorizationHandle struct {
	ID             string
	MintedAt       time.Time
	Mode           string // canonical_wallet.mode at mint time
	SnapshotID     string
	AttemptKind    string
	Segments       []AuthorizationSegment
	LeaseID        string
	EstimatedUnits int64
	// HoldArmed/HeldUnits (Phase 3.4b, §10.4): set when Authorize armed this
	// attempt's hold; false/0 when holds are off (3.4a byte-for-byte).
	HoldArmed bool
	HeldUnits int64
	// Continuation is the turn's continuation classification (3.2's
	// ClassifyWSContinuation), recorded at authorization time. Write-only in
	// 3.3b (3.8's observation and 3.7's top-up read it later).
	Continuation ContinuationKind
	// Refusal is set when Authorize refused this attempt in enforce mode. The handle
	// is still minted so the refusal carries an id; the decorator refuses any write
	// that carries a refused handle, whatever the handler did with the error.
	Refusal *AuthorizationRefusedError

	mu        sync.Mutex
	seq       uint64
	writes    []AuthorizationWrite
	abandoned string
	// onOutcome (§10.4): installed by Authorize when a hold was armed; invoked
	// by RecordOutcome AFTER h.mu is released — the callback does a Redis
	// round trip and must never re-enter the handle.
	beforeWrite           func(context.Context, string) error
	writeEnded            func()
	onOutcome             func(token string, outcome AuthorizationOutcome, err error)
	retryCheck            func(context.Context) error
	renewAfterZero        func(context.Context) (*AuthorizationHandle, error)
	renewMu               sync.Mutex
	renewed               atomic.Pointer[AuthorizationHandle]
	stageUsage            func(context.Context, UsageRecordTask) (UsageRecordTask, error)
	sealEvidence          func(context.Context, string) error
	dispatchEvidence      func(context.Context) error
	readerEvidence        *WalletReaderEvidence
	readerEvidenceErr     error
	readerJournal         *walletReaderJournal
	readerBillingFamily   BillingFamily
	readerPlatform        string
	readerTokenOnly       bool
	readerCountKind       string
	readerNormalization   *WalletReaderNormalization
	requiresReaderJournal bool
	consumeHTTP           func(int, WalletReaderEvidence, error)
	readerStarted         func() error
	readerObserved        func(WalletReaderEvidence) error
	headerObserved        func(int) error
	pendingZero           bool
	zeroAcknowledged      bool
}

func (h *AuthorizationHandle) activeAttempt() *AuthorizationHandle {
	for h != nil {
		next := h.renewed.Load()
		if next == nil {
			return h
		}
		h = next
	}
	return nil
}

func (h *AuthorizationHandle) nextHTTPAttempt(ctx context.Context) (*AuthorizationHandle, error) {
	h = h.activeAttempt()
	h.renewMu.Lock()
	defer h.renewMu.Unlock()
	if next := h.renewed.Load(); next != nil {
		return next.nextHTTPAttempt(ctx)
	}
	writes := h.Writes()
	if len(writes) == 0 {
		return h, nil
	}
	if h.renewAfterZero == nil {
		if h.beforeWrite != nil {
			return nil, ErrWalletUnknownCostRetry
		}
		return h, nil
	}
	outcome := writes[len(writes)-1].Outcome
	h.mu.Lock()
	knownZero := h.zeroAcknowledged
	h.mu.Unlock()
	if !knownZero && outcome != AuthorizationOutcomeRejected && outcome != AuthorizationOutcomeNotWritten {
		if h.beforeWrite != nil {
			return nil, ErrWalletUnknownCostRetry
		}
		return h, nil
	}
	next, err := h.renewAfterZero(ctx)
	if err != nil {
		return nil, err
	}
	h.renewed.Store(next)
	return next, nil
}

func newAuthorizationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "auth_" + hex.EncodeToString(b[:]), nil
}

func newAuthorizationHandle(mode string) (*AuthorizationHandle, error) {
	id, err := newAuthorizationID()
	if err != nil {
		return nil, fmt.Errorf("mint authorization id: %w", err)
	}
	// The `minted` counter is incremented by Authorize AFTER its disabled-mode
	// early return, so disabled mode moves no counter (Task 10 Step 5).
	return &AuthorizationHandle{ID: id, MintedAt: time.Now().UTC(), Mode: mode}, nil
}

// MintWriteToken mints the token for the next write under this handle.
func (h *AuthorizationHandle) MintWriteToken() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	tok := fmt.Sprintf("%s.%d", h.ID, h.seq)
	h.writes = append(h.writes, AuthorizationWrite{Token: tok, StartedAt: time.Now().UTC()})
	return tok
}

// RecordOutcome stores the decorator's classification of the write named by token
// and, when Authorize armed a hold, invokes the outcome callback OUTSIDE h.mu
// (§10.4): the callback does a Redis round trip, takes no handle lock, and must
// never re-enter the handle.
func (h *AuthorizationHandle) RecordOutcome(token string, outcome AuthorizationOutcome, err error) {
	if h == nil || token == "" {
		return
	}
	h.mu.Lock()
	var cb func(string, AuthorizationOutcome, error)
	for i := len(h.writes) - 1; i >= 0; i-- {
		if h.writes[i].Token == token {
			if h.beforeWrite != nil && h.writes[i].Outcome != AuthorizationOutcomeUnknown {
				break
			}
			h.writes[i].Outcome = outcome
			h.writes[i].EndedAt = time.Now().UTC()
			h.writes[i].Err = err
			cb = h.onOutcome
			break
		}
	}
	h.mu.Unlock()
	if cb != nil {
		cb(token, outcome, err) // OUTSIDE h.mu (§10.4)
	}
}

// Writes returns a copy of the write log.
func (h *AuthorizationHandle) Writes() []AuthorizationWrite {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]AuthorizationWrite, len(h.writes))
	copy(out, h.writes)
	return out
}

// LastWriteToken is the token of the most recent write, whatever its outcome.
func (h *AuthorizationHandle) LastWriteToken() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.writes) == 0 {
		return ""
	}
	return h.writes[len(h.writes)-1].Token
}

// SettledToken is the token of the most recent write that obtained a result — the
// write whose response the settlement is about — or "" when no write did.
func (h *AuthorizationHandle) SettledToken() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.writes) - 1; i >= 0; i-- {
		if h.writes[i].Outcome == AuthorizationOutcomeResult {
			return h.writes[i].Token
		}
	}
	return ""
}

// MarkAbandoned records that ownership of the attempt's result never reached the
// settling code (a dropped worker-pool submission, a panic). Spec §4: "we submitted
// it" is not a transfer. Recorded once; 3.4b turns it into an abort of the hold.
func (h *AuthorizationHandle) MarkAbandoned(reason string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.abandoned != "" {
		return
	}
	h.abandoned = reason
	authorizationMetrics.abandoned.Add(1)
}

func (h *AuthorizationHandle) Abandoned() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.abandoned
}

// AuthorizationTokenOf / AuthorizationIDOf are the nil-safe accessors the handlers
// use when filling the usage-recording input (spec §3.5: explicit fields).
func AuthorizationTokenOf(h *AuthorizationHandle) string { return h.activeAttempt().SettledToken() }

func AuthorizationIDOf(h *AuthorizationHandle) string {
	if h == nil {
		return ""
	}
	return h.activeAttempt().ID
}

type authorizationHandleContextKey struct{}

// WithAuthorizationHandle attaches the handle to ctx (the convenience mirror the
// decorator reads off req.Context()). A nil handle attaches nothing.
func WithAuthorizationHandle(ctx context.Context, h *AuthorizationHandle) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, authorizationHandleContextKey{}, h)
}

func AuthorizationHandleFromContext(ctx context.Context) *AuthorizationHandle {
	if ctx == nil {
		return nil
	}
	h, _ := ctx.Value(authorizationHandleContextKey{}).(*AuthorizationHandle)
	return h
}

// NonBillableReason says why a write through the port takes no authorization.
// It is a NEW context key, independent of HTTPUpstreamProfile (spec §2.0).
type NonBillableReason string

const (
	NonBillableProbe          NonBillableReason = "probe"
	NonBillableModelListing   NonBillableReason = "model_listing"
	NonBillableQuota          NonBillableReason = "quota"
	NonBillableCountTokens    NonBillableReason = "count_tokens"
	NonBillableMediaFetch     NonBillableReason = "media_fetch"
	NonBillableAccountTest    NonBillableReason = "account_test"
	NonBillableUsageFetch     NonBillableReason = "usage_fetch"
	NonBillableInterTurnFrame NonBillableReason = "inter_turn_frame"    // passthrough client frames between response.create turns (spec §2.0)
	NonBillableLiveSideband   NonBillableReason = "live_sideband"       // the Live observer's frames incl. session.close (spec §2.3)
	NonBillableWSPrewarm      NonBillableReason = "ws_generate_prewarm" // the generate:false warm-up, openai_ws_forwarder_support.go:79-81
)

type nonBillableUpstreamContextKey struct{}

// WithNonBillableUpstream marks every upstream request built from ctx as
// non-billable. The mark is pinned at call-site granularity by the egress test
// (internal/egress): a billable site whose function carries it fails the test.
func WithNonBillableUpstream(ctx context.Context, reason NonBillableReason) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, nonBillableUpstreamContextKey{}, reason)
}

func NonBillableUpstreamFromContext(ctx context.Context) (NonBillableReason, bool) {
	if ctx == nil {
		return "", false
	}
	reason, ok := ctx.Value(nonBillableUpstreamContextKey{}).(NonBillableReason)
	return reason, ok
}

// AuthorizationMetrics is the counter snapshot 3.8's observation reads.
type AuthorizationMetrics struct {
	Minted, Refused                                                       int64
	WritesAuthorized, WritesNonBillable, WritesUnmarked, WritesRefused    int64
	MisplacedMarks                                                        int64
	OutcomeResult, OutcomeNotWritten, OutcomeIndeterminate                int64
	SnapshotMissing, EstimateFailed, IdentityMissing, CurrencyUnsupported int64
	LeaseUnavailable, LeaseCapReached, BalanceShortfall                   int64
	Abandoned                                                             int64
	LiveProvisionalWritten, LiveProvisionalActivated                      int64
	LiveProvisionalAborted, LiveProvisionalFinalized                      int64
	LiveProvisionalStoreUnavailable, LiveProvisionalSettlementNotEnqueued int64
	// Phase 3.4b (§10.4): the hold arm counters.
	HoldsArmed, HoldArmRetried, HoldArmRefused int64
}

var authorizationMetrics struct {
	minted, refused                                                       atomic.Int64
	writesAuthorized, writesNonBillable, writesUnmarked, writesRefused    atomic.Int64
	misplacedMarks                                                        atomic.Int64
	outcomeResult, outcomeNotWritten, outcomeIndeterminate                atomic.Int64
	snapshotMissing, estimateFailed, identityMissing, currencyUnsupported atomic.Int64
	leaseUnavailable, leaseCapReached, balanceShortfall                   atomic.Int64
	abandoned                                                             atomic.Int64
	liveProvisionalWritten, liveProvisionalActivated                      atomic.Int64
	liveProvisionalAborted, liveProvisionalFinalized                      atomic.Int64
	liveProvisionalStoreUnavailable, liveProvisionalSettlementNotEnqueued atomic.Int64
	// Phase 3.4b (§10.4): the hold arm counters.
	holdsArmed, holdArmRetried, holdArmRefused atomic.Int64
}

func AuthorizationMetricsSnapshot() AuthorizationMetrics {
	m := &authorizationMetrics
	return AuthorizationMetrics{
		Minted: m.minted.Load(), Refused: m.refused.Load(),
		WritesAuthorized: m.writesAuthorized.Load(), WritesNonBillable: m.writesNonBillable.Load(),
		WritesUnmarked: m.writesUnmarked.Load(), WritesRefused: m.writesRefused.Load(),
		MisplacedMarks: m.misplacedMarks.Load(),
		OutcomeResult:  m.outcomeResult.Load(), OutcomeNotWritten: m.outcomeNotWritten.Load(), OutcomeIndeterminate: m.outcomeIndeterminate.Load(),
		SnapshotMissing: m.snapshotMissing.Load(), EstimateFailed: m.estimateFailed.Load(),
		IdentityMissing: m.identityMissing.Load(), CurrencyUnsupported: m.currencyUnsupported.Load(),
		LeaseUnavailable: m.leaseUnavailable.Load(), LeaseCapReached: m.leaseCapReached.Load(), BalanceShortfall: m.balanceShortfall.Load(),
		Abandoned:                            m.abandoned.Load(),
		LiveProvisionalWritten:               m.liveProvisionalWritten.Load(),
		LiveProvisionalActivated:             m.liveProvisionalActivated.Load(),
		LiveProvisionalAborted:               m.liveProvisionalAborted.Load(),
		LiveProvisionalFinalized:             m.liveProvisionalFinalized.Load(),
		LiveProvisionalStoreUnavailable:      m.liveProvisionalStoreUnavailable.Load(),
		LiveProvisionalSettlementNotEnqueued: m.liveProvisionalSettlementNotEnqueued.Load(),
		HoldsArmed:                           m.holdsArmed.Load(),
		HoldArmRetried:                       m.holdArmRetried.Load(),
		HoldArmRefused:                       m.holdArmRefused.Load(),
	}
}

type LiveProvisionalMetrics struct {
	Written               int64
	Activated             int64
	Aborted               int64
	Finalized             int64
	StoreUnavailable      int64
	SettlementNotEnqueued int64
}

func LiveProvisionalMetricsSnapshot() LiveProvisionalMetrics {
	m := &authorizationMetrics
	return LiveProvisionalMetrics{
		Written:               m.liveProvisionalWritten.Load(),
		Activated:             m.liveProvisionalActivated.Load(),
		Aborted:               m.liveProvisionalAborted.Load(),
		Finalized:             m.liveProvisionalFinalized.Load(),
		StoreUnavailable:      m.liveProvisionalStoreUnavailable.Load(),
		SettlementNotEnqueued: m.liveProvisionalSettlementNotEnqueued.Load(),
	}
}

func ResetAuthorizationMetricsForTest() {
	m := &authorizationMetrics
	for _, c := range []*atomic.Int64{
		&m.minted, &m.refused, &m.writesAuthorized, &m.writesNonBillable, &m.writesUnmarked, &m.writesRefused,
		&m.misplacedMarks, &m.outcomeResult, &m.outcomeNotWritten, &m.outcomeIndeterminate,
		&m.snapshotMissing, &m.estimateFailed, &m.identityMissing, &m.currencyUnsupported,
		&m.leaseUnavailable, &m.leaseCapReached, &m.balanceShortfall, &m.abandoned,
		&m.liveProvisionalWritten, &m.liveProvisionalActivated, &m.liveProvisionalAborted,
		&m.liveProvisionalFinalized, &m.liveProvisionalStoreUnavailable, &m.liveProvisionalSettlementNotEnqueued,
		&m.holdsArmed, &m.holdArmRetried, &m.holdArmRefused,
	} {
		c.Store(0)
	}
}

func resetAuthorizationMetricsForTest() {
	ResetAuthorizationMetricsForTest()
}

func (h *AuthorizationHandle) prepareWrite(ctx context.Context, token string) error {
	if h != nil && h.beforeWrite != nil {
		return h.beforeWrite(ctx, token)
	}
	return nil
}

// ErrWalletUnknownCostRetry prevents another potentially billable write when
// the previous attempt has no durable, acknowledged known-zero resolution.
var ErrWalletUnknownCostRetry = errors.New("upstream attempt cost is unresolved; automatic replay stopped")

// WalletAttemptMayRetry checks the same cost boundary for handler account
// failover and provider-internal retries. Non-wallet paths remain unchanged.
func WalletAttemptMayRetry(ctx context.Context) bool {
	h := AuthorizationHandleFromContext(ctx).activeAttempt()
	if h == nil || h.beforeWrite == nil {
		return true
	}
	writes := h.Writes()
	if len(writes) == 0 {
		return true
	}
	last := writes[len(writes)-1]
	h.mu.Lock()
	knownZero := h.zeroAcknowledged
	h.mu.Unlock()
	if !knownZero && last.Outcome != AuthorizationOutcomeRejected && last.Outcome != AuthorizationOutcomeNotWritten {
		return false
	}
	return h.retryCheck != nil && h.retryCheck(ctx) == nil
}
