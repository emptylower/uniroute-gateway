package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	LeaseID        string
	EstimatedUnits int64
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

// RecordOutcome stores the decorator's classification of the write named by token.
func (h *AuthorizationHandle) RecordOutcome(token string, outcome AuthorizationOutcome, err error) {
	if h == nil || token == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.writes) - 1; i >= 0; i-- {
		if h.writes[i].Token == token {
			h.writes[i].Outcome = outcome
			h.writes[i].EndedAt = time.Now().UTC()
			h.writes[i].Err = err
			return
		}
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
func AuthorizationTokenOf(h *AuthorizationHandle) string { return h.SettledToken() }

func AuthorizationIDOf(h *AuthorizationHandle) string {
	if h == nil {
		return ""
	}
	return h.ID
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
	LeaseUnavailable, BalanceShortfall                                    int64
	Abandoned                                                             int64
}

var authorizationMetrics struct {
	minted, refused                                                       atomic.Int64
	writesAuthorized, writesNonBillable, writesUnmarked, writesRefused    atomic.Int64
	misplacedMarks                                                        atomic.Int64
	outcomeResult, outcomeNotWritten, outcomeIndeterminate                atomic.Int64
	snapshotMissing, estimateFailed, identityMissing, currencyUnsupported atomic.Int64
	leaseUnavailable, balanceShortfall                                    atomic.Int64
	abandoned                                                             atomic.Int64
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
		LeaseUnavailable: m.leaseUnavailable.Load(), BalanceShortfall: m.balanceShortfall.Load(),
		Abandoned: m.abandoned.Load(),
	}
}

func ResetAuthorizationMetricsForTest() {
	m := &authorizationMetrics
	for _, c := range []*atomic.Int64{&m.minted, &m.refused, &m.writesAuthorized, &m.writesNonBillable, &m.writesUnmarked, &m.writesRefused, &m.misplacedMarks, &m.outcomeResult, &m.outcomeNotWritten, &m.outcomeIndeterminate, &m.snapshotMissing, &m.estimateFailed, &m.identityMissing, &m.currencyUnsupported, &m.leaseUnavailable, &m.balanceShortfall, &m.abandoned} {
		c.Store(0)
	}
}

func resetAuthorizationMetricsForTest() {
	ResetAuthorizationMetricsForTest()
}
