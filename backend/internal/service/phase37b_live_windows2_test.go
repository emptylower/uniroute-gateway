//go:build integration

// Phase 3.7b (redesign §13.2.1–§13.2.4, §13.3 tests 49–53): the Live window
// clock, per-window settlement, re-authorization and the hard stop — driven
// end to end through a REAL gatewayCache (LiveCallStore + LiveUsageStore +
// LiveFinalizationStore + the wallet store, via the external bridge), a REAL
// liveProvisionalStore on the test Postgres, the §3 fake control plane with
// the v2 settlements route, and a test-driven sideband (liveTestFrameConn).
//
// The observer's clocks are REAL (constraint 4): every test uses the minimum
// legal lease_ttl_seconds=30 / live_window_min_seconds=5 /
// live_controller_takeover_seconds=2 and polls (50 ms, bounded deadline) —
// no sleeps. Every observer runs under the fixture's context, cancelled at
// cleanup alongside the bridge's Close.
package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// The shared stores: one Redis client, one Postgres database, one fake per
// TEST (not per fixture) — tests 50/52 build two fixture instances "on the
// same stores" by handing these to both.
// ---------------------------------------------------------------------------

type liveWindowShared struct {
	rdb         *redis.Client
	db          *sql.DB
	fake        *fakeEnsureControlPlane
	snapService *BillingSnapshotService
	apiKey      *APIKey
	user        *User
	account     *Account
	resolver    *ModelPricingResolver
	billing     *BillingService
	fx          *ExchangeRateService
}

func newLiveWindowShared(t *testing.T) *liveWindowShared {
	t.Helper()
	ResetAuthorizationMetricsForTest()
	ResetLiveWindowMetricsForTest()
	EnableOpenAIAdvancedSchedulerForTest()
	ctx := context.Background()

	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx) // the outbox table
	for _, migration := range []string{"209_wallet_billing_snapshot.sql", "211_wallet_live_provisional.sql"} {
		content, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(content))
		require.NoError(t, err)
	}

	fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })

	// The snapshot fixture's parts (billing, resolver, fx, apiKey, user,
	// account) with a REAL store on this database, so the re-authorization's
	// snapshots.Load reads what the session's Freeze persisted.
	_, apiKey, user, account, billing, resolver, fx := NewSnapshotTestFixtureForTest(t)
	user.PlatformUserID = "shipany-user-" + t.Name()
	apiKey.User = user
	snapCfg := &config.Config{}
	snapCfg.Default.RateMultiplier = 1
	snapCfg.CanonicalWallet.BillingSnapshotMode = "record"
	snapCfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	snapService := NewBillingSnapshotService(snapCfg, resolver, billing, fx, NewBillingSnapshotStoreForTest(t, db))

	account.Platform = PlatformOpenAI
	account.Type = AccountTypeOAuth
	account.Concurrency = 4
	account.Credentials = map[string]any{"access_token": "test-access-token", "chatgpt_account_id": "acct_test"}

	return &liveWindowShared{
		rdb: rdb, db: db, fake: fake, snapService: snapService,
		apiKey: apiKey, user: user, account: account,
		resolver: resolver, billing: billing, fx: fx,
	}
}

// ---------------------------------------------------------------------------
// The fixture: one OpenAIGatewayService (one bridge, one observer context,
// one sideband conn) on the shared stores.
// ---------------------------------------------------------------------------

type liveWindowFixture struct {
	t           *testing.T
	mode        string
	ctx         context.Context
	cancel      context.CancelFunc
	svc         *OpenAIGatewayService
	bridge      *CanonicalWalletBridge
	store       CanonicalWalletLeaseStore
	liveStore   LiveCallStore
	provisional LiveProvisionalStore
	db          *sql.DB
	rdb         *redis.Client
	fake        *fakeEnsureControlPlane
	user        *User
	apiKey      *APIKey
	account     *Account
	conn        *liveTestFrameConn
	dialer      *liveTestDialer
	cfg         *config.Config
	callSeq     atomic.Int64
}

// newLiveWindowTestFixture builds a fixture on its OWN shared stores (one
// Redis claim, one database, one fake).
func newLiveWindowTestFixture(t *testing.T, mode string) *liveWindowFixture {
	t.Helper()
	return newLiveWindowShared(t).newFixture(t, mode)
}

func (sh *liveWindowShared) newFixture(t *testing.T, mode string) *liveWindowFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet = canonicalWalletTestConfig(mode)
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.CanonicalWallet.Holds = "on"
	cfg.CanonicalWallet.RequestTimeoutMS = 100
	cfg.CanonicalWallet.LeaseTTLSeconds = 30
	cfg.CanonicalWallet.ExpirySkewMarginMS = 50
	cfg.CanonicalWallet.LiveWindowMinSeconds = 5
	cfg.CanonicalWallet.LiveControllerTakeoverSeconds = 2
	cfg.CanonicalWallet.LeaseBudgetUnits = 2_000_000_000 // 20 CNY per lease — comfortably above the session estimate
	cfg.CanonicalWallet.OrphanGraceSeconds = 900
	cfg.CanonicalWallet.OrphanSweepIntervalSeconds = 60
	cfg.CanonicalWallet.OrphanSweepBatch = 200
	cfg.JWT.Secret = "test-jwt-secret-32-bytes-long!!!"
	cfg.Gateway.Live.MaxSessionDurationSeconds = 60

	cache := NewRealGatewayCacheForTest(t, sh.rdb)
	store := cache.(CanonicalWalletLeaseStore)
	liveStore := cache.(LiveCallStore)
	outbox := &outboxStoreForTest{db: sh.db}
	// The §3 fake with the v2 route, reached through the REAL HTTP client —
	// the wire, not the in-process interface (clock nil = real time).
	cfg.CanonicalWallet.ControlPlaneURL = sh.fake.Server.URL
	cfg.CanonicalWallet.Secret = strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg.CanonicalWallet, sh.fake.Server.Client())
	bridge := newCanonicalWalletBridge(cfg.CanonicalWallet, store, client, sh.db, outbox, 0, nil)
	authorizer := NewCanonicalWalletAuthorizer(cfg, bridge, sh.snapService)

	provStore := LiveProvisionalStore(newLiveProvisionalStore(sh.db))
	conn := newLiveTestFrameConn()
	dialer := &liveTestDialer{conn: conn}
	f := &liveWindowFixture{
		t: t, mode: mode, ctx: ctx, cancel: cancel,
		svc: nil, bridge: bridge, store: store, liveStore: liveStore, provisional: provStore,
		db: sh.db, rdb: sh.rdb, fake: sh.fake, user: sh.user, apiKey: sh.apiKey, account: sh.account,
		conn: conn, dialer: dialer, cfg: cfg,
	}
	f.svc = &OpenAIGatewayService{
		cfg:                       cfg,
		cache:                     cache,
		concurrencyService:        NewConcurrencyService(&liveTestConcurrencyCache{}),
		liveProvisional:           provStore,
		httpUpstream:              &p37bHTTPUpstream{fixture: f},
		openaiWSPassthroughDialer: dialer,
		openaiScheduler:           &LiveRestartSchedulerStub{Account: sh.account},
		accountRepo:               &liveTestAccountRepo{account: sh.account},
		liveAttestation:           liveAttestationStub{header: `{"v":1,"s":0,"t":"v1.test"}`},
		liveAttestationCipher:     newLiveAttestationCipher(cfg),
		resolver:                  sh.resolver,
		billingService:            sh.billing,
		exchangeRates:             sh.fx,
		billingSnapshotSettler:    billingSnapshotSettler{snapshots: sh.snapService},
		authorizer:                authorizer,
		canonicalWallet:           bridge,
		deferredService:           NewDeferredService(nil, nil, time.Second),
		usageBillingRepo:          &openAIRecordUsageBillingRepoStub{},
		usageLogRepo:              &liveTestUsageRepo{},
	}

	// Every observer this fixture starts runs under f.ctx: CreateLiveCall's
	// internal spawn reads the provider; the test's own observeLiveCall calls
	// pass f.ctx explicitly. Cancelled at cleanup, before the bridge closes.
	prev := liveObserverContextProvider
	liveObserverContextProvider = func() context.Context { return f.ctx }
	t.Cleanup(func() {
		liveObserverContextProvider = prev
		cancel()
		bridge.Close()
	})
	return f
}

// p37bHTTPUpstream answers each CreateLiveCall with a DISTINCT call id (the
// fixed-id stub would collide two sessions onto one hash).
type p37bHTTPUpstream struct {
	fixture *liveWindowFixture
}

func (h *p37bHTTPUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	id := h.fixture.callSeq.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Location": {"/backend-api/codex/call_p37b_" + strconv.FormatInt(id, 10)}},
		Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
	}, nil
}

func (h *p37bHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return h.Do(req, proxyURL, accountID, accountConcurrency)
}

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

func (f *liveWindowFixture) createSession(t *testing.T) (callHash string, created *LiveCallCreated) {
	t.Helper()
	f.fund(20_000_000_000) // 200 CNY — plenty for the default legs; shortfall legs fund separately
	created, err := f.svc.CreateLiveCall(context.Background(), &LiveCallRequest{
		SDP:     "v=0\r\n",
		Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096,"instructions":"p37b live window session"}`),
	}, LiveCallIdentity{
		APIKeyID:        f.apiKey.ID,
		UserID:          f.user.ID,
		APIKey:          f.apiKey,
		User:            f.user,
		GroupID:         f.apiKey.GroupID,
		BillingCurrency: "CNY",
		RateMultiplier:  1.0,
		BillingModel:    "claude-sonnet-4",
	}, 5)
	require.NoError(t, err)
	require.NotNil(t, created)
	return hashLiveCallID(created.CallID), created
}

func (f *liveWindowFixture) fund(units int64) {
	f.fake.fund(f.user.PlatformUserID, units)
}

// ---------------------------------------------------------------------------
// Usage pumping. The units formula (recorded per the plan): one output token
// prices to OutputPricePerToken × ExchangeRate × RateMultiplier × 1e8 units
// (the same arithmetic liveUsageUnits runs: sourceCost × fx × rate →
// canonicalWalletUnitsFromCNY, units per CNY = 1e8).
// ---------------------------------------------------------------------------

func (f *liveWindowFixture) unitsPerOutputToken(t *testing.T, callHash string) float64 {
	t.Helper()
	rec := f.recordByHash(t, callHash)
	return rec.OutputPricePerToken * rec.ExchangeRate * rec.RateMultiplier * float64(canonicalWalletUnitsPerCNY)
}

func (f *liveWindowFixture) unitsForTokens(t *testing.T, rec *LiveCallRecord, input, output, cacheRead int) int64 {
	t.Helper()
	inputTokens := input - cacheRead
	if inputTokens < 0 {
		inputTokens = 0
	}
	source := float64(inputTokens)*rec.InputPricePerToken + float64(output)*rec.OutputPricePerToken + float64(cacheRead)*rec.CacheReadPricePerToken
	units, err := canonicalWalletUnitsFromCNY(source * rec.ExchangeRate * rec.RateMultiplier)
	require.NoError(t, err)
	return units
}

// pumpUsage writes one response.done frame into the sideband conn.
func (f *liveWindowFixture) pumpUsage(responseID string, inputTokens, outputTokens int) {
	payload := fmt.Sprintf(`{"type":"response.done","response":{"id":%q,"usage":{"input_tokens":%d,"output_tokens":%d}}}`, responseID, inputTokens, outputTokens)
	f.conn.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(payload)}
}

// pumpAtRate pumps one frame per second whose usage prices to unitsPerSecond
// under the fixture's snapshot, until ctx is cancelled (the caller bounds the
// duration). Each frame is one "pumped response" for the invariant's tail
// term.
func (f *liveWindowFixture) pumpAtRate(ctx context.Context, t *testing.T, unitsPerSecond int64, callHash string) {
	t.Helper()
	tokens := int(math.Round(float64(unitsPerSecond) / f.unitsPerOutputToken(t, callHash)))
	if tokens <= 0 {
		t.Fatalf("rate %d units/s prices to zero tokens at %f units/token", unitsPerSecond, f.unitsPerOutputToken(t, callHash))
	}
	seq := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			seq++
			f.pumpUsage(fmt.Sprintf("resp-rate-%d", seq), 0, tokens)
		}
	}
}

// ---------------------------------------------------------------------------
// Polling helpers (the file's 50 ms idiom, bounded deadlines)
// ---------------------------------------------------------------------------

func (f *liveWindowFixture) provRecord(t *testing.T, callHash string) *LiveProvisionalRecord {
	t.Helper()
	rec, err := f.provisional.GetByCallHash(f.ctx, callHash)
	require.NoError(t, err)
	return rec
}

func (f *liveWindowFixture) recordByHash(t *testing.T, callHash string) *LiveCallRecord {
	t.Helper()
	rec, err := f.liveStore.GetLiveCall(f.ctx, callHash)
	require.NoError(t, err)
	return rec
}

func (f *liveWindowFixture) pollWindowsLen(t *testing.T, callHash string, want int, deadline time.Duration) *LiveProvisionalRecord {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		rec, err := f.provisional.GetByCallHash(f.ctx, callHash)
		if err == nil && len(rec.Windows) >= want {
			return rec
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("provisional record never reached %d windows", want)
	return nil
}

func (f *liveWindowFixture) pollRowStatus(t *testing.T, eventID, want string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		var status string
		err := f.db.QueryRowContext(f.ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("outbox row %s never reached %s", eventID, want)
}

type p37bOutboxRow struct {
	EventID   string
	GatewayID string
	AuthID    string
	Amount    int64
	Status    string
	LeaseID   string
}

func (f *liveWindowFixture) outboxRow(t *testing.T, eventID string) *p37bOutboxRow {
	t.Helper()
	var row p37bOutboxRow
	var auth, lease sql.NullString
	require.NoError(t, f.db.QueryRowContext(f.ctx,
		`SELECT event_id, gateway_request_id, authorization_id, amount_units, status, lease_id FROM wallet_settlement_outbox WHERE event_id = $1`, eventID,
	).Scan(&row.EventID, &row.GatewayID, &auth, &row.Amount, &row.Status, &lease))
	row.AuthID = auth.String
	row.LeaseID = lease.String
	return &row
}

func (f *liveWindowFixture) outboxRowCount(t *testing.T, eventID string) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&n))
	return n
}

func (f *liveWindowFixture) holdIDs(t *testing.T) []string {
	t.Helper()
	ids, err := f.store.ListCanonicalWalletHolds(f.ctx, f.user.PlatformUserID, 100)
	require.NoError(t, err)
	return ids
}

func (f *liveWindowFixture) holdState(t *testing.T, authID string) string {
	t.Helper()
	hold, err := f.store.GetCanonicalWalletHold(f.ctx, f.user.PlatformUserID, authID)
	require.NoError(t, err)
	return hold.State
}

func (f *liveWindowFixture) waitDeliveredThroughFake(t *testing.T, eventID string, deadline time.Duration) {
	t.Helper()
	f.pollRowStatus(t, eventID, "delivered", deadline)
	f.fake.mu.Lock()
	_, captured := f.fake.events[f.user.PlatformUserID][eventID]
	f.fake.mu.Unlock()
	require.True(t, captured, "event %s delivered on the wire (captured by the fake)", eventID)
}

// ---------------------------------------------------------------------------
// Test 49 — a session crossing windows: close, settle, re-authorize
// ---------------------------------------------------------------------------

// TestPhase37bWindowsCloseSettleAndReauthorize (test 49, first three legs;
// the last-window-at-finalization leg is Task 6):
//   - clock (ii) with the floor: usage reaching E_w does NOT close the window
//     before live_window_min_seconds, and closes right after, settling exactly
//     its usage under window 1's event id (the session id), converting T(1),
//     and arming exactly one new hold T(2);
//   - clock (i) idle: an idle window advances at the lease horizon with no
//     outbox row, its hold released zero_cost, the next armed;
//   - three windows sum to the total, distinct event ids, no payload
//     conflict, at most one armed hold on every sample.
func TestPhase37bWindowsCloseSettleAndReauthorize(t *testing.T) {
	t.Run("clock-ii-closes-only-past-the-floor", func(t *testing.T) {
		f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
		callHash, _ := f.createSession(t)

		prov := f.provRecord(t, callHash)
		E := prov.EstimatedUnits
		require.Greater(t, E, int64(0))

		// Pump enough usage to cross E_w within one read (< 1 s). The
		// estimate is bounded at the snapshot's multipliers (group × account)
		// while actual usage prices at the record's identity rate, so E/price
		// tokens would undershoot — 2×E worth of tokens crosses it with
		// margin, and the conversion's excess branch absorbs the overshoot
		// within the lease budget.
		tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))
		f.pumpUsage("resp-49a", 0, tokens)

		// The floor: no close before 5 s after opened_at_ms.
		openedAt := time.UnixMilli(prov.Windows[0].OpenedAtMS)
		require.False(t, openedAt.IsZero(), "window 1 carries opened_at_ms")
		negativeEnd := openedAt.Add(4900 * time.Millisecond)
		for time.Now().Before(negativeEnd) {
			rec := f.provRecord(t, callHash)
			require.Len(t, rec.Windows, 1, "the window must not close before live_window_min_seconds")
			time.Sleep(50 * time.Millisecond)
		}

		// Closes past the floor: len == 2, window 1 settled to its exact usage.
		prov = f.pollWindowsLen(t, callHash, 2, 8*time.Second)
		require.Equal(t, int64(0), prov.Windows[0].PendingUnits)
		A1 := prov.Windows[0].SettledUnits
		require.Greater(t, A1, int64(0))
		require.Equal(t, f.unitsForTokens(t, f.recordByHash(t, callHash), 0, tokens, 0), A1)

		// The outbox row: window 1's event id is the session id, the amount is
		// A_1, the authorization is T(1), delivered through the fake.
		T1 := prov.Token
		eventID := CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")
		f.waitDeliveredThroughFake(t, eventID, 15*time.Second)
		row := f.outboxRow(t, eventID)
		require.Equal(t, callHash, row.GatewayID)
		require.Equal(t, A1, row.Amount)
		require.Equal(t, T1, row.AuthID)

		// T(1)'s hold converted (settled); exactly one armed hold remains
		// (T(2)); window 2 has a lease.
		require.Equal(t, "settled", f.holdState(t, T1))
		ids := f.holdIDs(t)
		require.Len(t, ids, 1, "exactly one armed hold")
		require.Equal(t, prov.Windows[1].Token, ids[0], "the armed hold is T(2)")
		require.NotEmpty(t, prov.Windows[1].LeaseID)
		require.Equal(t, 2, prov.Windows[1].WindowSeq)
		require.False(t, time.UnixMilli(prov.Windows[1].OpenedAtMS).IsZero(), "the advanced window carries opened_at_ms")
	})

	t.Run("clock-i-idle-window-advances-at-the-horizon", func(t *testing.T) {
		f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
		callHash, _ := f.createSession(t)
		prov := f.provRecord(t, callHash)
		T1 := prov.Token

		zeroCostBefore := canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load()
		conflictsBefore := canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()

		// Pump nothing; the window closes at opened_at + lease_ttl − skew.
		prov = f.pollWindowsLen(t, callHash, 2, 35*time.Second)
		require.Equal(t, int64(0), prov.Windows[0].SettledUnits, "an idle window settles nothing")
		require.Equal(t, int64(0), prov.Windows[0].PendingUnits)

		// No outbox row for the idle window — none is expected (§13.2.3).
		eventID := CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")
		require.Equal(t, 0, f.outboxRowCount(t, eventID))
		require.Equal(t, int64(0), canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()-conflictsBefore)

		// T(1) released zero_cost; exactly one hold armed (T(2)).
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load()-zeroCostBefore)
		require.Equal(t, "released", f.holdState(t, T1))
		ids := f.holdIDs(t)
		require.Len(t, ids, 1)
		require.Equal(t, prov.Windows[1].Token, ids[0])

		// The session is NOT closed.
		require.NotEqual(t, LiveControllerClosed, f.recordByHash(t, callHash).Controller)
	})

	t.Run("three-windows-sum-to-the-total", func(t *testing.T) {
		f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
		callHash, _ := f.createSession(t)
		prov := f.provRecord(t, callHash)
		E := prov.EstimatedUnits
		tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))

		// Sampler: at most one armed hold on every 50 ms sample. The sampler
		// stops on its OWN stop channel — a deferred receive on samplerDone
		// runs before t.Cleanup cancels f.ctx, so waiting on f.ctx alone
		// would deadlock the test goroutine.
		var maxArmed int32
		samplerStop := make(chan struct{})
		samplerDone := make(chan struct{})
		defer func() { close(samplerStop); <-samplerDone }()
		go func() {
			defer close(samplerDone)
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-samplerStop:
					return
				case <-f.ctx.Done():
					return
				case <-ticker.C:
					if n := len(f.holdIDs(t)); int32(n) > atomic.LoadInt32(&maxArmed) {
						atomic.StoreInt32(&maxArmed, int32(n))
					}
				}
			}
		}()

		conflictsBefore := canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()

		// Pump one E-crossing frame per window; windows close past their
		// floors (~5 s each) until three are settled.
		f.pumpUsage("resp-49c-1", 0, tokens)
		f.pollWindowsLen(t, callHash, 2, 8*time.Second)
		f.pumpUsage("resp-49c-2", 0, tokens)
		f.pollWindowsLen(t, callHash, 3, 8*time.Second)
		f.pumpUsage("resp-49c-3", 0, tokens)
		prov = f.pollWindowsLen(t, callHash, 4, 8*time.Second)

		require.Equal(t, int64(0), canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()-conflictsBefore)

		// Σ == the fixture's own pricing of the pumped tokens (liveUsageUnits
		// over the record's counters) — three equal frames.
		rec := f.recordByHash(t, callHash)
		totalUnits, err := liveUsageUnits(rec)
		require.NoError(t, err)
		require.Equal(t, totalUnits, prov.Windows[0].SettledUnits+prov.Windows[1].SettledUnits+prov.Windows[2].SettledUnits,
			"Σ settled == units(total)")

		// Three distinct event ids: window 1 = the session id, 2–3 = :window:n.
		id1 := CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")
		id2 := CanonicalWalletSettlementEventID(liveWindowRequestID(callHash, 2), f.user.PlatformUserID, "CNY")
		id3 := CanonicalWalletSettlementEventID(liveWindowRequestID(callHash, 3), f.user.PlatformUserID, "CNY")
		require.Equal(t, 1, f.outboxRowCount(t, id1))
		require.Equal(t, 1, f.outboxRowCount(t, id2))
		require.Equal(t, 1, f.outboxRowCount(t, id3))

		f.waitDeliveredThroughFake(t, id2, 15*time.Second)
		f.waitDeliveredThroughFake(t, id3, 15*time.Second)
		require.Equal(t, id1, f.outboxRow(t, id1).EventID, "window 1's row exists under the session id")
		require.LessOrEqual(t, atomic.LoadInt32(&maxArmed), int32(1), "at most one armed hold on every sample")
	})
}
