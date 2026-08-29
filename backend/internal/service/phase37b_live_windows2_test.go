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
	"errors"
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
	fixtureSeq  atomic.Int64
	callSeq     atomic.Int64
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

type liveTestClock struct {
	offset atomic.Int64
}

func newLiveTestClock() *liveTestClock {
	return &liveTestClock{}
}

func (c *liveTestClock) Now() time.Time {
	return time.Now().UTC().Add(time.Duration(c.offset.Load()))
}

func (c *liveTestClock) Advance(d time.Duration) {
	c.offset.Add(int64(d))
}

type liveWindowFixture struct {
	t           *testing.T
	sh          *liveWindowShared
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
	clock       *liveTestClock
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
	return sh.newFixtureCfg(t, mode, nil)
}

// newFixtureCfg mutates the wallet config BEFORE the bridge copies it — the
// ensure's requested budget rides the bridge's copy, so per-test budget
// shapes (test 51's exactly-one-lease funding) must be set here.
func (sh *liveWindowShared) newFixtureCfg(t *testing.T, mode string, mutate func(*config.CanonicalWalletConfig)) *liveWindowFixture {
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
	if mutate != nil {
		mutate(&cfg.CanonicalWallet)
	}

	fID := sh.fixtureSeq.Add(1)
	userCopy := *sh.user
	userCopy.ID = fID
	userCopy.PlatformUserID = fmt.Sprintf("%s-%d", sh.user.PlatformUserID, fID)
	apiKeyCopy := *sh.apiKey
	apiKeyCopy.ID = fID
	apiKeyCopy.User = &userCopy
	apiKeyCopy.UserID = userCopy.ID

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
	clock := newLiveTestClock()
	f := &liveWindowFixture{
		t: t, sh: sh, mode: mode, ctx: ctx, cancel: cancel,
		svc: nil, bridge: bridge, store: store, liveStore: liveStore, provisional: provStore,
		db: sh.db, rdb: sh.rdb, fake: sh.fake, user: &userCopy, apiKey: &apiKeyCopy, account: sh.account,
		conn: conn, dialer: dialer, cfg: cfg, clock: clock,
	}
	f.svc = &OpenAIGatewayService{
		cfg:                       cfg,
		cache:                     cache,
		concurrencyService:        NewConcurrencyService(&liveTestConcurrencyCache{}),
		liveProvisional:           provStore,
		httpUpstream:              &p37bHTTPUpstream{fixture: f},
		openaiWSPassthroughDialer: &p37bFakeOnlyDialer{inner: dialer, fake: conn, t: t},
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
		liveClock:                 clock,
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

// p37bFakeOnlyDialer (execution review MINOR-3): the fixture's sideband dialer
// hands the service the fixture's OWN conn and nothing else — the sideband
// never leaves the fake. A re-plumbed dialer fails HERE by name; a nil
// passthrough dialer bypasses this wrapper entirely (dialLiveSideband builds
// the real one, which dials the real wss://chatgpt.com) and is caught by the
// dial-record check createSessionFunded registers — a silent 403 either way
// becomes a named failure. t.Errorf, not require: Dial runs on observer
// goroutines, where FailNow is illegal.
type p37bFakeOnlyDialer struct {
	inner *liveTestDialer
	fake  *liveTestFrameConn
	t     *testing.T
}

func (d *p37bFakeOnlyDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	conn, status, respHeaders, err := d.inner.Dial(ctx, wsURL, headers, proxyURL)
	if err == nil && conn != openAIWSClientConn(d.fake) {
		d.t.Errorf("the fixture's sideband conn must be the fixture's fake, got %T", conn)
		return nil, http.StatusForbidden, nil, fmt.Errorf("non-fake sideband conn: %T", conn)
	}
	return conn, status, respHeaders, err
}

// p37bHTTPUpstream answers each CreateLiveCall with a DISTINCT call id (the
// fixed-id stub would collide two sessions onto one hash).
type p37bHTTPUpstream struct {
	fixture *liveWindowFixture
}

func (h *p37bHTTPUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	var id int64
	if h.fixture != nil && h.fixture.sh != nil {
		id = h.fixture.sh.callSeq.Add(1)
	} else if h.fixture != nil {
		id = h.fixture.callSeq.Add(1)
	}
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
	return f.createSessionFunded(t, 20_000_000_000) // 200 CNY — plenty for the default legs; shortfall legs fund separately
}

func (f *liveWindowFixture) createSessionFunded(t *testing.T, fundUnits int64) (callHash string, created *LiveCallCreated) {
	t.Helper()
	f.fund(fundUnits)
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
	// Execution review MINOR-3: the session's sideband must have gone through
	// the fixture's fake — a nil passthrough dialer bypasses the fake entirely
	// and silently dials the real wss://chatgpt.com (a silent 403); the
	// missing dial record names that shape here.
	t.Cleanup(func() {
		require.NotEmpty(t, f.dialer.url,
			"the session's sideband dialed through the fixture's fake — a nil passthrough dialer falls through to the real wss://chatgpt.com")
	})
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
		t.Errorf("rate %d units/s prices to zero tokens at %f units/token", unitsPerSecond, f.unitsPerOutputToken(t, callHash))
		return
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
	rec, err := f.provisional.GetByCallHash(context.Background(), callHash) // reads outlive the fixture's observer context (test 52 polls after killing A)
	require.NoError(t, err)
	return rec
}

func (f *liveWindowFixture) recordByHash(t *testing.T, callHash string) *LiveCallRecord {
	t.Helper()
	rec, err := f.liveStore.GetLiveCall(context.Background(), callHash)
	require.NoError(t, err)
	return rec
}

func (f *liveWindowFixture) pollWindowsLen(t *testing.T, callHash string, want int, deadline time.Duration) *LiveProvisionalRecord {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		rec, err := f.provisional.GetByCallHash(context.Background(), callHash)
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
		rec := f.provRecord(t, callHash)
		require.Len(t, rec.Windows, 1, "the window must not close before live_window_min_seconds")

		// Advance fake clock past the 5s floor
		f.clock.Advance(6 * time.Second)

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
		f.clock.Advance(31 * time.Second)
		prov = f.pollWindowsLen(t, callHash, 2, 8*time.Second)
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
		f.clock.Advance(6 * time.Second)
		f.pollWindowsLen(t, callHash, 2, 8*time.Second)
		f.pumpUsage("resp-49c-2", 0, tokens)
		f.clock.Advance(6 * time.Second)
		f.pollWindowsLen(t, callHash, 3, 8*time.Second)
		f.pumpUsage("resp-49c-3", 0, tokens)
		f.clock.Advance(6 * time.Second)
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

// ---------------------------------------------------------------------------
// Test 49, last leg — the last window settles at finalization through the
// REAL path (§13.3): after two closed windows the session ends; the third
// window settles under :window:3 through the bridge, the outbox and the
// fake's v2 route, and the fake's captured figure equals the windows' sum.
// ---------------------------------------------------------------------------

func TestPhase37bLastWindowSettlesAtFinalization(t *testing.T) {
	f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
	callHash, _ := f.createSession(t)
	prov := f.provRecord(t, callHash)
	E := prov.EstimatedUnits
	tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))

	f.pumpUsage("resp-49d-1", 0, tokens)
	f.clock.Advance(6 * time.Second)
	prov = f.pollWindowsLen(t, callHash, 2, 8*time.Second)
	f.pumpUsage("resp-49d-2", 0, tokens)
	f.clock.Advance(6 * time.Second)
	prov = f.pollWindowsLen(t, callHash, 3, 8*time.Second)
	T3 := prov.Windows[2].Token

	// The tail: usage accruing after window 2's close — the last window's
	// remainder (the response in flight when the session ended). Without it
	// the last window would finalize idle (remainder zero, no row).
	tailTokens := 1000
	f.pumpUsage("resp-49d-tail", 0, tailTokens)

	// End the session: the fake sideband emits session.closed.
	f.conn.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(`{"type":"session.closed"}`)}

	// The provisional row reaches finalized (poll).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		prov = f.provRecord(t, callHash)
		if prov.Status == LiveProvisionalStatusFinalized {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, LiveProvisionalStatusFinalized, prov.Status, "the row finalizes after session.closed")

	// A third outbox row: :window:3, T(3)'s authorization, the remainder.
	rec := f.recordByHash(t, callHash)
	totalUnits, err := liveUsageUnits(rec)
	require.NoError(t, err)
	remainder := totalUnits - prov.Windows[0].SettledUnits - prov.Windows[1].SettledUnits
	require.Equal(t, f.unitsForTokens(t, rec, 0, tailTokens, 0), remainder)
	id3 := CanonicalWalletSettlementEventID(liveWindowRequestID(callHash, 3), f.user.PlatformUserID, "CNY")
	f.waitDeliveredThroughFake(t, id3, 15*time.Second)
	row := f.outboxRow(t, id3)
	require.Equal(t, liveWindowRequestID(callHash, 3), row.GatewayID)
	require.Equal(t, T3, row.AuthID)
	require.Equal(t, remainder, row.Amount)

	// Σ windows == units(total); the fake captured exactly the same figure on
	// the session's lease.
	sum := prov.Windows[0].SettledUnits + prov.Windows[1].SettledUnits + prov.Windows[2].SettledUnits
	require.Equal(t, totalUnits, sum)
	f.fake.mu.Lock()
	leaseID := prov.Windows[1].LeaseID
	captured := f.fake.lease(f.user.PlatformUserID, leaseID).Captured
	f.fake.mu.Unlock()
	require.Equal(t, totalUnits, captured, "the fake's captured sum on the session's lease equals the windows' sum")
}

// ---------------------------------------------------------------------------
// Test 50 — a crash between the pending_units write and the advance
// re-issues the SAME amount (§13.2.2's recovery rule), via the established
// fault-injection idiom (3.5-G's test 34 crash leg; round-1 MINOR-2). The
// takeover-based crash is test 52's job — this one is deterministic.
// ---------------------------------------------------------------------------

type p37bFaultProvisionalStore struct {
	LiveProvisionalStore
	advanceFailures int32
}

func (s *p37bFaultProvisionalStore) AdvanceLiveWindow(ctx context.Context, token string, seq int, settledUnits int64, next LiveWindow) error {
	if atomic.AddInt32(&s.advanceFailures, -1) >= 0 {
		return errors.New("injected advance failure (crash between pending and advance)")
	}
	return s.LiveProvisionalStore.AdvanceLiveWindow(ctx, token, seq, settledUnits, next)
}

func TestPhase37bCrashBetweenPendingAndAdvanceReissuesSameAmount(t *testing.T) {
	f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
	faulty := &p37bFaultProvisionalStore{LiveProvisionalStore: f.provisional, advanceFailures: 1}
	f.svc.liveProvisional = faulty
	callHash, _ := f.createSession(t)
	prov := f.provRecord(t, callHash)
	E := prov.EstimatedUnits
	tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))
	conflictsBefore := canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()

	f.pumpUsage("resp-50", 0, tokens)
	f.clock.Advance(6 * time.Second)

	// The crash point: pending persisted, advance failed — the window stays
	// pending with the persisted amount (poll; the transient lasts a tick).
	var persisted int64
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		prov = f.provRecord(t, callHash)
		if len(prov.Windows) == 1 && prov.Windows[0].PendingUnits > 0 {
			persisted = prov.Windows[0].PendingUnits
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Greater(t, persisted, int64(0), "the pending amount was persisted before the advance failed")

	// The next tick re-issues THAT amount and the advance completes.
	prov = f.pollWindowsLen(t, callHash, 2, 5*time.Second)
	require.Equal(t, persisted, prov.Windows[0].SettledUnits, "the re-issued amount is the persisted pending, never a recomputation")
	require.Equal(t, int64(0), prov.Windows[0].PendingUnits)

	// Exactly one outbox row for window 1 — the duplicate submission deduped.
	eventID := CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")
	require.Equal(t, 1, f.outboxRowCount(t, eventID))
	require.Equal(t, persisted, f.outboxRow(t, eventID).Amount)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()-conflictsBefore, "no payload conflict from the re-issue")

	// End the session; Σ == units(total).
	f.conn.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(`{"type":"session.closed"}`)}
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		prov = f.provRecord(t, callHash)
		if prov.Status == LiveProvisionalStatusFinalized {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, LiveProvisionalStatusFinalized, prov.Status)
	rec := f.recordByHash(t, callHash)
	totalUnits, err := liveUsageUnits(rec)
	require.NoError(t, err)
	require.Equal(t, totalUnits, prov.Windows[0].SettledUnits+prov.Windows[1].SettledUnits)
}

// ---------------------------------------------------------------------------
// Test 50(b) — the disabled-mode horizon (§13.2.3, execution review MAJOR-1):
// in disabled mode no settlement is attempted and the clock advances on the
// horizon ALONE so the record shape stays uniform. The advance is keyed on
// the provisional ROW's token — windows ≥ 2 carry no token of their own in
// disabled mode, so a window-keyed advance loses its CAS on every 250 ms
// tick past window 1 (reauthRetry climbing, the window list frozen).
// ---------------------------------------------------------------------------

func TestPhase37bDisabledModeAdvancesOnTheHorizon(t *testing.T) {
	f := newLiveWindowTestFixture(t, config.CanonicalWalletModeDisabled)
	// Two lease horizons need ~60 s — outlive the fixture's 60 s session cap
	// so the max-duration timer cannot race the second horizon.
	f.cfg.Gateway.Live.MaxSessionDurationSeconds = 180
	callHash, _ := f.createSession(t)

	// Disabled mode writes no provisional row of its own (saveLiveProvisional
	// is a no-op there), so seed the row a mid-session mode flip leaves
	// behind — the record an enforce/shadow session had when the gateway was
	// turned down to disabled (3.8's rollback shape), the only shape in which
	// this branch runs. Window 1's token IS the row's token (§13.2.6).
	rec := f.recordByHash(t, callHash)
	now := f.clock.Now().UTC()
	rowToken := "p37b-disabled-row"
	seed := &LiveProvisionalRecord{
		Token:             rowToken,
		AuthorizationID:   rowToken,
		UserID:            rec.UserID,
		APIKeyID:          rec.APIKeyID,
		AccountID:         rec.AccountID,
		PlatformUserID:    rec.PlatformUserID,
		BillingCurrency:   rec.BillingCurrency,
		BillingSnapshotID: rec.BillingSnapshotID,
		EstimatedUnits:    1_000_000,
		Status:            LiveProvisionalStatusProvisional,
		CallHash:          callHash,
		Windows:           []LiveWindow{{WindowSeq: 1, Token: rowToken, OpenedAtMS: now.UnixMilli()}},
		CreatedAt:         now,
	}
	require.NoError(t, f.provisional.Save(context.Background(), seed))
	require.NoError(t, f.provisional.Activate(context.Background(), rowToken, callHash, now))

	reauthBefore := LiveWindowMetricsSnapshot().ReauthRetry

	// Two lease horizons (30 s − 50 ms skew each): windows 2 AND 3 appear.
	f.clock.Advance(31 * time.Second)
	f.pollWindowsLen(t, callHash, 2, 5*time.Second)
	f.clock.Advance(31 * time.Second)
	prov := f.pollWindowsLen(t, callHash, 3, 5*time.Second)

	var windowsLen int
	require.NoError(t, f.db.QueryRowContext(f.ctx,
		`SELECT jsonb_array_length(windows) FROM wallet_live_provisional WHERE call_hash = $1`, callHash).Scan(&windowsLen))
	require.Equal(t, 3, windowsLen, "jsonb_array_length(windows) == 3 — the clock advanced on the horizon alone")
	for _, w := range prov.Windows {
		require.Equal(t, int64(0), w.SettledUnits, "disabled mode settles nothing")
	}
	require.Equal(t, reauthBefore, LiveWindowMetricsSnapshot().ReauthRetry,
		"the horizon advance must never retry: a window-keyed CAS loses on every tick past window 1")
}

// ---------------------------------------------------------------------------
// Test 51 — a refused re-authorization closes the session; the invariant's
// three terms (§13.2.4, §13.3). Legs: (a) balance shortfall; (b)
// lease_cap_reached under §3.3's cap; (c) shadow admits.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Test 51 — refused re-authorizations (consolidated, Phase 4.3-G Task 4):
// three fixtures over ONE newLiveWindowShared claim (one Redis claim, one DB),
// all three sessions created BEFORE any wait so the 30 s horizons elapse
// concurrently; ONE bounded poll waits for all three terminal states; then
// three assertion blocks named BalanceShortfall, LeaseCapReached, and
// ShadowAdmits verify their respective terminal invariants.
// ---------------------------------------------------------------------------

func TestPhase37bRefusedReauthorizations(t *testing.T) {
	sh := newLiveWindowShared(t)

	// Fixture 1: Enforce Mode — Balance Shortfall
	// The budget rides min_headroom (requested budget 0): the session's one
	// lease is granted at exactly E, spent by its own arm — the gateway lease
	// ends with zero remaining, so the boundary's ensure must ISSUE. Funding
	// for the create is drained to zero right after it: balance 1 − E spent
	// at issue → shortfall at the boundary.
	f1 := sh.newFixtureCfg(t, config.CanonicalWalletModeEnforce, func(c *config.CanonicalWalletConfig) {
		c.LeaseBudgetUnits = 0
	})
	f1.fund(0)
	callHash1, created1 := f1.createSessionFunded(t, 20_000_000_000)
	require.NotNil(t, created1)

	// Fixture 2: Enforce Mode — Lease Cap Reached
	// The user's ONE lease (the session's own window-1 lease, purpose
	// authorize, active) does not cover the window (consumed = E,
	// budget = E → headroom 0); the cap is 1, so the boundary's issue
	// attempt is refused with lease_cap_reached.
	f2 := sh.newFixtureCfg(t, config.CanonicalWalletModeEnforce, func(c *config.CanonicalWalletConfig) {
		c.LeaseBudgetUnits = 0
	})
	f2.fund(0)
	callHash2, created2 := f2.createSessionFunded(t, 20_000_000_000)
	require.NotNil(t, created2)

	// Fixture 3: Shadow Mode — Shadow Admits
	// The horizon close's re-authorization is refused — in shadow it is
	// admitted: no session.close, the session continues, the next window
	// opens with no hold.
	f3 := sh.newFixtureCfg(t, config.CanonicalWalletModeShadow, func(c *config.CanonicalWalletConfig) {
		c.LeaseBudgetUnits = 0
	})
	f3.fund(0)
	callHash3, _ := f3.createSessionFunded(t, 20_000_000_000)

	// Drain balance and set cap=1 so boundary re-authorizations are refused
	sh.fake.mu.Lock()
	sh.fake.balance[f1.user.PlatformUserID] = 0
	sh.fake.balance[f2.user.PlatformUserID] = 0
	sh.fake.balance[f3.user.PlatformUserID] = 0
	sh.fake.cap = 1
	sh.fake.mu.Unlock()

	prov1 := f1.provRecord(t, callHash1)
	E1 := prov1.EstimatedUnits
	R1 := E1 / 45
	tokens1 := int(math.Round(float64(R1) / f1.unitsPerOutputToken(t, callHash1)))
	f1.pumpUsage("resp-53a-1", 0, tokens1)

	prov2 := f2.provRecord(t, callHash2)
	E2 := prov2.EstimatedUnits
	R2 := E2 / 45
	tokens2 := int(math.Round(float64(R2) / f2.unitsPerOutputToken(t, callHash2)))
	f2.pumpUsage("resp-53b-1", 0, tokens2)

	prov3 := f3.provRecord(t, callHash3)
	E3 := prov3.EstimatedUnits
	R3 := E3 / 45
	tokens3 := int(math.Round(float64(R3) / f3.unitsPerOutputToken(t, callHash3)))
	f3.pumpUsage("resp-53c-1", 0, tokens3)

	time.Sleep(300 * time.Millisecond)

	f1.clock.Advance(31 * time.Second)
	f2.clock.Advance(31 * time.Second)
	f3.clock.Advance(31 * time.Second)

	// ALL THREE sessions created BEFORE any wait so the horizons elapse concurrently.
	// ONE bounded poll waiting for all three terminal states.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		done1 := f1.recordByHash(t, callHash1).Controller == LiveControllerClosed && f1.provRecord(t, callHash1).Status == LiveProvisionalStatusFinalized
		done2 := f2.recordByHash(t, callHash2).Controller == LiveControllerClosed && f2.provRecord(t, callHash2).Status == LiveProvisionalStatusFinalized
		done3 := len(f3.provRecord(t, callHash3).Windows) >= 2
		if done1 && done2 && done3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Run("BalanceShortfall", func(t *testing.T) {
		controller := f1.recordByHash(t, callHash1).Controller
		prov := f1.provRecord(t, callHash1)
		require.Equal(t, LiveControllerClosed, controller, "the hard stop closed the session")

		// The close frame was written on the sideband (non-billable).
		select {
		case frame := <-f1.conn.writes:
			require.Equal(t, coderws.MessageText, frame.messageType)
			require.JSONEq(t, `{"type":"session.close"}`, string(frame.payload))
		case <-time.After(2 * time.Second):
			t.Fatal("session.close never written on the sideband")
		}

		// The row finalizes; the last window settles the tail with no hold.
		require.Equal(t, LiveProvisionalStatusFinalized, prov.Status)
		eventID1 := CanonicalWalletSettlementEventID(callHash1, f1.user.PlatformUserID, "CNY")
		f1.waitDeliveredThroughFake(t, eventID1, 5*time.Second)
		require.Empty(t, f1.holdIDs(t), "no armed hold remains — the last window settled with no hold")

		// The terms (§13.2.4): the floor term with the known rate —
		// windows[0].settled_units ≤ E_w + R × live_window_min_seconds.
		require.LessOrEqual(t, prov.Windows[0].SettledUnits, E1+R1*5, "the floor term")
		// The tail ≤ the units of one pumped response (the in-flight frame).
		require.Greater(t, prov.Windows[0].SettledUnits, int64(0))
		if len(prov.Windows) > 1 && prov.Windows[1].SettledUnits > 0 {
			require.LessOrEqual(t, prov.Windows[1].SettledUnits, R1+1, "the tail after the refusal ≤ one response's units")
		}
	})

	t.Run("LeaseCapReached", func(t *testing.T) {
		controller := f2.recordByHash(t, callHash2).Controller
		prov := f2.provRecord(t, callHash2)
		require.Equal(t, LiveControllerClosed, controller, "the hard stop closed the session")

		// The close frame was written on the sideband (non-billable).
		select {
		case frame := <-f2.conn.writes:
			require.Equal(t, coderws.MessageText, frame.messageType)
			require.JSONEq(t, `{"type":"session.close"}`, string(frame.payload))
		case <-time.After(2 * time.Second):
			t.Fatal("session.close never written on the sideband")
		}

		// The row finalizes; the last window settles the tail with no hold.
		require.Equal(t, LiveProvisionalStatusFinalized, prov.Status)
		require.Empty(t, f2.holdIDs(t), "no armed hold remains — the last window settled with no hold")

		// The terms (§13.2.4): the floor term with the known rate —
		// windows[0].settled_units ≤ E_w + R × live_window_min_seconds.
		require.LessOrEqual(t, prov.Windows[0].SettledUnits, E2+R2*5, "the floor term")
		// The tail ≤ the units of one pumped response (the in-flight frame).
		require.Greater(t, prov.Windows[0].SettledUnits, int64(0))
		if len(prov.Windows) > 1 && prov.Windows[1].SettledUnits > 0 {
			require.LessOrEqual(t, prov.Windows[1].SettledUnits, R2+1, "the tail after the refusal ≤ one response's units")
		}
	})

	t.Run("ShadowAdmits", func(t *testing.T) {
		prov := f3.provRecord(t, callHash3)
		require.GreaterOrEqual(t, len(prov.Windows), 2, "the refused window advanced in shadow")
		require.Equal(t, int64(1), LiveWindowMetricsSnapshot().RefusedShadow)
		require.NotEqual(t, LiveControllerClosed, f3.recordByHash(t, callHash3).Controller, "no hard stop in shadow")
		require.Empty(t, f3.holdIDs(t), "the next window opens with no hold")
		select {
		case <-f3.conn.writes:
			t.Fatal("shadow must not write session.close")
		default:
		}
	})
}

// ---------------------------------------------------------------------------
// Test 52 — controller loss during an over-budget session: instance A dies
// mid-window (its context cancelled — the honest crash: the loop stops
// without releasing the controller or heartbeating); B takes over within
// 2 × live_controller_takeover_seconds, closes the window, and the hard stop
// still fires on the refused re-authorization; A revived against the same
// record gets ErrLiveControllerChanged on its first tick.
// ---------------------------------------------------------------------------

func TestPhase37bControllerLossAndTakeover(t *testing.T) {
	sh := newLiveWindowShared(t)
	// The exactly-E lease shape (requested budget 0) on BOTH instances: the
	// session's one lease ends with zero remaining, so the boundary's ensure
	// must ISSUE — and the fake's canned refusal answers it. Window 1's
	// over-estimate amount then converts through the {4} overrun path (the
	// hold released, the event unbound) — money-safe, and the over-budget
	// shape is exactly what this test exists to drive.
	shBudget := func(c *config.CanonicalWalletConfig) { c.LeaseBudgetUnits = 0 }
	fA := sh.newFixtureCfg(t, config.CanonicalWalletModeEnforce, shBudget)
	callHash, _ := fA.createSession(t)
	prov := fA.provRecord(t, callHash)
	E := prov.EstimatedUnits
	tokens := int(math.Ceil(2 * float64(E) / fA.unitsPerOutputToken(t, callHash)))

	// Pump past E_w on A's conn, then kill A mid-window (before the floor).
	fA.pumpUsage("resp-52a", 0, tokens)
	time.Sleep(300 * time.Millisecond) // let the frame be read and accumulated
	fA.cancel()

	// B on the same stores: its observer loops on the takeover interval.
	fB := sh.newFixtureCfg(t, config.CanonicalWalletModeEnforce, shBudget)
	// The fake refuses the re-authorization (balance shortfall): the hard
	// stop must fire from B after it closes the window. The canned response
	// is path-scoped — the settlements route (window 1's settle) is untouched.
	fB.fake.respondWith("/api/internal/v2/wallet/leases/ensure", 409,
		`{"code":-1,"message":"balance_shortfall","data":{"reason":"balance_shortfall"}}`, -1)
	go fB.svc.observeLiveCall(fB.ctx, callHash)

	// B takes over within 2 × takeover (4 s) — evidenced by its dial — and
	// the window closes under B.
	takeoverDeadline := time.Now().Add(2 * fB.svc.liveControllerTakeoverInterval() * 2)
	for time.Now().Before(takeoverDeadline) {
		if fB.dialer.url != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotEmpty(t, fB.dialer.url, "B took over within 2 takeover intervals and dialed the sideband")
	prov = fA.pollWindowsLen(t, callHash, 2, 10*time.Second)
	require.Greater(t, prov.Windows[0].SettledUnits, int64(0), "the window closed under B")

	// The hard stop fires from B: the session closes and B's conn carries the
	// non-billable session.close.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if fA.recordByHash(t, callHash).Controller == LiveControllerClosed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, LiveControllerClosed, fA.recordByHash(t, callHash).Controller, "the hard stop fired from B")
	select {
	case frame := <-fB.conn.writes:
		require.JSONEq(t, `{"type":"session.close"}`, string(frame.payload))
	case <-time.After(2 * time.Second):
		t.Fatal("B never wrote session.close")
	}

	// A revived against the same record: ErrLiveControllerChanged on its
	// first controller tick.
	done := make(chan error, 1)
	go func() {
		done <- fA.svc.runLiveObserverConnection(context.Background(), fA.recordByHash(t, callHash), newLiveTestFrameConn(), "owner-A-revived")
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrLiveControllerChanged)
	case <-time.After(3 * time.Second):
		t.Fatal("the revived observer never exited")
	}
}

// ---------------------------------------------------------------------------
// Test 53 — a disconnected sideband (G14's shape): the observer keeps
// retrying at waitForLiveObserverRetry's one-second cadence, nothing closes
// and nothing is billed during the outage; after reconnect the next close is
// normal. The outage's usage is UNOBSERVED — the outbox has no row for it.
// ---------------------------------------------------------------------------

type p37bFlakyDialer struct {
	inner    *liveTestDialer
	failures int32
	attempts int32
}

func (d *p37bFlakyDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	n := atomic.AddInt32(&d.attempts, 1)
	if atomic.LoadInt32(&d.failures) > 0 {
		atomic.AddInt32(&d.failures, -1)
		return nil, http.StatusServiceUnavailable, nil, fmt.Errorf("sideband outage (attempt %d)", n)
	}
	return d.inner.Dial(ctx, wsURL, headers, proxyURL)
}

func TestPhase37bDisconnectedSideband(t *testing.T) {
	f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
	flaky := &p37bFlakyDialer{inner: f.dialer, failures: 7}
	f.svc.openaiWSPassthroughDialer = flaky
	callHash, _ := f.createSession(t)
	prov := f.provRecord(t, callHash)
	E := prov.EstimatedUnits

	// Hold the outage for floor + 2 s = 7 s (7 failing dials at the observer's
	// one-second retry cadence). During the outage nothing is pumped —
	// nothing is connected, that is the point.
	conflictsBefore := canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()
	outageEnd := time.Now().Add(7 * time.Second)
	for time.Now().Before(outageEnd) {
		p := f.provRecord(t, callHash)
		require.Len(t, p.Windows, 1, "no window closes during the outage")
		require.Equal(t, int64(0), p.Windows[0].PendingUnits)
		time.Sleep(200 * time.Millisecond)
	}
	require.Zero(t, f.outboxRowCount(t, CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")), "no settlement during the outage")
	attempts := atomic.LoadInt32(&flaky.attempts)
	require.GreaterOrEqual(t, attempts, int32(5), "the observer keeps retrying (under-runs are diagnosable)")
	require.LessOrEqual(t, attempts, int32(9), "the retry cadence is the one-second waitForLiveObserverRetry")

	// Reconnect: the next dial succeeds; pump past E_w; the floor has long
	// passed, so the next close is normal — a row with the pumped amount.
	tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))
	f.pumpUsage("resp-53", 0, tokens)
	f.clock.Advance(6 * time.Second)
	prov = f.pollWindowsLen(t, callHash, 2, 8*time.Second)
	require.Equal(t, f.unitsForTokens(t, f.recordByHash(t, callHash), 0, tokens, 0), prov.Windows[0].SettledUnits)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.outboxPayloadConflict.Load()-conflictsBefore)
}

type staticLiveClock struct {
	t time.Time
}

func (c staticLiveClock) Now() time.Time {
	return c.t
}

func TestPhase37bLiveClockSeam(t *testing.T) {
	t.Run("default is realLiveClock", func(t *testing.T) {
		svc := &OpenAIGatewayService{liveClock: realLiveClock{}}
		before := time.Now().UTC()
		now := svc.liveNow()
		after := time.Now().UTC()
		require.False(t, now.Before(before))
		require.False(t, now.After(after))
	})

	t.Run("nil receiver safe", func(t *testing.T) {
		var svc *OpenAIGatewayService
		now := svc.liveNow()
		require.False(t, now.IsZero())
	})

	t.Run("injectable via SetLiveClockForTest", func(t *testing.T) {
		svc := &OpenAIGatewayService{liveClock: realLiveClock{}}
		pinned := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
		svc.SetLiveClockForTest(staticLiveClock{t: pinned})
		require.Equal(t, pinned, svc.liveNow())
	})
}
