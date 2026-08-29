//go:build integration

// Phase 3.7a pins (redesign §13.3): the stoppable dispatcher's own test
// (§13.2.6) and test 48 (G7) live here; test 46 (the streaming differential
// invariant) is added below by Task 5, and test 47 lives beside its harness
// in openai_ws_v2_passthrough_authorization_test.go under that file's tag.

package service

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// countingOutbox wraps an outbox store and counts ClaimPendingOutboxEvents
// calls — the observable heartbeat of the dispatcher's tick loop.
type countingOutbox struct {
	CanonicalWalletOutboxStore
	claims atomic.Int64
}

func (c *countingOutbox) ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]CanonicalWalletOutboxEvent, error) {
	c.claims.Add(1)
	return c.CanonicalWalletOutboxStore.ClaimPendingOutboxEvents(ctx, workerID, limit)
}

// hangingRoundTripper (3.7a review note 2) holds every request inside the
// transport until the request's own context cancels: the enqueued row's
// delivery is thereby provably IN FLIGHT when Close runs — entry is
// signalled, then the RoundTrip blocks — while the request never reaches
// the fake (no handler delay, no f.mu contention for the reaper leg below).
type hangingRoundTripper struct{ entered chan struct{} }

func (h *hangingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-r.Context().Done()
	return nil, r.Context().Err()
}

// TestPhase37DispatcherStopsOnClose (redesign §13.2.6): Close stops BOTH tick
// loops — the dispatcher's claim counter and the reaper's reaper_ticks are
// unchanged across a ten-tick window after Close returns, and Close is
// idempotent and nil-safe. Close has no production caller; this test is the
// facility's contract for every t.Cleanup(b.Close) the suite registers.
func TestPhase37DispatcherStopsOnClose(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	counting := &countingOutbox{CanonicalWalletOutboxStore: &outboxStoreForTest{db: db}}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 20 // a 20 ms tick
	cfg.LeaseBudgetUnits = 500_000_000
	hanging := &hangingRoundTripper{entered: make(chan struct{}, 1)}
	client := newCanonicalWalletHTTPClient(cfg, &http.Client{Transport: hanging})
	client.now = func() time.Time { return now }
	b := newCanonicalWalletBridge(cfg, store, client, db, counting, 0, func() time.Time { return now })
	t.Cleanup(b.Close)

	// The loop is live: poll (the file's idiom — 50 ms steps, bounded
	// deadline) until the claim counter reaches three ticks.
	deadline := time.Now().Add(30 * time.Second)
	for counting.claims.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.GreaterOrEqual(t, counting.claims.Load(), int64(3), "the dispatcher loop is live")

	// Review note 2: make the in-flight clause below true instead of
	// dropping it — enqueue one settlement row; its delivery's ensure POST
	// (a lease-less row resolves its lease first) is signalled and held in
	// the hanging transport until the per-event context cancels, so a
	// delivery is in flight across the Close that follows.
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-37a-stop-in-flight", PlatformUserID: user, Currency: "CNY", AmountUnits: 1_000_000, OccurredAt: now,
	}))
	select {
	case <-hanging.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the enqueued row's delivery never started — no delivery could be in flight at Close")
	}

	// Close blocks until both loops have exited — it must return well inside
	// the two-second bound even with a delivery in flight (the row above:
	// its ensure POST sits in the transport until the 20 ms per-event
	// context cancels, then the loop observes stop and exits).
	closed := make(chan struct{})
	go func() {
		b.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2 s")
	}

	// Absence window 1 of the plan's TWO (constraint 4): the absence of a
	// further claim cannot be polled for — there is no positive event to
	// await — so ten ticks of the 20 ms loop must pass silent. The reaper
	// leg below carries absence window 2 for the same reason.
	claimsAtClose := counting.claims.Load()
	<-time.After(200 * time.Millisecond)
	require.Equal(t, claimsAtClose, counting.claims.Load(), "no further claim after Close")

	// Idempotent: a second Close returns immediately.
	b.Close()
	// Nil-safe: a bare nil bridge is a no-op, not a panic.
	var nilBridge *CanonicalWalletBridge
	nilBridge.Close()

	// Leg 2 — the reaper loop stops too: holds on (p34bHoldsConfig) and a
	// non-nil outboxDB start runHoldReaper alongside the dispatcher; the
	// global reaper_ticks counter (incremented once per tick before
	// reapOnce) is its heartbeat, asserted as a delta on the global.
	holdCfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
	holdCfg.ControlPlaneURL, holdCfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	holdCfg.ExpirySkewMarginMS, holdCfg.RequestTimeoutMS = 50, 20
	holdCfg.LeaseBudgetUnits = 500_000_000
	holdCfg.OrphanSweepIntervalSeconds = 1
	holdClient := newCanonicalWalletHTTPClient(holdCfg, fake.Server.Client())
	holdClient.now = func() time.Time { return now }
	reaper := newCanonicalWalletBridge(holdCfg, store, holdClient, db, counting, 0, func() time.Time { return now })
	t.Cleanup(reaper.Close)

	// Review note 3: the delta below is asserted on the GLOBAL
	// canonicalWalletBridgeMetrics.reaperTicks — sound only because this
	// integration package runs strictly sequentially and every bridge is
	// closed at its cleanup (both properties verified by the 3.7a review);
	// 3.7c makes the same two properties load-bearing for its shared Redis.
	ticksBase := canonicalWalletBridgeMetrics.reaperTicks.Load()
	deadline = time.Now().Add(30 * time.Second)
	for canonicalWalletBridgeMetrics.reaperTicks.Load() == ticksBase && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.Greater(t, canonicalWalletBridgeMetrics.reaperTicks.Load(), ticksBase, "the reaper loop is live")
	reaper.Close()
	ticksAtClose := canonicalWalletBridgeMetrics.reaperTicks.Load()
	// Absence window 2 of 2 (constraint 4): as above, the absence of a
	// further reaper tick has no positive form to poll for.
	<-time.After(200 * time.Millisecond)
	require.Equal(t, ticksAtClose, canonicalWalletBridgeMetrics.reaperTicks.Load(), "reapOnce is never entered again after Close")
}

// p37Authorize is p34bAuthorize's body returning the fixture too (p34bAuthorize
// itself is unchanged): the real authorization point with the snapshot
// fixture available under the integration tag. The lease budget ask is 600M
// (not p34bAuthorize's 500M) so test 46's leg-2 overrun — 50 × 512 output
// tokens ≈ 504M units at the fixture's prices — stays WITHIN the lease's
// budget (§10.5's within-budget branch, "fund the lease so it is").
func p37Authorize(t *testing.T, ctx context.Context, mode string, b *CanonicalWalletBridge, user, body string) (*AuthorizationHandle, *BillingSnapshotService, *BillingSnapshot) {
	t.Helper()
	snapshots, apiKey, _, account, _, _, _ := NewSnapshotTestFixtureForTest(t)
	snap, err := snapshots.Freeze(ctx, FreezeInput{APIKey: apiKey, User: apiKey.User, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	require.NoError(t, err)
	require.NotNil(t, snap)
	apiKey.User.PlatformUserID = user
	apiKey.User.BillingCurrency = "CNY"
	cfg := &config.Config{}
	cfg.CanonicalWallet = p34bHoldsConfig(mode)
	cfg.CanonicalWallet.RequestTimeoutMS = 300
	cfg.CanonicalWallet.LeaseBudgetUnits = 600_000_000
	auth := NewCanonicalWalletAuthorizer(cfg, b, snapshots)
	h, err := auth.Authorize(ctx, AuthorizeInput{Snapshot: snap, Estimate: EstimateInputFromRequestBody([]byte(body), EstimateInputOptions{}), User: apiKey.User})
	if err != nil {
		// enforce mode legitimately refuses; anything else is a failure
		require.True(t, errors.Is(err, ErrAuthorizationRefused), "unexpected authorize error: %v", err)
	}
	require.NotNil(t, h)
	return h, snapshots, snap
}

// Test 46 (§13.1/§13.3) — the END-TO-END streaming form of the differential
// invariant: the request's max_output_tokens = N bounds the attempt, the
// SSE terminal event's usage settles at or below the finite estimate, and
// the hold converts A ≤ E. Leg 2 pins the one overrun the gateway cannot
// prevent — the upstream exceeding its own declared maximum — as §10.5's
// PRICED excess, never a refusal.
func TestPhase37StreamingInvariant(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 600_000_000) // one lease, funded so even leg 2's overrun fits

	cfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 300
	cfg.LeaseBudgetUnits = 600_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, func() time.Time { return now })
	t.Cleanup(b.Close)

	// Leg 1 "the upstream honours max_output_tokens": a ~2 KB body, N = 512.
	const N = 512
	body := `{"model":"claude-sonnet-4","max_tokens":` + itoa(N) + `,"input":"` + strings.Repeat("a", 2000) + `"}`
	h1, snapshots1, snap1 := p37Authorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, body)
	require.Nil(t, h1.Refusal)
	require.True(t, h1.HoldArmed)

	// parse the terminal SSE event the passthrough path parses — on the zero
	// value of the service (the parsing path reads no receiver field).
	sse := `{"type":"response.completed","response":{"usage":{"input_tokens":1500,"output_tokens":` + itoa(N) + `}}}`
	var usage OpenAIUsage
	(&OpenAIGatewayService{}).parseSSEUsageBytes([]byte(sse), &usage)
	require.Equal(t, 1500, usage.InputTokens)
	require.Equal(t, N, usage.OutputTokens)

	cost1, err := snapshots1.billing.CalculateCostFromSnapshot(snap1, SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens}})
	require.NoError(t, err)
	A := p37SettledUnits(t, snapshots1, snap1, cost1)
	t.Logf("test 46 leg 1: E=%d A=%d", h1.EstimatedUnits, A)
	require.LessOrEqual(t, A, h1.EstimatedUnits, "the SSE-reported usage settles at or below the finite estimate")

	convertedBase := canonicalWalletBridgeMetrics.holdConverted.Load()
	overrunBase := canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Load()
	releasedBefore := p34bHashField(t, ctx, store, user, h1.LeaseID, "released_units")
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-46", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: h1.ID}))
	hold1, err := store.GetCanonicalWalletHold(ctx, user, h1.ID)
	require.NoError(t, err)
	require.Equal(t, "settled", hold1.State)
	require.Equal(t, h1.EstimatedUnits-A, mustHashUnits(t, p34bHashField(t, ctx, store, user, h1.LeaseID, "released_units"))-mustHashUnits(t, releasedBefore), "released_units delta == E − A")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdConverted.Load()-convertedBase)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Load()-overrunBase)

	// Leg 2 "the upstream ignores its own maximum" (the only overrun the
	// gateway cannot prevent): a small body so the estimate is
	// output-dominated, output_tokens = 50 × N — far beyond the declared
	// maximum. Its own user, so the lease hash's consumed == A' exactly.
	user2 := "shipany-user-" + uuid.NewString()
	fake.fund(user2, 600_000_000)
	smallBody := `{"model":"claude-sonnet-4","max_tokens":` + itoa(N) + `,"input":"hi"}`
	h2, snapshots2, snap2 := p37Authorize(t, ctx, config.CanonicalWalletModeEnforce, b, user2, smallBody)
	require.Nil(t, h2.Refusal)
	require.True(t, h2.HoldArmed)
	sse2 := `{"type":"response.completed","response":{"usage":{"input_tokens":40,"output_tokens":` + itoa(50*N) + `}}}`
	var usage2 OpenAIUsage
	(&OpenAIGatewayService{}).parseSSEUsageBytes([]byte(sse2), &usage2)
	require.Equal(t, 50*N, usage2.OutputTokens)
	cost2, err := snapshots2.billing.CalculateCostFromSnapshot(snap2, SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: usage2.InputTokens, OutputTokens: usage2.OutputTokens}})
	require.NoError(t, err)
	A2 := p37SettledUnits(t, snapshots2, snap2, cost2)
	t.Logf("test 46 leg 2: E=%d A'=%d (output_tokens=%d)", h2.EstimatedUnits, A2, usage2.OutputTokens)
	// Unconditional: if this fails the fixture is not exercising the overrun
	// and the leg pins nothing — adjust the usage until it holds (§13.3).
	require.Greater(t, A2, h2.EstimatedUnits, "50 × N output tokens must overrun the N-bounded estimate")

	// §10.5's priced excess, within budget (the lease was funded so A' fits):
	// consumed += excess → consumed == A', the hold settles, nothing releases.
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-46-overrun", PlatformUserID: user2, Currency: "CNY", AmountUnits: A2, OccurredAt: now, AuthorizationID: h2.ID}))
	require.Equal(t, A2, mustHashUnits(t, p34bHashField(t, ctx, store, user2, h2.LeaseID, "consumed_units")), "consumed_units == A' — the excess was priced, never refused")
	hold2, err := store.GetCanonicalWalletHold(ctx, user2, h2.ID)
	require.NoError(t, err)
	require.Equal(t, "settled", hold2.State)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Load()-overrunBase, "within budget: the overrun released nothing")
}

// mustHashUnits parses a lease hash field's numeric string.
func mustHashUnits(t *testing.T, v string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(v, 10, 64)
	require.NoError(t, err)
	return n
}

// p37SettledUnits mirrors settledUnitsForTest (billing_snapshot_estimator_
// test.go, unit-tagged — invisible under the integration tag): ResolveCost-
// Settlement rewrites cost.ActualCost into CNY in place with the snapshot's
// pinned FX, then canonicalWalletUnitsFromSnapshot converts to units.
func p37SettledUnits(t *testing.T, svc *BillingSnapshotService, snap *BillingSnapshot, cost *CostBreakdown) int64 {
	t.Helper()
	_, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), cost, &User{BillingCurrency: "CNY"}, false, svc.exchangeRates, svc.cfg)
	require.NoError(t, err)
	units, err := canonicalWalletUnitsFromCNY(cost.ActualCost)
	require.NoError(t, err)
	return units
}

// Test 48 (G7, redesign §13.3) — two dispatchers against one outbox deliver
// a shared work set exactly once: every row delivered, no dead-letter, and
// the fake captured each event id exactly once with per-user captured sums
// equal to the issued amounts. The TOTAL settlement-request count is an
// OBSERVATION, not an assertion: if a stale reclaim ever raced a redelivery
// the fake's per-event identity answers duplicate and the exactly-once
// property (on captures) still holds — a count above N is recorded, never a
// failure.
func TestPhase37TwoDispatchersDeliverExactlyOnce(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce) // holds off — the dispatcher's own claim race
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 100 // a 100 ms tick
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	clock := func() time.Time { return now }
	a := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, clock)
	t.Cleanup(a.Close)
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, clock)
	t.Cleanup(b.Close)
	require.NotEqual(t, a.workerID, b.workerID, "distinct worker ids by construction")

	const n = 40
	users := make([]string, n/4) // leases are shared across each user's events
	want := make(map[string]int64)
	for i := range users {
		users[i] = "shipany-user-" + uuid.NewString()
		fake.fund(users[i], 10_000_000_000)
	}
	for i := 0; i < n; i++ {
		u := users[i%(n/4)]
		amt := int64(1_000_000) * int64(i+1)
		want[u] += amt
		bridge := a
		if i%2 == 1 {
			bridge = b // 20 on A, 20 on B
		}
		require.True(t, bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-48-" + itoa(i), PlatformUserID: u, Currency: "CNY", AmountUnits: amt, OccurredAt: now,
		}))
	}

	// Poll (the file's idiom) until every row is delivered.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var delivered int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'delivered'`).Scan(&delivered); err == nil && delivered == n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var delivered int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'delivered'`).Scan(&delivered))
	require.Equal(t, n, delivered, "every row delivered")

	var dead, inFlight, pending int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'dead_letter'`).Scan(&dead))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'in_flight'`).Scan(&inFlight))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'pending'`).Scan(&pending))
	require.Equal(t, 0, dead, "no dead_letter")
	require.Equal(t, 0, inFlight, "no in_flight")
	require.Equal(t, 0, pending, "no pending")

	// Each event id captured exactly once; per-user captured sums exact.
	fake.mu.Lock()
	distinct := 0
	for _, byEvent := range fake.events {
		distinct += len(byEvent)
	}
	for i := 0; i < n; i++ {
		u := users[i%(n/4)]
		eventID := CanonicalWalletSettlementEventID("req-48-"+itoa(i), u, "CNY")
		ev, ok := fake.events[u][eventID]
		require.True(t, ok, "event %s captured", eventID)
		require.Equal(t, int64(1_000_000)*int64(i+1), ev.Units, "event %s captured at its exact amount", eventID)
	}
	capturedByUser := make(map[string]int64)
	for u, leases := range fake.leases {
		for _, l := range leases {
			capturedByUser[u] += l.Captured
		}
	}
	totalReqs := len(fake.settlementReqs)
	fake.mu.Unlock()
	require.Equal(t, n, distinct, "exactly N distinct event ids captured")
	for u, sum := range want {
		require.Equal(t, sum, capturedByUser[u], "user %s: the fake's captured sum equals its Σ amounts", u)
	}
	t.Logf("test 48: total settlement requests received == %d (N == %d; an observation — exactly-once is on captures)", totalReqs, n)
}
