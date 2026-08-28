//go:build integration

package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Phase 3.4 prototype, Sub2API half — redesign §7 tests 14–20.
// Every lease expiry in these tests is decided by the bridge's injectable
// clock (bridge.now), never by waiting; Redis keys are given a real
// expires_at ten seconds ahead so Redis itself never evicts them mid-test.
// Server-side state on the fake is set under fake.mu (see the fake file).

const p34Margin = 100 * time.Millisecond

// p34Bridge builds a bridge over real Redis and the given control plane with
// a fixed, injectable clock. cfgMode is shadow or enforce.
func p34Bridge(t *testing.T, ctx context.Context, cfgMode string, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane, now time.Time) *CanonicalWalletBridge {
	t.Helper()
	cfg := canonicalWalletTestConfig(cfgMode)
	cfg.ExpirySkewMarginMS = int(p34Margin / time.Millisecond)
	cfg.LeaseBudgetUnits = 500_000_000 // 5 CNY, the production default
	b := newCanonicalWalletBridge(cfg, store, control, nil, nil, 0)
	b.now = func() time.Time { return now }
	return b
}

// Test 15a (seal): after a seal, a NEW reservation on the lease is refused by
// the reserve script's budget guard; a retry carrying an existing marker still
// succeeds (duplicate check runs before the budget check — verified against
// reserveCanonicalWalletLeaseScript's order: EXISTS → currency → expiry →
// marker → budget); the current pointer is deleted; the pre-seal consumed is
// returned; a second seal is idempotent.
func TestPhase34Proto15aSealRefusesNewReservationsButHonoursMarkers(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	user := "shipany-user-" + uuid.NewString()
	expires := time.Now().UTC().Add(10 * time.Second)
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-seal", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	_, err := store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-before", 30_000_000, time.Now().UTC())
	require.NoError(t, err)

	pre, err := store.SealCanonicalWalletLease(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, int64(30_000_000), pre, "the seal returns the PRE-seal consumed")

	sealed, err := store.GetCanonicalWalletLeaseByID(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, sealed.BudgetUnits, sealed.ConsumedUnits, "consumed == budget after the seal")
	_, err = store.GetCanonicalWalletLease(ctx, user)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing, "the current pointer is deleted by the seal")

	_, err = store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-after", 1, time.Now().UTC())
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExhausted, "a new reservation on a sealed lease is refused")

	dup, err := store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-before", 30_000_000, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, dup.Duplicate, "a retry carrying its marker still succeeds on the sealed lease")

	again, err := store.SealCanonicalWalletLease(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, sealed.BudgetUnits, again, "a second seal is idempotent and returns the already-sealed consumed (== budget)")

	_, err = store.SealCanonicalWalletLease(ctx, user, "lease-absent")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing)
}

// Test 18 — skew margin: a lease inside the margin is treated as expired.
func TestPhase34Proto18SkewMarginTreatsNearExpiryAsExpired(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	user := "shipany-user-" + uuid.NewString()
	wall := time.Now().UTC()
	expires := wall.Add(10 * time.Second) // Redis keeps the key alive for the whole test
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-margin", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	control := &canonicalWalletControlStub{} // records ensure calls; returns a fresh lease when asked

	// 50 ms before expiry, inside the 100 ms margin → expired → ensure is called.
	control.lease = CanonicalWalletLease{LeaseID: "lease-fresh", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires.Add(time.Minute)}
	b := p34Bridge(t, ctx, config.CanonicalWalletModeEnforce, store, control, expires.Add(-50*time.Millisecond))
	lease, err := b.ensureLease(ctx, user, "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "lease-fresh", lease.LeaseID)
	require.Equal(t, 1, control.ensureCalls)

	// 150 ms before expiry, outside the margin → cache hit, no ensure.
	control2 := &canonicalWalletControlStub{}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-margin-2", PlatformUserID: user + "-b", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	b2 := p34Bridge(t, ctx, config.CanonicalWalletModeEnforce, store, control2, expires.Add(-150*time.Millisecond))
	lease, err = b2.ensureLease(ctx, user+"-b", "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "lease-margin-2", lease.LeaseID)
	require.Equal(t, 0, control2.ensureCalls)
}

// p34HTTPBridge: real Redis + the §3 fake over HTTP + a fixed clock shared by both.
func p34HTTPBridge(t *testing.T, ctx context.Context, fake *fakeEnsureControlPlane, store CanonicalWalletLeaseStore, now time.Time) *CanonicalWalletBridge {
	t.Helper()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS = int(p34Margin / time.Millisecond)
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	b := newCanonicalWalletBridge(cfg, store, client, nil, nil, 0)
	b.now = func() time.Time { return now }
	return b
}

// p34Fill issues (or reuses) a lease for a 100,000,000 ask, exhausts it on the
// GATEWAY (reserve 450,000,000 → remaining 50,000,000) and settles it partially
// on the SERVER (captured → headroom below the ask, above zero): the lease is
// then non-covering for the next ask, still slot-holding, and its drain does
// not verify (captured ≠ the reported consumed) unless captured == 450,000,000.
func p34Fill(t *testing.T, ctx context.Context, b *CanonicalWalletBridge, store CanonicalWalletLeaseStore, fake *fakeEnsureControlPlane, user, tag string, captured int64, now time.Time) *CanonicalWalletLease {
	t.Helper()
	l, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, user, l.LeaseID, "CNY", "evt-"+tag, 450_000_000, now)
	require.NoError(t, err)
	fake.setCaptured(user, l.LeaseID, captured)
	return l
}

// Test 14 — Redis loss with a covering lease: ensure answers reused, zero
// issuance; the reinstalled consumed is captured + released; the re-authorized
// total is bounded by budget − captured (not merely budget); the install's
// max(current, incoming) keeps consumed monotone.
func TestPhase34Proto14RedisLossReinstallsCoveringLease(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	b := p34HTTPBridge(t, ctx, fake, store, now)

	first, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, 1, fake.issuances)
	// live gateway reservations before the loss: 200,000,000 held locally …
	_, err = store.ReserveCanonicalWalletLease(ctx, user, first.LeaseID, "CNY", "evt-live", 200_000_000, now)
	require.NoError(t, err)
	// … of which 100,000,000 has been captured on the server, so the bound
	// below is budget − captured = 400,000,000 and not merely budget.
	fake.setCaptured(user, first.LeaseID, 100_000_000)

	require.NoError(t, rdb.FlushAll(ctx).Err()) // the loss

	again, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, first.LeaseID, again.LeaseID, "reused, not issued")
	require.Equal(t, 1, fake.issuances, "zero issuance on a Redis loss with a covering lease")
	require.Empty(t, fake.requests[len(fake.requests)-1].Drained, "nothing in hand to drain after the loss")
	require.Equal(t, int64(100_000_000), again.ConsumedUnits, "reinstalled consumed = captured + released: the outstanding 100,000,000 of the live reservation is unknown to the server")
	require.Equal(t, int64(400_000_000), again.RemainingUnits(), "budget − captured is the re-authorizable bound")

	// The gateway can reserve up to budget − captured again, and no more.
	_, err = store.ReserveCanonicalWalletLease(ctx, user, first.LeaseID, "CNY", "evt-post-1", 400_000_000, now)
	require.NoError(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, user, first.LeaseID, "CNY", "evt-post-2", 1, now)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExhausted)

	// Monotone install: a reinstall with a LOWER consumed never lowers Redis's consumed.
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{LeaseID: first.LeaseID, PlatformUserID: user, Currency: "CNY", BudgetUnits: first.BudgetUnits, ConsumedUnits: 0, ExpiresAt: first.ExpiresAt}))
	cur, err := store.GetCanonicalWalletLeaseByID(ctx, user, first.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(500_000_000), cur.ConsumedUnits, "max(current, incoming) kept consumed at the budget")
}

// Test 15 (exhaustion and cap) — exhaustion in-window → a second lease; the
// replaced lease is sealed and sent as drained with the pre-seal consumed and
// is NOT closed (captured ≠ consumed); at the cap → ErrCanonicalWalletLeaseCapReached
// on the authorize path (the sealed lease's headroom forfeited — §3.3's stated
// price) and an issuance on the settle path; caller_slot_ttl_seconds is on the wire.
func TestPhase34Proto15ExhaustionSealsDrainsAndHitsTheCap(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	b := p34HTTPBridge(t, ctx, fake, store, now)

	l1 := p34Fill(t, ctx, b, store, fake, user, "1", 420_000_000, now)
	require.Equal(t, 1, fake.issuances)
	require.Equal(t, 1800, fake.requests[0].CallerSlotTTLSeconds, "caller_slot_ttl_seconds on the wire")

	// exhaustion in-window: seal l1, drain it (450,000,000 ≠ 420,000,000 → not closed), issue l2
	l2 := p34Fill(t, ctx, b, store, fake, user, "2", 420_000_000, now)
	require.NotEqual(t, l1.LeaseID, l2.LeaseID)
	require.Equal(t, 2, fake.issuances)
	drainReq := fake.requests[1]
	require.Len(t, drainReq.Drained, 1)
	require.Equal(t, l1.LeaseID, drainReq.Drained[0].LeaseID)
	require.Equal(t, int64(450_000_000), drainReq.Drained[0].GatewayConsumedUnits, "the PRE-seal consumed is what is sent")
	sealed, err := store.GetCanonicalWalletLeaseByID(ctx, user, l1.LeaseID)
	require.NoError(t, err)
	require.Equal(t, sealed.BudgetUnits, sealed.ConsumedUnits, "l1 is sealed")
	require.Equal(t, "active", fake.status(user, l1.LeaseID), "not closed: captured (420,000,000) ≠ consumed (450,000,000)")

	l3 := p34Fill(t, ctx, b, store, fake, user, "3", 420_000_000, now)
	require.NotEqual(t, l2.LeaseID, l3.LeaseID)
	require.Equal(t, 3, fake.issuances)

	// at the cap (l1, l2, l3 all slot-holding): authorize refuses, settle issues
	_, err = b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseCapReached)
	require.Equal(t, 3, fake.issuances)
	l4, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeSettle, "")
	require.NoError(t, err)
	require.Equal(t, 4, fake.issuances)
	require.Equal(t, "settle", fake.purpose(user, l4.LeaseID))

	// lease_contention is transient: surfaced as its own error, nothing installed
	fake.setContentionOnce()
	_, err = b.ensureLease(ctx, user+"-c", "CNY", 1, canonicalWalletLeasePurposeAuthorize, "")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseContention)
}

// Test 15 (drain half) — the cap opens iff the server's captured_units equals
// the pre-seal consumed the gateway reports on the SAME ensure that seals it
// (§3.3: on a refused ensure the seal is not undone, so a later drain of an
// already-sealed lease can never verify).
func TestPhase34Proto15DrainOpensTheSlotWhenCapturedEqualsConsumed(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	b := p34HTTPBridge(t, ctx, fake, store, now)

	p34Fill(t, ctx, b, store, fake, user, "1", 420_000_000, now)
	p34Fill(t, ctx, b, store, fake, user, "2", 420_000_000, now)
	l3 := p34Fill(t, ctx, b, store, fake, user, "3", 450_000_000, now) // fully settled on the server
	require.Equal(t, 3, fake.issuances)

	// the next ensure seals l3 (pre-seal consumed 450,000,000), drains it, and
	// the server verifies 450,000,000 == 450,000,000 → closed in the same
	// transaction → the slot opens → issued.
	l5, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "closed", fake.status(user, l3.LeaseID), "the drain verified")
	require.Equal(t, 4, fake.issuances, "the slot opened in the same transaction")
	require.NotEqual(t, l3.LeaseID, l5.LeaseID)
	last := fake.requests[len(fake.requests)-1]
	require.Equal(t, l3.LeaseID, last.Drained[0].LeaseID)
	require.Equal(t, int64(450_000_000), last.Drained[0].GatewayConsumedUnits)
}

// Test 15a (drain half): a drain WITHOUT a seal against a lease that took a
// concurrent smaller reservation is not closed by the server (captured below
// the reported consumed). The "money returns through the grace sweep" clause
// is the ShipAny half's test 11; here the server keeps the lease active.
func TestPhase34Proto15aDrainWithoutSealDoesNotClose(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	fake.seedLease(user, "srv-a", "authorize", 500_000_000, 100_000_000, now.Add(5*time.Minute)) // a concurrent smaller reservation was captured
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	// a drain claiming consumed == 0 (an unsealed, stale read) must not close it
	res, err := client.EnsureLease(ctx, canonicalWalletEnsureRequest{
		PlatformUserID: user, Currency: "CNY", Purpose: "authorize", MinHeadroomUnits: 1, RequestedBudgetUnits: 500_000_000, RequestedTTLSeconds: 300,
		Drained: []canonicalWalletDrainEntry{{LeaseID: "srv-a", GatewayConsumedUnits: 0}}, CallerSlotTTLSeconds: 1800,
	})
	require.NoError(t, err)
	require.Equal(t, "active", fake.status(user, "srv-a"), "captured (100,000,000) is not the reported consumed (0): ignored")
	require.Equal(t, "srv-a", res.Lease.LeaseID, "and it is still covering, so it is reused")
	require.Equal(t, "reused", res.Outcome)
}

// Test 17 — the frozen design's dense rising fixture: every event gets a lease
// with headroom, never a request below the amount, never a doubled budget.
func TestPhase34Proto17DenseRisingFixture(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	fake.cap = 8 // headroom only: every lease here is closed by its own verified drain, so slot-holding never actually climbs
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 1_000_000_000_000)
	b := p34HTTPBridge(t, ctx, fake, store, now)
	const cny = int64(100_000_000)
	for i, amountCNY := range []int64{6, 7, 8, 9, 10, 11, 12, 13} {
		amount := amountCNY * cny
		lease, err := b.ensureLease(ctx, user, "CNY", amount, canonicalWalletLeasePurposeAuthorize, "")
		require.NoError(t, err, "event %d", i)
		require.GreaterOrEqual(t, lease.RemainingUnits(), amount, "headroom covers the amount")
		req := fake.requests[len(fake.requests)-1]
		require.Equal(t, amount, req.MinHeadroomUnits, "never a request below the amount")
		require.Equal(t, int64(500_000_000), req.RequestedBudgetUnits, "requested_budget is the configured lease budget; the server takes the max")
		require.Equal(t, amount, lease.BudgetUnits, "never a doubled budget: budget == max(cfg, amount) == amount here")
		// exhaust it on the gateway and settle it FULLY on the server: captured == the
		// pre-seal consumed, so this lease's drain VERIFIES on the next ensure and it is
		// closed — the next, larger ask issues against an empty slot set. (A second,
		// independent proof of the drain-close path beside test 15's drain half.)
		_, err = store.ReserveCanonicalWalletLease(ctx, user, lease.LeaseID, "CNY", "evt-"+itoa(i), amount, now)
		require.NoError(t, err)
		fake.setCaptured(user, lease.LeaseID, amount)
	}
	require.Equal(t, 8, fake.issuances)
}

// Test 20 — a Redis loss under a retry carrying its lease id recovers the same
// lease through prefer_lease_id (§4 explicit-id branch); before 3.4 this failed
// with ErrCanonicalWalletLeaseMissing.
func TestPhase34Proto20ExplicitIDBranchRecoversThroughPreferLeaseID(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	b := p34HTTPBridge(t, ctx, fake, store, now)

	first := CanonicalWalletSettlementEvent{GatewayRequestID: "req-retry", PlatformUserID: user, Currency: "CNY", AmountUnits: 100_000_000}
	allowed, err := b.CheckAndReserve(ctx, first)
	require.NoError(t, err)
	require.True(t, allowed)
	cur, err := store.GetCanonicalWalletLease(ctx, user)
	require.NoError(t, err)

	require.NoError(t, rdb.FlushAll(ctx).Err()) // the loss

	retry := first
	retry.LeaseID = cur.LeaseID // the retry carries the lease id it reserved against
	allowed, err = b.CheckAndReserve(ctx, retry)
	require.NoError(t, err, "recovered through prefer_lease_id instead of ErrCanonicalWalletLeaseMissing")
	require.True(t, allowed)
	last := fake.requests[len(fake.requests)-1]
	require.Equal(t, cur.LeaseID, last.PreferLeaseID)
	require.Empty(t, last.Drained, "nothing in hand to drain after the loss")
	require.Equal(t, 1, fake.issuances, "reused, not issued")
	re, err := store.GetCanonicalWalletLeaseByID(ctx, user, cur.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(100_000_000), re.ConsumedUnits, "the retry's reservation landed on the recovered lease")
}
