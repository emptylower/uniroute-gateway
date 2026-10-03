//go:build integration

// Phase 3.6-G pins (redesign §12.1): test 44 pins the re-scoped replenishment
// clause — at most one authorize-purpose `ensure` round trip per lease
// exhaustion, none on a cache hit, seal+drain riding the same call that
// issues; the §10.4 `{4}` re-ensure is the one named exception, bounded at two
// ensures per exhaustion. Test 45 (added by Task 2) pins the fail-closed arms
// no existing test covers. No production file changes: a pin that cannot pass
// is a finding, never a weakening.

package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// p36CountPurpose counts the fake's captured ensure requests by purpose under
// its mutex — test 16's existing pattern; the fake itself is untouched.
func p36CountPurpose(fake *fakeEnsureControlPlane, purpose string) int {
	n := 0
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, req := range fake.requests {
		if req.Purpose == purpose {
			n++
		}
	}
	return n
}

// Test 44 (§12.1, the re-scoped replenishment clause): across a run of
// authorizations that exhausts a lease, exactly one authorize-purpose `ensure`
// is issued at the exhaustion point and none on the cache-hit path — counted
// on the fake with settle-purpose calls excluded. The primary leg runs with
// holds off (§12: the `{4}` re-ensure must not fire there), consuming the
// lease the way production does — through the reserve script, test 21's
// exhaust pattern. The holds-on sub-test drives the one exception: the arm
// answers `{4}` once, and the §10.4 re-ensure bounds the exhaustion at two
// ensures.
func TestPhase36OneEnsurePerExhaustion(t *testing.T) {
	t.Run("holds off — exactly one authorize-purpose ensure per exhaustion, none on a cache hit", func(t *testing.T) {
		ctx := context.Background()
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		now := time.Now().UTC()
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		const budget = int64(500_000_000)
		fake.fund(user, 2*budget) // exactly two leases of LeaseBudgetUnits

		// The holds-off bridge, test 22's OFF leg: Authorize calls ensureLease
		// and arms nothing, so the run consumes the lease through the reserve
		// script exactly as production does with holds off.
		cfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
		cfg.Holds = "off"
		cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
		cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 300
		cfg.LeaseBudgetUnits = budget
		client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
		client.now = func() time.Time { return now }
		b := newCanonicalWalletBridge(cfg, store, client, nil, nil, 0, func() time.Time { return now })
		t.Cleanup(b.Close)

		const E = budget / 4
		authorize := func() *AuthorizationHandle {
			return p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
		}
		reserve := func(leaseID string, step int) {
			_, err := store.ReserveCanonicalWalletLease(ctx, user, leaseID, "USD", "evt-44-"+itoa(step), E, now)
			require.NoError(t, err, "step %d", step)
		}

		// Authorization 1: the empty-cache issue — the first authorize-purpose ensure.
		h1 := authorize()
		require.Nil(t, h1.Refusal)
		require.Equal(t, "srv-lease-1", h1.LeaseID)
		require.Equal(t, 1, p36CountPurpose(fake, "authorize"))
		reserve(h1.LeaseID, 1)

		// Authorizations 2–4: cache hits (leaseCovers from the hash) — no call.
		for step := 2; step <= 4; step++ {
			h := authorize()
			require.Nil(t, h.Refusal, "step %d", step)
			require.Equal(t, h1.LeaseID, h.LeaseID, "step %d: still L1", step)
			require.Equal(t, 1, p36CountPurpose(fake, "authorize"), "step %d: a cache hit makes no control-plane call", step)
			reserve(h1.LeaseID, step)
		}
		cur, err := store.GetCanonicalWalletLease(ctx, user)
		require.NoError(t, err)
		require.Equal(t, int64(0), cur.RemainingUnits(), "four reservations of E exhausted L1")

		// Authorization 5, the exhaustion point: RemainingUnits() == 0 < E, so
		// ensureLease seals L1 and sends the drain on the SAME call that
		// issues L2 — count 2, never 3.
		h5 := authorize()
		require.Nil(t, h5.Refusal)
		require.NotEqual(t, h1.LeaseID, h5.LeaseID)
		require.Equal(t, 2, p36CountPurpose(fake, "authorize"), "exactly one ensure at the exhaustion point")
		fake.mu.Lock()
		last := fake.requests[len(fake.requests)-1]
		require.Equal(t, "authorize", last.Purpose)
		require.Len(t, last.Drained, 1, "seal + drain ride the one call that issues")
		require.Equal(t, h1.LeaseID, last.Drained[0].LeaseID)
		require.Equal(t, budget, mustUnits(last.Drained[0].GatewayConsumed), "the pre-seal consumed is L1's whole budget")
		fake.mu.Unlock()
		reserve(h5.LeaseID, 5)

		// Authorizations 6–8: cache hits on L2 until it in turn is exhausted —
		// the run is bounded at two leases (L1's drain is unverified, so L1
		// stays marked and slot-holding, test 21's shape; a third exhaustion
		// would run into the cap).
		for step := 6; step <= 8; step++ {
			h := authorize()
			require.Nil(t, h.Refusal, "step %d", step)
			require.Equal(t, h5.LeaseID, h.LeaseID, "step %d: still L2", step)
			require.Equal(t, 2, p36CountPurpose(fake, "authorize"), "step %d: a cache hit makes no control-plane call", step)
			reserve(h5.LeaseID, step)
		}
		fake.mu.Lock()
		require.Equal(t, 2, fake.issuances, "bounded at two leases — a silent lease_cap_reached cannot masquerade as the count")
		fake.mu.Unlock()
		require.Equal(t, 0, p36CountPurpose(fake, "settle"), "no settlement is observed here")
	})

	t.Run("holds on — the {4} re-ensure is bounded at two ensures per exhaustion", func(t *testing.T) {
		ctx := context.Background()
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		now := time.Now().UTC()
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 10_000_000_000)
		resetAuthorizationMetricsForTest()

		// The lease the fake will issue SECOND is pre-exhausted on the gateway
		// under its deterministic id (test 29's trick: the fake's seq is
		// per-instance and this is its first issuance) — a concurrent
		// reservation's headroom loss between ensureLease's covering read and
		// the arm, §10.4's {4}. Pre-seeded BEFORE the first authorize so the
		// current pointer ends the setup on the real L1.
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: "srv-lease-2", PlatformUserID: user, Currency: "USD",
			BudgetUnits: 500_000_000, ConsumedUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))

		b := p34bBridge(t, ctx, config.CanonicalWalletModeEnforce, fake, store, now)
		authorize := func() *AuthorizationHandle {
			return p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
		}

		// Authorization 1: the empty-cache issue — count 1; L1 arms.
		h1 := authorize()
		require.Nil(t, h1.Refusal)
		require.True(t, h1.HoldArmed)
		require.Equal(t, "srv-lease-1", h1.LeaseID)
		require.Equal(t, 1, p36CountPurpose(fake, "authorize"))

		// Pre-exhaust L1 on the gateway so the next authorization sits at an
		// exhaustion point (test 21's exhaust pattern: reserve the remaining).
		cur, err := store.GetCanonicalWalletLease(ctx, user)
		require.NoError(t, err)
		_, err = store.ReserveCanonicalWalletLease(ctx, user, h1.LeaseID, "USD", "evt-44h-exhaust", cur.RemainingUnits(), now)
		require.NoError(t, err)

		// Authorization 2 at the exhaustion point: the sealing ensure issues
		// the pre-exhausted srv-lease-2, the arm answers {4} once, the §10.4
		// re-ensure issues srv-lease-3 and arms — TWO ensures at this
		// exhaustion, never more.
		h2 := authorize()
		require.Nil(t, h2.Refusal)
		require.True(t, h2.HoldArmed, "the re-ensured lease arms")
		require.Equal(t, "srv-lease-3", h2.LeaseID)
		require.Equal(t, 3, p36CountPurpose(fake, "authorize"), "one ensure before the exhaustion + exactly two at it")
		require.Equal(t, int64(1), authorizationMetrics.holdArmRetried.Load(), "the {4} fired exactly once")
		fake.mu.Lock()
		require.Equal(t, 3, fake.issuances)
		fake.mu.Unlock()
		require.Equal(t, 0, p36CountPurpose(fake, "settle"))
	})
}

// Test 45 (§12.1, the fail-closed row): exactly the arms Task 0 confirmed
// unpinned — the enforce-refuses half for the CONTROL-PLANE error is already
// pinned (canonical_wallet_authorizer_test.go's "control plane down" case and
// its sentinel table), and TestCanonicalWalletCheckAndReserveFailsClosedOnlyInEnforceMode
// pins CheckAndReserve's ensureLease-error arm while
// TestHasCanonicalWalletHeadroomEnforceBranches pins HasCanonicalWalletHeadroom's
// Missing/exhausted/funded branches. The store-outage injection is the stub's
// getErr (a transport error is not a cache miss) — no closed-port Redis
// client, which would be a different failure class.
func TestPhase36FailClosedArmsUnpinned(t *testing.T) {
	t.Run("(a) Authorize: shadow admits on a store outage and counts leaseUnavailable; enforce refuses lease_unavailable", func(t *testing.T) {
		ctx := context.Background()
		outage := errors.New("redis down")
		resetAuthorizationMetricsForTest()

		shadowStore := &canonicalWalletStoreStub{getErr: outage}
		shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), shadowStore, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(shadow.Close)
		hs := p34bAuthorize(t, ctx, config.CanonicalWalletModeShadow, shadow, "shipany-user-"+uuid.NewString(), `{"max_tokens":64}`)
		require.Nil(t, hs.Refusal, "shadow admits despite the store outage")
		require.Equal(t, "", hs.LeaseID)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().LeaseUnavailable, "counted, and admitted anyway")
		require.Equal(t, 1, shadowStore.getCalls, "the outage path was actually driven")

		enforceStore := &canonicalWalletStoreStub{getErr: outage}
		enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), enforceStore, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(enforce.Close)
		he := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, enforce, "shipany-user-"+uuid.NewString(), `{"max_tokens":64}`)
		require.NotNil(t, he.Refusal)
		require.Equal(t, AuthorizationRefusalLeaseUnavailable, he.Refusal.Reason, "the store-error variant of the already-pinned control-plane refusal")
	})

	t.Run("(b) HasCanonicalWalletHeadroom: the shadow return precedes the error check; enforce surfaces the transport error", func(t *testing.T) {
		ctx := context.Background()
		outage := errors.New("redis down")

		shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{getErr: outage}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(shadow.Close)
		ok, err := shadow.HasCanonicalWalletHeadroom(ctx, "user-45b", "USD")
		require.True(t, ok, "shadow never denies admission — the shadow return precedes the error check")
		require.NoError(t, err)

		enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), &canonicalWalletStoreStub{getErr: outage}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(enforce.Close)
		ok, err = enforce.HasCanonicalWalletHeadroom(ctx, "user-45b", "USD")
		require.False(t, ok)
		require.ErrorIs(t, err, outage, "a transport error — the existing test pins only ErrCanonicalWalletLeaseMissing here")
	})

	t.Run("(c) CheckAndReserve: the reserve error fails closed in enforce; shadow still admits", func(t *testing.T) {
		ctx := context.Background()
		reserveFailure := errors.New("reserve script failed")
		// a covering cached lease so ensureLease is a pure cache hit — the
		// arm under test is the RESERVE error, not the ensureLease error the
		// existing test pins.
		coveringLease := func() *CanonicalWalletLease {
			return &CanonicalWalletLease{LeaseID: "lease-45c", Currency: "USD", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(5 * time.Minute)}
		}
		event := CanonicalWalletSettlementEvent{GatewayRequestID: "req-45c", PlatformUserID: "user-45c", Currency: "USD", AmountUnits: 100}

		shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{lease: coveringLease(), reserveErr: reserveFailure}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(shadow.Close)
		allowed, err := shadow.CheckAndReserve(ctx, event)
		require.True(t, allowed, "shadow observes the failed reservation and admits")
		require.NoError(t, err)

		enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), &canonicalWalletStoreStub{lease: coveringLease(), reserveErr: reserveFailure}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(enforce.Close)
		allowed, err = enforce.CheckAndReserve(ctx, event)
		require.False(t, allowed)
		require.ErrorIs(t, err, reserveFailure, "the reserve-error arm the existing test does not pin")
	})

	// The narrowing from five entry points to four is deliberate
	// (§13.1): EnsureCanonicalWalletHeadroom was deleted by 3.7a.
	t.Run("(d) disabled: every entry point returns its disabled value with the store untouched (four — EnsureCanonicalWalletHeadroom was deleted by 3.7a, §13.1)", func(t *testing.T) {
		ctx := context.Background()
		store := &canonicalWalletStoreStub{
			getErr:     errors.New("redis down"),
			reserveErr: errors.New("reserve failed"),
		}
		b := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeDisabled), store, &canonicalWalletControlStub{}, nil, nil, 0, nil)
		t.Cleanup(b.Close)
		user := "shipany-user-" + uuid.NewString()

		h := p34bAuthorize(t, ctx, config.CanonicalWalletModeDisabled, b, user, `{"max_tokens":64}`)
		require.Nil(t, h.Refusal)
		require.Equal(t, "", h.LeaseID, "the handle carries only its id")

		ok, err := b.HasCanonicalWalletHeadroom(ctx, user, "USD")
		require.True(t, ok)
		require.NoError(t, err)

		allowed, err := b.CheckAndReserve(ctx, CanonicalWalletSettlementEvent{GatewayRequestID: "req-45d", PlatformUserID: user, Currency: "USD", AmountUnits: 100})
		require.True(t, allowed)
		require.NoError(t, err)

		require.False(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-45d-obs", PlatformUserID: user, Currency: "USD", AmountUnits: 100}), "disabled mode observes nothing")

		require.Equal(t, 0, store.getCalls)
		require.Equal(t, 0, store.installCalls)
		require.Equal(t, 0, store.reserveCalls)
	})
}
