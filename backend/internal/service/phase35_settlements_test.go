//go:build integration

package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Phase 3.5 (redesign §11.2/§11.10): the settlements v2 client's tests —
// G5's live round trip (test 32) and named_lease_id's release (test 33).

// Test 32 — the v2 wire is units-exact: 500,000,001 units are captured as
// exactly 500,000,001 (G5's live round trip through the fake), the granted
// budget is ≥ the ask, and the drift detector's quantum holds: a
// canonical_balance within one credit (1,000,000 units) of the local figure
// is rounding, not a mismatch; beyond it counts balanceMismatch; a canonical
// figure BELOW the local one by more than the quantum also counts
// balanceBehindLocal (§11.2's drift detector).
func TestPhase35V2WireIsUnitsExact(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	b := p34DispatcherBridge(t, fake, store, db, outbox, now)

	const A = int64(500_000_001) // the non-round fixture amount, to the unit
	const fund = int64(10_000_000_000)

	mismatchBase := canonicalWalletBridgeMetrics.balanceMismatch.Load()
	behindBase := canonicalWalletBridgeMetrics.balanceBehindLocal.Load()

	// (a) local == canonical → within the quantum → no counter.
	userA := "shipany-user-" + uuid.NewString()
	fake.fund(userA, fund)
	localA := fund - A // the fake's canonical_balance after the capture: fund − captured (one lease, fully headroom-open)
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-32a", PlatformUserID: userA, Currency: "CNY", AmountUnits: A,
		LocalBalanceAfterUnits: &localA, OccurredAt: now,
	}))
	p34WaitOutboxStatus(t, ctx, db, "req-35-32a", "delivered")
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.balanceMismatch.Load()-mismatchBase, "a canonical figure within the 1,000,000-unit quantum of local is rounding, not a mismatch")

	// (b) canonical ABOVE local by more than the quantum → balanceMismatch
	// only (the local figure is behind, not the canonical one).
	userB := "shipany-user-" + uuid.NewString()
	fake.fund(userB, fund)
	localB := fund - A - 2_000_000
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-32b", PlatformUserID: userB, Currency: "CNY", AmountUnits: A,
		LocalBalanceAfterUnits: &localB, OccurredAt: now,
	}))
	p34WaitOutboxStatus(t, ctx, db, "req-35-32b", "delivered")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.balanceMismatch.Load()-mismatchBase, "beyond the quantum the mismatch counts")
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.balanceBehindLocal.Load()-behindBase, "the canonical figure is ABOVE local — not the behind-local key")

	// (c) canonical BELOW local by more than the quantum → balanceMismatch
	// AND balanceBehindLocal (grant-batch expiry steps the canonical figure
	// down with no gateway counterpart — §11.2).
	userC := "shipany-user-" + uuid.NewString()
	fake.fund(userC, fund)
	localC := fund - A + 2_000_000
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-32c", PlatformUserID: userC, Currency: "CNY", AmountUnits: A,
		LocalBalanceAfterUnits: &localC, OccurredAt: now,
	}))
	p34WaitOutboxStatus(t, ctx, db, "req-35-32c", "delivered")
	require.Equal(t, int64(2), canonicalWalletBridgeMetrics.balanceMismatch.Load()-mismatchBase)
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.balanceBehindLocal.Load()-behindBase, "a canonical figure below local by more than the quantum logs under its own key")

	// G5's evidence: the fake captured exactly 500,000,001 on each lease, the
	// amount object it received carried "500000001", and the granted budget
	// is ≥ the ask (the route's own ceiling).
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.NotEmpty(t, fake.settlementReqs)
	for _, req := range fake.settlementReqs {
		require.Equal(t, "500000001", req.Amount.AmountUnits, "the amount object the fake received carries the event's units exactly")
		require.Equal(t, "cny-e8-v1", req.Amount.UnitVersion)
		require.Equal(t, 8, req.Amount.Scale)
		require.Equal(t, "CNY", req.Amount.Currency)
	}
	for _, user := range []string{userA, userB, userC} {
		var leaseID string
		var budget, captured int64
		for _, l := range fake.leases[user] {
			if l.Captured > 0 {
				leaseID, budget, captured = l.ID, l.Budget, l.Captured
			}
		}
		require.NotEmpty(t, leaseID)
		require.Equal(t, A, captured, "G5: 500,000,001 units settled = 500,000,001 units captured, to the unit")
		require.GreaterOrEqual(t, budget, A, "the granted budget is ≥ the ask")
	}
}

// Test 33 — named_lease_id: a redelivery bound to lease X for an event the
// control plane captured on Y answers duplicate: true with named_lease_id:
// X, and the dispatcher releases the retry's whole reservation on X with the
// FULL form (marker dropped, released_units == A). A duplicate whose
// redelivery named the captured lease itself (named_lease_id: null) releases
// nothing.
func TestPhase35NamedLeaseIDReleasesTheRetryReservation(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	b := p34DispatcherBridge(t, fake, store, db, outbox, now)
	releasedBase := canonicalWalletBridgeMetrics.namedLeaseReleased.Load()

	const A = int64(30_000_000)
	const Y = "srv-lease-1" // the fake's first issuance captures the first delivery

	// First delivery: the event captures on Y.
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-33", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now,
	}))
	p34WaitOutboxStatus(t, ctx, db, "req-35-33", "delivered")
	fake.mu.Lock()
	require.Equal(t, A, fake.lease(user, Y).Captured, "the first delivery captured on the issued lease")
	fake.mu.Unlock()

	// The retry's state: the row is redelivered bound to X (a lease the
	// gateway installed and reserved A on — a cross-instance retry that
	// bound and reserved before the first delivery's capture landed). Build
	// exactly that state by hand: X in Redis and on the fake, the row
	// in_flight under this bridge's worker with lease_id = X, and the
	// event's reservation marker on X with consumed += A. The first
	// delivery's marker on Y has aged out (§11.2's precondition: "the
	// marker for this event exists there" — on the NAMED lease), so DEL it
	// before arming X's.
	const X = "srv-lease-x"
	fake.seedLease(user, X, "settle", 500_000_000, 0, now.Add(5*time.Minute))
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: X, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
	}))
	eventID := CanonicalWalletSettlementEventID("req-35-33", user, "CNY")
	require.NoError(t, rdb.Del(ctx, testCanonicalWalletReservationKey(user, eventID)).Err())
	_, err := store.ReserveCanonicalWalletLease(ctx, user, X, "CNY", eventID, A, now)
	require.NoError(t, err)

	var id int64
	require.NoError(t, db.QueryRowContext(ctx,
		`UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now(), lease_id = $3 WHERE event_id = $1 RETURNING id`,
		eventID, b.workerID, X).Scan(&id))
	leased, err := store.GetCanonicalWalletLeaseByID(ctx, user, X)
	require.NoError(t, err)
	require.Equal(t, A, leased.ConsumedUnits, "the retry reserved A on X at delivery")

	b.deliverOutboxEvent(ctx, CanonicalWalletOutboxEvent{
		ID: id, EventID: eventID, GatewayRequestID: "req-35-33", PlatformUserID: user, LeaseID: X,
		Currency: "CNY", AmountUnits: A, OccurredAt: now,
	})

	var status string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status))
	require.Equal(t, "delivered", status, "the row is delivered — a duplicate is an outcome, not a failure")
	// The full form's evidence: released_units == A on X, the marker gone.
	require.Equal(t, "30000000", p34bHashField(t, ctx, store, user, X, "released_units"))
	markerErr := rdb.Get(ctx, testCanonicalWalletReservationKey(user, eventID)).Err()
	require.ErrorIs(t, markerErr, redis.Nil, "X's reservation marker is gone — the DEL is the full form's idempotency")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.namedLeaseReleased.Load()-releasedBase, "counted once")
	fake.mu.Lock()
	require.Equal(t, A, fake.lease(user, Y).Captured, "Y still shows exactly one capture — nothing was captured twice")
	fake.mu.Unlock()

	// The null-named-lease leg: a duplicate whose redelivery named the
	// CAPTURED lease (Y) answers named_lease_id: null and releases nothing.
	fake.mu.Lock()
	yConsumedBefore := fake.lease(user, Y).Captured
	fake.mu.Unlock()
	var id2 int64
	require.NoError(t, db.QueryRowContext(ctx,
		`UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now(), lease_id = $3 WHERE event_id = $1 RETURNING id`,
		eventID, b.workerID, Y).Scan(&id2))
	b.deliverOutboxEvent(ctx, CanonicalWalletOutboxEvent{
		ID: id2, EventID: eventID, GatewayRequestID: "req-35-33", PlatformUserID: user, LeaseID: Y,
		Currency: "CNY", AmountUnits: A, OccurredAt: now,
	})
	require.NoError(t, db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE id = $1`, id2).Scan(&status))
	require.Equal(t, "delivered", status)
	require.Equal(t, "0", p34bHashField(t, ctx, store, user, Y, "released_units"), "a null named_lease_id releases nothing")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.namedLeaseReleased.Load()-releasedBase, "still exactly one release")
	fake.mu.Lock()
	require.Equal(t, yConsumedBefore, fake.lease(user, Y).Captured)
	fake.mu.Unlock()
}
