//go:build integration

package service

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
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

// Test 37 — §11.5's classification table, minus the two rows that are
// mechanisms rather than classifications (lease_over_capture and
// lease_not_capturable — Tasks 5/6, asserted by delivery, not a status).
// Every row is driven through the REAL dispatcher against the fake's
// respondWith hook: a terminal row dead-letters on the FIRST attempt with
// the stated reason; a transient row survives past two attempts (the
// backoff) with no reason persisted.
func TestPhase35ClassificationTable(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()

	const settlementsPath = "/api/internal/v2/wallet/settlements"
	const ensurePath = "/api/internal/v2/wallet/leases/ensure"
	refusal := func(reason string) string {
		return `{"code":-1,"message":"` + reason + `","data":{"reason":"` + reason + `"}}`
	}

	rows := []struct {
		name           string
		path           string
		status         int
		body           string
		terminalReason string // "" = transient
	}{
		{"401 unauthorized on settlements is transient", settlementsPath, 401, refusal("unauthorized"), ""},
		{"401 unauthorized on ensure is transient", ensurePath, 401, refusal("unauthorized"), ""},
		{"404 lease_not_found on settlements is contract_violation", settlementsPath, 404, refusal("lease_not_found"), "contract_violation"},
		{"409 lease_owner_mismatch on ensure is contract_violation", ensurePath, 409, refusal("lease_owner_mismatch"), "contract_violation"},
		{"400 invalid_request on ensure is contract_violation", ensurePath, 400, refusal("invalid_request"), "contract_violation"},
		{"400 invalid_request on settlements is contract_violation", settlementsPath, 400, refusal("invalid_request"), "contract_violation"},
		{"400 invalid_amount on settlements is contract_violation", settlementsPath, 400, refusal("invalid_amount"), "contract_violation"},
		{"409 settlement_payload_conflict is payload_conflict", settlementsPath, 409, refusal("settlement_payload_conflict"), "payload_conflict"},
		{"500 internal_error is transient", settlementsPath, 500, refusal("internal_error"), ""},
		{"503 flag off is transient", settlementsPath, 503, refusal("unavailable"), ""},
	}

	for i, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			// A throwaway outbox per row: every bridge keeps its dispatcher
			// ticker alive for the life of the test binary, so a shared table
			// would let an earlier row's bridge claim a later row against
			// its own fake. Redis is shared — every user is uuid-distinct.
			db := startCanonicalWalletTestPostgres(t, ctx)
			outbox := &outboxStoreForTest{db: db}
			fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
			user := "shipany-user-" + uuid.NewString()
			fake.fund(user, 100_000_000_000)
			fake.respondWith(row.path, row.status, row.body, -1)
			b := p34DispatcherBridge(t, fake, store, db, outbox, now)
			reqID := "req-35-37-" + itoa(i)
			b.ObserveSettlement(CanonicalWalletSettlementEvent{
				GatewayRequestID: reqID, PlatformUserID: user, Currency: "CNY", AmountUnits: 10_000_000, OccurredAt: now,
			})

			readRow := func() (string, int, sql.NullString) {
				var status string
				var attempts int
				var reason sql.NullString
				require.NoError(t, db.QueryRowContext(ctx,
					`SELECT status, attempt_count, dead_letter_reason FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, reqID).
					Scan(&status, &attempts, &reason))
				return status, attempts, reason
			}

			if row.terminalReason != "" {
				p34WaitOutboxStatus(t, ctx, db, reqID, "dead_letter")
				status, attempts, reason := readRow()
				require.Equal(t, "dead_letter", status)
				require.Equal(t, 1, attempts, "terminal rows dead-letter on the first attempt — no backoff retries hide a wire bug")
				require.True(t, reason.Valid)
				require.Equal(t, row.terminalReason, reason.String)
				return
			}

			// Transient: the row must survive past TWO attempts on the
			// backoff (simulatedNow is the fixed `now`; the real ticker
			// re-claims once now+backoff passes) and never carry a reason.
			deadline := time.Now().Add(30 * time.Second)
			for {
				status, attempts, reason := readRow()
				if status == "pending" && attempts >= 2 {
					require.False(t, reason.Valid, "a retried row carries no dead-letter reason")
					break
				}
				require.NotEqual(t, "dead_letter", status, "a transient row must never dead-letter")
				if time.Now().After(deadline) {
					t.Fatalf("transient row %s never reached a second attempt (status %s, attempts %d)", reqID, status, attempts)
				}
				time.Sleep(100 * time.Millisecond)
			}
		})
	}
}

// p35RowState reads one outbox row's split-relevant columns by event id.
type p35RowState struct {
	Status         string
	AmountUnits    int64
	PendingRelease sql.NullInt64
	PayloadHash    string
	LeaseID        sql.NullString
	AttemptCount   int
	Reason         sql.NullString
}

func p35ReadRow(t *testing.T, ctx context.Context, db *sql.DB, eventID string) p35RowState {
	t.Helper()
	var s p35RowState
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT status, amount_units, pending_release_units, payload_hash, lease_id, attempt_count, dead_letter_reason
		FROM wallet_settlement_outbox WHERE event_id = $1`, eventID,
	).Scan(&s.Status, &s.AmountUnits, &s.PendingRelease, &s.PayloadHash, &s.LeaseID, &s.AttemptCount, &s.Reason))
	return s
}

// helpers used by test 34 — filled in with the implementation.
func p35WaitForRemainder(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM wallet_settlement_outbox WHERE event_id = $1)`, eventID).Scan(&exists); err == nil && exists {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("remainder row %s never appeared", eventID)
}

// p35WaitPendingReleaseCleared polls until the row's pending_release_units
// is NULL — the normal split delivery's own end state (release + clear in
// the same delivery).
func p35WaitPendingReleaseCleared(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var pending sql.NullInt64
		if err := db.QueryRowContext(ctx, `SELECT pending_release_units FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&pending); err == nil && !pending.Valid {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("row %s: pending_release_units was never cleared", eventID)
}

func p35WaitRemainderDelivered(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status); err == nil && status == "delivered" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("remainder row %s never reached delivered", eventID)
}

func p35ParentEventID(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, bool) {
	t.Helper()
	var parent sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT parent_event_id FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&parent))
	return parent.String, parent.Valid
}

// Test 34 — the durable over-capture split (§11.3): A > H > 0 splits the row
// (parent H, remainder A−H), releases the reserved remainder to the lease
// (partial form — the marker survives), delivers the parent on the refusing
// lease and the remainder on a settle lease, never touches the parent's
// payload_hash, and ends with the lease's identity holding:
// consumed A == captured H + released (A − H). The H = 0 leg is split_full;
// an absent headroom field is H = 0; a redelivery of the captured parent is
// a duplicate. The crash legs fault-inject the release and its clear, and
// prove the release marker makes every re-run idempotent.
func TestPhase35OverCaptureSplits(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	// p35Outbox gives each subtest its OWN throwaway outbox: every bridge's
	// dispatcher ticker stays alive for the whole binary, so a shared table
	// would let an earlier subtest's bridge claim a later row against its
	// own fake (Redis is shared — every user is uuid-distinct).
	p35Outbox := func(t *testing.T) (*sql.DB, *outboxStoreForTest) {
		t.Helper()
		db := startCanonicalWalletTestPostgres(t, ctx)
		return db, &outboxStoreForTest{db: db}
	}

	// A is deliberately large: after reserving A on L (budget 500M), the
	// gateway's own remaining on L (100M) no longer covers the remainder
	// (A − H), so the remainder's ensure moves it to a FRESH settle lease
	// instead of rebinding it to L — §11.3's "delivers as an ordinary
	// unbound event".
	const A = int64(400_000_000)
	const H = int64(50_000_000)

	// seedOverCapturable installs the divergence the split exists for: the
	// fake's server view of L has headroom `headroom`, while the gateway's
	// Redis view starts unreserved.
	seedOverCapturable := func(t *testing.T, fake *fakeEnsureControlPlane, store CanonicalWalletLeaseStore, user, L string, headroom int64) {
		t.Helper()
		fake.seedLease(user, L, "authorize", 500_000_000, 500_000_000-headroom, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
	}

	t.Run("A > H > 0 splits and both halves deliver", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L = "srv-split-l"
		seedOverCapturable(t, fake, store, user, L, H)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34", user, "CNY")
		hashBefore := p35ReadRow(t, ctx, db, eventID).PayloadHash

		// First tick: the fake refuses over-capture with headroom H → the
		// split, then the partial release and its clear IN THE SAME delivery.
		// The remainder row becomes visible at the split's commit; the
		// release+clear follow — poll for the cleared column (the normal
		// path's own end state) rather than racing the read.
		p35WaitForRemainder(t, ctx, db, eventID+":r1")
		p35WaitPendingReleaseCleared(t, ctx, db, eventID)
		parent := p35ReadRow(t, ctx, db, eventID)
		require.Equal(t, int64(H), parent.AmountUnits, "the parent carries H")
		require.False(t, parent.PendingRelease.Valid, "the release ran in the same delivery — the column is cleared")
		require.Equal(t, hashBefore, parent.PayloadHash, "the split NEVER touches the parent's payload_hash (§11.3)")
		remainder := p35ReadRow(t, ctx, db, eventID+":r1")
		require.Equal(t, A-H, remainder.AmountUnits)
		require.Equal(t, "pending", remainder.Status)
		require.False(t, remainder.LeaseID.Valid, "the remainder is unbound")
		parentID, ok := p35ParentEventID(t, ctx, db, eventID+":r1")
		require.True(t, ok)
		require.Equal(t, eventID, parentID)

		// The lease: the reserved remainder went back (partial form — the
		// marker survives for the parent's {5} redelivery).
		require.Equal(t, "350000000", p34bHashField(t, ctx, store, user, L, "released_units"), "released_units == A − H")
		marker, err := rdb.Get(ctx, testCanonicalWalletReservationKey(user, eventID)).Result()
		require.NoError(t, err)
		require.Equal(t, L, marker, "the reservation marker survives the partial release")
		// The release marker ages with the lease: its PTTL equals L's within
		// a second.
		releaseTTL, err := rdb.PTTL(ctx, testCanonicalWalletReleaseMarkerKey(user, eventID)).Result()
		require.NoError(t, err)
		leaseTTL, err := rdb.PTTL(ctx, testCanonicalWalletLeaseKey(user, L)).Result()
		require.NoError(t, err)
		require.LessOrEqual(t, releaseTTL-leaseTTL, time.Second)
		require.GreaterOrEqual(t, releaseTTL-leaseTTL, -time.Second)

		// The next ticks deliver both halves: the parent captures H on L
		// (the reserve answers {5}); the remainder delivers on a settle lease.
		p34WaitOutboxStatus(t, ctx, db, "req-35-34", "delivered")
		p35WaitRemainderDelivered(t, ctx, db, eventID+":r1")
		fake.mu.Lock()
		require.Equal(t, int64(500_000_000), fake.lease(user, L).Captured, "L captured the seed 450M plus exactly H")
		var settleCaptured int64
		for _, l := range fake.leases[user] {
			if l.Purpose == "settle" {
				settleCaptured += l.Captured
			}
		}
		fake.mu.Unlock()
		require.Equal(t, A-H, settleCaptured, "the remainder captured on a settle-purpose lease")
		// The lease's identity: consumed A == captured H + released (A − H) —
		// this event's figures on L, in PRE-seAL form (§10.2: the seal caps
		// the stored consumed at budget; the identity is what rides the
		// drain wire). The remainder's ensure sealed L and drained it with
		// exactly these figures.
		rawReleased, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, L), "released_units").Int64()
		require.NoError(t, err)
		require.Equal(t, A-H, rawReleased)
		fake.mu.Lock()
		var drainedConsumed, drainedReleased int64
		for _, req := range fake.requests {
			for _, d := range req.Drained {
				if d.LeaseID == L {
					drainedConsumed = mustUnits(d.GatewayConsumed)
					if d.GatewayReleased != nil {
						drainedReleased = mustUnits(*d.GatewayReleased)
					}
				}
			}
		}
		fake.mu.Unlock()
		require.Equal(t, A, drainedConsumed, "the drain carried pre-seal consumed == A")
		require.Equal(t, A-H, drainedReleased, "the drain carried released == A − H")
	})

	t.Run("H = 0 is split_full", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L0 = "srv-split-l0"
		seedOverCapturable(t, fake, store, user, L0, 0)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)
		splitFullBase := canonicalWalletBridgeMetrics.settlementSplitFull.Load()

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34z", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L0, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34z", user, "CNY")
		p34WaitOutboxStatus(t, ctx, db, "req-35-34z", "delivered")
		parent := p35ReadRow(t, ctx, db, eventID)
		require.Equal(t, "delivered", parent.Status, "split_full resolves the parent in the split transaction")
		require.Equal(t, int64(0), parent.AmountUnits, "split_full: the parent's amount is H = 0")
		require.Equal(t, A, p35ReadRow(t, ctx, db, eventID+":r1").AmountUnits, "the remainder carries the whole amount")
		released, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, L0), "released_units").Int64()
		require.NoError(t, err)
		require.Equal(t, A, released, "released_units == A")
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.settlementSplitFull.Load()-splitFullBase, "the split_full leg counted")
		p35WaitRemainderDelivered(t, ctx, db, eventID+":r1")
	})

	t.Run("absent headroom is H = 0", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		fake.mu.Lock()
		fake.omitHeadroom = true
		fake.mu.Unlock()
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L1 = "srv-split-l1"
		seedOverCapturable(t, fake, store, user, L1, 0)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34o", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L1, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34o", user, "CNY")
		p34WaitOutboxStatus(t, ctx, db, "req-35-34o", "delivered")
		require.Equal(t, A, p35ReadRow(t, ctx, db, eventID+":r1").AmountUnits, "an absent headroom field is treated as H = 0")
		released, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, L1), "released_units").Int64()
		require.NoError(t, err)
		require.Equal(t, A, released, "the whole reservation went back")
	})

	t.Run("redelivery of the captured parent is a duplicate", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L2 = "srv-split-l2"
		seedOverCapturable(t, fake, store, user, L2, H)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34d", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L2, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34d", user, "CNY")
		p34WaitOutboxStatus(t, ctx, db, "req-35-34d", "delivered")
		fake.mu.Lock()
		capturedBefore := fake.lease(user, L2).Captured
		fake.mu.Unlock()
		require.Equal(t, int64(500_000_000), capturedBefore, "the seed 450M plus exactly H")

		var id int64
		require.NoError(t, db.QueryRowContext(ctx,
			`UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now() WHERE event_id = $1 RETURNING id`,
			eventID, b.workerID).Scan(&id))
		b.deliverOutboxEvent(ctx, CanonicalWalletOutboxEvent{
			ID: id, EventID: eventID, GatewayRequestID: "req-35-34d", PlatformUserID: user, LeaseID: L2,
			Currency: "CNY", AmountUnits: H, OccurredAt: now,
		})
		status, err := outbox.OutboxEventStatus(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "delivered", status, "a redelivery of the captured parent is a duplicate, not an error")
		fake.mu.Lock()
		require.Equal(t, capturedBefore, fake.lease(user, L2).Captured, "nothing was captured twice")
		fake.mu.Unlock()
	})

	// p35FaultyStore fails ReleaseCanonicalWalletReservation exactly once —
	// the crash between the split transaction and the release.
	t.Run("release fault is repaired on redelivery", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L3 = "srv-split-l3"
		seedOverCapturable(t, fake, store, user, L3, H)
		faulty := &p35FaultyStore{inner: store}
		b := p34DispatcherBridge(t, fake, faulty, db, outbox, now)

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34c1", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L3, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34c1", user, "CNY")
		p35WaitForRemainder(t, ctx, db, eventID+":r1")

		// The release failed after the split: the column still records the
		// debt. The parent is pending and its next delivery re-runs the
		// release FIRST (§11.3's repair), then captures H.
		require.True(t, p35ReadRow(t, ctx, db, eventID).PendingRelease.Valid, "a failed release keeps pending_release_units set")
		p34WaitOutboxStatus(t, ctx, db, "req-35-34c1", "delivered")
		require.False(t, p35ReadRow(t, ctx, db, eventID).PendingRelease.Valid, "the repair cleared the debt")
		released, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, L3), "released_units").Int64()
		require.NoError(t, err)
		require.Equal(t, A-H, released, "the release ran EXACTLY once — never 2 × (A − H)")
		fake.mu.Lock()
		require.Equal(t, int64(500_000_000), fake.lease(user, L3).Captured, "the seed 450M plus exactly H — the repair captured once")
		fake.mu.Unlock()
		p35WaitRemainderDelivered(t, ctx, db, eventID+":r1")
	})

	// p35FaultyOutbox fails ClearPendingRelease exactly once AFTER a
	// successful release — the crash between the release and its clear. The
	// next delivery re-runs the release; the release marker answers {7} and
	// released_units never moves a second time.
	t.Run("clear fault replays through the release marker", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L4 = "srv-split-l4"
		seedOverCapturable(t, fake, store, user, L4, H)
		faultyOutbox := &p35FaultyOutbox{inner: outbox}
		b := p34DispatcherBridge(t, fake, store, db, faultyOutbox, now)
		replayedBase := canonicalWalletBridgeMetrics.pendingReleaseReplayed.Load()

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-34c2", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L4, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-34c2", user, "CNY")
		p35WaitForRemainder(t, ctx, db, eventID+":r1")

		// The release landed but its clear failed: the column still set. The
		// next delivery re-runs the release first — the marker answers {7},
		// released_units is untouched, and the row proceeds to capture H.
		require.True(t, p35ReadRow(t, ctx, db, eventID).PendingRelease.Valid, "a failed clear keeps the column set")
		p34WaitOutboxStatus(t, ctx, db, "req-35-34c2", "delivered")
		require.False(t, p35ReadRow(t, ctx, db, eventID).PendingRelease.Valid)
		released, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, L4), "released_units").Int64()
		require.NoError(t, err)
		require.Equal(t, A-H, released, "the replayed release wrote nothing — {7} on the marker")
		require.GreaterOrEqual(t, canonicalWalletBridgeMetrics.pendingReleaseReplayed.Load()-replayedBase, int64(1), "the replay was counted")
		fake.mu.Lock()
		require.Equal(t, int64(500_000_000), fake.lease(user, L4).Captured, "the seed 450M plus exactly H — the replay captured once")
		fake.mu.Unlock()
		p35WaitRemainderDelivered(t, ctx, db, eventID+":r1")
	})
}

// p35FaultyStore delegates to the real adapter but fails
// ReleaseCanonicalWalletReservation on its first call.
type p35FaultyStore struct {
	inner         CanonicalWalletLeaseStore
	failRelease   bool
	releaseCalled int
}

func (s *p35FaultyStore) InstallCanonicalWalletLease(ctx context.Context, lease CanonicalWalletLease) error {
	return s.inner.InstallCanonicalWalletLease(ctx, lease)
}
func (s *p35FaultyStore) GetCanonicalWalletLease(ctx context.Context, u string) (*CanonicalWalletLease, error) {
	return s.inner.GetCanonicalWalletLease(ctx, u)
}
func (s *p35FaultyStore) GetCanonicalWalletLeaseByID(ctx context.Context, u, id string) (*CanonicalWalletLease, error) {
	return s.inner.GetCanonicalWalletLeaseByID(ctx, u, id)
}
func (s *p35FaultyStore) ReserveCanonicalWalletLease(ctx context.Context, u, l, c, e string, a int64, n time.Time) (*CanonicalWalletReservation, error) {
	return s.inner.ReserveCanonicalWalletLease(ctx, u, l, c, e, a, n)
}
func (s *p35FaultyStore) SealCanonicalWalletLease(ctx context.Context, u, l string) (int64, int64, error) {
	return s.inner.SealCanonicalWalletLease(ctx, u, l)
}
func (s *p35FaultyStore) ArmCanonicalWalletHold(ctx context.Context, u, l, c, a string, units int64, g int64, n time.Time) (string, int64, bool, error) {
	return s.inner.ArmCanonicalWalletHold(ctx, u, l, c, a, units, g, n)
}
func (s *p35FaultyStore) ReleaseCanonicalWalletHold(ctx context.Context, u, a, st, cl string) (int64, error) {
	return s.inner.ReleaseCanonicalWalletHold(ctx, u, a, st, cl)
}
func (s *p35FaultyStore) ConvertCanonicalWalletHold(ctx context.Context, u, a, e string, units int64, n time.Time) (CanonicalWalletHoldConversion, error) {
	return s.inner.ConvertCanonicalWalletHold(ctx, u, a, e, units, n)
}
func (s *p35FaultyStore) GetCanonicalWalletHold(ctx context.Context, u, a string) (*CanonicalWalletHold, error) {
	return s.inner.GetCanonicalWalletHold(ctx, u, a)
}
func (s *p35FaultyStore) ListCanonicalWalletHolds(ctx context.Context, u string, l int) ([]string, error) {
	return s.inner.ListCanonicalWalletHolds(ctx, u, l)
}
func (s *p35FaultyStore) ListCanonicalWalletHoldUsers(ctx context.Context, c uint64, n int64) ([]string, uint64, error) {
	return s.inner.ListCanonicalWalletHoldUsers(ctx, c, n)
}
func (s *p35FaultyStore) PruneCanonicalWalletHoldUser(ctx context.Context, u string) error {
	return s.inner.PruneCanonicalWalletHoldUser(ctx, u)
}
func (s *p35FaultyStore) TryCanonicalWalletReaperLease(ctx context.Context, ttl time.Duration) (bool, error) {
	return s.inner.TryCanonicalWalletReaperLease(ctx, ttl)
}
func (s *p35FaultyStore) ForgetCanonicalWalletHold(ctx context.Context, u, a string) error {
	return s.inner.ForgetCanonicalWalletHold(ctx, u, a)
}
func (s *p35FaultyStore) MarkCanonicalWalletHoldUserEmpty(ctx context.Context, u string, ttl time.Duration) (bool, error) {
	return s.inner.MarkCanonicalWalletHoldUserEmpty(ctx, u, ttl)
}
func (s *p35FaultyStore) ClearCanonicalWalletHoldUserEmpty(ctx context.Context, u string) error {
	return s.inner.ClearCanonicalWalletHoldUserEmpty(ctx, u)
}
func (s *p35FaultyStore) MarkCanonicalWalletHoldClass(ctx context.Context, u, a, c string) (*CanonicalWalletHold, error) {
	return s.inner.MarkCanonicalWalletHoldClass(ctx, u, a, c)
}
func (s *p35FaultyStore) ReleaseCanonicalWalletReservation(ctx context.Context, u, l, e string, units int64, drop bool) (bool, error) {
	s.releaseCalled++
	if s.releaseCalled == 1 {
		return false, errors.New("injected release failure (crash between the split and the release)")
	}
	return s.inner.ReleaseCanonicalWalletReservation(ctx, u, l, e, units, drop)
}

// p35FaultyOutbox delegates to the real outbox but fails ClearPendingRelease
// on its first call.
type p35FaultyOutbox struct {
	inner       CanonicalWalletOutboxStore
	clearCalled int
}

func (o *p35FaultyOutbox) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, e CanonicalWalletSettlementEvent) error {
	return o.inner.InsertOutboxEventTx(ctx, tx, e)
}
func (o *p35FaultyOutbox) ClaimPendingOutboxEvents(ctx context.Context, w string, l int) ([]CanonicalWalletOutboxEvent, error) {
	return o.inner.ClaimPendingOutboxEvents(ctx, w, l)
}
func (o *p35FaultyOutbox) MarkOutboxEventDelivered(ctx context.Context, id int64, w string) error {
	return o.inner.MarkOutboxEventDelivered(ctx, id, w)
}
func (o *p35FaultyOutbox) MarkOutboxEventFailed(ctx context.Context, id int64, w string, n time.Time) error {
	return o.inner.MarkOutboxEventFailed(ctx, id, w, n)
}
func (o *p35FaultyOutbox) MarkOutboxEventDeadLetter(ctx context.Context, id int64, w, r string) error {
	return o.inner.MarkOutboxEventDeadLetter(ctx, id, w, r)
}
func (o *p35FaultyOutbox) BindOutboxEventLease(ctx context.Context, id int64, w, l string) error {
	return o.inner.BindOutboxEventLease(ctx, id, w, l)
}
func (o *p35FaultyOutbox) ReclaimStaleInFlightEvents(ctx context.Context, d time.Duration) (int64, error) {
	return o.inner.ReclaimStaleInFlightEvents(ctx, d)
}
func (o *p35FaultyOutbox) SplitOutboxEvent(ctx context.Context, id int64, w string, c int64, r string, res bool) (int64, error) {
	return o.inner.SplitOutboxEvent(ctx, id, w, c, r, res)
}
func (o *p35FaultyOutbox) ClearPendingRelease(ctx context.Context, id int64) error {
	o.clearCalled++
	if o.clearCalled == 1 {
		return errors.New("injected clear failure (crash between the release and its clear)")
	}
	return o.inner.ClearPendingRelease(ctx, id)
}
func (o *p35FaultyOutbox) SumDeadLetterUnits(ctx context.Context, r string) (int64, error) {
	return o.inner.SumDeadLetterUnits(ctx, r)
}

// Test 35 — the settle-purpose under-grant splits PROACTIVELY (§11.3): the
// under-granted lease is installed (authorize keeps refusing — its sibling
// in canonical_wallet_bridge_test.go), the dispatcher splits A → B + (A − B)
// before reserving, both capture; a chain that would need a ninth split
// dead-letters split_exhausted with the first eight captures summing to
// exactly 8 units.
func TestPhase35SettleUnderGrantSplits(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	// Per-subtest throwaway outboxes, same reason as test 34.
	p35Outbox := func(t *testing.T) (*sql.DB, *outboxStoreForTest) {
		t.Helper()
		db := startCanonicalWalletTestPostgres(t, ctx)
		return db, &outboxStoreForTest{db: db}
	}

	t.Run("A above the maximum lease budget splits into B + remainder", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		const A = int64(500_000_000)
		const B = int64(200_000_000)
		fake.fund(user, 1_000_000_000)
		fake.mu.Lock()
		fake.grantBudgetOnce = B // the non-conformant under-grant: B < A
		fake.mu.Unlock()
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-35", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-35", user, "CNY")
		p34WaitOutboxStatus(t, ctx, db, "req-35-35", "delivered")
		p35WaitRemainderDelivered(t, ctx, db, eventID+":r1")

		// The under-granted lease WAS installed (§11.3's deliberate install)
		// and captured exactly B; the remainder captured A − B on a
		// conformant settle lease.
		fake.mu.Lock()
		var budgetB, capturedB, capturedRest int64
		for _, l := range fake.leases[user] {
			if l.Budget == B && l.Captured == B {
				budgetB, capturedB = l.Budget, l.Captured
			} else if l.Captured == A-B {
				capturedRest = l.Captured
			}
		}
		fake.mu.Unlock()
		require.Equal(t, B, budgetB, "the under-granted lease exists with budget B")
		require.Equal(t, B, capturedB, "the under-granted lease captured B")
		require.Equal(t, A-B, capturedRest, "the remainder captured A − B")
		// The under-granted lease's hash exists in Redis (the install).
		fake.mu.Lock()
		var underLeaseID string
		for _, l := range fake.leases[user] {
			if l.Budget == B && l.Purpose == "settle" {
				underLeaseID = l.ID
			}
		}
		fake.mu.Unlock()
		_, err := store.GetCanonicalWalletLeaseByID(ctx, user, underLeaseID)
		require.NoError(t, err, "the under-granted settle lease was installed (its hash exists)")
	})

	t.Run("the ninth split is split_exhausted and the first eight capture 1 each", func(t *testing.T) {
		db, outbox := p35Outbox(t)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000)
		fake.mu.Lock()
		fake.alwaysRefuseHeadroom = 1 // every capture above 1 unit is refused with headroom = 1
		fake.mu.Unlock()
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)
		exhaustedBase := canonicalWalletBridgeMetrics.settlementSplitExhausted.Load()

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-35x", PlatformUserID: user, Currency: "CNY", AmountUnits: 9_000_000, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-35x", user, "CNY")

		// The chain terminates: exactly one dead_letter row with reason
		// split_exhausted, and the fake's total captures sum to 8 units.
		deadline := time.Now().Add(60 * time.Second)
		for {
			var n int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'dead_letter' AND dead_letter_reason = 'split_exhausted'`).Scan(&n))
			if n == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the chain never dead-lettered split_exhausted (found %d)", n)
			}
			time.Sleep(100 * time.Millisecond)
		}
		require.GreaterOrEqual(t, canonicalWalletBridgeMetrics.settlementSplitExhausted.Load()-exhaustedBase, int64(1))
		fake.mu.Lock()
		var totalCaptured int64
		for _, l := range fake.leases[user] {
			totalCaptured += l.Captured
		}
		fake.mu.Unlock()
		require.Equal(t, int64(8), totalCaptured, "the first eight splits captured 1 unit each — the chain's shape, not only its terminus")
		_ = eventID
	})
}

// Test 36 — the late capture (§11.4): a settlement bound to a lease the
// control plane has closed releases the reservation IN FULL (the full form —
// the marker goes with it, no release marker), unbinds, and re-targets a
// settle-purpose lease; with the balance exhausted the settle ensure's
// insufficient_balance is the receivable (balance_shortfall dead-letters and
// the gauge sums their units); a split-then-uncollectable chain sums to
// exactly A − H.
func TestPhase35LateCaptureRetargetsAndTheReceivable(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	// p35Harness: a fresh outbox + Redis + fake per leg (the gauge sums the
	// WHOLE table, so legs must not share one).
	p35Harness := func(t *testing.T) (*sql.DB, *outboxStoreForTest, *redis.Client, *gatewayCacheAdapterForTest, *fakeEnsureControlPlane) {
		t.Helper()
		db := startCanonicalWalletTestPostgres(t, ctx)
		rdb := startCanonicalWalletTestRedis(t, ctx)
		return db, &outboxStoreForTest{db: db}, rdb, &gatewayCacheAdapterForTest{rdb: rdb}, newFakeEnsureControlPlane(t, func() time.Time { return now })
	}

	t.Run("a closed lease releases in full, unbinds and re-targets", func(t *testing.T) {
		db, outbox, rdb, store, fake := p35Harness(t)
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L = "srv-late-l"
		fake.seedLease(user, L, "authorize", 500_000_000, 0, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
		fake.close(user, L)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)
		retargetedBase := canonicalWalletBridgeMetrics.lateCaptureRetargeted.Load()

		const A = int64(30_000_000)
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-36", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-36", user, "CNY")

		// The first delivery: ShipAny refuses lease_not_capturable → release
		// in full, unbind, one failed attempt (the row retries unbound).
		p34WaitOutboxLeaseUnbound(t, ctx, db, eventID)
		require.Equal(t, "30000000", p34bHashField(t, ctx, store, user, L, "released_units"), "released_units == A — the FULL form")
		require.ErrorIs(t, rdb.Get(ctx, testCanonicalWalletReservationKey(user, eventID)).Err(), redis.Nil, "the reservation marker is gone")
		require.Equal(t, int64(0), rdb.Exists(ctx, testCanonicalWalletReleaseMarkerKey(user, eventID)).Val(), "no release marker — the full form is pinned (§11.4)")
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.lateCaptureRetargeted.Load()-retargetedBase)

		// The END state, not the transient pending: the still-running
		// dispatcher re-claims the row on its next tick (pending →
		// in_flight), so which of the two an instantaneous read catches is
		// a race (round-1 MAJOR-1). Delivered, bound to a lease that is
		// NOT the closed one, one failed attempt, not dead-lettered — the
		// stable proof the charge landed on a fresh settle lease.
		p34WaitOutboxStatus(t, ctx, db, "req-35-36", "delivered")
		var finalLease string
		var attempts int
		var deadLetterReason sql.NullString
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT COALESCE(lease_id, ''), attempt_count, dead_letter_reason FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&finalLease, &attempts, &deadLetterReason))
		require.NotEmpty(t, finalLease, "the delivered charge is bound to a lease")
		require.NotEqual(t, L, finalLease, "the charge landed on a fresh settle lease, not the closed one")
		require.Equal(t, 1, attempts, "exactly one failed attempt — the row retried unbound")
		require.False(t, deadLetterReason.Valid, "the row retried rather than dead-lettered")
		fake.mu.Lock()
		var settleCaptured int64
		for _, l := range fake.leases[user] {
			if l.Purpose == "settle" {
				settleCaptured += l.Captured
			}
		}
		require.Equal(t, int64(0), fake.lease(user, L).Captured, "the closed lease captured nothing")
		fake.mu.Unlock()
		require.Equal(t, A, settleCaptured, "the charge landed once, on a fresh settle lease")

		b.refreshReceivableGauge(ctx)
		require.Equal(t, int64(0), canonicalWalletBridgeStatsValue("settlement_uncollectable_units"), "nothing is uncollectable in this leg (the gauge reads this leg's table)")
	})

	t.Run("an exhausted balance is the receivable", func(t *testing.T) {
		db, outbox, _, store, fake := p35Harness(t)
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 0) // nothing left to issue against
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)

		const A = int64(30_000_000)
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-36b", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now,
		}))
		p34WaitOutboxStatus(t, ctx, db, "req-35-36b", "dead_letter")
		var reason sql.NullString
		require.NoError(t, db.QueryRowContext(ctx, `SELECT dead_letter_reason FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-35-36b'`).Scan(&reason))
		require.True(t, reason.Valid)
		require.Equal(t, "balance_shortfall", reason.String)

		b.refreshReceivableGauge(ctx)
		require.Equal(t, A, canonicalWalletBridgeStatsValue("settlement_uncollectable_units"), "the dead-lettered units are the receivable (§11.4)")
	})

	t.Run("a split-then-uncollectable chain sums to A − H", func(t *testing.T) {
		db, outbox, _, store, fake := p35Harness(t)
		user := "shipany-user-" + uuid.NewString()
		const A = int64(400_000_000)
		const H = int64(50_000_000)
		const L = "srv-late-l3"
		// The server's lease has headroom H; A is large enough that the
		// cached lease's own remaining (100M) no longer covers the remainder
		// (350M), so the remainder's ensure reaches the server — whose
		// balance (10M) cannot cover it: the parent captures H, the
		// remainder dead-letters balance_shortfall, and the receivable sums
		// to exactly A − H (the parent's rewrite is what makes that true).
		fake.seedLease(user, L, "authorize", 500_000_000, 450_000_000, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
		fake.fund(user, 10_000_000)
		b := p34DispatcherBridge(t, fake, store, db, outbox, now)

		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-36c", PlatformUserID: user, Currency: "CNY", AmountUnits: A, LeaseID: L, OccurredAt: now,
		}))
		eventID := CanonicalWalletSettlementEventID("req-35-36c", user, "CNY")
		p34WaitOutboxStatus(t, ctx, db, "req-35-36c", "delivered") // the parent captures H on L
		deadline := time.Now().Add(30 * time.Second)
		for {
			var n int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'dead_letter' AND dead_letter_reason = 'balance_shortfall'`).Scan(&n))
			if n == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the remainder never dead-lettered (found %d)", n)
			}
			time.Sleep(100 * time.Millisecond)
		}
		fake.mu.Lock()
		require.Equal(t, int64(500_000_000), fake.lease(user, L).Captured, "the parent captured exactly the seed 450M + H")
		fake.mu.Unlock()

		b.refreshReceivableGauge(ctx)
		require.Equal(t, A-H, canonicalWalletBridgeStatsValue("settlement_uncollectable_units"), "the split-then-uncollectable chain sums to exactly A − H")
		_ = eventID
	})
}

// p34WaitOutboxLeaseUnbound polls until the row's binding is released.
func p34WaitOutboxLeaseUnbound(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var leaseID string
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(lease_id, '') FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&leaseID); err == nil && leaseID == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("row %s was never unbound", eventID)
}

// Test 38 — §11.6's four token states, each end-to-end through the v2 route:
// for every state the fake's captured amount equals the settled amount to
// the unit, with the hold metric deltas §10.5/§11.6 name.
func TestPhase35FourTokenStatesToTheUnit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	const A = int64(30_000_000)

	// state: a fresh holds-on world (own Redis + outbox so the reaper's
	// global sets and the gauge stay hermetic).
	state := func(t *testing.T) (*sql.DB, *outboxStoreForTest, *redis.Client, *gatewayCacheAdapterForTest, *fakeEnsureControlPlane, *CanonicalWalletBridge) {
		t.Helper()
		db := startCanonicalWalletTestPostgres(t, ctx)
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		b := p34bDispatcherBridge(t, fake, store, db, &outboxStoreForTest{db: db}, now, 300)
		return db, &outboxStoreForTest{db: db}, rdb, store, fake, b
	}
	capturedByUser := func(t *testing.T, fake *fakeEnsureControlPlane, user string) int64 {
		t.Helper()
		fake.mu.Lock()
		defer fake.mu.Unlock()
		var total int64
		for _, l := range fake.leases[user] {
			total += l.Captured
		}
		return total
	}

	t.Run("null token reserves at delivery", func(t *testing.T) {
		db, _, _, store, fake, b := state(t)
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		missingBase := canonicalWalletBridgeMetrics.holdMissingAtSettlement.Load()
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-38a", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now,
		}))
		p34WaitOutboxStatus(t, ctx, db, "req-35-38a", "delivered")
		require.Equal(t, A, capturedByUser(t, fake, user), "captured == settled, to the unit")
		require.Equal(t, int64(0), canonicalWalletBridgeMetrics.holdMissingAtSettlement.Load()-missingBase, "a token-less settlement converts nothing (§11.6 null)")
		_ = store
		_ = b
	})

	t.Run("live armed hold converts", func(t *testing.T) {
		db, _, _, store, fake, b := state(t)
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L = "srv-38-l"
		fake.seedLease(user, L, "authorize", 500_000_000, 0, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
		const E = int64(40_000_000)
		_, _, _, err := store.ArmCanonicalWalletHold(ctx, user, L, "CNY", "auth-38b", E, 900_000, now)
		require.NoError(t, err)
		convertedBase := canonicalWalletBridgeMetrics.holdConverted.Load()
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-38b", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-38b",
		}))
		p34WaitOutboxStatus(t, ctx, db, "req-35-38b", "delivered")
		require.Equal(t, A, capturedByUser(t, fake, user), "captured == settled, to the unit")
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdConverted.Load()-convertedBase)
	})

	t.Run("a not_written-released hold settles unbound", func(t *testing.T) {
		db, _, _, store, fake, b := state(t)
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L = "srv-38-l"
		fake.seedLease(user, L, "authorize", 500_000_000, 0, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
		_, _, _, err := store.ArmCanonicalWalletHold(ctx, user, L, "CNY", "auth-38c", A, 900_000, now)
		require.NoError(t, err)
		_, err = store.ReleaseCanonicalWalletHold(ctx, user, "auth-38c", "released", "not_written")
		require.NoError(t, err)
		afterReleaseBase := canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-38c", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-38c",
		}))
		p34WaitOutboxStatus(t, ctx, db, "req-35-38c", "delivered")
		require.Equal(t, A, capturedByUser(t, fake, user), "captured == settled, to the unit — on a settle lease")
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()-afterReleaseBase)
		require.Equal(t, "0", p34bHashField(t, ctx, store, user, L, "captured_units"), "L itself captured nothing (the field is absent = 0)")
	})

	t.Run("an abandoned hold settles unbound", func(t *testing.T) {
		db, _, rdb, store, fake, b := state(t)
		// the reaper's liveness read and outcome row need 211/213 on this
		// leg's throwaway database (the p34bApplyMigration pattern).
		p34bApplyMigration(t, ctx, db, "211_wallet_live_provisional.sql")
		p34bApplyMigration(t, ctx, db, "213_wallet_hold_outcome.sql")
		user := "shipany-user-" + uuid.NewString()
		fake.fund(user, 100_000_000_000)
		const L = "srv-38-l"
		fake.seedLease(user, L, "authorize", 500_000_000, 0, now.Add(5*time.Minute))
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: L, PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
		_, _, _, err := store.ArmCanonicalWalletHold(ctx, user, L, "CNY", "auth-38d", A, 900_000, now)
		require.NoError(t, err)
		require.NoError(t, rdb.Del(ctx, testCanonicalWalletReaperTickKey).Err())
		b.reapOnce(ctx, now.Add(20*time.Minute)) // past the 900s grace
		require.Equal(t, "abandoned", p34bHoldState(t, ctx, store, user, "auth-38d"))
		afterReleaseBase := canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-35-38d", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-38d",
		}))
		p34WaitOutboxStatus(t, ctx, db, "req-35-38d", "delivered")
		require.Equal(t, A, capturedByUser(t, fake, user), "captured == settled, to the unit — on a settle lease")
		require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()-afterReleaseBase)
	})
}

// Test 39 — §11.7's zero-cost abort points and the token-less alert: each
// early return of the observer releases an armed hold (state = released,
// class = zero_cost) when holds are on and an authorization id is in hand;
// holds off writes nothing; a settlement that reaches ObserveSettlement
// carrying no authorization under holds is counted and logged — with the row
// still inserted (no behaviour change).
func TestPhase35ZeroCostAbortPointsReleaseTheHold(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()

	// newWorld: a fresh fake + a FRESH throwaway outbox per leg (every
	// bridge's dispatcher ticker outlives its leg — a shared table would let
	// a foreign bridge deliver a later leg's row against its own fake).
	newWorld := func(t *testing.T, holds string) (*fakeEnsureControlPlane, *gatewayCacheAdapterForTest, *CanonicalWalletBridge, *sql.DB) {
		t.Helper()
		db := startCanonicalWalletTestPostgres(t, ctx)
		fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
		cfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
		cfg.Holds = holds
		cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
		cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 300
		cfg.LeaseBudgetUnits = 500_000_000
		client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
		client.now = func() time.Time { return now }
		return fake, store, newCanonicalWalletBridge(cfg, store, client, db, &outboxStoreForTest{db: db}, 0, func() time.Time { return now }), db
	}
	arm := func(t *testing.T, user, authID string) {
		t.Helper()
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: "srv-39-l", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: now.Add(30 * time.Minute),
		}))
		_, _, _, err := store.ArmCanonicalWalletHold(ctx, user, "srv-39-l", "CNY", authID, 10_000_000, 900_000, now)
		require.NoError(t, err)
	}

	zeroCostBase := canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load()
	legs := []struct {
		name string
		cost *CostBreakdown
		sub  bool
		appl bool
	}{
		{"subscription-billed releases", &CostBreakdown{ActualCost: 1}, true, true},
		{"billing not applied releases", &CostBreakdown{ActualCost: 1}, false, false},
		{"zero cost releases", &CostBreakdown{ActualCost: 0}, false, true},
		{"rounds to zero releases", &CostBreakdown{ActualCost: 0.000000004}, false, true},
	}
	for i, leg := range legs {
		t.Run(leg.name, func(t *testing.T) {
			_, _, b, _ := newWorld(t, "on")
			user := &User{ID: int64(100 + i), PlatformUserID: "shipany-user-39-" + itoa(i), BillingCurrency: "CNY", Balance: 10}
			authID := "auth-39-" + itoa(i)
			arm(t, user.PlatformUserID, authID)
			require.False(t, observeCanonicalWalletSettlement(b, "req-39-"+itoa(i), user, leg.cost, leg.sub, leg.appl, nil, "tok", authID))
			hold, err := store.GetCanonicalWalletHold(ctx, user.PlatformUserID, authID)
			require.NoError(t, err)
			require.Equal(t, "released", hold.State)
			require.Equal(t, "zero_cost", hold.Class)
		})
	}
	require.Equal(t, int64(len(legs)), canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load()-zeroCostBase, "each abort point released exactly one hold")

	// holds OFF: nothing is written — the manually-armed hold stays armed.
	t.Run("holds off writes nothing", func(t *testing.T) {
		_, _, b, _ := newWorld(t, "off")
		user := &User{ID: int64(200), PlatformUserID: "shipany-user-39-off", BillingCurrency: "CNY", Balance: 10}
		authID := "auth-39-off"
		arm(t, user.PlatformUserID, authID)
		base := canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load()
		require.False(t, observeCanonicalWalletSettlement(b, "req-39-off", user, &CostBreakdown{ActualCost: 0}, false, true, nil, "tok", authID))
		hold, err := store.GetCanonicalWalletHold(ctx, user.PlatformUserID, authID)
		require.NoError(t, err)
		require.Equal(t, "armed", hold.State, "holds off is 3.4a byte-for-byte — no abort-point release")
		require.Equal(t, base, canonicalWalletBridgeMetrics.holdReleasedZeroCost.Load())
	})

	// The token-less alert: a settlement with AuthorizationID == "" under
	// holds is counted (and logged with the gateway request id) while the row
	// is still inserted — no behaviour change.
	t.Run("a token-less settlement is counted and still enqueued", func(t *testing.T) {
		fake, _, b, tlDB := newWorld(t, "on")
		fake.fund("shipany-user-39-tl", 100_000_000_000)
		user := "shipany-user-39-tl"
		tokenlessBase := canonicalWalletBridgeStatsValue("settlement_without_authorization")
		require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-39-tl", PlatformUserID: user, Currency: "CNY", AmountUnits: 10_000_000, OccurredAt: now,
		}))
		require.Equal(t, tokenlessBase+1, canonicalWalletBridgeStatsValue("settlement_without_authorization"), "counted once")
		var rows int
		require.NoError(t, tlDB.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-39-tl'`).Scan(&rows))
		require.Equal(t, 1, rows, "no behaviour change — the row is inserted")
		p34WaitOutboxStatus(t, ctx, tlDB, "req-39-tl", "delivered")
	})
}

// Test 40 — §11.8's repricing, pinned not mechanised: the same
// GatewayRequestID with a different amount either dedups (same hash) or is
// rejected as a payload conflict (different hash — observed: the conflict)
// with exactly one row and queueDropped counted; on the wire, a redelivery
// of a captured event with a different amount is the fake's
// settlement_payload_conflict → terminal payload_conflict (Task 4).
func TestPhase35RepricingIsPinned(t *testing.T) {
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

	const A1 = int64(30_000_000)
	const A2 = int64(31_000_000)
	droppedBase := canonicalWalletBridgeStatsValue("queue_dropped")

	// In-process: the first submission enqueues; the repriced resubmission
	// under the same event id is a payload conflict — false, one row,
	// queueDropped counted. (CanonicalWalletSettlementEventID excludes the
	// amount, so both carry the SAME event id.)
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-40", PlatformUserID: user, Currency: "CNY", AmountUnits: A1, OccurredAt: now,
	}))
	require.False(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-35-40", PlatformUserID: user, Currency: "CNY", AmountUnits: A2, OccurredAt: now,
	}), "the repriced resubmission is rejected by the payload-hash identity")
	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-35-40'`).Scan(&rows))
	require.Equal(t, 1, rows, "exactly one outbox row exists")
	require.Equal(t, droppedBase+1, canonicalWalletBridgeStatsValue("queue_dropped"), "the rejected resubmission is counted queue_dropped")

	// On the wire: the row delivers at A1; a redelivery of the captured
	// event carrying A2 is the fake's settlement_payload_conflict → terminal
	// payload_conflict.
	p34WaitOutboxStatus(t, ctx, db, "req-35-40", "delivered")
	eventID := CanonicalWalletSettlementEventID("req-35-40", user, "CNY")
	var id int64
	require.NoError(t, db.QueryRowContext(ctx,
		`UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now(), amount_units = $3 WHERE event_id = $1 RETURNING id`,
		eventID, b.workerID, A2).Scan(&id))
	b.deliverOutboxEvent(ctx, CanonicalWalletOutboxEvent{
		ID: id, EventID: eventID, GatewayRequestID: "req-35-40", PlatformUserID: user,
		Currency: "CNY", AmountUnits: A2, OccurredAt: now,
	})
	var status string
	var attempts int
	var reason sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT status, attempt_count, dead_letter_reason FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status, &attempts, &reason))
	require.Equal(t, "dead_letter", status)
	require.Equal(t, 1, attempts, "terminal on the first attempt")
	require.True(t, reason.Valid)
	require.Equal(t, "payload_conflict", reason.String)
	fake.mu.Lock()
	stored := fake.events[user][eventID]
	require.Equal(t, A1, stored.Units, "the captured event keeps the FIRST amount (§10.5's accepted drift)")
	fake.mu.Unlock()
}
