//go:build integration

package service

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func p34DispatcherBridge(t *testing.T, fake *fakeEnsureControlPlane, store CanonicalWalletLeaseStore, db *sql.DB, outbox CanonicalWalletOutboxStore, now time.Time) *CanonicalWalletBridge {
	t.Helper()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 300 // 300 ms: the per-event budget AND the tick, as the existing dispatcher test uses
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	// §9.5: the clock goes through the CONSTRUCTOR — the dispatcher goroutine
	// sees it from its first tick (the old post-construction assignment raced it).
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, func() time.Time { return now })
	t.Cleanup(b.Close)
	return b
}

// p34WaitOutboxStatus polls the row (status values are the strings
// wallet_outbox.go writes: pending | in_flight | delivered | dead_letter).
func p34WaitOutboxStatus(t *testing.T, ctx context.Context, db *sql.DB, requestID, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, requestID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("outbox row %s never reached status %s", requestID, want)
}

// Test 16 — K = 16 dispatchers fund only each event's exact settlement amount.
// Every event captures once on its own funding, with no window idempotency key.
func TestPhase34Proto16SixteenDispatchersExactSettlementBudgetsNoWindowKey(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)

	// an exhausted current lease in Redis, and NON-COVERING on the server too
	// (captured 495,000,000 → headroom 5,000,000 < the 10,000,000 ask): §3 step 2
	// would otherwise reuse it forever — see Known limits.
	fake.seedLease(user, "srv-exhausted", "authorize", 500_000_000, 495_000_000, now.Add(5*time.Minute))
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{LeaseID: "srv-exhausted", PlatformUserID: user, Currency: "USD", BudgetUnits: 500_000_000, ConsumedUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute)}))

	bridges := make([]*CanonicalWalletBridge, 0, 16)
	for i := 0; i < 16; i++ {
		bridges = append(bridges, p34DispatcherBridge(t, fake, store, db, outbox, now))
	}
	for i := 0; i < 16; i++ {
		bridges[i%16].ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-k16-" + itoa(i), PlatformUserID: user, Currency: "USD", AmountUnits: 10_000_000})
	}
	for i := 0; i < 16; i++ {
		p34WaitOutboxStatus(t, ctx, db, "req-k16-"+itoa(i), "delivered")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 16, fake.issuances, "each exact settlement budget backs one event")
	require.Len(t, fake.events[user], 16, "every event captured exactly once")
	var totalCaptured int64
	seenLeases := map[string]bool{}
	for i := 0; i < 16; i++ {
		eventID := CanonicalWalletSettlementEventID("req-k16-"+itoa(i), user, "USD")
		event, ok := fake.events[user][eventID]
		require.True(t, ok, "each durable event has a matching control-plane capture")
		var boundLease string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_id FROM wallet_settlement_outbox WHERE event_id=$1`, eventID).Scan(&boundLease))
		require.Equal(t, boundLease, event.LeaseID, "the durable binding matches the captured funding")
		require.Equal(t, int64(10_000_000), event.Units)
		require.False(t, seenLeases[event.LeaseID], "exact funding cannot be charged for two events")
		seenLeases[event.LeaseID] = true
		lease := fake.lease(user, event.LeaseID)
		require.Equal(t, "settle", lease.FundingScope)
		require.Equal(t, event.Units, lease.Budget)
		require.Equal(t, event.Units, lease.Captured)
		totalCaptured += event.Units
	}
	require.Equal(t, int64(160_000_000), totalCaptured)
	require.Equal(t, int64(100_000_000_000)-totalCaptured, fake.balance[user], "unleased balance and captured fees conserve the grant")
	require.Equal(t, int64(495_000_000), fake.lease(user, "srv-exhausted").Captured, "the original source was not charged again")
	require.Equal(t, int64(100_500_000_000), fake.fakeCanonicalBalance(user)+totalCaptured+fake.lease(user, "srv-exhausted").Captured, "all available credit and captured fees conserve the grant plus seeded source")
	require.NotEmpty(t, fake.requests)
	for _, req := range fake.requests {
		require.Equal(t, "settle", req.Purpose, "the dispatcher ensures with purpose = settle")
		require.Equal(t, "settle", req.FundingScope)
		require.Equal(t, int64(10_000_000), mustUnits(req.RequestedBudget))
	}
	require.NotEmpty(t, fake.ensureHeaders, "the assertion below must have had something to check")
	for _, h := range fake.ensureHeaders {
		require.Empty(t, h.Get("Idempotency-Key"), "no gwlease_ window key on the ensure wire")
	}
}

// Test 19 — dispatcher under the cap: with the user's authorize leases at the
// cap and every one non-covering, a settlement is delivered via a settle-purpose
// lease, never dead-lettered; ErrCanonicalWalletBalanceShortfall dead-letters
// immediately (attempt_count 1) with reason balance_shortfall; lease_contention
// retries on the backoff and delivers.
func TestPhase34Proto19DispatcherUnderCapShortfallAndContention(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	base := canonicalWalletBridgeMetrics.deadLetterBalanceShortfall.Load()

	// under the cap: three slot-holding authorize leases, none covering a 10,000,000 ask
	capped := "shipany-user-" + uuid.NewString()
	fake.fund(capped, 100_000_000_000)
	for i := 0; i < 3; i++ {
		fake.seedLease(capped, "srv-cap-"+itoa(i), "authorize", 500_000_000, 495_000_000, now.Add(5*time.Minute))
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{LeaseID: "srv-cap-2", PlatformUserID: capped, Currency: "USD", BudgetUnits: 500_000_000, ConsumedUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute)}))
	// shortfall
	broke := "shipany-user-" + uuid.NewString()
	fake.fund(broke, 1_000_000) // 0.01 CNY — below the 10,000,000-unit settlement, so the clamp lands under min_headroom_units
	// contention (transient): the first ensure for this user is refused with lease_contention
	racy := "shipany-user-" + uuid.NewString()
	fake.fund(racy, 10_000_000_000)

	b := p34DispatcherBridge(t, fake, store, db, outbox, now)
	b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-capped", PlatformUserID: capped, Currency: "USD", AmountUnits: 10_000_000})
	b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-broke", PlatformUserID: broke, Currency: "USD", AmountUnits: 10_000_000})
	p34WaitOutboxStatus(t, ctx, db, "req-capped", "delivered")
	p34WaitOutboxStatus(t, ctx, db, "req-broke", "dead_letter")

	settle := 0
	fake.mu.Lock()
	for _, l := range fake.leases[capped] {
		if l.Purpose == "settle" {
			settle++
		}
	}
	fake.mu.Unlock()
	fake.setContentionOnce() // set only after req-capped/req-broke are terminal: no other row is claimable, so the racy user's FIRST ensure gets it
	require.Equal(t, 1, settle, "delivered through an issued settle-purpose lease at the cap")

	var attempts int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT attempt_count FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-broke'`).Scan(&attempts))
	require.Equal(t, 1, attempts, "balance shortfall is terminal: dead-lettered on the first attempt, no backoff retries")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.deadLetterBalanceShortfall.Load()-base, "counted under reason balance_shortfall")

	b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-racy", PlatformUserID: racy, Currency: "USD", AmountUnits: 10_000_000})
	p34WaitOutboxStatus(t, ctx, db, "req-racy", "delivered") // first attempt: lease_contention → MarkOutboxEventFailed (2 s backoff); second: issued → delivered
	var racyAttempts int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT attempt_count FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-racy'`).Scan(&racyAttempts))
	require.Equal(t, 1, racyAttempts, "lease_contention is transient: one failed attempt, then delivered on the backoff")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.deadLetterBalanceShortfall.Load()-base, "contention never dead-letters")

	// §9.3's under-grant leg (retargeted by §11.3, Task 5): the fake answers
	// headroom < min once (a non-conformant server's grant) → the settle
	// purpose now INSTALLS the under-granted lease and the dispatcher SPLITS
	// before reserving (H = the grant, remainder 1 unit) → both halves
	// deliver, no failed attempt, never dead-lettered.
	under := "shipany-user-" + uuid.NewString()
	fake.fund(under, 10_000_000_000)
	fake.mu.Lock()
	fake.grantBelowMinOnce = true // no other row is claimable: only the under user's first ensure sees it
	fake.mu.Unlock()
	b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-under", PlatformUserID: under, Currency: "USD", AmountUnits: 10_000_000})
	p34WaitOutboxStatus(t, ctx, db, "req-under", "delivered") // the under-grant splits (H = 9,999,999, remainder 1) and both halves deliver
	var underAttempts int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT attempt_count FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-under'`).Scan(&underAttempts))
	require.Equal(t, 0, underAttempts, "§11.3: the under-grant's split consumes no attempt — the grant is not a failure")
	var underReason sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT dead_letter_reason FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-under'`).Scan(&underReason))
	require.False(t, underReason.Valid, "an under-grant never dead-letters, so no reason is persisted")
}

// Test 19b — the dispatcher's missing-bound-lease path puts prefer_lease_id on the wire
// (test 20's dispatcher twin; resolveOutboxEventLease's Missing branch).
func TestPhase34Proto19bDispatcherRecoversABoundLeaseThroughPreferLeaseID(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	const X = "srv-bound-x"
	// X exists on the server, covering and unexpired; it is NOT installed in Redis (the loss).
	fake.seedLease(user, X, "authorize", 500_000_000, 0, now.Add(5*time.Minute))
	b := p34DispatcherBridge(t, fake, store, db, outbox, now)
	// The row is born bound: ObserveSettlement with LeaseID set (BindOutboxEventLease is
	// claim-guarded and cannot be called by a test that does not own the row).
	b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-19b", PlatformUserID: user, Currency: "USD", AmountUnits: 10_000_000, LeaseID: X})
	p34WaitOutboxStatus(t, ctx, db, "req-19b", "delivered")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	last := fake.requests[len(fake.requests)-1]
	require.Equal(t, X, last.PreferLeaseID, "the missing bound lease is recovered through prefer_lease_id")
	require.Empty(t, last.Drained)
	require.Equal(t, 0, fake.issuances, "reused, never issued")
}
