//go:build integration

package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Phase 3.4b (redesign §10.8, tests 22–29): holds armed at authorization,
// released on not_written, converted at settlement, the seal's third value on
// the drain wire — against real Redis + the §3 fake (+ Postgres wherever the
// dispatcher is exercised). Every clock is the bridge's injectable one.

// p34bHoldsConfig: the Phase-34 test config with canonical_wallet.holds on
// and the reaper's numbers at their defaults.
func p34bHoldsConfig(mode string) config.CanonicalWalletConfig {
	cfg := canonicalWalletTestConfig(mode)
	cfg.Holds = "on"
	cfg.OrphanGraceSeconds = 900
	cfg.OrphanSweepIntervalSeconds = 60
	cfg.OrphanSweepBatch = 200
	return cfg
}

// p34bBridge: real Redis + the §3 fake over HTTP + a fixed clock, holds ON.
func p34bBridge(t *testing.T, ctx context.Context, mode string, fake *fakeEnsureControlPlane, store CanonicalWalletLeaseStore, now time.Time) *CanonicalWalletBridge {
	t.Helper()
	cfg := p34bHoldsConfig(mode)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS = int(p34Margin / time.Millisecond)
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	return newCanonicalWalletBridge(cfg, store, client, nil, nil, 0, func() time.Time { return now })
}

// p34bDispatcherBridge: the p34DispatcherBridge shape with holds ON and a
// per-attempt budget the test can widen (test 25 keeps the dispatcher's first
// tick far away so an authorize's drain is asserted deterministically before
// delivery).
func p34bDispatcherBridge(t *testing.T, fake *fakeEnsureControlPlane, store CanonicalWalletLeaseStore, db *sql.DB, outbox CanonicalWalletOutboxStore, now time.Time, requestTimeoutMS int) *CanonicalWalletBridge {
	t.Helper()
	cfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, requestTimeoutMS
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	return newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, func() time.Time { return now })
}

// p34bAuthorize drives the real authorization point (the authorizer + the
// bridge + the snapshot fixture available under the integration tag) and
// returns the handle.
func p34bAuthorize(t *testing.T, ctx context.Context, mode string, b *CanonicalWalletBridge, user, body string) *AuthorizationHandle {
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
	cfg.CanonicalWallet.LeaseBudgetUnits = 500_000_000
	auth := NewCanonicalWalletAuthorizer(cfg, b, snapshots)
	h, err := auth.Authorize(ctx, AuthorizeInput{Snapshot: snap, Estimate: EstimateInputFromRequestBody([]byte(body), EstimateInputOptions{}), User: apiKey.User})
	if err != nil {
		// enforce mode legitimately refuses; anything else is a failure
		require.True(t, errors.Is(err, ErrAuthorizationRefused), "unexpected authorize error: %v", err)
	}
	require.NotNil(t, h)
	return h
}

func p34bHashField(t *testing.T, ctx context.Context, store CanonicalWalletLeaseStore, user, leaseID, field string) string {
	t.Helper()
	adapter := store.(*gatewayCacheAdapterForTest)
	v, err := adapter.rdb.HGet(ctx, testCanonicalWalletLeaseKey(user, leaseID), field).Result()
	if err == redis.Nil {
		return "0" // an absent amount field is zero by definition (§10.2)
	}
	require.NoError(t, err)
	return v
}

// Test 22 — arm on authorize; holds off arms nothing and the dispatcher
// reserves at delivery (3.4a byte-for-byte, §10.1).
func TestPhase34bHoldArmedAtAuthorize(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()

	// OFF leg first, on a clean keyspace: after Authorize no hold key exists,
	// consumed_units is unchanged and the handle arms nothing; a settlement
	// through the dispatcher then reserves at delivery exactly as 3.4a's
	// test 19 asserts.
	fakeOff := newFakeEnsureControlPlane(t, func() time.Time { return now })
	userOff := "shipany-user-" + uuid.NewString()
	fakeOff.fund(userOff, 10_000_000_000)
	offCfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
	offCfg.Holds = "off"
	offCfg.ControlPlaneURL, offCfg.Secret = fakeOff.Server.URL, strings.Repeat("s", 32)
	offCfg.ExpirySkewMarginMS, offCfg.RequestTimeoutMS = 50, 300
	offCfg.LeaseBudgetUnits = 500_000_000
	offClient := newCanonicalWalletHTTPClient(offCfg, fakeOff.Server.Client())
	offClient.now = func() time.Time { return now }
	bOff := newCanonicalWalletBridge(offCfg, store, offClient, db, outbox, 0, func() time.Time { return now })
	hOff := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, bOff, userOff, `{"max_tokens":64}`)
	require.False(t, hOff.HoldArmed, "holds off: 3.4a byte-for-byte")
	keys, err := rdb.Keys(ctx, "canonical_wallet:hold*").Result()
	require.NoError(t, err)
	require.Empty(t, keys, "no hold key is ever written with holds off")
	require.Equal(t, "0", p34bHashField(t, ctx, store, userOff, hOff.LeaseID, "consumed_units"), "nothing was armed, nothing consumed")
	// the settlement reserves at delivery (no AuthorizationID, no conversion):
	// consumed on the lease rises by A only when the dispatcher delivers.
	bOff.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-22-off", PlatformUserID: userOff, Currency: "CNY", AmountUnits: 10_000_000, OccurredAt: now})
	p34WaitOutboxStatus(t, ctx, db, "req-22-off", "delivered")
	require.Equal(t, "10000000", p34bHashField(t, ctx, store, userOff, hOff.LeaseID, "consumed_units"), "the dispatcher reserved the settlement amount at delivery, as today")

	// ON leg: the hold hash exists with state=armed, held_units == E, the
	// lease's consumed rose by E, both indexes carry the ids.
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	b := p34bBridge(t, ctx, config.CanonicalWalletModeEnforce, fake, store, now)
	h := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
	require.True(t, h.HoldArmed)
	require.Equal(t, h.EstimatedUnits, h.HeldUnits)
	hold, err := store.GetCanonicalWalletHold(ctx, user, h.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	require.Equal(t, h.LeaseID, hold.LeaseID)
	require.Equal(t, h.EstimatedUnits, hold.HeldUnits)
	require.Equal(t, "0", p34bHashField(t, ctx, store, user, h.LeaseID, "released_units"))
	require.Equal(t, fmt.Sprintf("%d", h.EstimatedUnits), p34bHashField(t, ctx, store, user, h.LeaseID, "consumed_units"), "consumed rose by exactly E")
	ids, err := store.ListCanonicalWalletHolds(ctx, user, 100)
	require.NoError(t, err)
	require.Contains(t, ids, h.ID)
	users, _, err := store.ListCanonicalWalletHoldUsers(ctx, 0, 100)
	require.NoError(t, err)
	require.Contains(t, users, user)
	// a re-arm with the same authorization id is the {5} duplicate
	_, held, dup, err := store.ArmCanonicalWalletHold(ctx, user, h.LeaseID, "CNY", h.ID, h.EstimatedUnits, 900_000, now)
	require.NoError(t, err)
	require.True(t, dup)
	require.Equal(t, h.EstimatedUnits, held)
}

// Test 23 — not_written releases; cancelled contexts are kept indeterminate;
// a result writes nothing to the hold hash (§10.5).
func TestPhase34bNotWrittenReleases(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)
	b := p34bBridge(t, ctx, config.CanonicalWalletModeEnforce, fake, store, now)

	h := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
	tok := h.MintWriteToken()
	h.RecordOutcome(tok, AuthorizationOutcomeNotWritten, errors.New("dial tcp: connection refused"))
	hold, err := store.GetCanonicalWalletHold(ctx, user, h.ID)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	require.Equal(t, "not_written", hold.Class)
	require.Equal(t, fmt.Sprintf("%d", h.EstimatedUnits), p34bHashField(t, ctx, store, user, h.LeaseID, "released_units"), "released_units += E")
	ids, err := store.ListCanonicalWalletHolds(ctx, user, 100)
	require.NoError(t, err)
	require.NotContains(t, ids, h.ID, "the set no longer carries the released hold")
	// a second identical call is a no-op — no second increment
	h.RecordOutcome(tok, AuthorizationOutcomeNotWritten, errors.New("dial tcp: connection refused"))
	require.Equal(t, fmt.Sprintf("%d", h.EstimatedUnits), p34bHashField(t, ctx, store, user, h.LeaseID, "released_units"))

	// context.Canceled is kept indeterminate (§10.5's cross-check)
	h2 := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
	tok2 := h2.MintWriteToken()
	h2.RecordOutcome(tok2, AuthorizationOutcomeNotWritten, context.Canceled)
	hold2, err := store.GetCanonicalWalletHold(ctx, user, h2.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold2.State, "a cancelled context never releases")
	require.Equal(t, "indeterminate", hold2.Class)

	// a result writes nothing: the hash is byte-identical before/after
	h3 := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, b, user, `{"max_tokens":64}`)
	tok3 := h3.MintWriteToken()
	before, err := rdb.HGetAll(ctx, testCanonicalWalletHoldKey(user, h3.ID)).Result()
	require.NoError(t, err)
	h3.RecordOutcome(tok3, AuthorizationOutcomeResult, nil)
	after, err := rdb.HGetAll(ctx, testCanonicalWalletHoldKey(user, h3.ID)).Result()
	require.NoError(t, err)
	require.Equal(t, before, after, "a result outcome writes nothing to the hold hash")
}

// Test 24 — result then settlement A < E converts the hold in-process; a
// retried submission is {7} with the event id; a not_written release followed
// by a real settlement proceeds unbound on a settle-purpose lease (§10.5).
func TestPhase34bSettlementConvertsTheHold(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	b := p34bDispatcherBridge(t, fake, store, db, outbox, now, 300)

	dupBase := canonicalWalletBridgeMetrics.holdConvertDuplicate.Load()
	afterReleaseBase := canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()

	l1, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	const E = int64(100_000_000)
	const A = int64(40_000_000)
	_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, l1.LeaseID, "CNY", "auth-24", E, 900_000, now)
	require.NoError(t, err)

	eventID := CanonicalWalletSettlementEventID("req-24", user, "CNY")
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-24", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-24"}))

	hold, err := store.GetCanonicalWalletHold(ctx, user, "auth-24")
	require.NoError(t, err)
	require.Equal(t, "settled", hold.State)
	require.Equal(t, eventID, hold.EventID)
	require.Equal(t, "60000000", p34bHashField(t, ctx, store, user, l1.LeaseID, "released_units"), "released += E−A")
	marker, err := rdb.Get(ctx, testCanonicalWalletReservationKey(user, eventID)).Result()
	require.NoError(t, err)
	require.Equal(t, l1.LeaseID, marker, "the conversion pre-writes the event's reservation marker")
	var rowLease string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_id FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-24'`).Scan(&rowLease))
	require.Equal(t, l1.LeaseID, rowLease, "the outbox row binds the hold's lease")

	p34WaitOutboxStatus(t, ctx, db, "req-24", "delivered")
	require.Equal(t, "100000000", p34bHashField(t, ctx, store, user, l1.LeaseID, "consumed_units"), "consumed unchanged across delivery — the reserve answered duplicate")
	fake.mu.Lock()
	require.Equal(t, A, fake.lease(user, l1.LeaseID).Captured, "exactly one settlement of A landed on the fake")
	fake.mu.Unlock()

	// a retried submission with the same event id: {7} with the event id, no
	// second outbox row, holdConvertDuplicate +1
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-24", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-24"}))
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdConvertDuplicate.Load()-dupBase)
	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-24'`).Scan(&rows))
	require.Equal(t, 1, rows, "the outbox's payload-hash idempotency dedups the retried submission")

	// {7}-empty leg (a): a not_written release, then a retried write that
	// settles A → the event proceeds unbound on a settle-purpose lease, with
	// lease 1's consumed unchanged (§10.5).
	_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, l1.LeaseID, "CNY", "auth-24b", E, 900_000, now)
	require.NoError(t, err)
	_, err = store.ReleaseCanonicalWalletHold(ctx, user, "auth-24b", "released", "not_written")
	require.NoError(t, err)
	// exhaust lease 1 so the settle path cannot reuse it (its remaining is
	// the released money: guard 200M − 160M + 300M ≤ 500M holds, so the
	// reserve takes the Go-side remaining 300M)
	_, err = store.ReserveCanonicalWalletLease(ctx, user, l1.LeaseID, "CNY", "evt-24-exhaust", 300_000_000, now)
	require.NoError(t, err)
	consumedBefore := p34bHashField(t, ctx, store, user, l1.LeaseID, "consumed_units")
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-24b", PlatformUserID: user, Currency: "CNY", AmountUnits: 30_000_000, OccurredAt: now, AuthorizationID: "auth-24b"}))
	p34WaitOutboxStatus(t, ctx, db, "req-24b", "delivered")
	require.Equal(t, consumedBefore, p34bHashField(t, ctx, store, user, l1.LeaseID, "consumed_units"), "lease 1's consumed is untouched by the unbound delivery")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdSettlementAfterRelease.Load()-afterReleaseBase)
	fake.mu.Lock()
	var settleLease string
	for _, l := range fake.leases[user] {
		if l.Purpose == "settle" && l.Captured == 30_000_000 {
			settleLease = l.ID
		}
	}
	fake.mu.Unlock()
	require.NotEmpty(t, settleLease, "delivered on a settle-purpose lease")
}

// Test 25 — A > E within budget adds the excess; beyond budget releases the
// hold and delivers unbound; the next authorize's drain carries
// gateway_released == E and the fake closes lease 1 (§10.5, §10.2).
func TestPhase34bOverrun(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	// a wide per-attempt budget keeps the dispatcher's first tick (and the
	// settle delivery) far away so the authorize's drain below is asserted
	// deterministically, never raced against the tick.
	b := p34bDispatcherBridge(t, fake, store, db, outbox, now, 5000)

	// leg 1: A > E within budget → consumed == A, marker, settled
	l1, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	const E = int64(100_000_000)
	const A = int64(150_000_000)
	_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, l1.LeaseID, "CNY", "auth-25", E, 900_000, now)
	require.NoError(t, err)
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-25", PlatformUserID: user, Currency: "CNY", AmountUnits: A, OccurredAt: now, AuthorizationID: "auth-25"}))
	require.Equal(t, "150000000", p34bHashField(t, ctx, store, user, l1.LeaseID, "consumed_units"), "consumed += excess → A")
	hold, err := store.GetCanonicalWalletHold(ctx, user, "auth-25")
	require.NoError(t, err)
	require.Equal(t, "settled", hold.State)
	eventID := CanonicalWalletSettlementEventID("req-25", user, "CNY")
	marker, err := rdb.Get(ctx, testCanonicalWalletReservationKey(user, eventID)).Result()
	require.NoError(t, err)
	require.Equal(t, l1.LeaseID, marker)
	p34WaitOutboxStatus(t, ctx, db, "req-25", "delivered")
	fake.mu.Lock()
	require.Equal(t, A, fake.lease(user, l1.LeaseID).Captured)
	fake.mu.Unlock()

	// leg 2: A > E beyond the remaining budget → {4}: released += E, state=
	// released, the event proceeds unbound
	user2 := "shipany-user-" + uuid.NewString()
	fake.fund(user2, 100_000_000_000)
	l2, err := b.ensureLease(ctx, user2, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	const E2 = int64(300_000_000)
	const A2 = int64(600_000_000)
	_, _, _, err = store.ArmCanonicalWalletHold(ctx, user2, l2.LeaseID, "CNY", "auth-25b", E2, 900_000, now)
	require.NoError(t, err)
	overrunBase := canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Load()
	require.True(t, b.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "req-25b", PlatformUserID: user2, Currency: "CNY", AmountUnits: A2, OccurredAt: now, AuthorizationID: "auth-25b"}))
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.holdConvertOverrunReleased.Load()-overrunBase)
	require.Equal(t, "300000000", p34bHashField(t, ctx, store, user2, l2.LeaseID, "released_units"), "the overrun released the whole hold")
	hold2, err := store.GetCanonicalWalletHold(ctx, user2, "auth-25b")
	require.NoError(t, err)
	require.Equal(t, "released", hold2.State)
	require.Equal(t, "", hold2.EventID)
	var boundLease sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT lease_id FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-25b'`).Scan(&boundLease))
	require.Equal(t, "", boundLease.String, "the {4} event proceeds with LeaseID unset")

	// the next AUTHORIZE's ensure carries drained[0].gateway_released == E and
	// the fake closes lease 1 on the verified identity (consumed E == captured
	// 0 + released E) — no mark. The ask (300M) exceeds l2's Go-side remaining
	// (500M − 300M consumed = 200M) so the cached lease cannot cover it.
	_, err = b.ensureLease(ctx, user2, "CNY", 300_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	fake.mu.Lock()
	last := fake.requests[len(fake.requests)-1]
	require.Len(t, last.Drained, 1)
	require.Equal(t, l2.LeaseID, last.Drained[0].LeaseID)
	require.NotNil(t, last.Drained[0].GatewayReleased)
	require.Equal(t, E2, mustUnits(*last.Drained[0].GatewayReleased), "gateway_released == E on the wire")
	require.Equal(t, E2, mustUnits(last.Drained[0].GatewayConsumed))
	require.Equal(t, "closed", fake.lease(user2, l2.LeaseID).Status, "the drain verified: consumed E == captured 0 + released E")
	require.Nil(t, fake.lease(user2, l2.LeaseID).DrainedAt, "a verified drain never marks")
	fake.mu.Unlock()

	// and the unbound event still delivers, on a settle-purpose lease
	p34WaitOutboxStatus(t, ctx, db, "req-25b", "delivered")
	fake.mu.Lock()
	var settleLease string
	for _, l := range fake.leases[user2] {
		if l.Purpose == "settle" && l.Captured == A2 {
			settleLease = l.ID
		}
	}
	fake.mu.Unlock()
	require.NotEmpty(t, settleLease, "the overrun delivered on a settle-purpose lease")
}

// Test 28 — the seal reports released; the next ensure carries
// gateway_released and the fake verifies consumed == captured + released →
// the lease closes (test 21's alternation, now with releases; §10.2).
func TestPhase34bSealReportsReleasedAndTheDrainVerifies(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 100_000_000_000)
	b := p34bBridge(t, ctx, config.CanonicalWalletModeEnforce, fake, store, now)

	l1, err := b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	const B = int64(500_000_000) // the arm exhausts the lease: E == budget
	const A = int64(200_000_000)
	_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, l1.LeaseID, "CNY", "auth-28", B, 900_000, now)
	require.NoError(t, err)
	conv, err := store.ConvertCanonicalWalletHold(ctx, user, "auth-28", "ev-28", A, now)
	require.NoError(t, err)
	require.Equal(t, 0, conv.Code)
	fake.setCaptured(user, l1.LeaseID, A) // the server-side settlement of A

	// the next authorize seals L1 and drains it with both figures
	_, err = b.ensureLease(ctx, user, "CNY", 100_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	fake.mu.Lock()
	last := fake.requests[len(fake.requests)-1]
	require.Len(t, last.Drained, 1)
	require.Equal(t, l1.LeaseID, last.Drained[0].LeaseID)
	require.Equal(t, B, mustUnits(last.Drained[0].GatewayConsumed), "gateway_consumed is the pre-seal consumed")
	require.NotNil(t, last.Drained[0].GatewayReleased)
	require.Equal(t, B-A, mustUnits(*last.Drained[0].GatewayReleased), "gateway_released is the seal's third value")
	require.Equal(t, "closed", fake.lease(user, l1.LeaseID).Status, "the identity verified: B == captured A + released (B−A)")
	require.Nil(t, fake.lease(user, l1.LeaseID).DrainedAt)
	fake.mu.Unlock()
}

// Test 29 — shadow arms and never refuses; enforce refuses lease_unavailable
// when the arm guard refuses twice (§10.1, §10.4).
func TestPhase34bShadowArmsAndNeverRefuses(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	resetAuthorizationMetricsForTest()

	// shadow arms: the hash exists, the handle is armed
	shadowUser := "shipany-user-" + uuid.NewString()
	fake.fund(shadowUser, 10_000_000_000)
	shadow := p34bBridge(t, ctx, config.CanonicalWalletModeShadow, fake, store, now)
	hs := p34bAuthorize(t, ctx, config.CanonicalWalletModeShadow, shadow, shadowUser, `{"max_tokens":64}`)
	require.Nil(t, hs.Refusal, "shadow never refuses")
	require.True(t, hs.HoldArmed)
	_, err := store.GetCanonicalWalletHold(ctx, shadowUser, hs.ID)
	require.NoError(t, err)

	// force {4} twice: pre-seeded zero-remaining lease hashes under the exact
	// ids the fake will issue next (srv-lease-2, srv-lease-3 — the fake's seq
	// is deterministic and the shadow arm above took srv-lease-1), so both
	// the first arm and the re-ensured arm land on a lease the guard refuses.
	racyUser := "shipany-user-" + uuid.NewString()
	fake.fund(racyUser, 100_000_000_000)
	for _, id := range []string{"srv-lease-2", "srv-lease-3"} {
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: id, PlatformUserID: racyUser, Currency: "CNY",
			BudgetUnits: 500_000_000, ConsumedUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
	}
	shadowRacy := p34bBridge(t, ctx, config.CanonicalWalletModeShadow, fake, store, now)
	hr := p34bAuthorize(t, ctx, config.CanonicalWalletModeShadow, shadowRacy, racyUser, `{"max_tokens":64}`)
	require.Nil(t, hr.Refusal, "shadow admits the twice-refused arm")
	require.False(t, hr.HoldArmed)
	require.Equal(t, int64(1), authorizationMetrics.holdArmRefused.Load(), "counted once, admitted anyway")
	require.Equal(t, int64(1), authorizationMetrics.holdArmRetried.Load(), "the re-ensure happened exactly once")

	// enforce: the same setup refuses lease_unavailable
	enforceUser := "shipany-user-" + uuid.NewString()
	fake.fund(enforceUser, 100_000_000_000)
	for _, id := range []string{"srv-lease-4", "srv-lease-5"} {
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
			LeaseID: id, PlatformUserID: enforceUser, Currency: "CNY",
			BudgetUnits: 500_000_000, ConsumedUnits: 500_000_000, ExpiresAt: now.Add(5 * time.Minute),
		}))
	}
	enforce := p34bBridge(t, ctx, config.CanonicalWalletModeEnforce, fake, store, now)
	he := p34bAuthorize(t, ctx, config.CanonicalWalletModeEnforce, enforce, enforceUser, `{"max_tokens":64}`)
	require.NotNil(t, he.Refusal)
	require.Equal(t, AuthorizationRefusalLeaseUnavailable, he.Refusal.Reason)
	require.False(t, he.HoldArmed)
}
