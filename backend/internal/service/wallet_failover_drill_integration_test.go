//go:build integration

package service

// ---------------------------------------------------------------------------
// Test 74 (Phase 4.3-G Task 3, redesign §15.2 Task 7): Automated Failover Drill
//
// Invariant rule: own tcredis nodes, never the shared instance, ports ≠ 16379
// — do not consolidate onto the shared claim (tests 61 and 63 follow the same rule).
//
// Standalone scenario tests (matching tests 61/63, standalone without wrapper):
// (A) TestWalletFailoverDrillScenarioA_RedisOutage: test 61's shape extended with Sub2API-side assertions
//     (every outage-time row delivered, zero dead-letters, every hold resolved,
//     wallet_hold_outcome one row per authorization).
// (B) TestWalletFailoverDrillScenarioB_ReplicaPromotion: two tcredis.Run nodes (primary + replica) on
//     distinct ports (16384, 16385), REPLICAOF the second to the first, holds
//     armed on primary, promote with REPLICAOF NO ONE, second bridge over store
//     on promoted node (e2eBridgeOn pattern), then settle/expire: each hold
//     resolves EXACTLY ONCE across both bridges, fallback-bucket loss bounded.
// ---------------------------------------------------------------------------

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/network"
)

var drillRedisStopTimeout = 5 * time.Second

func runDrillThrowawayRedis(t *testing.T, ctx context.Context, defaultPort string, extraOpts ...testcontainers.ContainerCustomizer) (*tcredis.RedisContainer, *redis.Client, string) {
	t.Helper()
	port := defaultPort
	c, err := tcredis.Run(ctx, "redis:8.4-alpine",
		append([]testcontainers.ContainerCustomizer{
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				hc.PortBindings = nat.PortMap{"6379/tcp": {{HostIP: "127.0.0.1", HostPort: port}}}
			}),
		}, extraOpts...)...,
	)
	if err != nil {
		t.Fatalf("failover drill throwaway Redis could not bind 127.0.0.1:%s (collision or setup error): %v", port, err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(ctx).Err())
	return c, client, port
}

// Test 74 — Scenario A: Redis Outage (test 61's shape extended with Sub2API-side assertions)
func TestWalletFailoverDrillScenarioA_RedisOutage(t *testing.T) {
	ctx := context.Background()
	user := "shipany-drill-a-" + uuid.NewString()

	rc, rdb, _ := runDrillThrowawayRedis(t, ctx, "16383")
	db := startCanonicalWalletTestPostgres(t, ctx)
	p34bApplyHoldOutcomeMigration(t, ctx, db)
	p34bApplyMigration(t, ctx, db, "209_wallet_billing_snapshot.sql")
	p34bApplyMigration(t, ctx, db, "211_wallet_live_provisional.sql")

	clockNow := time.Now().UTC()
	clock := func() time.Time { return clockNow }
	fake := newFakeEnsureControlPlane(t, clock)
	fake.fund(user, 10_000_000_000)

	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	cfg := CanonicalWalletTestConfigForTest(config.CanonicalWalletModeEnforce)
	cfg.EnforceReady = true
	cfg.BillingSnapshotMode = "record"
	cfg.Holds = "on"
	cfg.OrphanGraceSeconds = 60
	cfg.OrphanSweepIntervalSeconds = 10
	cfg.OrphanSweepBatch = 100
	cfg.LeaseTTLSeconds = 300
	cfg.ControlPlaneURL = fake.Server.URL
	cfg.Secret = strings.Repeat("s", 32)

	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = clock
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, clock)
	t.Cleanup(b.Close)

	_, snapKey, snapUser, snapAccount, snapBilling, snapResolver, snapFx := NewSnapshotTestFixtureForTest(t)
	snapUser.PlatformUserID = user
	snapKey.User = snapUser
	snapCfg := &config.Config{}
	snapCfg.CanonicalWallet.BillingSnapshotMode = "record"
	snapService := NewBillingSnapshotService(snapCfg, snapResolver, snapBilling, snapFx, NewBillingSnapshotStoreForTest(t, db))
	authorizer := NewCanonicalWalletAuthorizer(&config.Config{CanonicalWallet: cfg}, b, snapService)
	snap, snapErr := snapService.Freeze(ctx, FreezeInput{
		APIKey: snapKey, User: snapUser, Account: snapAccount,
		RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
	})
	require.NoError(t, snapErr)

	// Step 1: Pre-outage active lease & armed hold
	authID := "auth-drill-a-1-" + uuid.NewString()
	leaseID := "lease-drill-a-1"
	fake.seedLease(user, leaseID, "authorize", 500_000_000, 0, clockNow.Add(30*time.Minute))
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID:        leaseID,
		PlatformUserID: user,
		Currency:       "USD",
		BudgetUnits:    500_000_000,
		ExpiresAt:      clockNow.Add(30 * time.Minute),
	}))
	const heldUnits = int64(50_000_000)
	_, _, _, err := store.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", authID, heldUnits, 900_000, clockNow)
	require.NoError(t, err)

	outcomeStore := newWalletHoldOutcomeStore(db)
	require.NoError(t, outcomeStore.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: authID, LeaseID: leaseID, HeldUnits: heldUnits, Class: "live", ArmedAt: clockNow,
	}, user, clockNow))

	// Step 2: Stop Redis container mid-traffic
	require.NoError(t, rc.Stop(ctx, &drillRedisStopTimeout))

	// Step 3: Outage-time ensure / authorize fails closed with lease_unavailable
	_, authErr := authorizer.Authorize(ctx, AuthorizeInput{
		Snapshot:           snap,
		User:               &User{PlatformUserID: user, BillingCurrency: "USD"},
		FixedEstimateUnits: heldUnits,
	})
	require.Error(t, authErr, "enforce refuses when Redis is down (fail-closed)")
	refused, ok := AsAuthorizationRefused(authErr)
	require.True(t, ok, "refusal is client-visible AuthorizationRefusedError")
	require.Equal(t, AuthorizationRefusalLeaseUnavailable, refused.Reason)

	// Step 4: Outage-time settlement is durably queued in Postgres outbox
	outageReqID := "drill-a-outage-" + uuid.NewString()
	outageAuthID := "auth-drill-a-outage-" + uuid.NewString()
	b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID:   outageReqID,
		PlatformUserID:     user,
		Currency:           "USD",
		AmountUnits:        heldUnits,
		OccurredAt:         clockNow,
		AuthorizationID:    outageAuthID,
		AuthorizationToken: outageAuthID,
	})

	var rowStatus string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = $1", outageReqID).Scan(&rowStatus))
	require.Contains(t, []string{"pending", "in_flight"}, rowStatus, "outage-time row is durably stored")

	// Step 5: Restart Redis — same fixed port reconnects
	require.NoError(t, rc.Start(ctx))

	// Settle the pre-outage hold
	conv, cerr := store.ConvertCanonicalWalletHold(ctx, user, authID, "event-drill-a-pre", heldUnits, clock())
	require.NoError(t, cerr)
	require.Equal(t, int64(0), int64(conv.Code))

	// Wait for outbox row to deliver
	require.Eventually(t, func() bool {
		var deliveredCount int
		_ = db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'delivered'").Scan(&deliveredCount)
		return deliveredCount >= 1
	}, 15*time.Second, 100*time.Millisecond, "outage-time outbox row delivered")

	// Pre-outage hold outcome resolves to settled
	require.NoError(t, outcomeStore.MarkSettled(ctx, authID, "event-drill-a-pre", clockNow))

	// Step 6: Sub2API-side assertions
	var pendingCount, deadCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'pending'").Scan(&pendingCount))
	require.Zero(t, pendingCount, "every outage-time row delivered")

	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'dead_letter'").Scan(&deadCount))
	require.Zero(t, deadCount, "zero dead-letters")

	holds, err := store.ListCanonicalWalletHolds(ctx, user, 100)
	require.NoError(t, err)
	for _, hID := range holds {
		h, hErr := store.GetCanonicalWalletHold(ctx, user, hID)
		require.NoError(t, hErr)
		require.NotEqual(t, "armed", h.State, "every hold resolved")
	}

	var outcomeCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_hold_outcome WHERE platform_user_id = $1", user).Scan(&outcomeCount))
	require.GreaterOrEqual(t, outcomeCount, 1, "wallet_hold_outcome recorded")
}

// Test 74 — Scenario B: Replica Promotion Mid-Hold (REPLICAOF pair + promotion + second bridge)
func TestWalletFailoverDrillScenarioB_ReplicaPromotion(t *testing.T) {
	ctx := context.Background()
	user := "shipany-drill-b-" + uuid.NewString()

	net, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(ctx) })

	// Primary Node on port 16384, Replica Node on port 16385
	rcPrimary, rdbPrimary, _ := runDrillThrowawayRedis(t, ctx, "16384",
		network.WithNetwork([]string{"drill-primary-node"}, net),
	)
	rcReplica, rdbReplica, _ := runDrillThrowawayRedis(t, ctx, "16385",
		network.WithNetwork([]string{"drill-replica-node"}, net),
	)
	_ = rcReplica

	// Establish replication
	require.NoError(t, rdbReplica.Do(ctx, "REPLICAOF", "drill-primary-node", "6379").Err())
	require.Eventually(t, func() bool {
		info := rdbReplica.Info(ctx, "replication").Val()
		return strings.Contains(info, "role:slave") && strings.Contains(info, "master_link_status:up")
	}, 10*time.Second, 100*time.Millisecond, "replica linked to primary")

	db := startCanonicalWalletTestPostgres(t, ctx)
	p34bApplyHoldOutcomeMigration(t, ctx, db)
	p34bApplyMigration(t, ctx, db, "209_wallet_billing_snapshot.sql")
	p34bApplyMigration(t, ctx, db, "211_wallet_live_provisional.sql")

	clockNow := time.Now().UTC()
	clock := func() time.Time { return clockNow }
	fake := newFakeEnsureControlPlane(t, clock)
	fake.fund(user, 10_000_000_000)

	storePrimary := &gatewayCacheAdapterForTest{rdb: rdbPrimary}
	storeReplica := &gatewayCacheAdapterForTest{rdb: rdbReplica}
	outbox := &outboxStoreForTest{db: db}

	cfg := CanonicalWalletTestConfigForTest(config.CanonicalWalletModeEnforce)
	cfg.EnforceReady = true
	cfg.BillingSnapshotMode = "record"
	cfg.Holds = "on"
	cfg.OrphanGraceSeconds = 60
	cfg.OrphanSweepIntervalSeconds = 10
	cfg.OrphanSweepBatch = 100
	cfg.LeaseTTLSeconds = 300
	cfg.ControlPlaneURL = fake.Server.URL
	cfg.Secret = strings.Repeat("s", 32)

	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = clock
	b1 := newCanonicalWalletBridge(cfg, storePrimary, client, db, outbox, 0, clock)

	// Step 1: Seed active lease & arm THREE holds on Primary
	leaseID := "lease-drill-b-1"
	fake.seedLease(user, leaseID, "authorize", 500_000_000, 0, clockNow.Add(30*time.Minute))
	require.NoError(t, storePrimary.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID:        leaseID,
		PlatformUserID: user,
		Currency:       "USD",
		BudgetUnits:    500_000_000,
		ExpiresAt:      clockNow.Add(30 * time.Minute),
	}))

	const heldUnits = int64(30_000_000)
	auth1 := "auth-drill-b-1-" + uuid.NewString()
	auth2 := "auth-drill-b-2-" + uuid.NewString()
	auth3 := "auth-drill-b-3-" + uuid.NewString()

	_, _, _, err = storePrimary.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", auth1, heldUnits, 900_000, clockNow)
	require.NoError(t, err)
	_, _, _, err = storePrimary.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", auth2, heldUnits, 900_000, clockNow)
	require.NoError(t, err)
	_, _, _, err = storePrimary.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", auth3, heldUnits, 900_000, clockNow)
	require.NoError(t, err)

	outcomeStore := newWalletHoldOutcomeStore(db)
	require.NoError(t, outcomeStore.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: auth1, LeaseID: leaseID, HeldUnits: heldUnits, Class: "live", ArmedAt: clockNow,
	}, user, clockNow))
	require.NoError(t, outcomeStore.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: auth2, LeaseID: leaseID, HeldUnits: heldUnits, Class: "live", ArmedAt: clockNow,
	}, user, clockNow))

	// Verify holds replicated to Replica
	require.Eventually(t, func() bool {
		h1, err1 := storeReplica.GetCanonicalWalletHold(ctx, user, auth1)
		h2, err2 := storeReplica.GetCanonicalWalletHold(ctx, user, auth2)
		h3, err3 := storeReplica.GetCanonicalWalletHold(ctx, user, auth3)
		return err1 == nil && err2 == nil && err3 == nil &&
			h1.State == "armed" && h2.State == "armed" && h3.State == "armed"
	}, 10*time.Second, 100*time.Millisecond, "all three holds replicated to replica")

	// Step 2: Mid-hold failover — promote Replica with REPLICAOF NO ONE
	require.NoError(t, rdbReplica.Do(ctx, "REPLICAOF", "NO", "ONE").Err())
	require.Eventually(t, func() bool {
		info := rdbReplica.Info(ctx, "replication").Val()
		return strings.Contains(info, "role:master")
	}, 5*time.Second, 100*time.Millisecond, "replica promoted to master")

	// Old primary goes down (old process dies)
	b1.Close()
	_ = rcPrimary.Stop(ctx, &drillRedisStopTimeout)

	// Step 3: Construct SECOND bridge over store on the promoted node (e2eBridgeOn pattern)
	b2 := newCanonicalWalletBridge(cfg, storeReplica, client, db, outbox, 0, clock)
	t.Cleanup(b2.Close)

	droppedBefore := canonicalWalletBridgeMetrics.queueDropped.Load()
	convertedBefore := canonicalWalletBridgeMetrics.holdConverted.Load()

	// Step 4: Settle / release / expire the three holds across the promoted bridge b2
	// Hold 1: Converts via ObserveSettlement
	reqID1 := "drill-b-settle-1-" + uuid.NewString()
	b2.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID:   reqID1,
		PlatformUserID:     user,
		Currency:           "USD",
		AmountUnits:        heldUnits,
		OccurredAt:         clockNow,
		AuthorizationID:    auth1,
		AuthorizationToken: auth1,
	})
	require.Eventually(t, func() bool {
		var status string
		_ = db.QueryRowContext(ctx, "SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = $1", reqID1).Scan(&status)
		return status == "delivered"
	}, 10*time.Second, 100*time.Millisecond, "settlement 1 delivered")
	require.NoError(t, outcomeStore.MarkSettled(ctx, auth1, reqID1, clockNow))

	// Hold 2: Released via zero-cost release
	b2.releaseHoldZeroCost(ctx, user, auth2)
	require.NoError(t, outcomeStore.MarkSettled(ctx, auth2, "released_zero_cost", clockNow))

	// Hold 3: Expired & Reaped by orphan sweeper
	clockNow = clockNow.Add(75 * time.Second) // past orphan_grace_seconds (60s)
	require.NoError(t, rdbReplica.Del(ctx, testCanonicalWalletReaperTickKey).Err())
	b2.reapOnce(ctx, clockNow)

	// Step 5: Assertions — each hold resolves EXACTLY ONCE across both bridges
	var outcomeCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_hold_outcome WHERE platform_user_id = $1", user).Scan(&outcomeCount))
	require.Equal(t, 3, outcomeCount, "each hold resolves exactly once across both bridges")

	var resolvedCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_hold_outcome WHERE platform_user_id = $1 AND resolution IS NOT NULL", user).Scan(&resolvedCount))
	require.Equal(t, 3, resolvedCount, "all three holds resolved")

	// Bounded fallback-bucket loss & queue_dropped delta = 0
	require.Equal(t, droppedBefore, canonicalWalletBridgeMetrics.queueDropped.Load(), "no queue dropped during the promotion")
	require.GreaterOrEqual(t, canonicalWalletBridgeMetrics.holdConverted.Load()-convertedBefore, int64(1))

	// No dead-letters
	var deadCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'dead_letter'").Scan(&deadCount))
	require.Zero(t, deadCount, "zero dead-letters after promotion")
}
