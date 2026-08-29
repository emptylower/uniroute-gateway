//go:build integration

package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type drillFaultyLeaseStore struct {
	CanonicalWalletLeaseStore
	disconnected atomic.Bool
}

func (s *drillFaultyLeaseStore) TryCanonicalWalletReaperLease(ctx context.Context, ttl time.Duration) (bool, error) {
	if s.disconnected.Load() {
		return false, errors.New("redis connection refused (simulated network partition)")
	}
	return s.CanonicalWalletLeaseStore.TryCanonicalWalletReaperLease(ctx, ttl)
}

func (s *drillFaultyLeaseStore) ListCanonicalWalletHoldUsers(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	if s.disconnected.Load() {
		return nil, 0, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.ListCanonicalWalletHoldUsers(ctx, cursor, count)
}

func (s *drillFaultyLeaseStore) ListCanonicalWalletHolds(ctx context.Context, user string, limit int) ([]string, error) {
	if s.disconnected.Load() {
		return nil, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.ListCanonicalWalletHolds(ctx, user, limit)
}

func (s *drillFaultyLeaseStore) GetCanonicalWalletHold(ctx context.Context, user, authID string) (*CanonicalWalletHold, error) {
	if s.disconnected.Load() {
		return nil, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.GetCanonicalWalletHold(ctx, user, authID)
}

func (s *drillFaultyLeaseStore) ForgetCanonicalWalletHold(ctx context.Context, user, authID string) error {
	if s.disconnected.Load() {
		return errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.ForgetCanonicalWalletHold(ctx, user, authID)
}

func (s *drillFaultyLeaseStore) ReleaseCanonicalWalletHold(ctx context.Context, user, authID, state, class string) (int64, error) {
	if s.disconnected.Load() {
		return 0, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.ReleaseCanonicalWalletHold(ctx, user, authID, state, class)
}

func (s *drillFaultyLeaseStore) MarkCanonicalWalletHoldUserEmpty(ctx context.Context, user string, ttl time.Duration) (bool, error) {
	if s.disconnected.Load() {
		return false, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.MarkCanonicalWalletHoldUserEmpty(ctx, user, ttl)
}

func (s *drillFaultyLeaseStore) ClearCanonicalWalletHoldUserEmpty(ctx context.Context, user string) error {
	if s.disconnected.Load() {
		return errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.ClearCanonicalWalletHoldUserEmpty(ctx, user)
}

func (s *drillFaultyLeaseStore) PruneCanonicalWalletHoldUser(ctx context.Context, user string) error {
	if s.disconnected.Load() {
		return errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.PruneCanonicalWalletHoldUser(ctx, user)
}

func (s *drillFaultyLeaseStore) MarkCanonicalWalletHoldClass(ctx context.Context, user, authID, class string) (*CanonicalWalletHold, error) {
	if s.disconnected.Load() {
		return nil, errors.New("redis connection refused")
	}
	return s.CanonicalWalletLeaseStore.MarkCanonicalWalletHoldClass(ctx, user, authID, class)
}

// Test 74 (Phase 4.3-G Task 3): Automated failover drill proving sweep recovery
// across simulated Gateway crash and Redis network partition under enforce mode.
func TestOpenAILiveFailoverDrillTest74(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	p34bApplyHoldOutcomeMigration(t, ctx, db)
	p34bApplyMigration(t, ctx, db, "211_wallet_live_provisional.sql")

	underlyingStore := &gatewayCacheAdapterForTest{rdb: rdb}
	store := &drillFaultyLeaseStore{CanonicalWalletLeaseStore: underlyingStore}
	outbox := &outboxStoreForTest{db: db}

	clockNow := time.Now().UTC()
	clock := func() time.Time { return clockNow }

	fake := newFakeEnsureControlPlane(t, clock)
	platformUserID := "shipany-drill-" + uuid.NewString()
	fake.fund(platformUserID, 10_000_000_000)

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

	// Step A: Seed active lease in both Control Plane and Redis
	leaseID := "lease-failover-drill-1"
	fake.seedLease(platformUserID, leaseID, "authorize", 500_000_000, 0, clockNow.Add(30*time.Minute))
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID:        leaseID,
		PlatformUserID: platformUserID,
		Currency:       "CNY",
		BudgetUnits:    500_000_000,
		ExpiresAt:      clockNow.Add(30 * time.Minute),
	}))

	// Step B: Start an SSE live stream via live provisional store with 2 windows
	authID := "auth-live-drill-" + uuid.NewString()
	callHash := "call-hash-" + uuid.NewString()
	liveStore := newLiveProvisionalStore(db)

	require.NoError(t, liveStore.Save(ctx, &LiveProvisionalRecord{
		Token:             authID,
		AuthorizationID:   authID,
		PlatformUserID:    platformUserID,
		UserID:            101,
		APIKeyID:          1001,
		AccountID:         2001,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "snap-drill-1",
		EstimatedUnits:    50_000_000,
		Status:            LiveProvisionalStatusProvisional,
		Windows: []LiveWindow{
			{WindowSeq: 1, LeaseID: leaseID, Token: authID, PendingUnits: 0, SettledUnits: 0, OpenedAtMS: clockNow.UnixMilli()},
		},
		CreatedAt: clockNow,
	}))
	require.NoError(t, liveStore.Activate(ctx, authID, callHash, clockNow))

	// Emit 2 provisional windows
	require.NoError(t, liveStore.SetLiveWindowPending(ctx, authID, 1, 20_000_000))
	require.NoError(t, liveStore.AdvanceLiveWindow(ctx, authID, 1, 10_000_000, LiveWindow{
		Token:      authID,
		WindowSeq:  2,
		LeaseID:    leaseID,
		OpenedAtMS: clockNow.Add(5 * time.Second).UnixMilli(),
	}))
	require.NoError(t, liveStore.SetLiveWindowPending(ctx, authID, 2, 25_000_000))

	// Arm hold in Redis for this active live session
	const heldUnits = int64(50_000_000)
	_, _, _, err := store.ArmCanonicalWalletHold(ctx, platformUserID, leaseID, "CNY", authID, heldUnits, 900_000, clockNow)
	require.NoError(t, err)

	holdBefore, err := store.GetCanonicalWalletHold(ctx, platformUserID, authID)
	require.NoError(t, err)
	require.Equal(t, "armed", holdBefore.State)
	require.Equal(t, heldUnits, holdBefore.HeldUnits)

	// Step C: Simulate Gateway Crash
	// Gateway abruptly crashes mid-stream. The session reaches terminal state (finalized with partial window 1 usage)
	// but the gateway crashed before it could release or settle the armed hold in Redis.
	claimed, err := liveStore.ClaimFinalization(ctx, authID, clockNow.Add(15*time.Second))
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, liveStore.CompleteFinalization(ctx, authID, "gwusg_drill_event_1", 10_000_000, 2, clockNow.Add(15*time.Second)))

	// Advance clock past the 60s orphan grace period
	clockNow = clockNow.Add(75 * time.Second)

	// Step D: Simulate Redis partition / disconnect mid-sweep
	store.disconnected.Store(true)

	// Reaper tick runs during Redis outage — must log and return safely without panic
	require.NotPanics(t, func() {
		b.reapOnce(ctx, clockNow)
	})

	// Hold is still armed (unmodified) because Redis was unreachable
	store.disconnected.Store(false)
	holdDuring, err := store.GetCanonicalWalletHold(ctx, platformUserID, authID)
	require.NoError(t, err)
	require.Equal(t, "armed", holdDuring.State, "hold remains armed while Redis was partitioned")

	// Step E: Redis is restored — clear tick leader lock to allow immediate sweep tick
	require.NoError(t, rdb.Del(ctx, testCanonicalWalletReaperTickKey).Err())

	// Reaper tick executes and converges cleanly
	b.reapOnce(ctx, clockNow)

	// Step F: Assertions — verify complete convergence and no leaks
	// 1. Hold in Redis transitioned to abandoned
	holdAfter, err := store.GetCanonicalWalletHold(ctx, platformUserID, authID)
	require.NoError(t, err)
	require.Equal(t, "abandoned", holdAfter.State)

	// 2. Postgres wallet_hold_outcome recorded the abandoned hold
	var outcomePlatformUser, outcomeLeaseID, outcomeResolution string
	var outcomeHeldUnits int64
	err = db.QueryRowContext(ctx, "SELECT platform_user_id, lease_id, held_units, resolution FROM wallet_hold_outcome WHERE authorization_id = $1", authID).Scan(
		&outcomePlatformUser, &outcomeLeaseID, &outcomeHeldUnits, &outcomeResolution,
	)
	require.NoError(t, err)
	require.Equal(t, platformUserID, outcomePlatformUser)
	require.Equal(t, leaseID, outcomeLeaseID)
	require.Equal(t, heldUnits, outcomeHeldUnits)
	require.Equal(t, "abandoned", outcomeResolution)

	// 3. User hold list in Redis is cleaned up on next empty sweep pass
	clockNow = clockNow.Add(15 * time.Second)
	require.NoError(t, rdb.Del(ctx, testCanonicalWalletReaperTickKey).Err())
	b.reapOnce(ctx, clockNow)
	clockNow = clockNow.Add(15 * time.Second)
	require.NoError(t, rdb.Del(ctx, testCanonicalWalletReaperTickKey).Err())
	b.reapOnce(ctx, clockNow)

	holdsRemaining, err := store.ListCanonicalWalletHolds(ctx, platformUserID, 100)
	require.NoError(t, err)
	require.Empty(t, holdsRemaining, "all hold keys for user pruned from Redis")

	// 4. Verify durable consistency in Postgres tables
	var holdOutcomeCount int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_hold_outcome WHERE platform_user_id = $1", platformUserID).Scan(&holdOutcomeCount)
	require.NoError(t, err)
	require.Equal(t, 1, holdOutcomeCount)
}
