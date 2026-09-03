//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCanonicalWalletOldLeaseReservationSurvivesNewLeaseIssuance(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.CanonicalWalletLeaseStore)
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)

	oldLease := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 500_00000000, ExpiresAt: now.Add(2 * time.Minute), // 500 CNY, 2 min TTL
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, oldLease))

	// An in-flight request reserves against the old lease before it's replaced.
	inFlight, err := store.ReserveCanonicalWalletLease(ctx, platformUserID, oldLease.LeaseID, "CNY", "event-inflight-1", 12_000000, now)
	require.NoError(t, err)
	require.False(t, inFlight.Duplicate)

	// The renewal watermark fires and a fresh, larger lease is issued for the
	// same user before the in-flight request settles.
	newLease := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 1000_00000000, ExpiresAt: now.Add(10 * time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, newLease))

	// A brand-new request now resolves the CURRENT lease and gets the new one.
	current, err := store.GetCanonicalWalletLease(ctx, platformUserID)
	require.NoError(t, err)
	require.Equal(t, newLease.LeaseID, current.LeaseID)

	// The in-flight request's eventual retry (e.g. a network timeout on the
	// first attempt) must still resolve against the OLD lease it was
	// actually reserved against, not the new current one, and must be
	// recognized as a duplicate rather than reserving a second time.
	retry, err := store.ReserveCanonicalWalletLease(ctx, platformUserID, oldLease.LeaseID, "CNY", "event-inflight-1", 12_000000, now)
	require.NoError(t, err)
	require.True(t, retry.Duplicate)
	require.Equal(t, oldLease.LeaseID, retry.Lease.LeaseID)
	require.Equal(t, int64(12_000000), retry.Lease.ConsumedUnits)

	// The old lease's remaining budget is untouched by the new lease's issuance.
	oldSnapshot, err := store.GetCanonicalWalletLeaseByID(ctx, platformUserID, oldLease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(12_000000), oldSnapshot.ConsumedUnits)
	require.Equal(t, int64(500_00000000), oldSnapshot.BudgetUnits)
}

func TestCanonicalWalletNewReservationAgainstExpiredOldLeaseFails(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.CanonicalWalletLeaseStore)
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)

	oldLease := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_00000000, ExpiresAt: now.Add(1 * time.Second),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, oldLease))

	// A genuinely NEW request (never reserved before) must never be allowed
	// to target an already-expired lease, even if it somehow knows the ID.
	_, err := store.ReserveCanonicalWalletLease(ctx, platformUserID, oldLease.LeaseID, "CNY", "event-new-after-expiry", 1_000000, now.Add(2*time.Second))
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExpired)
}

// TestCanonicalWalletRedisSafeIntegerGuardHoldsAgainstRealRedis complements
// the miniredis-based unit test in canonical_wallet_store_test.go (Task 3
// Step 6) with the SAME check against a genuine Redis server — the guard
// itself lives entirely in Go (ensureRedisLuaSafeInt64, called BEFORE either
// script ever runs), so this is mostly confirming the guard still fires the
// same way through the real client — the value in this test never actually
// reaches Lua either way, by design.
func TestCanonicalWalletRedisSafeIntegerGuardHoldsAgainstRealRedis(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.CanonicalWalletLeaseStore)
	tooLarge := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: "shipany-user-" + uuid.NewString(), Currency: "CNY",
		BudgetUnits: 1_000_000_000_001_000_000, ExpiresAt: time.Now().UTC().Add(time.Minute), // Phase 0 fixture's exceeds_number_max_safe_integer case
	}
	require.Error(t, store.InstallCanonicalWalletLease(ctx, tooLarge))
}

// TestCanonicalWalletReservationAgainstRetainedExpiredLeaseIsRefused (Wallet
// Lease Phase 5-G, Task 3 Step 4): the safety property the retention change
// rests on. A lease whose expires_at_ms is in the past but whose key is still
// present (retained through the caller slot TTL so the next ensure can drain
// it) must still REFUSE a new reservation — the reserve script's own guard
// (expires_at <= now → {3}), not the key TTL, decides liveness. Without the
// retention this fails with ErrCanonicalWalletLeaseMissing instead (the key
// aged out); with it, the refusal is the named expired error.
func TestCanonicalWalletReservationAgainstRetainedExpiredLeaseIsRefused(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.CanonicalWalletLeaseStore)
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)

	lease := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: now.Add(1 * time.Second), RetainUntil: now.Add(1800 * time.Second),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	time.Sleep(1200 * time.Millisecond) // the real clock passes expires_at; the key is retained

	_, err := store.ReserveCanonicalWalletLease(ctx, platformUserID, lease.LeaseID, "CNY", "event-retained-expired-1", 1_000000, time.Now().UTC())
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExpired, "a retained-but-expired lease must never be reservable — liveness is expires_at_ms, never the key TTL")
}

// TestCanonicalWalletConvertInsideRetentionKeepsTheMarkerAliveWithTheLease
// (Wallet Lease Phase 5-G, Task 3 Step 4b(i)): inside the retention window
// the convert script's exact-match branch (actual ≤ held) must write the
// redelivery-idempotency marker with the LEASE KEY'S remaining life, not
// PEXPIREAT expires_at — with expires_at in the past that PEXPIREAT deleted
// the marker at once and a conversion landing inside the window lost its
// dedup marker. Both keys must age out together, by construction.
func TestCanonicalWalletConvertInsideRetentionKeepsTheMarkerAliveWithTheLease(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.CanonicalWalletLeaseStore)
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)

	lease := service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: now.Add(1 * time.Second), RetainUntil: now.Add(1800 * time.Second),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	authorizationID := "auth-" + uuid.NewString()
	_, held, duplicate, err := store.ArmCanonicalWalletHold(ctx, platformUserID, lease.LeaseID, "CNY", authorizationID, 10_000000, 1_800_000, now)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.Equal(t, int64(10_000000), held)

	time.Sleep(1200 * time.Millisecond) // the real clock passes expires_at; the hash is retained

	conv, err := store.ConvertCanonicalWalletHold(ctx, platformUserID, authorizationID, "event-convert-in-retention", 10_000000, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 0, conv.Code, "the exact-match branch converts (the lease hash is present inside the retention window)")
	require.Equal(t, lease.LeaseID, conv.LeaseID)

	leasePTTL, err := integrationRedis.PTTL(ctx, canonicalWalletLeaseKey(platformUserID, lease.LeaseID)).Result()
	require.NoError(t, err)
	require.Greater(t, leasePTTL, time.Duration(0), "the lease key must still be alive inside the retention window")
	markerPTTL, err := integrationRedis.PTTL(ctx, canonicalWalletReservationKey(platformUserID, "event-convert-in-retention")).Result()
	require.NoError(t, err)
	require.Greater(t, markerPTTL, time.Duration(0), "the redelivery-idempotency marker must exist after a conversion inside the retention window")
	require.InDelta(t, leasePTTL.Seconds(), markerPTTL.Seconds(), 1.0, "marker and lease key age out together — the marker carries the lease key's remaining life")
}
