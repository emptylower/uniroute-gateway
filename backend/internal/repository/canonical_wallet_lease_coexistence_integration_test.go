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
