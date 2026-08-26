package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCanonicalWalletLeaseStoreIsAtomicAndIdempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	lease := service.CanonicalWalletLease{
		LeaseID: "lease-1", PlatformUserID: "shipany-user-1", Currency: "CNY",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))

	first, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "CNY", "event-1", 600, now)
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	require.Equal(t, int64(600), first.Lease.ConsumedUnits)

	duplicate, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "CNY", "event-1", 600, now)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, int64(600), duplicate.Lease.ConsumedUnits)

	// A concurrent lease-acquire response for the same lease must not reset
	// consumption to the stale control-plane snapshot.
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))
	preserved, err := store.GetCanonicalWalletLease(context.Background(), lease.PlatformUserID)
	require.NoError(t, err)
	require.Equal(t, int64(600), preserved.ConsumedUnits)

	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "CNY", "event-2", 401, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExhausted)
	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "USD", "event-3", 1, now)
	require.Error(t, err, "a non-CNY currency must be rejected outright by RequireCNYBillingCurrency, not coerced")
}

func TestCanonicalWalletReservationCannotCrossLeaseBoundary(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	firstLease := service.CanonicalWalletLease{LeaseID: "lease-a", PlatformUserID: "shipany-user-3", Currency: "CNY", BudgetUnits: 100, ExpiresAt: now.Add(time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), firstLease))
	_, err := store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, firstLease.LeaseID, "CNY", "event-stable", 10, now)
	require.NoError(t, err)

	secondLease := firstLease
	secondLease.LeaseID = "lease-b"
	secondLease.ExpiresAt = now.Add(2 * time.Minute)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), secondLease))

	// Retrying the SAME event against the SAME lease it was originally
	// reserved against is now a legitimate duplicate — this is Task 3's
	// whole point, and the property the OLD version of this test got
	// backwards by asserting a conflict here instead.
	retrySameLease, err := store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, firstLease.LeaseID, "CNY", "event-stable", 10, now)
	require.NoError(t, err)
	require.True(t, retrySameLease.Duplicate)
	require.Equal(t, firstLease.LeaseID, retrySameLease.Lease.LeaseID)

	// But reusing the SAME event_id against a DIFFERENT lease id is a real
	// conflict, not a silent duplicate carrying the wrong lease's snapshot
	// — THIS is the actual "cannot cross lease boundary" property.
	_, err = store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, secondLease.LeaseID, "CNY", "event-stable", 10, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletReservationConflict)
}

func TestCanonicalWalletLeaseStoreExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{
		LeaseID: "lease-expiring", PlatformUserID: "shipany-user-2", Currency: "CNY", BudgetUnits: 100, ExpiresAt: now.Add(time.Second),
	}))
	mr.FastForward(2 * time.Second)
	_, err := store.GetCanonicalWalletLease(context.Background(), "shipany-user-2")
	require.True(t, errors.Is(err, service.ErrCanonicalWalletLeaseMissing))
}

func TestCanonicalWalletCurrentPointerIgnoresDelayedOlderLease(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	platformUserID := "shipany-user-pointer-order"

	newer := service.CanonicalWalletLease{LeaseID: "lease-newer", PlatformUserID: platformUserID, Currency: "CNY", BudgetUnits: 1000, ExpiresAt: now.Add(10 * time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), newer))

	// A delayed/reordered response for an OLDER lease (shorter expiry)
	// arrives AFTER the newer one already became current — the pointer
	// must not regress backward in time.
	older := service.CanonicalWalletLease{LeaseID: "lease-older", PlatformUserID: platformUserID, Currency: "CNY", BudgetUnits: 500, ExpiresAt: now.Add(2 * time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), older))

	current, err := store.GetCanonicalWalletLease(context.Background(), platformUserID)
	require.NoError(t, err)
	require.Equal(t, newer.LeaseID, current.LeaseID, "the pointer must still point at the newer (later-expiring) lease")

	// The older lease's own hash still exists independently (an in-flight
	// reservation anchored to it must still resolve) — only the "current"
	// pointer was protected from regressing.
	olderSnapshot, err := store.GetCanonicalWalletLeaseByID(context.Background(), platformUserID, older.LeaseID)
	require.NoError(t, err)
	require.Equal(t, older.LeaseID, olderSnapshot.LeaseID)
}

func TestCanonicalWalletRedisOperationsRejectValuesBeyondLuaSafeIntegerRange(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)

	// The Phase 0 fixture's own "exceeds_number_max_safe_integer" case
	// (1,000,000,000,001,000,000) is comfortably past this bound.
	tooLarge := service.CanonicalWalletLease{
		LeaseID: "lease-too-large", PlatformUserID: "shipany-user-oversized", Currency: "CNY",
		BudgetUnits: 1_000_000_000_001_000_000, ExpiresAt: now.Add(time.Minute),
	}
	err := store.InstallCanonicalWalletLease(context.Background(), tooLarge)
	require.Error(t, err, "a budget beyond Lua's exact integer range must be rejected before it reaches the script, not silently truncated inside it")
}
