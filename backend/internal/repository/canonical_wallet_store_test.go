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
		BudgetMicros: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))

	first, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, "CNY", "event-1", 600, now)
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	require.Equal(t, int64(600), first.Lease.ConsumedMicros)

	duplicate, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, "CNY", "event-1", 600, now)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, int64(600), duplicate.Lease.ConsumedMicros)

	// A concurrent lease-acquire response for the same lease must not reset
	// consumption to the stale control-plane snapshot.
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))
	preserved, err := store.GetCanonicalWalletLease(context.Background(), lease.PlatformUserID)
	require.NoError(t, err)
	require.Equal(t, int64(600), preserved.ConsumedMicros)

	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, "CNY", "event-2", 401, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExhausted)
	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, "USD", "event-3", 1, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseCurrencyMismatch)
}

func TestCanonicalWalletReservationCannotCrossLeaseBoundary(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	firstLease := service.CanonicalWalletLease{LeaseID: "lease-a", PlatformUserID: "shipany-user-3", Currency: "CNY", BudgetMicros: 100, ExpiresAt: now.Add(time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), firstLease))
	_, err := store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, "CNY", "event-stable", 10, now)
	require.NoError(t, err)

	secondLease := firstLease
	secondLease.LeaseID = "lease-b"
	secondLease.ExpiresAt = now.Add(2 * time.Minute)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), secondLease))
	_, err = store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, "CNY", "event-stable", 10, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletReservationConflict)
}

func TestCanonicalWalletLeaseStoreExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{
		LeaseID: "lease-expiring", PlatformUserID: "shipany-user-2", Currency: "CNY", BudgetMicros: 100, ExpiresAt: now.Add(time.Second),
	}))
	mr.FastForward(2 * time.Second)
	_, err := store.GetCanonicalWalletLease(context.Background(), "shipany-user-2")
	require.True(t, errors.Is(err, service.ErrCanonicalWalletLeaseMissing))
}
