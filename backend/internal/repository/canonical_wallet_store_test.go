package repository

import (
	"context"
	"errors"
	"strconv"
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

func TestCanonicalWalletStoreValidationRejectsInvalidLeases(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	valid := service.CanonicalWalletLease{LeaseID: "lease-v", PlatformUserID: "shipany-user-validate", Currency: "CNY", BudgetUnits: 100, ExpiresAt: now.Add(time.Minute)}

	err := store.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{})
	require.Error(t, err, "empty platform user id and lease id are rejected")

	bad := valid
	bad.LeaseID = ""
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad))

	bad = valid
	bad.PlatformUserID = ""
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad))

	bad = valid
	bad.BudgetUnits = 0
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad), "a zero budget can never back a reservation")

	bad = valid
	bad.ExpiresAt = time.Time{}
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad))

	bad = valid
	bad.ConsumedUnits = -1
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad))

	bad = valid
	bad.ConsumedUnits = valid.BudgetUnits + 1
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad))

	bad = valid
	bad.BudgetUnits = 1_000_000_000_001_000_000 // past Lua's 2^53-1 safe integer ceiling
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad), "amounts beyond Lua's exact integer range fail loudly before the script")

	bad = valid
	bad.Currency = "USD"
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad), "cny-e8-v1 is CNY-only — reject, never coerce")

	// A nil gatewayCache (or one without a Redis client) reports
	// unavailability instead of panicking.
	var nilStore *gatewayCache
	require.Error(t, nilStore.InstallCanonicalWalletLease(ctx, valid))
	require.Error(t, (&gatewayCache{}).InstallCanonicalWalletLease(ctx, valid))
	_, err = nilStore.GetCanonicalWalletLease(ctx, "shipany-user-validate")
	require.Error(t, err)
	_, err = nilStore.ReserveCanonicalWalletLease(ctx, "shipany-user-validate", "lease-v", "CNY", "event-x", 1, now)
	require.Error(t, err)
}

func TestCanonicalWalletReserveValidationAndRealConflictPaths(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	lease := service.CanonicalWalletLease{LeaseID: "lease-paths", PlatformUserID: "shipany-user-paths", Currency: "CNY", BudgetUnits: 1000, ExpiresAt: now.Add(time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	// Argument validation.
	_, err := store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "", "CNY", "e", 1, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "CNY", " ", 1, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "CNY", "e", 0, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "CNY", "e-oversized", 1_000_000_000_001_000_000, now)
	require.Error(t, err, "amounts beyond Lua's safe integer range are rejected before the script")
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "e-usd", 1, now)
	require.Error(t, err, "a non-CNY request is rejected outright, never coerced")

	// Unknown lease id -> code 1.
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "lease-nope", "CNY", "e-missing", 1, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing)

	// A genuinely stored NON-CNY lease (raw state, as if installed before a
	// stricter install policy existed) triggers the script's own currency
	// guard (code 2).
	usdKey := canonicalWalletLeaseKey(lease.PlatformUserID, "lease-usd")
	require.NoError(t, client.HSet(ctx, usdKey, map[string]any{
		"lease_id": "lease-usd", "platform_user_id": lease.PlatformUserID, "currency": "USD",
		"budget_units": "1000", "consumed_units": "0", "expires_at_ms": strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10),
	}).Err())
	require.NoError(t, client.Set(ctx, canonicalWalletCurrentKey(lease.PlatformUserID), "lease-usd", 0).Err())
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "lease-usd", "CNY", "e-cur", 1, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseCurrencyMismatch)

	// Corrupt stored integers surface as parse errors, never silent zeros.
	require.NoError(t, client.HSet(ctx, usdKey, "budget_units", "not-a-number").Err())
	_, err = store.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, "lease-usd")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse canonical wallet budget")

	// Missing lease reads report missing, both by-pointer and by-current.
	_, err = store.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, "lease-gone")
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing)
}

func TestRedisResultInt64AcceptsEveryRealShape(t *testing.T) {
	n, err := redisResultInt64(int64(7))
	require.NoError(t, err)
	require.Equal(t, int64(7), n)

	n, err = redisResultInt64("42")
	require.NoError(t, err)
	require.Equal(t, int64(42), n)

	n, err = redisResultInt64([]byte("99"))
	require.NoError(t, err)
	require.Equal(t, int64(99), n)

	_, err = redisResultInt64(struct{}{})
	require.Error(t, err, "an unexpected shape must be reported, not silently zeroed")

	_, err = redisResultInt64("NaN")
	require.Error(t, err)
}

func TestCanonicalWalletStoreNilClientGuardsOnEveryMethod(t *testing.T) {
	ctx := context.Background()
	empty := &gatewayCache{}
	now := time.Now().UTC()
	require.Error(t, empty.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "l", PlatformUserID: "u", Currency: "CNY", BudgetUnits: 1, ExpiresAt: now.Add(time.Minute),
	}))
	_, err := empty.GetCanonicalWalletLease(ctx, "u")
	require.Error(t, err)
	_, err = empty.GetCanonicalWalletLeaseByID(ctx, "u", "l")
	require.Error(t, err)
	_, err = empty.ReserveCanonicalWalletLease(ctx, "u", "l", "CNY", "e", 1, now)
	require.Error(t, err)
}

func TestParseCanonicalWalletLeaseRejectsEveryCorruptField(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "lease-corrupt", PlatformUserID: "shipany-user-corrupt", Currency: "CNY",
		BudgetUnits: 1000, ExpiresAt: now.Add(time.Minute),
	}))
	key := canonicalWalletLeaseKey("shipany-user-corrupt", "lease-corrupt")

	for _, field := range []string{"budget_units", "consumed_units", "expires_at_ms"} {
		require.NoError(t, client.HSet(ctx, key, field, "garbage").Err())
		_, err := store.GetCanonicalWalletLeaseByID(ctx, "shipany-user-corrupt", "lease-corrupt")
		require.Error(t, err, field)
		require.Contains(t, err.Error(), "parse canonical wallet")
		require.NoError(t, client.HSet(ctx, key,
			"budget_units", "1000", "consumed_units", "0",
			"expires_at_ms", strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10),
		).Err(), "restore valid state for the next field")
	}
}

// A real Redis that goes away mid-flight is the one reservation failure that
// is NOT a script code — the transport error has to surface to the caller
// rather than being swallowed into a nil reservation. miniredis is a real
// server, so closing it is a real outage, not a faked driver response.
func TestCanonicalWalletReserveSurfacesRedisTransportFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	lease := service.CanonicalWalletLease{
		LeaseID: "lease-transport", PlatformUserID: "shipany-user-transport", Currency: "CNY",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))

	mr.Close()

	_, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "CNY", "event-transport", 10, now)
	require.Error(t, err, "a Redis outage during reservation must surface, never be reported as a successful no-op")
	require.NotErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing, "a transport failure is not a missing lease — conflating them would let a retry rebind the event to a different lease")
}

// The dispatcher's stale-binding release (Phase R Fix 2) is only safe if an
// event's reservation marker cannot outlive the lease it belongs to: when the
// bound lease is gone, resolveOutboxEventLease falls back to a fresh lease, and
// a surviving marker would make that reservation conflict.
//
// The marker used to be given a RELATIVE ttl computed from the GATEWAY's clock
// (`PX expires_at - ARGV[4]`) while the lease hash was given an ABSOLUTE one
// (`PEXPIREAT expires_at`). Redis applies a relative ttl at ITS execution
// instant, so the marker outlived its lease by the round-trip latency plus any
// clock offset.
//
// Passing a gateway timestamp deliberately behind Redis's clock reproduces
// exactly that skew — no mock, no fake driver, just the real script told the
// truth about a clock that is behind.
func TestCanonicalWalletReservationMarkerCannotOutliveItsLease(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)

	now := time.Now().UTC().Truncate(time.Millisecond)
	// Freeze the store's clock. miniredis does not advance TTLs on its own,
	// so without this the two PTTLs below are read against an unpinned clock
	// and cannot be compared for equality — only for inequality, which is
	// exactly the weakness this test used to have.
	mr.SetTime(now)

	lease := service.CanonicalWalletLease{
		LeaseID: "lease-deadline", PlatformUserID: "shipany-user-deadline", Currency: "CNY",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	// A gateway clock five seconds behind the store's — the same shape as the
	// round-trip latency and clock offset that used to hand the marker extra
	// life under the old relative ttl.
	skewed := now.Add(-5 * time.Second)
	_, err := store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "CNY", "event-deadline", 10, skewed)
	require.NoError(t, err)

	leaseKey := canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID)
	markerKey := canonicalWalletReservationKey(lease.PlatformUserID, "event-deadline")

	leasePTTL, err := client.PTTL(ctx, leaseKey).Result()
	require.NoError(t, err)
	markerPTTL, err := client.PTTL(ctx, markerKey).Result()
	require.NoError(t, err)

	// Positive, not merely "no greater than the lease": PTTL returns -1 for a
	// key with no expiry and -2 for a missing one, and BOTH are less than any
	// positive lease PTTL. Without this assertion, deleting the marker's
	// PEXPIREAT entirely — leaving every marker persistent forever, the worst
	// form of this bug — would pass.
	require.Positive(t, leasePTTL, "the lease must still be alive for the rest of this test to mean anything")
	require.Positive(t, markerPTTL, "the marker must carry an expiry at all: a persistent marker outlives every lease")
	require.Equal(t, leasePTTL, markerPTTL,
		"the marker and its lease must share one absolute deadline: once the lease is gone the dispatcher rebinds the event to a fresh lease, and a surviving marker makes that reservation conflict until the row dead-letters")

	// And the property the dispatcher actually depends on, stated directly:
	// past the lease's expiry, neither key is left behind.
	mr.FastForward(61 * time.Second)
	leaseExists, err := client.Exists(ctx, leaseKey).Result()
	require.NoError(t, err)
	markerExists, err := client.Exists(ctx, markerKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), leaseExists, "the lease must be gone once its deadline passes")
	require.Equal(t, int64(0), markerExists, "a dead lease must imply a dead marker — this is the invariant resolveOutboxEventLease relies on when it releases a stale binding")
}
