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
		LeaseID: "lease-1", PlatformUserID: "shipany-user-1", Currency: "USD",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))

	first, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "USD", "event-1", 600, now)
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	require.Equal(t, int64(600), first.Lease.ConsumedUnits)

	duplicate, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "USD", "event-1", 600, now)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, int64(600), duplicate.Lease.ConsumedUnits)

	// A concurrent lease-acquire response for the same lease must not reset
	// consumption to the stale control-plane snapshot.
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))
	preserved, err := store.GetCanonicalWalletLease(context.Background(), lease.PlatformUserID)
	require.NoError(t, err)
	require.Equal(t, int64(600), preserved.ConsumedUnits)

	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "USD", "event-2", 401, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExhausted)
	_, err = store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "CNY", "event-3", 1, now)
	require.Error(t, err, "a non-USD currency must be rejected outright by RequireUSDBillingCurrency, not coerced")
}

func TestCanonicalWalletReservationCannotCrossLeaseBoundary(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	firstLease := service.CanonicalWalletLease{LeaseID: "lease-a", PlatformUserID: "shipany-user-3", Currency: "USD", BudgetUnits: 100, ExpiresAt: now.Add(time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), firstLease))
	_, err := store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, firstLease.LeaseID, "USD", "event-stable", 10, now)
	require.NoError(t, err)

	secondLease := firstLease
	secondLease.LeaseID = "lease-b"
	secondLease.ExpiresAt = now.Add(2 * time.Minute)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), secondLease))

	// Retrying the SAME event against the SAME lease it was originally
	// reserved against is now a legitimate duplicate — this is Task 3's
	// whole point, and the property the OLD version of this test got
	// backwards by asserting a conflict here instead.
	retrySameLease, err := store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, firstLease.LeaseID, "USD", "event-stable", 10, now)
	require.NoError(t, err)
	require.True(t, retrySameLease.Duplicate)
	require.Equal(t, firstLease.LeaseID, retrySameLease.Lease.LeaseID)

	// But reusing the SAME event_id against a DIFFERENT lease id is a real
	// conflict, not a silent duplicate carrying the wrong lease's snapshot
	// — THIS is the actual "cannot cross lease boundary" property.
	_, err = store.ReserveCanonicalWalletLease(context.Background(), firstLease.PlatformUserID, secondLease.LeaseID, "USD", "event-stable", 10, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletReservationConflict)
}

func TestCanonicalWalletLeaseStoreExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{
		LeaseID: "lease-expiring", PlatformUserID: "shipany-user-2", Currency: "USD", BudgetUnits: 100, ExpiresAt: now.Add(time.Second),
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

	newer := service.CanonicalWalletLease{LeaseID: "lease-newer", PlatformUserID: platformUserID, Currency: "USD", BudgetUnits: 1000, ExpiresAt: now.Add(10 * time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), newer))

	// A delayed/reordered response for an OLDER lease (shorter expiry)
	// arrives AFTER the newer one already became current — the pointer
	// must not regress backward in time.
	older := service.CanonicalWalletLease{LeaseID: "lease-older", PlatformUserID: platformUserID, Currency: "USD", BudgetUnits: 500, ExpiresAt: now.Add(2 * time.Minute)}
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
		LeaseID: "lease-too-large", PlatformUserID: "shipany-user-oversized", Currency: "USD",
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

	valid := service.CanonicalWalletLease{LeaseID: "lease-v", PlatformUserID: "shipany-user-validate", Currency: "USD", BudgetUnits: 100, ExpiresAt: now.Add(time.Minute)}

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
	bad.Currency = "CNY"
	require.Error(t, store.InstallCanonicalWalletLease(ctx, bad), "usd-e8-v1 is USD-only — reject, never coerce")

	// A nil store (or one without a Redis client) reports unavailability
	// instead of panicking. Phase 5-G Task 2 moved the lease-store methods
	// onto canonicalWalletRedisStore — both cache types reach them through
	// the embedded pointer, so the nil-receiver guarantee is pinned on the
	// store type itself.
	var nilStore *canonicalWalletRedisStore
	require.Error(t, nilStore.InstallCanonicalWalletLease(ctx, valid))
	require.Error(t, (&canonicalWalletRedisStore{}).InstallCanonicalWalletLease(ctx, valid))
	_, err = nilStore.GetCanonicalWalletLease(ctx, "shipany-user-validate")
	require.Error(t, err)
	_, err = nilStore.ReserveCanonicalWalletLease(ctx, "shipany-user-validate", "lease-v", "USD", "event-x", 1, now)
	require.Error(t, err)
}

func TestCanonicalWalletReserveValidationAndRealConflictPaths(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewGatewayCache(client).(service.CanonicalWalletLeaseStore)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	lease := service.CanonicalWalletLease{LeaseID: "lease-paths", PlatformUserID: "shipany-user-paths", Currency: "USD", BudgetUnits: 1000, ExpiresAt: now.Add(time.Minute)}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	// Argument validation.
	_, err := store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "", "USD", "e", 1, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", " ", 1, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "e", 0, now)
	require.Error(t, err)
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "e-oversized", 1_000_000_000_001_000_000, now)
	require.Error(t, err, "amounts beyond Lua's safe integer range are rejected before the script")
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "CNY", "e-usd", 1, now)
	require.Error(t, err, "a non-USD request is rejected outright, never coerced")

	// Unknown lease id -> code 1.
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "lease-nope", "USD", "e-missing", 1, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing)

	// A genuinely stored NON-USD lease (raw state, as if installed before a
	// stricter install policy existed) triggers the script's own currency
	// guard (code 2).
	usdKey := canonicalWalletLeaseKey(lease.PlatformUserID, "lease-usd")
	require.NoError(t, client.HSet(ctx, usdKey, map[string]any{
		"lease_id": "lease-usd", "platform_user_id": lease.PlatformUserID, "currency": "CNY",
		"budget_units": "1000", "consumed_units": "0", "expires_at_ms": strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10),
	}).Err())
	require.NoError(t, client.Set(ctx, canonicalWalletCurrentKey(lease.PlatformUserID), "lease-usd", 0).Err())
	_, err = store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, "lease-usd", "USD", "e-cur", 1, now)
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
	empty := &canonicalWalletRedisStore{} // Phase 5-G Task 2: the lease-store methods' receiver
	now := time.Now().UTC()
	require.Error(t, empty.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "l", PlatformUserID: "u", Currency: "USD", BudgetUnits: 1, ExpiresAt: now.Add(time.Minute),
	}))
	_, err := empty.GetCanonicalWalletLease(ctx, "u")
	require.Error(t, err)
	_, err = empty.GetCanonicalWalletLeaseByID(ctx, "u", "l")
	require.Error(t, err)
	_, err = empty.ReserveCanonicalWalletLease(ctx, "u", "l", "USD", "e", 1, now)
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
		LeaseID: "lease-corrupt", PlatformUserID: "shipany-user-corrupt", Currency: "USD",
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
		LeaseID: "lease-transport", PlatformUserID: "shipany-user-transport", Currency: "USD",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), lease))

	mr.Close()

	_, err := store.ReserveCanonicalWalletLease(context.Background(), lease.PlatformUserID, lease.LeaseID, "USD", "event-transport", 10, now)
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
		LeaseID: "lease-deadline", PlatformUserID: "shipany-user-deadline", Currency: "USD",
		BudgetUnits: 1_000, ExpiresAt: now.Add(time.Minute),
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))

	// A gateway clock five seconds behind the store's — the same shape as the
	// round-trip latency and clock offset that used to hand the marker extra
	// life under the old relative ttl.
	skewed := now.Add(-5 * time.Second)
	_, err := store.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "event-deadline", 10, skewed)
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

// TestCanonicalWalletReserveGuardSubtractsReleased (Phase 3.4b, §10.2): the
// reserve guard is consumed − released + amount > budget — freed budget is
// reusable on the gateway — and the seal reports the released figure as its
// third value.
func TestCanonicalWalletReserveGuardSubtractsReleased(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := &gatewayCache{rdb: rdb, canonicalWalletRedisStore: &canonicalWalletRedisStore{rdb: rdb}} // Phase 5-G Task 2: the lease-store methods moved onto the embedded store
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	exp := now.Add(time.Minute)
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L", PlatformUserID: "u", Currency: "USD", BudgetUnits: 100, ExpiresAt: exp}))
	_, err := c.ReserveCanonicalWalletLease(ctx, "u", "L", "USD", "e1", 100, now)
	require.NoError(t, err)
	_, err = c.ReserveCanonicalWalletLease(ctx, "u", "L", "USD", "e2", 1, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExhausted) // the existing {4} sentinel (canonical_wallet_bridge.go:31)
	require.NoError(t, rdb.HIncrBy(ctx, canonicalWalletLeaseKey("u", "L"), "released_units", 40).Err())
	res, err := c.ReserveCanonicalWalletLease(ctx, "u", "L", "USD", "e3", 40, now)
	require.NoError(t, err)
	require.Equal(t, int64(140), res.Lease.ConsumedUnits, "consumed is monotone; the guard subtracted released")
	consumed, released, err := c.SealCanonicalWalletLease(ctx, "u", "L")
	require.NoError(t, err)
	require.Equal(t, int64(140), consumed)
	require.Equal(t, int64(40), released)
	sealedConsumed, err := rdb.HGet(ctx, canonicalWalletLeaseKey("u", "L"), "consumed_units").Result()
	require.NoError(t, err)
	require.Equal(t, "100", sealedConsumed, "the seal caps consumed at budget — it does not preserve the pre-seal figure when released > 0")
}

// mustHoldState reads one hold hash field via HGETALL for assertions.
func mustHoldState(t *testing.T, ctx context.Context, c *gatewayCache, user, authID string) map[string]string {
	t.Helper()
	vals, err := c.rdb.HGetAll(ctx, canonicalWalletHoldKey(user, authID)).Result()
	require.NoError(t, err)
	return vals
}

// TestCanonicalWalletHoldArmReleaseConvert (Phase 3.4b, §10.3/§10.5) — every
// branch of the three hold scripts:
//
//	arm: consumed += E, hash written, set membership; re-arm {5}; {4} when the
//	guard refuses; {1} missing lease; {2} currency; {3} expired
//	release(not_written): released += E iff state=armed; second release {7}
//	convert A<E: released += E−A, marker, settled, event_id; re-convert {7,settled,event}
//	convert A>E within budget: consumed += excess; beyond budget: released += E, {4}, state=released
//	convert on a released hold: {7, released, ""}
//	hold missing: {1}
//	convert A<=E with the lease hash gone: {3} — nothing to release or mark
//	release on an expired lease hash: no-op on the lease, hash still transitions
//	mark-class: HSET class iff state = armed
func TestCanonicalWalletHoldArmReleaseConvert(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	c := &gatewayCache{rdb: rdb, canonicalWalletRedisStore: &canonicalWalletRedisStore{rdb: rdb}} // Phase 5-G Task 2: the lease-store methods moved onto the embedded store
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	mr.SetTime(now)
	exp := now.Add(10 * time.Minute)
	const user = "hold-user-1"
	const graceMS = int64(30_000)
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L2", PlatformUserID: user, Currency: "USD", BudgetUnits: 1000, ExpiresAt: exp}))

	// arm a1 E=300: consumed 0−0+300 ≤ 1000 → hash, set membership, hold_users.
	leaseID, held, dup, err := c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "a1", 300, graceMS, now)
	require.NoError(t, err)
	require.Equal(t, "L2", leaseID)
	require.Equal(t, int64(300), held)
	require.False(t, dup)
	fields := mustHoldState(t, ctx, c, user, "a1")
	require.Equal(t, map[string]string{
		"lease_id": "L2", "held_units": "300", "armed_at_ms": strconv.FormatInt(now.UnixMilli(), 10),
		"class": "", "state": "armed", "event_id": "",
	}, fields)
	consumed, err := rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "consumed_units").Result()
	require.NoError(t, err)
	require.Equal(t, "300", consumed, "the arm raised consumed by E")
	members, err := rdb.SMembers(ctx, canonicalWalletHoldSetKey(user)).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"a1"}, members)
	users, next, err := c.ListCanonicalWalletHoldUsers(ctx, 0, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(0), next)
	require.Equal(t, []string{user}, users, "hold_users stores the platform user id itself, not its hash")
	// the hold's PEXPIREAT = the lease's expires_at_ms + grace (§10.3)
	pttl, err := rdb.PTTL(ctx, canonicalWalletHoldKey(user, "a1")).Result()
	require.NoError(t, err)
	require.InDelta(t, (10*time.Minute + time.Duration(graceMS)*time.Millisecond).Milliseconds(), pttl.Milliseconds(), 50, "the hold outlives its lease by one grace")

	// re-arm the same authorization id: {5} idempotent, consumed unchanged.
	leaseID, held, dup, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "a1", 300, graceMS, now)
	require.NoError(t, err)
	require.True(t, dup)
	require.Equal(t, "L2", leaseID)
	require.Equal(t, int64(300), held)
	consumed, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "consumed_units").Result()
	require.NoError(t, err)
	require.Equal(t, "300", consumed)

	// arm beyond budget: {4} the guard refuses (300 + 800 > 1000).
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "a2", 800, graceMS, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExhausted)

	// arm on a missing lease: {1}.
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L-missing", "USD", "a3", 1, graceMS, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing)
	// wrong currency: rejected outright by RequireUSDBillingCurrency in Go —
	// the reserve method's own convention (its test at :47 asserts Error, not
	// ErrorIs, for the same reason: the script's {2} is unreachable through
	// the validated entry point).
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "CNY", "a3", 1, graceMS, now)
	require.Error(t, err, "a non-USD currency must be rejected outright, not coerced")
	// an expired lease hash (the FIELD, so miniredis keeps the key alive): {3}.
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L-exp", PlatformUserID: user, Currency: "USD", BudgetUnits: 100, ExpiresAt: exp}))
	require.NoError(t, rdb.HSet(ctx, canonicalWalletLeaseKey(user, "L-exp"), "expires_at_ms", now.Add(-time.Second).UnixMilli()).Err())
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L-exp", "USD", "a3", 1, graceMS, now)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExpired)
	_, err = rdb.Del(ctx, canonicalWalletLeaseKey(user, "L-exp")).Result()
	require.NoError(t, err)

	// release(not_written) on a1: released += E, state/class written, set SREM'd.
	released, err := c.ReleaseCanonicalWalletHold(ctx, user, "a1", "released", "not_written")
	require.NoError(t, err)
	require.Equal(t, int64(300), released)
	rel, err := rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "300", rel)
	fields = mustHoldState(t, ctx, c, user, "a1")
	require.Equal(t, "released", fields["state"])
	require.Equal(t, "not_written", fields["class"])
	require.Equal(t, "300", fields["held_units"], "the hash keeps its fields for observability (§10.5)")
	members, err = rdb.SMembers(ctx, canonicalWalletHoldSetKey(user)).Result()
	require.NoError(t, err)
	require.Empty(t, members)

	// second release: {7} no-op, released_units unchanged.
	released, err = c.ReleaseCanonicalWalletHold(ctx, user, "a1", "released", "not_written")
	var notArmed *service.CanonicalWalletHoldNotArmedError
	require.ErrorAs(t, err, &notArmed)
	require.Equal(t, "released", notArmed.State)
	require.Equal(t, "", notArmed.EventID)
	require.Equal(t, int64(0), released)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "300", rel)

	// release on a missing hold: {1}.
	_, err = c.ReleaseCanonicalWalletHold(ctx, user, "a-missing", "released", "")
	require.ErrorIs(t, err, service.ErrCanonicalWalletHoldMissing)

	// convert A<E: released += E−A, marker == lease id, settled, event_id.
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "b3", 200, graceMS, now)
	require.NoError(t, err) // consumed 300−300+200 ≤ 1000 (the guard subtracts released)
	conv, err := c.ConvertCanonicalWalletHold(ctx, user, "b3", "ev-b3", 50, now)
	require.NoError(t, err)
	require.Equal(t, 0, conv.Code)
	require.Equal(t, "L2", conv.LeaseID)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "450", rel, "released += E−A: 300 + 150")
	marker, err := rdb.Get(ctx, canonicalWalletReservationKey(user, "ev-b3")).Result()
	require.NoError(t, err)
	require.Equal(t, "L2", marker)
	markerPTTL, err := rdb.PTTL(ctx, canonicalWalletReservationKey(user, "ev-b3")).Result()
	require.NoError(t, err)
	require.InDelta(t, 10*time.Minute.Milliseconds(), markerPTTL.Milliseconds(), 50, "the marker carries the lease's expires_at")
	fields = mustHoldState(t, ctx, c, user, "b3")
	require.Equal(t, "settled", fields["state"])
	require.Equal(t, "ev-b3", fields["event_id"])
	// consumed is unchanged by an A<E conversion; only releases raise released.
	consumed, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "consumed_units").Result()
	require.NoError(t, err)
	require.Equal(t, "500", consumed, "the arm of b3 raised consumed to 500; the conversion adds nothing")

	// re-convert the same event: {7, settled, ev-b3} — with the hold's
	// lease_id as the fourth element (§13.2.7).
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "b3", "ev-b3", 50, now)
	require.NoError(t, err)
	require.Equal(t, 7, conv.Code)
	require.Equal(t, "settled", conv.State)
	require.Equal(t, "ev-b3", conv.EventID)
	require.Equal(t, "L2", conv.LeaseID, "the {7} answer carries the hold's lease")
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "450", rel, "a re-convert releases nothing")

	// convert A>E within budget: consumed += excess.
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "b4", 100, graceMS, now)
	require.NoError(t, err) // 500−450+100 ≤ 1000
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "b4", "ev-b4", 150, now)
	require.NoError(t, err)
	require.Equal(t, 0, conv.Code)
	require.Equal(t, "L2", conv.LeaseID)
	consumed, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L2"), "consumed_units").Result()
	require.NoError(t, err)
	require.Equal(t, "650", consumed, "consumed += excess: 600 → 650")
	fields = mustHoldState(t, ctx, c, user, "b4")
	require.Equal(t, "settled", fields["state"])
	require.Equal(t, "ev-b4", fields["event_id"])

	// convert A>E beyond budget on a tight lease: {4}, released += E, state=released.
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L4", PlatformUserID: user, Currency: "USD", BudgetUnits: 100, ExpiresAt: exp}))
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L4", "USD", "b5", 100, graceMS, now)
	require.NoError(t, err)
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "b5", "ev-b5", 150, now)
	require.NoError(t, err)
	require.Equal(t, 4, conv.Code)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L4"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "100", rel)
	fields = mustHoldState(t, ctx, c, user, "b5")
	require.Equal(t, "released", fields["state"])
	require.Equal(t, "", fields["event_id"], "a {4} hold is never bound to an event")
	members, err = rdb.SMembers(ctx, canonicalWalletHoldSetKey(user)).Result()
	require.NoError(t, err)
	require.NotContains(t, members, "b5")

	// convert on a released hold: {7, released, ""} — with the hold's
	// lease_id as the fourth element (§13.2.7).
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "b5", "ev-b5", 150, now)
	require.NoError(t, err)
	require.Equal(t, 7, conv.Code)
	require.Equal(t, "released", conv.State)
	require.Equal(t, "", conv.EventID)
	require.Equal(t, "L4", conv.LeaseID, "the {7} answer carries the hold's lease even on a released hold")

	// hold missing: {1}.
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "b-missing", "ev-x", 1, now)
	require.NoError(t, err)
	require.Equal(t, 1, conv.Code)

	// convert A<=E with the lease hash gone: {3} — the event proceeds unbound.
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L2", "USD", "c1", 100, graceMS, now)
	require.NoError(t, err)
	require.NoError(t, rdb.Del(ctx, canonicalWalletLeaseKey(user, "L2")).Err())
	conv, err = c.ConvertCanonicalWalletHold(ctx, user, "c1", "ev-c1", 50, now)
	require.NoError(t, err)
	require.Equal(t, 3, conv.Code, "the fifth branch: the lease hash was already gone — nothing to release or mark")
	fields = mustHoldState(t, ctx, c, user, "c1")
	require.Equal(t, "settled", fields["state"])
	require.Equal(t, "ev-c1", fields["event_id"])

	// release on an expired lease hash: a no-op on the lease, the hash still
	// transitions (the reaper's abandoned path, §10.7).
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L6", PlatformUserID: user, Currency: "USD", BudgetUnits: 100, ExpiresAt: exp}))
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L6", "USD", "d1", 100, graceMS, now)
	require.NoError(t, err)
	require.NoError(t, rdb.Del(ctx, canonicalWalletLeaseKey(user, "L6")).Err())
	released, err = c.ReleaseCanonicalWalletHold(ctx, user, "d1", "abandoned", "")
	require.NoError(t, err)
	require.Equal(t, int64(100), released, "the hold's units are reported released even though the lease hash is gone")
	fields = mustHoldState(t, ctx, c, user, "d1")
	require.Equal(t, "abandoned", fields["state"])

	// mark-class: HSET class iff state = armed. (L6's hash was deleted above;
	// a fresh install re-arms cleanly.)
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L6", PlatformUserID: user, Currency: "USD", BudgetUnits: 100, ExpiresAt: exp}))
	_, _, _, err = c.ArmCanonicalWalletHold(ctx, user, "L6", "USD", "e1", 10, graceMS, now)
	require.NoError(t, err)
	hold, err := c.MarkCanonicalWalletHoldClass(ctx, user, "e1", "indeterminate")
	require.NoError(t, err)
	require.Equal(t, "indeterminate", hold.Class)
	require.Equal(t, "armed", hold.State)
	fields = mustHoldState(t, ctx, c, user, "e1")
	require.Equal(t, "indeterminate", fields["class"])
	// on a settled hold: {7}, the class is untouched.
	_, err = c.MarkCanonicalWalletHoldClass(ctx, user, "b4", "indeterminate")
	require.ErrorAs(t, err, &notArmed)
	fields = mustHoldState(t, ctx, c, user, "b4")
	require.Equal(t, "", fields["class"])

	// GetCanonicalWalletHold round-trips the armed_at timestamp.
	got, err := c.GetCanonicalWalletHold(ctx, user, "e1")
	require.NoError(t, err)
	require.Equal(t, service.CanonicalWalletHold{
		AuthorizationID: "e1", LeaseID: "L6", HeldUnits: 10, ArmedAt: now, Class: "indeterminate", State: "armed", EventID: "",
	}, *got)
	_, err = c.GetCanonicalWalletHold(ctx, user, "nope")
	require.ErrorIs(t, err, service.ErrCanonicalWalletHoldMissing)

	// List/Forget/Prune: the reaper's enumeration surface. Every hold above
	// except the armed e1 reached a terminal state and was SREM'd by its
	// script; the set carries armed holds only.
	ids, err := c.ListCanonicalWalletHolds(ctx, user, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"e1"}, ids)
	require.NoError(t, c.ForgetCanonicalWalletHold(ctx, user, "a1"))
	ids, err = c.ListCanonicalWalletHolds(ctx, user, 100)
	require.NoError(t, err)
	require.NotContains(t, ids, "a1")
	require.NoError(t, c.PruneCanonicalWalletHoldUser(ctx, user))
	users, _, err = c.ListCanonicalWalletHoldUsers(ctx, 0, 100)
	require.NoError(t, err)
	require.Empty(t, users)
}

// TestCanonicalWalletReaperLease (§10.7): the deployment-wide tick leader.
func TestCanonicalWalletReaperLease(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	c := &gatewayCache{rdb: rdb, canonicalWalletRedisStore: &canonicalWalletRedisStore{rdb: rdb}} // Phase 5-G Task 2: the lease-store methods moved onto the embedded store
	ctx := context.Background()
	interval := time.Second
	first, err := c.TryCanonicalWalletReaperLease(ctx, 2*interval)
	require.NoError(t, err)
	require.True(t, first, "the first taker leads the tick")
	second, err := c.TryCanonicalWalletReaperLease(ctx, 2*interval)
	require.NoError(t, err)
	require.False(t, second, "a second bridge in the same tick skips it")
	// the two-consecutive-empty-ticks marker, in Redis (§10.3/§10.7).
	seen, err := c.MarkCanonicalWalletHoldUserEmpty(ctx, "u-empty", 2*interval)
	require.NoError(t, err)
	require.False(t, seen, "the first observation writes the marker")
	seen, err = c.MarkCanonicalWalletHoldUserEmpty(ctx, "u-empty", 2*interval)
	require.NoError(t, err)
	require.True(t, seen, "the second observation within the window sees it")
	require.NoError(t, c.ClearCanonicalWalletHoldUserEmpty(ctx, "u-empty"))
	seen, err = c.MarkCanonicalWalletHoldUserEmpty(ctx, "u-empty", 2*interval)
	require.NoError(t, err)
	require.False(t, seen, "a cleared marker is written fresh again")
	mr.FastForward(2 * interval)
	again, err := c.TryCanonicalWalletReaperLease(ctx, 2*interval)
	require.NoError(t, err)
	require.True(t, again, "after the leader key expires a new tick can be led")
}

// TestCanonicalWalletReleaseReservationFullAndPartialForms (Phase 3.5,
// redesign §11.2/§11.3/§11.4): the full form (dropMarker = true —
// named_lease_id and the late capture) drops the event's reservation marker
// after one HINCRBY released_units; the partial form (dropMarker = false —
// the split) keeps the marker so the parent's redelivery answers {5}, and is
// gated on a per-event RELEASE marker that carries the lease's own
// PEXPIREAT — the idempotency key a redelivery or a crash-repair re-run
// needs, so released_units can never be raised twice for one split.
func TestCanonicalWalletReleaseReservationFullAndPartialForms(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	c := NewGatewayCache(rdb).(service.CanonicalWalletLeaseStore)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	user := "shipany-user-release"

	// FULL FORM (§11.2 named_lease_id / §11.4 late capture).
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L-full", PlatformUserID: user, Currency: "USD", BudgetUnits: 1_000, ExpiresAt: now.Add(10 * time.Minute)}))
	_, err := c.ReserveCanonicalWalletLease(ctx, user, "L-full", "USD", "ev-full", 500, now)
	require.NoError(t, err)

	released, err := c.ReleaseCanonicalWalletReservation(ctx, user, "L-full", "ev-full", 500, true)
	require.NoError(t, err)
	require.True(t, released, "the full form reports its write")
	rel, err := rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L-full"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "500", rel, "released_units == A")
	marker, err := rdb.Get(ctx, canonicalWalletReservationKey(user, "ev-full")).Result()
	require.ErrorIs(t, err, redis.Nil, "the reservation marker is gone — the DEL is the full form's idempotency")
	_ = marker

	// A second full-form call finds no marker: {1}, false, no write.
	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-full", "ev-full", 500, true)
	require.NoError(t, err)
	require.False(t, released)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L-full"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "500", rel, "nothing was written a second time")

	// PARTIAL FORM (§11.3 the split): a fresh reservation, release A−H.
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L-part", PlatformUserID: user, Currency: "USD", BudgetUnits: 1_000, ExpiresAt: now.Add(10 * time.Minute)}))
	_, err = c.ReserveCanonicalWalletLease(ctx, user, "L-part", "USD", "ev-part", 500, now)
	require.NoError(t, err)

	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-part", "ev-part", 200, false)
	require.NoError(t, err)
	require.True(t, released)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L-part"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "200", rel, "released_units == A − H")
	marker, err = rdb.Get(ctx, canonicalWalletReservationKey(user, "ev-part")).Result()
	require.NoError(t, err)
	require.Equal(t, "L-part", marker, "the reservation marker survives — the parent's redelivery must answer {5}")
	// The release marker exists and ages with the lease: its PTTL equals the
	// lease hash's own within a second (both PEXPIREATs are the lease's
	// expires_at_ms).
	releaseKey := canonicalWalletReleaseMarkerKey(user, "ev-part")
	storedLease, err := rdb.Get(ctx, releaseKey).Result()
	require.NoError(t, err)
	require.Equal(t, "L-part", storedLease)
	markerTTL, err := rdb.PTTL(ctx, releaseKey).Result()
	require.NoError(t, err)
	leaseTTL, err := rdb.PTTL(ctx, canonicalWalletLeaseKey(user, "L-part")).Result()
	require.NoError(t, err)
	require.LessOrEqual(t, markerTTL-leaseTTL, time.Second, "the release marker carries the lease's own PEXPIREAT")
	require.GreaterOrEqual(t, markerTTL-leaseTTL, -time.Second, "…within a second of the lease hash's")

	// A second identical partial call: {7} on the release marker — false, and
	// released_units is NOT raised a second time.
	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-part", "ev-part", 200, false)
	require.NoError(t, err)
	require.False(t, released, "the release marker is the partial form's idempotency gate")
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L-part"), "released_units").Result()
	require.NoError(t, err)
	require.Equal(t, "200", rel, "never 2 × (A − H) — §11.3's idempotency bullet")

	// A marker pointing at a DIFFERENT lease: {6} — false, no write.
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L-other", PlatformUserID: user, Currency: "USD", BudgetUnits: 1_000, ExpiresAt: now.Add(10 * time.Minute)}))
	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-other", "ev-part", 100, false)
	require.NoError(t, err)
	require.False(t, released)
	rel, err = rdb.HGet(ctx, canonicalWalletLeaseKey(user, "L-other"), "released_units").Result()
	require.ErrorIs(t, err, redis.Nil, "L-other's hash was never touched")

	// A MISSING lease hash with an existing reservation marker (§11.4: a
	// closed lease whose hash aged out): released_units is not written (there
	// is no hash), the marker follows its form, and the call reports true.
	require.NoError(t, c.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: "L-gone", PlatformUserID: user, Currency: "USD", BudgetUnits: 1_000, ExpiresAt: now.Add(10 * time.Minute)}))
	_, err = c.ReserveCanonicalWalletLease(ctx, user, "L-gone", "USD", "ev-gone-full", 300, now)
	require.NoError(t, err)
	_, err = c.ReserveCanonicalWalletLease(ctx, user, "L-gone", "USD", "ev-gone-part", 300, now)
	require.NoError(t, err)
	require.NoError(t, rdb.Del(ctx, canonicalWalletLeaseKey(user, "L-gone")).Err())

	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-gone", "ev-gone-full", 300, true)
	require.NoError(t, err)
	require.True(t, released, "the write path ran — the marker went with it")
	err = rdb.Get(ctx, canonicalWalletReservationKey(user, "ev-gone-full")).Err()
	require.ErrorIs(t, err, redis.Nil, "full form: the marker is deleted even without the lease hash")

	released, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-gone", "ev-gone-part", 100, false)
	require.NoError(t, err)
	require.True(t, released, "the write path ran — the release marker is still written")
	storedLease, err = rdb.Get(ctx, canonicalWalletReleaseMarkerKey(user, "ev-gone-part")).Result()
	require.NoError(t, err)
	require.Equal(t, "L-gone", storedLease, "partial form: the release marker is written even without the lease hash")
	markerGone, err := rdb.Get(ctx, canonicalWalletReservationKey(user, "ev-gone-part")).Result()
	require.NoError(t, err)
	require.Equal(t, "L-gone", markerGone, "partial form: the reservation marker survives even without the lease hash")

	// Validation: zero/negative units and the Lua-safe bound.
	_, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-part", "ev-zero", 0, false)
	require.Error(t, err, "units > 0 is required")
	_, err = c.ReleaseCanonicalWalletReservation(ctx, user, "L-part", "ev-huge", redisLuaMaxSafeInt64+1, false)
	require.Error(t, err, "the Lua-safe bound applies")
}
