//go:build integration

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// startCanonicalWalletTestRedis returns a client on the ONE shared Redis
// container per integration run, with an empty keyspace (Phase 3.7c:
// SharedTestRedisClientForTest flushes per call; the contract — a fresh
// *redis.Client on an empty keyspace, closed at t.Cleanup — is unchanged).
func startCanonicalWalletTestRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	return SharedTestRedisClientForTest(t)
}

func TestCanonicalWalletCheckAndReserveEnforcesRealHardCap(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb} // thin adapter over the real Lua-script store from Task 3
	platformUserID := "shipany-user-" + uuid.NewString()

	control := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 50_000000, ExpiresAt: time.Now().UTC().Add(time.Minute), // 0.5 CNY budget, deliberately small
	}}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil) // outboxDB/outbox nil — this test doesn't call ObserveSettlement
	t.Cleanup(bridge.Close)

	first := CanonicalWalletSettlementEvent{GatewayRequestID: "req-1", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 30_000000}
	allowed, err := bridge.CheckAndReserve(ctx, first)
	require.NoError(t, err)
	require.True(t, allowed)

	// A second request that would exceed the lease's remaining headroom
	// must be REJECTED — this is the actual overspend guard, not shadow
	// observation.
	second := CanonicalWalletSettlementEvent{GatewayRequestID: "req-2", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 30_000000}
	allowed, err = bridge.CheckAndReserve(ctx, second)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExhausted)
	require.False(t, allowed)
}

func TestCanonicalWalletCheckAndReserveHonorsExplicitLeaseIDOnRetry(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	oldLeaseID := "lease-old-" + uuid.NewString()
	control := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: oldLeaseID, PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil) // outboxDB/outbox nil — this test doesn't call ObserveSettlement
	t.Cleanup(bridge.Close)

	first := CanonicalWalletSettlementEvent{GatewayRequestID: "req-retry-1", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 10_000000}
	allowed, err := bridge.CheckAndReserve(ctx, first)
	require.NoError(t, err)
	require.True(t, allowed)

	// A new lease becomes current for the user in between (e.g. a renewal).
	control.lease = CanonicalWalletLease{
		LeaseID: "lease-new-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 5_000000, ExpiresAt: time.Now().UTC().Add(2 * time.Minute), // deliberately smaller than the retry amount
	}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, control.lease))

	// The retry supplies the SAME event id AND the ORIGINAL lease id — it
	// must resolve against that original lease (which still has headroom),
	// not silently fail against the new, smaller current lease.
	retry := CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-retry-1", PlatformUserID: platformUserID, Currency: "CNY",
		AmountUnits: 10_000000, EventID: first.EventID, LeaseID: oldLeaseID,
	}
	allowed, err = bridge.CheckAndReserve(ctx, retry)
	require.NoError(t, err, "a retry with an explicit LeaseID must resolve against that lease, not the new current one")
	require.True(t, allowed)
}

func TestBillingCacheServiceChecksBalanceEligibilityAgainstCanonicalWalletInEnforceMode(t *testing.T) {
	// This test goes through the REAL public entry point
	// (CheckBillingEligibility), not just the bridge directly.
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	// Every OTHER BillingCacheService dependency is irrelevant to what this
	// test exercises — CheckBillingEligibility returns immediately on
	// checkBalanceEligibility's error, before userPlatformQuotaRepo,
	// exchangeRates, apiKeyRateLimits, or RPM checks are ever reached. The
	// canonical wallet mechanism here is the SAME real-Redis, real-Lua-script
	// store every other test in this task uses; only the UNRELATED
	// dependencies use `nil`/the existing billingCacheWorkerStub.
	newSvc := func(t *testing.T, lease CanonicalWalletLease, control *canonicalWalletControlStub) *BillingCacheService {
		t.Helper()
		if lease.LeaseID != "" {
			require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))
		}
		if control == nil {
			control = &canonicalWalletControlStub{}
		}
		bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil) // outboxDB/outbox unused by this test
		t.Cleanup(bridge.Close)
		// A real bootstrap exchange rate keeps CheckBillingEligibility's
		// downstream currency-conversion step (reached when
		// user.BillingCurrency is non-empty) out of this test's way without
		// stubbing it.
		cfg := &config.Config{RunMode: config.RunModeStandard}
		cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
		return NewBillingCacheService(
			&billingCacheWorkerStub{}, nil, nil, nil, nil, nil,
			cfg, nil, bridge,
		)
	}
	user := &User{ID: 1, PlatformUserID: platformUserID, BillingCurrency: "CNY"}

	// Under the eligibility-bootstrap spec (2026-09-29 §2.2) an exhausted
	// lease is no longer a bare terminal refusal: the gate first makes one
	// bounded synchronous ensure. The fund-safety core of this leg survives
	// — the control plane's own "no money" answer (Shortfall) is still
	// ErrInsufficientBalance through the real entry point — and the ensure
	// call count pins that the bootstrap actually ran (otherwise the leg
	// would pass vacuously under the old semantics).
	deniedControl := &canonicalWalletControlStub{leaseErr: ErrCanonicalWalletBalanceShortfall}
	deniedSvc := newSvc(t, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 1, ConsumedUnits: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), // fully consumed: RemainingUnits() == 0
	}, deniedControl)
	err := deniedSvc.CheckBillingEligibility(ctx, user, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "an exhausted lease plus a zero-balance ensure answer must deny the request through the real CheckBillingEligibility entry point")
	require.Equal(t, 1, deniedControl.ensureCalls, "the exhausted leg must have gone through the bootstrap ensure, not a bare refusal")

	allowedUser := &User{ID: 2, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}
	allowedSvc := newSvc(t, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: allowedUser.PlatformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 0, ExpiresAt: time.Now().UTC().Add(time.Minute), // real headroom
	}, nil)
	require.NoError(t, allowedSvc.CheckBillingEligibility(ctx, allowedUser, nil, nil, nil, ""), "a lease with real headroom must admit the request through the real CheckBillingEligibility entry point")
}

// TestEnsureLeaseDrainsAnExpiredLeaseWithinTheGrace (Wallet Lease Phase 5-G,
// Task 3 Step 1): a lease that dies of EXPIRY (not exhaustion) must still be
// drained by the next ensure — redesign §3.3's seal+drain, not the 1800 s
// grace sweep. The install keeps the Redis lease hash and the current pointer
// alive through the caller slot TTL (RetainUntil), so after expires_at passes
// the cached read still finds the lease, ensureLease seals it, and the ensure
// request carries exactly one drained entry with the pre-seal consumed. At
// baseline the key ages out at expires_at, cached == nil, and the drain gate
// is structurally dead — soak window 1 measured 85.8 % insufficient_balance
// refusals against the closed form grace/(ttl+grace) = 85.7 %.
func TestEnsureLeaseDrainsAnExpiredLeaseWithinTheGrace(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	// The control plane grants a lease with 1 s of life and 40_000000 units
	// already consumed — the pre-seal consumed the drain entry must report.
	granted := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 40_000000, ExpiresAt: time.Now().UTC().Add(1 * time.Second),
	}
	control := &canonicalWalletControlStub{lease: granted}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 1800, nil)
	t.Cleanup(bridge.Close)

	first, err := bridge.ensureLease(ctx, platformUserID, "CNY", 10_000000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, granted.LeaseID, first.LeaseID)

	// Advance the REAL clock past expires_at — no fake clock reaches Redis's
	// PEXPIREAT, so the retention must actually hold the keys in Redis.
	time.Sleep(1200 * time.Millisecond)

	renewal := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	control.lease = renewal
	renewed, err := bridge.ensureLease(ctx, platformUserID, "CNY", 10_000000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, renewal.LeaseID, renewed.LeaseID)

	require.Equal(t, 2, control.ensureCalls)
	require.Len(t, control.lastEnsure.Drained, 1, "the next ensure after expiry must drain the finished lease, not wait out the grace sweep")
	entry := control.lastEnsure.Drained[0]
	require.Equal(t, granted.LeaseID, entry.LeaseID)
	require.Equal(t, "40000000", entry.GatewayConsumed.AmountUnits, "the drained entry carries the pre-seal consumed units")
}

// TestEnsureLeaseDrainCarriesGatewayReleasedAfterAPostExpiryRelease (Phase
// 5-G, Task 3 Step 4b(ii)): the release script's HINCRBY released_units is
// presence-gated on the lease hash. With the hash retained through the grace
// a release landing after expires_at now APPLIES instead of silently
// no-op'ing — the correct direction: the drain identity is consumed ==
// captured + released, and a dropped release is exactly what made drains
// fail to verify on ShipAny's side. The next ensure's drained entry must
// carry gateway_released so the identity can close.
func TestEnsureLeaseDrainCarriesGatewayReleasedAfterAPostExpiryRelease(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC()

	granted := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: now.Add(1 * time.Second), RetainUntil: now.Add(1800 * time.Second),
	}
	control := &canonicalWalletControlStub{lease: granted}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 1800, nil)
	t.Cleanup(bridge.Close)

	first, err := bridge.ensureLease(ctx, platformUserID, "CNY", 10_000000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)

	// A hold is armed while the lease is live; consumed rises by the held
	// figure (the arm script's own HINCRBY).
	authorizationID := "auth-" + uuid.NewString()
	_, held, duplicate, err := store.ArmCanonicalWalletHold(ctx, platformUserID, first.LeaseID, "CNY", authorizationID, 20_000000, 1_800_000, now)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.Equal(t, int64(20_000000), held)

	time.Sleep(1200 * time.Millisecond) // the real clock passes expires_at; the hash is retained

	releasedUnits, err := store.ReleaseCanonicalWalletHold(ctx, platformUserID, authorizationID, "released", "")
	require.NoError(t, err)
	require.Equal(t, int64(20_000000), releasedUnits)

	// The release APPLIED on the retained hash: released_units rose by held.
	stored, err := rdb.HGet(ctx, testCanonicalWalletLeaseKey(platformUserID, first.LeaseID), "released_units").Result()
	require.NoError(t, err, "the post-expiry release must land on the retained lease hash")
	require.Equal(t, "20000000", stored)

	renewal := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	control.lease = renewal
	_, err = bridge.ensureLease(ctx, platformUserID, "CNY", 10_000000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)

	require.Len(t, control.lastEnsure.Drained, 1)
	entry := control.lastEnsure.Drained[0]
	require.Equal(t, granted.LeaseID, entry.LeaseID)
	require.Equal(t, "20000000", entry.GatewayConsumed.AmountUnits, "pre-seal consumed is the arm's HINCRBY figure")
	require.NotNil(t, entry.GatewayReleased, "the drained entry must carry gateway_released so consumed == captured + released can verify")
	require.Equal(t, "20000000", entry.GatewayReleased.AmountUnits)
}

// gatewayCacheAdapterForTest duplicates repository.gatewayCache's four
// CanonicalWalletLeaseStore methods and their private dependencies (the two
// Lua scripts, the key-builder functions, ensureRedisLuaSafeInt64, and the
// result-parsing helpers) verbatim from canonical_wallet_store.go (Task
// 3) — intentional duplication across the service/repository package
// boundary (service cannot import repository; repository imports service).
// If Task 3's real canonical_wallet_store.go changes these scripts or
// helpers later, this copy must be updated to match or this test silently
// stops proving what it claims to.
type gatewayCacheAdapterForTest struct{ rdb *redis.Client }

const testCanonicalWalletLeasePrefix = "canonical_wallet:lease:"
const testCanonicalWalletCurrentPrefix = "canonical_wallet:current:"
const testRedisLuaMaxSafeInt64 = 9007199254740991

func ensureTestRedisLuaSafeInt64(values ...int64) error {
	for _, v := range values {
		if v < 0 || v > testRedisLuaMaxSafeInt64 {
			return fmt.Errorf("canonical wallet Redis operation requires every amount within Lua's exact integer range (0..%d); got %d", testRedisLuaMaxSafeInt64, v)
		}
	}
	return nil
}

var testInstallCanonicalWalletLeaseScript = redis.NewScript(`
	local incoming_consumed = tonumber(ARGV[4])
	local incoming_expires = tonumber(ARGV[5])
	local lease_key_prefix = ARGV[7]
	local retain_until = tonumber(ARGV[8])
	if redis.call('EXISTS', KEYS[1]) == 1 then
		local current_consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
		if current_consumed > incoming_consumed then
			incoming_consumed = current_consumed
		end
	end
	redis.call('HSET', KEYS[1],
		'lease_id', ARGV[1], 'platform_user_id', ARGV[2], 'currency', ARGV[3],
		'budget_units', ARGV[6], 'consumed_units', incoming_consumed, 'expires_at_ms', ARGV[5])
	redis.call('PEXPIREAT', KEYS[1], retain_until)

	local current_pointer_lease_id = redis.call('GET', KEYS[2])
	local should_advance_pointer = true
	if current_pointer_lease_id ~= false and current_pointer_lease_id ~= ARGV[1] then
		local pointer_key = lease_key_prefix .. current_pointer_lease_id
		local pointer_expires = tonumber(redis.call('HGET', pointer_key, 'expires_at_ms') or '0')
		if pointer_expires > incoming_expires then
			should_advance_pointer = false
		elseif pointer_expires == incoming_expires and current_pointer_lease_id > ARGV[1] then
			should_advance_pointer = false
		end
	end
	if should_advance_pointer then
		redis.call('SET', KEYS[2], ARGV[1])
		redis.call('PEXPIREAT', KEYS[2], retain_until)
	end
	return 1
`)

var testReserveCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local stored_lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local currency = redis.call('HGET', KEYS[1], 'currency')
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
	if currency ~= ARGV[1] then return {2} end
	if expires_at <= tonumber(ARGV[4]) then return {3} end
	local existing = redis.call('GET', KEYS[2])
	if existing ~= false then
		if existing == stored_lease_id then
			return {5, stored_lease_id, currency, budget, consumed, expires_at}
		end
		return {6}
	end
	local amount = tonumber(ARGV[2])
	if amount <= 0 or consumed - released + amount > budget then return {4} end
	local updated = consumed + amount
	redis.call('HSET', KEYS[1], 'consumed_units', updated)
	-- ABSOLUTE deadline, matching the lease hash's own PEXPIREAT. A relative
	-- PX computed from the GATEWAY's clock (ARGV[4]) is applied at REDIS's
	-- execution instant, so the marker outlived its lease by the round-trip
	-- latency plus any clock offset. Both keys now expire against one clock,
	-- which is what lets the dispatcher treat "lease gone" as "marker gone"
	-- when it releases a stale binding.
	redis.call('SET', KEYS[2], stored_lease_id)
	redis.call('PEXPIREAT', KEYS[2], expires_at)
	return {0, stored_lease_id, currency, budget, updated, expires_at}
`)

var testSealCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	redis.call('HSET', KEYS[1], 'consumed_units', budget)
	local current = redis.call('GET', KEYS[2])
	if current ~= false and current == ARGV[1] then
		redis.call('DEL', KEYS[2])
	end
	return {0, consumed, released}
`)

var testArmCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	if redis.call('HGET', KEYS[1], 'currency') ~= ARGV[1] then return {2} end
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
	if expires_at <= tonumber(ARGV[3]) then return {3} end
	if redis.call('EXISTS', KEYS[2]) == 1 then
		return {5, redis.call('HGET', KEYS[2], 'lease_id'), tonumber(redis.call('HGET', KEYS[2], 'held_units'))}
	end
	local units = tonumber(ARGV[2])
	if units <= 0 or consumed - released + units > budget then return {4} end
	redis.call('HSET', KEYS[1], 'consumed_units', consumed + units)
	redis.call('HSET', KEYS[2], 'lease_id', redis.call('HGET', KEYS[1], 'lease_id'), 'held_units', units, 'armed_at_ms', ARGV[3], 'class', '', 'state', 'armed', 'event_id', '')
	redis.call('PEXPIREAT', KEYS[2], expires_at + tonumber(ARGV[5]))
	redis.call('SADD', KEYS[3], ARGV[4])
	return {0, redis.call('HGET', KEYS[1], 'lease_id'), units}
`)

var testReleaseCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	if redis.call('HGET', KEYS[1], 'state') ~= 'armed' then return {7, redis.call('HGET', KEYS[1], 'state'), redis.call('HGET', KEYS[1], 'event_id')} end
	local held = tonumber(redis.call('HGET', KEYS[1], 'held_units'))
	if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('HINCRBY', KEYS[3], 'released_units', held) end
	redis.call('HSET', KEYS[1], 'state', ARGV[2])
	if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'class', ARGV[3]) end
	redis.call('SREM', KEYS[2], ARGV[1])
	return {0, held}
`)

var testConvertCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local state = redis.call('HGET', KEYS[1], 'state')
	if state ~= 'armed' then return {7, state, redis.call('HGET', KEYS[1], 'event_id'), redis.call('HGET', KEYS[1], 'lease_id')} end
	local held = tonumber(redis.call('HGET', KEYS[1], 'held_units'))
	local actual = tonumber(ARGV[3])
	local lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local lease_exists = redis.call('EXISTS', KEYS[3]) == 1
	local expires_at = tonumber(redis.call('HGET', KEYS[3], 'expires_at_ms') or '0')
	if actual <= held then
		if lease_exists then
			redis.call('HINCRBY', KEYS[3], 'released_units', held - actual)
			-- synced with production's Phase 5-G Step 3b line (execution review round 1,
			-- minor 1): the marker takes the lease key's remaining life, not expires_at.
			redis.call('SET', KEYS[4], lease_id); local ttl = redis.call('PTTL', KEYS[3]); if ttl > 0 then redis.call('PEXPIRE', KEYS[4], ttl) end
		end
		redis.call('HSET', KEYS[1], 'state', 'settled', 'event_id', ARGV[2]); redis.call('SREM', KEYS[2], ARGV[1])
		if not lease_exists then return {3} end
		return {0, lease_id}
	end
	local excess = actual - held
	if lease_exists then
		local budget = tonumber(redis.call('HGET', KEYS[3], 'budget_units') or '0')
		local consumed = tonumber(redis.call('HGET', KEYS[3], 'consumed_units') or '0')
		local released = tonumber(redis.call('HGET', KEYS[3], 'released_units') or '0')
		if expires_at > tonumber(ARGV[4]) and consumed - released + excess <= budget then
			redis.call('HINCRBY', KEYS[3], 'consumed_units', excess)
			redis.call('SET', KEYS[4], lease_id); redis.call('PEXPIREAT', KEYS[4], expires_at)
			redis.call('HSET', KEYS[1], 'state', 'settled', 'event_id', ARGV[2]); redis.call('SREM', KEYS[2], ARGV[1])
			return {0, lease_id}
		end
		redis.call('HINCRBY', KEYS[3], 'released_units', held)
	end
	redis.call('HSET', KEYS[1], 'state', 'released'); redis.call('SREM', KEYS[2], ARGV[1])
	return {4}
`)

var testMarkCanonicalWalletHoldClassScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local state = redis.call('HGET', KEYS[1], 'state')
	if state ~= 'armed' then return {7, state, redis.call('HGET', KEYS[1], 'event_id')} end
	redis.call('HSET', KEYS[1], 'class', ARGV[1])
	return {0, ARGV[1]}
`)

// testReleaseCanonicalWalletReservationScript is the verbatim twin of
// repository.releaseCanonicalWalletReservationScript (Phase 3.5, §11.2).
var testReleaseCanonicalWalletReservationScript = redis.NewScript(`
	local marker = redis.call('GET', KEYS[2])
	if marker == false then return {1} end
	if marker ~= ARGV[1] then return {6} end
	if ARGV[3] == '0' and redis.call('EXISTS', KEYS[3]) == 1 then return {7} end
	local lease_exists = redis.call('EXISTS', KEYS[1]) == 1
	if lease_exists then redis.call('HINCRBY', KEYS[1], 'released_units', tonumber(ARGV[2])) end
	if ARGV[3] == '0' then
		redis.call('SET', KEYS[3], ARGV[1])
		local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
		if expires_at > 0 then redis.call('PEXPIREAT', KEYS[3], expires_at) else redis.call('PEXPIRE', KEYS[3], 3600000) end
	else
		redis.call('DEL', KEYS[2])
	end
	return {0}
`)

func testCanonicalWalletUserHash(platformUserID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(platformUserID)))
	return hex.EncodeToString(sum[:])
}
func testCanonicalWalletLeaseKeyPrefix(platformUserID string) string {
	return testCanonicalWalletLeasePrefix + "{" + testCanonicalWalletUserHash(platformUserID) + "}:"
}
func testCanonicalWalletLeaseKey(platformUserID, leaseID string) string {
	return testCanonicalWalletLeaseKeyPrefix(platformUserID) + leaseID
}
func testCanonicalWalletCurrentKey(platformUserID string) string {
	return testCanonicalWalletCurrentPrefix + "{" + testCanonicalWalletUserHash(platformUserID) + "}"
}
func testCanonicalWalletReservationKey(platformUserID, eventID string) string {
	eventSum := sha256.Sum256([]byte(strings.TrimSpace(eventID)))
	return "canonical_wallet:reservation:{" + testCanonicalWalletUserHash(platformUserID) + "}:" + hex.EncodeToString(eventSum[:])
}

func testCanonicalWalletReleaseMarkerKey(platformUserID, eventID string) string {
	eventSum := sha256.Sum256([]byte(strings.TrimSpace(eventID)))
	return "canonical_wallet:released:{" + testCanonicalWalletUserHash(platformUserID) + "}:" + hex.EncodeToString(eventSum[:])
}

const testCanonicalWalletHoldPrefix = "canonical_wallet:hold:"
const testCanonicalWalletHoldSetPrefix = "canonical_wallet:holds:"
const testCanonicalWalletHoldUsersKey = "canonical_wallet:hold_users"
const testCanonicalWalletReaperTickKey = "canonical_wallet:reaper:tick"
const testCanonicalWalletHoldUserEmptyPrefix = "canonical_wallet:holds_empty:"

func testCanonicalWalletHoldKey(platformUserID, authorizationID string) string {
	return testCanonicalWalletHoldPrefix + "{" + testCanonicalWalletUserHash(platformUserID) + "}:" + authorizationID
}
func testCanonicalWalletHoldSetKey(platformUserID string) string {
	return testCanonicalWalletHoldSetPrefix + "{" + testCanonicalWalletUserHash(platformUserID) + "}"
}
func testCanonicalWalletHoldUserEmptyKey(platformUserID string) string {
	return testCanonicalWalletHoldUserEmptyPrefix + "{" + testCanonicalWalletUserHash(platformUserID) + "}"
}
func testParseCanonicalWalletLease(values map[string]string) (*CanonicalWalletLease, error) {
	budget, err := strconv.ParseInt(values["budget_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet budget: %w", err)
	}
	consumed, err := strconv.ParseInt(values["consumed_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet consumption: %w", err)
	}
	expiresAtMS, err := strconv.ParseInt(values["expires_at_ms"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet expiry: %w", err)
	}
	return &CanonicalWalletLease{
		LeaseID: values["lease_id"], PlatformUserID: values["platform_user_id"], Currency: values["currency"],
		BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
	}, nil
}
func testRedisResultInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}

func (c *gatewayCacheAdapterForTest) InstallCanonicalWalletLease(ctx context.Context, lease CanonicalWalletLease) error {
	if strings.TrimSpace(lease.PlatformUserID) == "" || strings.TrimSpace(lease.LeaseID) == "" || lease.BudgetUnits <= 0 || lease.ExpiresAt.IsZero() {
		return errors.New("invalid canonical wallet lease")
	}
	if lease.ConsumedUnits < 0 || lease.ConsumedUnits > lease.BudgetUnits {
		return errors.New("invalid canonical wallet lease consumption")
	}
	if err := ensureTestRedisLuaSafeInt64(lease.BudgetUnits, lease.ConsumedUnits); err != nil {
		return err
	}
	currency, err := RequireCNYBillingCurrency(lease.Currency)
	if err != nil {
		return err
	}
	// Mirrors the production store (Phase 5-G, Task 3): RetainUntil keeps
	// the hash and pointer alive through the grace window; unset means
	// baseline behaviour (age out at ExpiresAt).
	retainUntil := lease.RetainUntil
	if retainUntil.IsZero() || retainUntil.Before(lease.ExpiresAt) {
		retainUntil = lease.ExpiresAt
	}
	return testInstallCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), testCanonicalWalletCurrentKey(lease.PlatformUserID)},
		lease.LeaseID, strings.TrimSpace(lease.PlatformUserID), currency,
		lease.ConsumedUnits, lease.ExpiresAt.UnixMilli(), lease.BudgetUnits, testCanonicalWalletLeaseKeyPrefix(lease.PlatformUserID), retainUntil.UnixMilli(),
	).Err()
}

func (c *gatewayCacheAdapterForTest) GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*CanonicalWalletLease, error) {
	currentID, err := c.rdb.Get(ctx, testCanonicalWalletCurrentKey(platformUserID)).Result()
	if err == redis.Nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	if err != nil {
		return nil, err
	}
	return c.GetCanonicalWalletLeaseByID(ctx, platformUserID, currentID)
}

func (c *gatewayCacheAdapterForTest) GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*CanonicalWalletLease, error) {
	values, err := c.rdb.HGetAll(ctx, testCanonicalWalletLeaseKey(platformUserID, leaseID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	return testParseCanonicalWalletLease(values)
}

func (c *gatewayCacheAdapterForTest) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*CanonicalWalletReservation, error) {
	if strings.TrimSpace(eventID) == "" || amountUnits <= 0 || strings.TrimSpace(leaseID) == "" {
		return nil, errors.New("canonical wallet reservation requires a lease id, positive amount, and event id")
	}
	if err := ensureTestRedisLuaSafeInt64(amountUnits); err != nil {
		return nil, err
	}
	strictCurrency, err := RequireCNYBillingCurrency(currency)
	if err != nil {
		return nil, err
	}
	result, err := testReserveCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletLeaseKey(platformUserID, leaseID), testCanonicalWalletReservationKey(platformUserID, eventID)},
		strictCurrency, amountUnits, eventID, now.UnixMilli(),
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("canonical wallet reservation returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case 1:
		return nil, ErrCanonicalWalletLeaseMissing
	case 2:
		return nil, ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return nil, ErrCanonicalWalletLeaseExpired
	case 4:
		return nil, ErrCanonicalWalletLeaseExhausted
	case 6:
		return nil, ErrCanonicalWalletReservationConflict
	case 0, 5:
		if len(result) != 6 {
			return nil, errors.New("canonical wallet reservation returned an invalid snapshot")
		}
		budget, err := testRedisResultInt64(result[3])
		if err != nil {
			return nil, err
		}
		consumed, err := testRedisResultInt64(result[4])
		if err != nil {
			return nil, err
		}
		expiresAtMS, err := testRedisResultInt64(result[5])
		if err != nil {
			return nil, err
		}
		return &CanonicalWalletReservation{Lease: CanonicalWalletLease{
			LeaseID: fmt.Sprint(result[1]), PlatformUserID: strings.TrimSpace(platformUserID), Currency: fmt.Sprint(result[2]),
			BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
		}, Duplicate: code == 5}, nil
	default:
		return nil, fmt.Errorf("unknown canonical wallet reservation code %d", code)
	}
}

func (c *gatewayCacheAdapterForTest) SealCanonicalWalletLease(ctx context.Context, platformUserID, leaseID string) (int64, int64, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" {
		return 0, 0, errors.New("canonical wallet seal requires a platform user id and a lease id")
	}
	result, err := testSealCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletLeaseKey(platformUserID, leaseID), testCanonicalWalletCurrentKey(platformUserID)},
		leaseID,
	).Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(result) == 0 {
		return 0, 0, errors.New("canonical wallet seal returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return 0, 0, err
	}
	if code == 1 {
		return 0, 0, ErrCanonicalWalletLeaseMissing
	}
	if len(result) != 3 {
		return 0, 0, errors.New("canonical wallet seal returned an invalid result")
	}
	pre, err := testRedisResultInt64(result[1])
	if err != nil {
		return 0, 0, err
	}
	released, err := testRedisResultInt64(result[2])
	if err != nil {
		return 0, 0, err
	}
	return pre, released, nil
}

func (c *gatewayCacheAdapterForTest) ArmCanonicalWalletHold(ctx context.Context, platformUserID, leaseID, currency, authorizationID string, units int64, graceMS int64, now time.Time) (string, int64, bool, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" || strings.TrimSpace(authorizationID) == "" || units <= 0 || graceMS < 0 {
		return "", 0, false, errors.New("canonical wallet hold arm requires a platform user id, a lease id, an authorization id and positive units")
	}
	if err := ensureTestRedisLuaSafeInt64(units); err != nil {
		return "", 0, false, err
	}
	strictCurrency, err := RequireCNYBillingCurrency(currency)
	if err != nil {
		return "", 0, false, err
	}
	result, err := testArmCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletLeaseKey(platformUserID, leaseID), testCanonicalWalletHoldKey(platformUserID, authorizationID), testCanonicalWalletHoldSetKey(platformUserID)},
		strictCurrency, units, now.UnixMilli(), authorizationID, graceMS,
	).Slice()
	if err != nil {
		return "", 0, false, err
	}
	if len(result) == 0 {
		return "", 0, false, errors.New("canonical wallet hold arm returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return "", 0, false, err
	}
	switch code {
	case 1:
		return "", 0, false, ErrCanonicalWalletLeaseMissing
	case 2:
		return "", 0, false, ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return "", 0, false, ErrCanonicalWalletLeaseExpired
	case 4:
		return "", 0, false, ErrCanonicalWalletLeaseExhausted
	case 5:
		if len(result) != 3 {
			return "", 0, false, errors.New("canonical wallet hold arm returned an invalid duplicate reply")
		}
		held, err := testRedisResultInt64(result[2])
		if err != nil {
			return "", 0, false, err
		}
		return fmt.Sprint(result[1]), held, true, nil
	case 0:
		if len(result) != 3 {
			return "", 0, false, errors.New("canonical wallet hold arm returned an invalid reply")
		}
		held, err := testRedisResultInt64(result[2])
		if err != nil {
			return "", 0, false, err
		}
		if err := c.rdb.SAdd(ctx, testCanonicalWalletHoldUsersKey, strings.TrimSpace(platformUserID)).Err(); err != nil {
			return "", 0, false, err
		}
		return fmt.Sprint(result[1]), held, false, nil
	default:
		return "", 0, false, fmt.Errorf("unknown canonical wallet hold arm code %d", code)
	}
}

func (c *gatewayCacheAdapterForTest) ReleaseCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, stateAfter, classAfter string) (int64, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" {
		return 0, errors.New("canonical wallet hold release requires a platform user id and an authorization id")
	}
	if stateAfter != "released" && stateAfter != "abandoned" {
		return 0, errors.New("canonical wallet hold release state_after must be released or abandoned")
	}
	leaseID, err := c.rdb.HGet(ctx, testCanonicalWalletHoldKey(platformUserID, authorizationID), "lease_id").Result()
	if err == redis.Nil {
		return 0, ErrCanonicalWalletHoldMissing
	}
	if err != nil {
		return 0, err
	}
	result, err := testReleaseCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletHoldKey(platformUserID, authorizationID), testCanonicalWalletHoldSetKey(platformUserID), testCanonicalWalletLeaseKey(platformUserID, leaseID)},
		authorizationID, stateAfter, classAfter,
	).Slice()
	if err != nil {
		return 0, err
	}
	if len(result) == 0 {
		return 0, errors.New("canonical wallet hold release returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return 0, err
	}
	switch code {
	case 1:
		return 0, ErrCanonicalWalletHoldMissing
	case 7:
		state := fmt.Sprint(result[1])
		eventID := ""
		if len(result) > 2 {
			eventID = fmt.Sprint(result[2])
		}
		return 0, &CanonicalWalletHoldNotArmedError{State: state, EventID: eventID}
	case 0:
		if len(result) != 2 {
			return 0, errors.New("canonical wallet hold release returned an invalid reply")
		}
		return testRedisResultInt64(result[1])
	default:
		return 0, fmt.Errorf("unknown canonical wallet hold release code %d", code)
	}
}

func (c *gatewayCacheAdapterForTest) ConvertCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, eventID string, actualUnits int64, now time.Time) (CanonicalWalletHoldConversion, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" || strings.TrimSpace(eventID) == "" || actualUnits < 0 {
		return CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert requires a platform user id, an authorization id, an event id and non-negative units")
	}
	if err := ensureTestRedisLuaSafeInt64(actualUnits); err != nil {
		return CanonicalWalletHoldConversion{}, err
	}
	leaseID, err := c.rdb.HGet(ctx, testCanonicalWalletHoldKey(platformUserID, authorizationID), "lease_id").Result()
	if err == redis.Nil {
		return CanonicalWalletHoldConversion{Code: 1}, nil
	}
	if err != nil {
		return CanonicalWalletHoldConversion{}, err
	}
	result, err := testConvertCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{
			testCanonicalWalletHoldKey(platformUserID, authorizationID), testCanonicalWalletHoldSetKey(platformUserID),
			testCanonicalWalletLeaseKey(platformUserID, leaseID), testCanonicalWalletReservationKey(platformUserID, eventID),
		},
		authorizationID, eventID, actualUnits, now.UnixMilli(),
	).Slice()
	if err != nil {
		return CanonicalWalletHoldConversion{}, err
	}
	if len(result) == 0 {
		return CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return CanonicalWalletHoldConversion{}, err
	}
	conv := CanonicalWalletHoldConversion{Code: int(code)}
	switch code {
	case 0:
		if len(result) != 2 {
			return CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned an invalid reply")
		}
		conv.LeaseID = fmt.Sprint(result[1])
	case 7:
		if len(result) != 4 {
			return CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned an invalid not-armed reply")
		}
		conv.State = fmt.Sprint(result[1])
		conv.EventID = fmt.Sprint(result[2])
		conv.LeaseID = fmt.Sprint(result[3])
	}
	return conv, nil
}

func (c *gatewayCacheAdapterForTest) GetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) (*CanonicalWalletHold, error) {
	values, err := c.rdb.HGetAll(ctx, testCanonicalWalletHoldKey(platformUserID, authorizationID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrCanonicalWalletHoldMissing
	}
	held, err := strconv.ParseInt(values["held_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet hold units: %w", err)
	}
	armedAtMS, err := strconv.ParseInt(values["armed_at_ms"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet hold armed_at: %w", err)
	}
	return &CanonicalWalletHold{
		AuthorizationID: authorizationID, LeaseID: values["lease_id"], HeldUnits: held,
		ArmedAt: time.UnixMilli(armedAtMS).UTC(), Class: values["class"], State: values["state"], EventID: values["event_id"],
	}, nil
}

func (c *gatewayCacheAdapterForTest) ListCanonicalWalletHolds(ctx context.Context, platformUserID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	members, err := c.rdb.SMembers(ctx, testCanonicalWalletHoldSetKey(platformUserID)).Result()
	if err != nil {
		return nil, err
	}
	if len(members) > limit {
		members = members[:limit]
	}
	return members, nil
}

func (c *gatewayCacheAdapterForTest) ListCanonicalWalletHoldUsers(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	if count <= 0 {
		count = 100
	}
	return c.rdb.SScan(ctx, testCanonicalWalletHoldUsersKey, cursor, "", count).Result()
}

func (c *gatewayCacheAdapterForTest) PruneCanonicalWalletHoldUser(ctx context.Context, platformUserID string) error {
	return c.rdb.SRem(ctx, testCanonicalWalletHoldUsersKey, strings.TrimSpace(platformUserID)).Err()
}

func (c *gatewayCacheAdapterForTest) TryCanonicalWalletReaperLease(ctx context.Context, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, testCanonicalWalletReaperTickKey, "1", ttl).Result()
}

// TryCanonicalWalletReceivableCollectorLease (Phase 4.2-G Task 2): the
// collector's OWN leader key, mirroring the production gatewayCache.
func (c *gatewayCacheAdapterForTest) TryCanonicalWalletReceivableCollectorLease(ctx context.Context, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, "canonical_wallet:receivable_collector:tick", "1", ttl).Result()
}

func (c *gatewayCacheAdapterForTest) ForgetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) error {
	return c.rdb.SRem(ctx, testCanonicalWalletHoldSetKey(platformUserID), authorizationID).Err()
}

func (c *gatewayCacheAdapterForTest) MarkCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string, ttl time.Duration) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, testCanonicalWalletHoldUserEmptyKey(platformUserID), "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func (c *gatewayCacheAdapterForTest) ClearCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string) error {
	return c.rdb.Del(ctx, testCanonicalWalletHoldUserEmptyKey(platformUserID)).Err()
}

func (c *gatewayCacheAdapterForTest) MarkCanonicalWalletHoldClass(ctx context.Context, platformUserID, authorizationID, class string) (*CanonicalWalletHold, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" || strings.TrimSpace(class) == "" {
		return nil, errors.New("canonical wallet hold mark-class requires a platform user id, an authorization id and a class")
	}
	result, err := testMarkCanonicalWalletHoldClassScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletHoldKey(platformUserID, authorizationID)},
		class,
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("canonical wallet hold mark-class returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case 1:
		return nil, ErrCanonicalWalletHoldMissing
	case 7:
		state := fmt.Sprint(result[1])
		eventID := ""
		if len(result) > 2 {
			eventID = fmt.Sprint(result[2])
		}
		return nil, &CanonicalWalletHoldNotArmedError{State: state, EventID: eventID}
	case 0:
		return c.GetCanonicalWalletHold(ctx, platformUserID, authorizationID)
	default:
		return nil, fmt.Errorf("unknown canonical wallet hold mark-class code %d", code)
	}
}

var _ CanonicalWalletLeaseStore = (*gatewayCacheAdapterForTest)(nil)

// ReleaseCanonicalWalletReservation mirrors repository.gatewayCache's method
// verbatim (Phase 3.5, §11.2) — same script, same keys, same branch codes.
func (c *gatewayCacheAdapterForTest) ReleaseCanonicalWalletReservation(ctx context.Context, platformUserID, leaseID, eventID string, units int64, dropMarker bool) (bool, error) {
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" || strings.TrimSpace(eventID) == "" {
		return false, errors.New("canonical wallet reservation release requires a platform user id, a lease id and an event id")
	}
	if err := ensureTestRedisLuaSafeInt64(units); err != nil {
		return false, err
	}
	if units <= 0 {
		return false, errors.New("canonical wallet reservation release requires positive units")
	}
	drop := "0"
	if dropMarker {
		drop = "1"
	}
	result, err := testReleaseCanonicalWalletReservationScript.Run(ctx, c.rdb,
		[]string{
			testCanonicalWalletLeaseKey(platformUserID, leaseID),
			testCanonicalWalletReservationKey(platformUserID, eventID),
			testCanonicalWalletReleaseMarkerKey(platformUserID, eventID),
		},
		leaseID, units, drop,
	).Slice()
	if err != nil {
		return false, err
	}
	if len(result) == 0 {
		return false, errors.New("canonical wallet reservation release returned no result")
	}
	code, err := testRedisResultInt64(result[0])
	if err != nil {
		return false, err
	}
	switch code {
	case 0:
		return true, nil
	case 1, 6, 7:
		return false, nil
	default:
		return false, fmt.Errorf("unknown canonical wallet reservation release code %d", code)
	}
}

// TestCanonicalWalletShadowModeAllowsEverythingAndReservesNothing proves the
// disclosed Phase-2 contract for shadow mode across ALL THREE entry points:
// every request is allowed regardless of lease state, and nothing is ever
// reserved (the Redis keyspace stays empty of reservation markers).
func TestCanonicalWalletShadowModeAllowsEverythingAndReservesNothing(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), store, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)

	allowed, err := bridge.CheckAndReserve(ctx, CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-shadow", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 999_000000,
	})
	require.NoError(t, err)
	require.True(t, allowed, "shadow mode observes but always allows")

	headroom, err := bridge.HasCanonicalWalletHeadroom(ctx, platformUserID, "CNY")
	require.NoError(t, err)
	require.True(t, headroom, "shadow mode never denies admission")

	keys, err := rdb.Keys(ctx, "*canonical_wallet*").Result()
	require.NoError(t, err)
	require.Empty(t, keys, "shadow mode must not write any lease or reservation state")
}

func TestHasCanonicalWalletHeadroomEnforceBranches(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	now := time.Now().UTC().Truncate(time.Millisecond)

	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)

	// No lease ever issued: fail closed with the store's own error.
	_, err := bridge.HasCanonicalWalletHeadroom(ctx, "shipany-user-"+uuid.NewString(), "CNY")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing, "with no lease data there is nothing to admit against — fail closed in enforce mode")

	// NOTE on the `!lease.ExpiresAt.After(now)` branch: it is
	// defense-in-depth against Redis/Go clock skew and is NOT reachable
	// through the public store contract — Install writes ONE expires_at_ms
	// used both as the hash field and as PEXPIREAT, so a lease whose embedded
	// expiry has passed is already evicted by Redis itself (a past PEXPIREAT
	// deletes the key immediately). Fabricating contradictory raw state to
	// cover that line would be a coverage-only assertion.

	// Fully consumed current lease: not admissible.
	exhaustedUser := "shipany-user-" + uuid.NewString()
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-exh-" + uuid.NewString(), PlatformUserID: exhaustedUser, Currency: "CNY",
		BudgetUnits: 1, ConsumedUnits: 1, ExpiresAt: now.Add(time.Minute),
	}))
	allowed, err := bridge.HasCanonicalWalletHeadroom(ctx, exhaustedUser, "CNY")
	require.NoError(t, err)
	require.False(t, allowed, "a fully consumed lease has zero remaining units")

	// Real headroom: admissible.
	fundedUser := "shipany-user-" + uuid.NewString()
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-ok-" + uuid.NewString(), PlatformUserID: fundedUser, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 0, ExpiresAt: now.Add(time.Minute),
	}))
	allowed, err = bridge.HasCanonicalWalletHeadroom(ctx, fundedUser, "CNY")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestCheckBalanceEligibilityEnforceBranchesThroughRealEntryPoints(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}

	newBridge := func(control *canonicalWalletControlStub) *CanonicalWalletBridge {
		if control == nil {
			control = &canonicalWalletControlStub{}
		}
		b := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil)
		t.Cleanup(b.Close)
		return b
	}
	newCfg := func() *config.Config {
		cfg := &config.Config{RunMode: config.RunModeStandard}
		cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
		return cfg
	}

	// Blank platform identity under enforce mode fails closed.
	blankIDUser := &User{ID: 11, PlatformUserID: "   ", BillingCurrency: "CNY"}
	blankIDSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, newCfg(), nil, newBridge(nil))
	err := blankIDSvc.CheckBillingEligibility(ctx, blankIDUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "a user without a canonical identity cannot be checked — fail closed")

	// A non-CNY billing currency is rejected outright, never coerced to CNY.
	usdUser := &User{ID: 12, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "USD"}
	usdSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, newCfg(), nil, newBridge(nil))
	err = usdSvc.CheckBillingEligibility(ctx, usdUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "cny-e8-v1 is CNY-only — reject instead of admitting under the wrong wallet")

	// An absent lease is no longer a bare refusal (spec
	// 2026-09-29-wallet-lease-enforce-eligibility-bootstrap §4 test 2): the
	// gate first makes ONE bounded synchronous ensure, and the ensure's own
	// answer decides — funded grant admits, zero-balance is a terminal 403,
	// transport failure fails closed as retryable 503.
	fundedPlatformID := "shipany-user-" + uuid.NewString()
	funded := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: "lease-boot-" + uuid.NewString(), PlatformUserID: fundedPlatformID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}}
	fundedUser := &User{ID: 13, PlatformUserID: fundedPlatformID, BillingCurrency: "CNY"}
	fundedSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, newCfg(), nil, newBridge(funded))
	require.NoError(t, fundedSvc.CheckBillingEligibility(ctx, fundedUser, nil, nil, nil, ""),
		"an absent lease with a funded ensure must bootstrap and admit through the real entry point")
	require.Equal(t, 1, funded.ensureCalls)
	require.Equal(t, "authorize", funded.lastEnsure.Purpose, "the bootstrap is the authorization point's own ensure shape")
	require.Equal(t, "1", funded.lastEnsure.MinHeadroom.AmountUnits, "the bootstrap asks for any non-zero headroom — the real ceiling is the authorize point's estimate")

	zero := &canonicalWalletControlStub{leaseErr: ErrCanonicalWalletBalanceShortfall}
	zeroUser := &User{ID: 14, PlatformUserID: "shipany-user-boot-zero", BillingCurrency: "CNY"}
	zeroSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, newCfg(), nil, newBridge(zero))
	err = zeroSvc.CheckBillingEligibility(ctx, zeroUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "a zero-balance ensure answer is the terminal 403, not an infra failure")
	require.Equal(t, 1, zero.ensureCalls)

	transport := errors.New("canonical wallet control plane unreachable")
	transportErr := &canonicalWalletControlStub{leaseErr: transport}
	transportUser := &User{ID: 15, PlatformUserID: "shipany-user-boot-transport", BillingCurrency: "CNY"}
	transportSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, newCfg(), nil, newBridge(transportErr))
	err = transportSvc.CheckBillingEligibility(ctx, transportUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "a bootstrap transport failure fails closed — retryable 503, never admitted")
	require.Equal(t, 1, transportErr.ensureCalls)

	// The LEGACY float64 path still decides when no canonical wallet is
	// wired at all: an empty cache balance denies, a funded one admits.
	// (fixedBalanceCache is DI plumbing only — the reserve path above runs
	// on the real Redis store.)
	legacyDeny := NewBillingCacheService(&fixedBalanceCache{balance: 0}, nil, nil, nil, nil, nil, newCfg(), nil, nil)
	err = legacyDeny.CheckBillingEligibility(ctx, &User{ID: 16, BillingCurrency: "CNY"}, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "the legacy path still denies a zero balance")

	legacyAllow := NewBillingCacheService(&fixedBalanceCache{balance: 100}, nil, nil, nil, nil, nil, newCfg(), nil, nil)
	require.NoError(t, legacyAllow.CheckBillingEligibility(ctx, &User{ID: 17, BillingCurrency: "CNY"}, nil, nil, nil, ""))
}

// TestCheckBalanceEligibilityBootstrapsExpiredRetainedLease (spec
// 2026-09-29-wallet-lease-enforce-eligibility-bootstrap §4 test 5): the
// 2026-09-29 incident's permanent-403 shape — a lease that died of expiry
// but is still readable through RetainUntil — must self-heal INSIDE the
// eligibility gate: the read answers (false, nil), the gate's bootstrap
// ensure seals + drains the finished lease and installs the renewal, the
// re-read admits. The drained entry must carry the pre-seal consumed units
// (§3.3 + 5-G Task 3 convergence).
func TestCheckBalanceEligibilityBootstrapsExpiredRetainedLease(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	// The control plane first grants a 1 s lease with 40_000000 units
	// already consumed — the pre-seal consumed the drain entry must report.
	granted := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 40_000000, ExpiresAt: time.Now().UTC().Add(1 * time.Second),
	}
	control := &canonicalWalletControlStub{lease: granted}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 1800, nil)
	t.Cleanup(bridge.Close)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	svc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, bridge)
	user := &User{ID: 31, PlatformUserID: platformUserID, BillingCurrency: "CNY"}

	// Cold user: the first check bootstraps (no lease exists at all).
	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""), "the first request of a cold user must bootstrap a lease and admit")
	require.Equal(t, 1, control.ensureCalls)

	// Advance the REAL clock past expires_at — no fake clock reaches Redis's
	// PEXPIREAT, so the RetainUntil retention must actually hold the hash.
	time.Sleep(1200 * time.Millisecond)

	renewal := CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	control.lease = renewal

	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""),
		"an expired-retained lease must be renewed synchronously by the gate's bootstrap, not strand the user in 403s")
	require.Equal(t, 2, control.ensureCalls)
	require.Len(t, control.lastEnsure.Drained, 1, "the renewal ensure must drain the finished lease, not wait out the grace sweep")
	entry := control.lastEnsure.Drained[0]
	require.Equal(t, granted.LeaseID, entry.LeaseID)
	require.Equal(t, "40000000", entry.GatewayConsumed.AmountUnits, "the drained entry carries the pre-seal consumed units")
}

// TestCheckBillingEligibilityBootstrapIsOncePerLease (spec §4 test 6): after
// the first bootstrap installs a covering lease, every subsequent
// eligibility check takes the pure-read fast path — the control plane sees
// exactly ONE ensure per lease lifecycle.
func TestCheckBillingEligibilityBootstrapIsOncePerLease(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	control := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	svc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, bridge)
	user := &User{ID: 32, PlatformUserID: platformUserID, BillingCurrency: "CNY"}

	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	require.Equal(t, 1, control.ensureCalls, "only the FIRST check of a cold user pays the ensure round trip — the rest are pure reads")
}

// bootstrapBarrierControl is a one-shot control-plane double for the
// concurrent test: both EnsureLease calls rendezvous at a barrier INSIDE the
// control plane, so neither caller can install its grant before the other
// has entered — the exact overlap ShipAny's per-user serialization (G4)
// resolves as one issued + one reused.
type bootstrapBarrierControl struct {
	mu       sync.Mutex
	lease    CanonicalWalletLease
	calls    int
	outcomes []string
	barrier  *sync.WaitGroup
}

func (c *bootstrapBarrierControl) EnsureLease(_ context.Context, req canonicalWalletEnsureRequest) (*canonicalWalletEnsureResult, error) {
	c.mu.Lock()
	c.calls++
	outcome := "reused"
	if c.calls == 1 {
		outcome = "issued"
	}
	c.outcomes = append(c.outcomes, outcome)
	c.mu.Unlock()
	c.barrier.Done()
	c.barrier.Wait() // both control calls overlap; neither install has happened yet
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = req
	return &canonicalWalletEnsureResult{Lease: c.lease, Outcome: outcome, ClampedBy: "none"}, nil
}
func (c *bootstrapBarrierControl) SubmitSettlement(context.Context, CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	return &CanonicalWalletSettlementResult{Accepted: true}, nil
}

// TestCheckBillingEligibilityBootstrapConcurrentSingleIssuance (spec §4
// test 7): two concurrent eligibility checks on the same cold user both
// admit, the control plane answers exactly one "issued" and one "reused" —
// G4 (per-user serialization → single live budget) holds at the gate.
func TestCheckBillingEligibilityBootstrapConcurrentSingleIssuance(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()

	barrier := &sync.WaitGroup{}
	barrier.Add(2)
	control := &bootstrapBarrierControl{
		lease: CanonicalWalletLease{
			LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
			BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
		},
		barrier: barrier,
	}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	svc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, bridge)
	user := &User{ID: 33, PlatformUserID: platformUserID, BillingCurrency: "CNY"}

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- svc.CheckBillingEligibility(ctx, user, nil, nil, nil, "") }()
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs, "both concurrent cold-start checks must admit")
	}
	control.mu.Lock()
	calls, outcomes := control.calls, append([]string(nil), control.outcomes...)
	control.mu.Unlock()
	require.Equal(t, 2, calls, "both goroutines entered the control plane before either could install")
	require.Len(t, outcomes, 2)
	require.ElementsMatch(t, []string{"issued", "reused"}, outcomes, "the serialized answers must be exactly one issued + one reused")
}

// TestCheckBillingEligibilityBootstrapTransportFailureFailsClosed (spec §4
// test 8): a bootstrap transport error is ErrBillingServiceUnavailable (the
// incident's terminal 403 becomes a retryable 503), each failed check trips
// the breaker EXACTLY once, and the opened breaker still refuses — the
// fund-safety assertions survive the semantic change.
func TestCheckBillingEligibilityBootstrapTransportFailureFailsClosed(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}

	transport := errors.New("canonical wallet control plane unreachable")
	control := &canonicalWalletControlStub{leaseErr: transport}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	// Threshold 2: after ONE failed check the breaker must still be closed
	// with failures==1 — a double OnFailure would already have opened it.
	cfg.Billing.CircuitBreaker = config.CircuitBreakerConfig{Enabled: true, FailureThreshold: 2, ResetTimeoutSeconds: 3600}
	svc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, bridge)
	user := &User{ID: 34, PlatformUserID: "shipany-user-transport", BillingCurrency: "CNY"}

	err := svc.CheckBillingEligibility(ctx, user, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "a bootstrap transport failure fails closed as a retryable 503")
	require.Equal(t, 1, control.ensureCalls)
	svc.circuitBreaker.mu.Lock()
	require.Equal(t, 1, svc.circuitBreaker.failures, "one failed check = exactly one OnFailure")
	require.Equal(t, billingCircuitClosed, svc.circuitBreaker.state)
	svc.circuitBreaker.mu.Unlock()

	// A second failure opens the breaker; a third check is then refused AT
	// Allow() — it must never reach the control plane again.
	require.ErrorIs(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""), ErrBillingServiceUnavailable)
	svc.circuitBreaker.mu.Lock()
	require.Equal(t, 2, svc.circuitBreaker.failures)
	require.Equal(t, billingCircuitOpen, svc.circuitBreaker.state)
	svc.circuitBreaker.mu.Unlock()
	require.ErrorIs(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""), ErrBillingServiceUnavailable, "the opened breaker still refuses — fund safety intact")
	require.Equal(t, 2, control.ensureCalls, "the breaker-blocked check must not have reached the control plane")
}

// TestCheckBillingEligibilityBootstrapShortfallIsTerminalNoBreaker (spec §4
// test 9): the control plane's insufficient_balance refusal is a TERMINAL
// ErrInsufficientBalance and never touches the breaker — a wave of broke
// users must not open the circuit for everyone.
func TestCheckBillingEligibilityBootstrapShortfallIsTerminalNoBreaker(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}

	control := &canonicalWalletControlStub{leaseErr: ErrCanonicalWalletBalanceShortfall}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil, 0, nil)
	t.Cleanup(bridge.Close)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	cfg.Billing.CircuitBreaker = config.CircuitBreakerConfig{Enabled: true, FailureThreshold: 1, ResetTimeoutSeconds: 3600}
	svc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, bridge)
	user := &User{ID: 35, PlatformUserID: "shipany-user-shortfall", BillingCurrency: "CNY"}

	err := svc.CheckBillingEligibility(ctx, user, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "a zero-balance wallet is still denied — the fund-safety core of the old fail-closed test")
	require.Equal(t, 1, control.ensureCalls)
	svc.circuitBreaker.mu.Lock()
	require.Equal(t, 0, svc.circuitBreaker.failures, "a shortfall is terminal, not an infrastructure failure — no breaker action")
	require.Equal(t, billingCircuitClosed, svc.circuitBreaker.state)
	svc.circuitBreaker.mu.Unlock()

	// And it stays terminal and breaker-free on the retry.
	require.ErrorIs(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""), ErrInsufficientBalance)
	require.Equal(t, 2, control.ensureCalls)
	svc.circuitBreaker.mu.Lock()
	require.Equal(t, 0, svc.circuitBreaker.failures)
	svc.circuitBreaker.mu.Unlock()
}

// fixedBalanceCache implements the BillingCache interface with a constant
// balance — used only to exercise the LEGACY float64 eligibility branch,
// which reads a plain cached balance and has no canonical-wallet semantics.
type fixedBalanceCache struct{ balance float64 }

func (c *fixedBalanceCache) GetUserBalance(context.Context, int64) (float64, error) {
	return c.balance, nil
}
func (c *fixedBalanceCache) SetUserBalance(context.Context, int64, float64) error { return nil }
func (c *fixedBalanceCache) DeductUserBalance(context.Context, int64, float64) error {
	return nil
}
func (c *fixedBalanceCache) InvalidateUserBalance(context.Context, int64) error {
	return nil
}
func (c *fixedBalanceCache) GetSubscriptionCache(context.Context, int64, int64) (*SubscriptionCacheData, error) {
	return nil, nil
}
func (c *fixedBalanceCache) SetSubscriptionCache(context.Context, int64, int64, *SubscriptionCacheData) error {
	return nil
}
func (c *fixedBalanceCache) UpdateSubscriptionUsage(context.Context, int64, int64, float64) error {
	return nil
}
func (c *fixedBalanceCache) InvalidateSubscriptionCache(context.Context, int64, int64) error {
	return nil
}
func (c *fixedBalanceCache) GetUserPlatformQuotaCache(context.Context, int64, string) (*UserPlatformQuotaCacheEntry, bool, error) {
	return nil, false, nil
}
func (c *fixedBalanceCache) SetUserPlatformQuotaCache(context.Context, int64, string, *UserPlatformQuotaCacheEntry, time.Duration) error {
	return nil
}
func (c *fixedBalanceCache) DeleteUserPlatformQuotaCache(context.Context, int64, string) error {
	return nil
}
func (c *fixedBalanceCache) IncrUserPlatformQuotaUsageCache(context.Context, int64, string, float64, time.Duration, bool) error {
	return nil
}
func (c *fixedBalanceCache) PopDirtyUserPlatformQuotaKeys(context.Context, int) ([]UserPlatformQuotaKey, error) {
	return nil, nil
}
func (c *fixedBalanceCache) ReaddDirtyUserPlatformQuotaKeys(context.Context, []UserPlatformQuotaKey) error {
	return nil
}
func (c *fixedBalanceCache) BatchGetUserPlatformQuotaCache(context.Context, []UserPlatformQuotaKey) ([]*UserPlatformQuotaCacheEntry, error) {
	return nil, nil
}
func (c *fixedBalanceCache) GetAPIKeyRateLimit(context.Context, int64) (*APIKeyRateLimitCacheData, error) {
	return nil, nil
}
func (c *fixedBalanceCache) SetAPIKeyRateLimit(context.Context, int64, *APIKeyRateLimitCacheData) error {
	return nil
}
func (c *fixedBalanceCache) UpdateAPIKeyRateLimitUsage(context.Context, int64, float64) error {
	return nil
}
func (c *fixedBalanceCache) InvalidateAPIKeyRateLimit(context.Context, int64) error {
	return nil
}

func TestProvideBillingCacheServiceDerivesCanonicalWalletBridge(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2 // keep the downstream currency step out of the way

	// A cache that ALSO implements CanonicalWalletLeaseStore yields a wired
	// bridge: enforce mode then consults the real Redis lease store.
	rdb := startCanonicalWalletTestRedis(t, ctx)
	leaseStore := &gatewayCacheAdapterForTest{rdb: rdb}
	dual := struct {
		*fixedBalanceCache
		*gatewayCacheAdapterForTest
	}{&fixedBalanceCache{balance: 5}, leaseStore}
	svc := ProvideBillingCacheService(dual, nil, nil, nil, nil, nil, cfg, nil, nil, nil)
	require.NotNil(t, svc)
	// Tasks 0–2 review MINOR-2: this was the only constructed bridge in the
	// suite without t.Cleanup(Close). Harmless today (db and outbox are nil,
	// so no dispatcher/reaper starts) but the 3.7c inventory demands it.
	if bridge := svc.canonicalWallet; bridge != nil {
		t.Cleanup(func() { bridge.Close() })
	}
	platformUserID := "shipany-user-" + uuid.NewString()
	require.NoError(t, leaseStore.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))
	require.NoError(t, svc.CheckBillingEligibility(ctx, &User{ID: 21, PlatformUserID: platformUserID, BillingCurrency: "CNY"}, nil, nil, nil, ""),
		"the derived bridge must actually gate eligibility through the real lease store")

	// A cache WITHOUT the lease-store interface leaves the canonical wallet
	// unwired: the legacy balance path decides instead. Phase 3.8-G Task 1
	// retargets this leg to disabled mode: in enforce the construction now
	// REFUSES (requireCanonicalWalletStore panics — the gate below), so the
	// legacy fallback is asserted where it is still reachable; the enforce
	// outcome itself is TestCanonicalWalletWiringGateAtProvideBillingCacheService.
	disabledCfg := &config.Config{RunMode: config.RunModeStandard}
	disabledCfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeDisabled)
	disabledCfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	plain := ProvideBillingCacheService(&fixedBalanceCache{balance: 0}, nil, nil, nil, nil, nil, disabledCfg, nil, nil, nil)
	err := plain.CheckBillingEligibility(ctx, &User{ID: 22, BillingCurrency: "CNY"}, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "without a derivable lease store, eligibility falls back to the legacy balance check")
}

// TestCanonicalWalletWiringGateAtProvideBillingCacheService (Phase 3.8-G
// Task 1, redesign §14.3 a): the third bridge site is proven AT the
// constructor — the :1041 call shape verbatim with a BillingCache stub that
// does NOT implement CanonicalWalletLeaseStore. Enforce panics (the gate
// would fail OPEN otherwise — 3.3a's hand-on); shadow constructs with the
// bridge unwired and counts canonicalWalletWiringMissing.
func TestCanonicalWalletWiringGateAtProvideBillingCacheService(t *testing.T) {
	nonStore := &fixedBalanceCache{balance: 0}

	enforceCfg := &config.Config{RunMode: config.RunModeStandard}
	enforceCfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		_ = ProvideBillingCacheService(nonStore, nil, nil, nil, nil, nil, enforceCfg, nil, nil, nil)
	}()
	require.NotNil(t, panicked, "enforce must refuse to start when the cache does not provide the wallet store")
	msg, ok := panicked.(string)
	require.True(t, ok, "the gate panics with a string message, got %T", panicked)
	require.Contains(t, msg, "ProvideBillingCacheService")
	require.Contains(t, msg, "does not provide the wallet store")

	shadowCfg := &config.Config{RunMode: config.RunModeStandard}
	shadowCfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	before := canonicalWalletWiringMissing.Load()
	shadow := ProvideBillingCacheService(nonStore, nil, nil, nil, nil, nil, shadowCfg, nil, nil, nil)
	require.NotNil(t, shadow)
	require.Nil(t, shadow.canonicalWallet, "shadow constructs with the bridge unwired")
	require.Equal(t, before+1, canonicalWalletWiringMissing.Load(), "shadow counts canonicalWalletWiringMissing")
}
