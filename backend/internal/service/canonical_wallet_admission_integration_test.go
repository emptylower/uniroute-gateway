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
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// startCanonicalWalletTestRedis follows the per-test container pattern
// already used by internal/server/routes/auth_rate_limit_integration_test.go
// (this package does not have a package-level TestMain harness).
func startCanonicalWalletTestRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	container, err := tcredis.Run(ctx, "redis:8.4-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", host, port.Int())})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(ctx).Err())
	return client
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
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil) // outboxDB/outbox nil — this test doesn't call ObserveSettlement

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
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil) // outboxDB/outbox nil — this test doesn't call ObserveSettlement

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

func TestCanonicalWalletMidStreamOverrunToppedUpAtomicallyBeforeExceeding(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()
	leaseID := "lease-" + uuid.NewString()

	control := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: leaseID, PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, control, nil, nil) // outboxDB/outbox nil — this test doesn't call ObserveSettlement

	initial := CanonicalWalletSettlementEvent{GatewayRequestID: "req-stream-1", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 40_000000}
	allowed, err := bridge.CheckAndReserve(ctx, initial)
	require.NoError(t, err)
	require.True(t, allowed)

	topUpOK, err := bridge.EnsureCanonicalWalletHeadroom(ctx, initial.GatewayRequestID, platformUserID, leaseID, "CNY", 1, 30_000000)
	require.NoError(t, err)
	require.True(t, topUpOK, "40+30=70 of a 100 budget must succeed")

	tooMuch, err := bridge.EnsureCanonicalWalletHeadroom(ctx, initial.GatewayRequestID, platformUserID, leaseID, "CNY", 2, 40_000000)
	require.NoError(t, err)
	require.False(t, tooMuch, "40+30+40=110 exceeds the 100 budget — generation must stop, not overspend")

	// A genuine retry of the SAME numbered top-up (network retry of the
	// exact same call) must be recognized as a duplicate, not attempted
	// twice.
	retrySame, err := bridge.EnsureCanonicalWalletHeadroom(ctx, initial.GatewayRequestID, platformUserID, leaseID, "CNY", 1, 30_000000)
	require.NoError(t, err)
	require.True(t, retrySame, "retrying the same numbered top-up must still succeed (duplicate), not fail")
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
	newSvc := func(t *testing.T, lease CanonicalWalletLease) *BillingCacheService {
		t.Helper()
		require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))
		bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, &canonicalWalletControlStub{}, nil, nil) // control/outboxDB/outbox unused by HasCanonicalWalletHeadroom
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

	deniedSvc := newSvc(t, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 1, ConsumedUnits: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), // fully consumed: RemainingUnits() == 0
	})
	err := deniedSvc.CheckBillingEligibility(ctx, user, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "an exhausted canonical wallet lease must deny the request through the real CheckBillingEligibility entry point, not just the bridge directly")

	allowedUser := &User{ID: 2, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}
	allowedSvc := newSvc(t, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: allowedUser.PlatformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 0, ExpiresAt: time.Now().UTC().Add(time.Minute), // real headroom
	})
	require.NoError(t, allowedSvc.CheckBillingEligibility(ctx, allowedUser, nil, nil, nil, ""), "a lease with real headroom must admit the request through the real CheckBillingEligibility entry point")
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
	if redis.call('EXISTS', KEYS[1]) == 1 then
		local current_consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
		if current_consumed > incoming_consumed then
			incoming_consumed = current_consumed
		end
	end
	redis.call('HSET', KEYS[1],
		'lease_id', ARGV[1], 'platform_user_id', ARGV[2], 'currency', ARGV[3],
		'budget_units', ARGV[6], 'consumed_units', incoming_consumed, 'expires_at_ms', ARGV[5])
	redis.call('PEXPIREAT', KEYS[1], incoming_expires)

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
		redis.call('PEXPIREAT', KEYS[2], incoming_expires)
	end
	return 1
`)

var testReserveCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local stored_lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local currency = redis.call('HGET', KEYS[1], 'currency')
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
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
	if amount <= 0 or consumed + amount > budget then return {4} end
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
	return testInstallCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{testCanonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), testCanonicalWalletCurrentKey(lease.PlatformUserID)},
		lease.LeaseID, strings.TrimSpace(lease.PlatformUserID), currency,
		lease.ConsumedUnits, lease.ExpiresAt.UnixMilli(), lease.BudgetUnits, testCanonicalWalletLeaseKeyPrefix(lease.PlatformUserID),
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

var _ CanonicalWalletLeaseStore = (*gatewayCacheAdapterForTest)(nil)

// TestCanonicalWalletShadowModeAllowsEverythingAndReservesNothing proves the
// disclosed Phase-2 contract for shadow mode across ALL THREE entry points:
// every request is allowed regardless of lease state, and nothing is ever
// reserved (the Redis keyspace stays empty of reservation markers).
func TestCanonicalWalletShadowModeAllowsEverythingAndReservesNothing(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	platformUserID := "shipany-user-" + uuid.NewString()
	leaseID := "lease-" + uuid.NewString()

	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), store, &canonicalWalletControlStub{}, nil, nil)

	allowed, err := bridge.CheckAndReserve(ctx, CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-shadow", PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 999_000000,
	})
	require.NoError(t, err)
	require.True(t, allowed, "shadow mode observes but always allows")

	topUpOK, err := bridge.EnsureCanonicalWalletHeadroom(ctx, "req-shadow", platformUserID, leaseID, "CNY", 1, 500_000000)
	require.NoError(t, err)
	require.True(t, topUpOK, "shadow mode never blocks a mid-stream top-up")

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

	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, &canonicalWalletControlStub{}, nil, nil)

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

	newBridge := func() *CanonicalWalletBridge {
		return newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, &canonicalWalletControlStub{}, nil, nil)
	}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2

	// Blank platform identity under enforce mode fails closed.
	blankIDUser := &User{ID: 11, PlatformUserID: "   ", BillingCurrency: "CNY"}
	blankIDSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, newBridge())
	err := blankIDSvc.CheckBillingEligibility(ctx, blankIDUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "a user without a canonical identity cannot be checked — fail closed")

	// A non-CNY billing currency is rejected outright, never coerced to CNY.
	usdUser := &User{ID: 12, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "USD"}
	usdSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, newBridge())
	err = usdSvc.CheckBillingEligibility(ctx, usdUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "cny-e8-v1 is CNY-only — reject instead of admitting under the wrong wallet")

	// A user with NO lease data: genuine store error -> infrastructure
	// classification (not the ordinary insufficient-balance denial).
	noLeaseSvc := NewBillingCacheService(&billingCacheWorkerStub{}, nil, nil, nil, nil, nil, cfg, nil, newBridge())
	noLeaseUser := &User{ID: 13, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}
	err = noLeaseSvc.CheckBillingEligibility(ctx, noLeaseUser, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrBillingServiceUnavailable, "a missing lease fails closed as unavailable, not as insufficient balance")

	// The LEGACY float64 path still decides when no canonical wallet is
	// wired at all: an empty cache balance denies, a funded one admits.
	// (fixedBalanceCache is DI plumbing only — the reserve path above runs
	// on the real Redis store.)
	legacyDeny := NewBillingCacheService(&fixedBalanceCache{balance: 0}, nil, nil, nil, nil, nil, cfg, nil, nil)
	err = legacyDeny.CheckBillingEligibility(ctx, &User{ID: 14, BillingCurrency: "CNY"}, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "the legacy path still denies a zero balance")

	legacyAllow := NewBillingCacheService(&fixedBalanceCache{balance: 100}, nil, nil, nil, nil, nil, cfg, nil, nil)
	require.NoError(t, legacyAllow.CheckBillingEligibility(ctx, &User{ID: 15, BillingCurrency: "CNY"}, nil, nil, nil, ""))
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
	platformUserID := "shipany-user-" + uuid.NewString()
	require.NoError(t, leaseStore.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))
	require.NoError(t, svc.CheckBillingEligibility(ctx, &User{ID: 21, PlatformUserID: platformUserID, BillingCurrency: "CNY"}, nil, nil, nil, ""),
		"the derived bridge must actually gate eligibility through the real lease store")

	// A cache WITHOUT the lease-store interface leaves the canonical wallet
	// unwired: the legacy balance path decides instead.
	plain := ProvideBillingCacheService(&fixedBalanceCache{balance: 0}, nil, nil, nil, nil, nil, cfg, nil, nil, nil)
	err := plain.CheckBillingEligibility(ctx, &User{ID: 22, BillingCurrency: "CNY"}, nil, nil, nil, "")
	require.ErrorIs(t, err, ErrInsufficientBalance, "without a derivable lease store, eligibility falls back to the legacy balance check")
}
