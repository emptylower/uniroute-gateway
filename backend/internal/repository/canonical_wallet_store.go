package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const canonicalWalletLeasePrefix = "canonical_wallet:lease:"
const canonicalWalletCurrentPrefix = "canonical_wallet:current:"

// redisLuaMaxSafeInt64 is 2^53-1. Redis embeds Lua 5.1, which has exactly
// one numeric type — an IEEE-754 double, the same representation as
// JavaScript's `number` — so any value this script passes through
// `tonumber(...)` (every HGET read in both scripts below) loses exact
// precision above this bound, identical to JS's Number.MAX_SAFE_INTEGER
// (Phase 0's `exceeds_number_max_safe_integer` fixture case exists
// specifically to catch this class of bug). Rather than hand-rolling
// string-based bigint arithmetic inside Lua (a much larger undertaking),
// values are checked against this bound in Go BEFORE either script ever
// runs — this is a real, disclosed limitation, the same kind already
// accepted elsewhere in this project (lease.ts's planAllocation().limit(1000),
// MySQL's CHECK-constraint version dependency): at ~90,071,992 CNY per
// value, it is far beyond any realistic single lease/reservation amount
// (the default lease budget is 5 CNY — see Task 5), so this guard is not
// expected to ever fire in real operation, only to fail loudly instead of
// silently corrupting a value in the pathological case that it would.
const redisLuaMaxSafeInt64 = 9007199254740991

func ensureRedisLuaSafeInt64(values ...int64) error {
	for _, v := range values {
		if v < 0 || v > redisLuaMaxSafeInt64 {
			return fmt.Errorf(
				"canonical wallet Redis operation requires every amount within Lua's exact integer range (0..%d); got %d",
				redisLuaMaxSafeInt64, v,
			)
		}
	}
	return nil
}

// installCanonicalWalletLeaseScript writes/updates ONE lease's own hash
// (KEYS[1]) and repoints the user's "current lease" pointer (KEYS[2]) to
// it — but ONLY if this lease is at least as new (by expiry) as whatever
// the pointer currently targets. It never touches any other lease's own
// hash — an old lease with an in-flight reservation keeps living under its
// own key until its own TTL.
//
// The pointer comparison before advancing stops a delayed or reordered
// lease-issuance response (e.g. two concurrent renewals racing, or a
// retried HTTP call whose original response arrives late) from making an
// OLDER lease become "current" after a genuinely newer one already has —
// silently regressing fresh requests onto a lease with less remaining
// headroom than they should see. ARGV[7] is the lease-key prefix
// (everything before the lease id) so the script can read the
// CURRENTLY-pointed-to lease's own expiry — safe to build and access this
// key inside the script without declaring it as a separate `KEYS[]` entry
// because it shares the same `{user_hash}` hash tag as KEYS[1]/KEYS[2],
// so it always lands in the same Redis Cluster slot. An exact-millisecond
// expiry tie is broken by lexicographic lease_id comparison — deterministic
// regardless of arrival order, since it depends only on the two leases'
// own IDs.
var installCanonicalWalletLeaseScript = redis.NewScript(`
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

// reserveCanonicalWalletLeaseScript reserves against the EXACT lease named
// by KEYS[1] — never "whatever the current pointer says" — so a retry of an
// in-flight reservation resolves correctly even after a newer lease has
// become current for fresh requests. The duplicate check requires the
// stored reservation marker to equal THIS call's own lease id (read fresh
// from KEYS[1]); a mismatch returns code 6 (genuine conflict) instead of a
// false "duplicate" carrying the wrong lease's snapshot.
var reserveCanonicalWalletLeaseScript = redis.NewScript(`
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
	local ttl = expires_at - tonumber(ARGV[4])
	redis.call('SET', KEYS[2], stored_lease_id, 'PX', ttl)
	return {0, stored_lease_id, currency, budget, updated, expires_at}
`)

func canonicalWalletLeaseKeyPrefix(platformUserID string) string {
	return canonicalWalletLeasePrefix + "{" + canonicalWalletUserHash(platformUserID) + "}:"
}

func canonicalWalletLeaseKey(platformUserID, leaseID string) string {
	return canonicalWalletLeaseKeyPrefix(platformUserID) + leaseID
}

func canonicalWalletCurrentKey(platformUserID string) string {
	return canonicalWalletCurrentPrefix + "{" + canonicalWalletUserHash(platformUserID) + "}"
}

func canonicalWalletReservationKey(platformUserID, eventID string) string {
	eventSum := sha256.Sum256([]byte(strings.TrimSpace(eventID)))
	return "canonical_wallet:reservation:{" + canonicalWalletUserHash(platformUserID) + "}:" + hex.EncodeToString(eventSum[:])
}

func canonicalWalletUserHash(platformUserID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(platformUserID)))
	return hex.EncodeToString(sum[:])
}

func (c *gatewayCache) InstallCanonicalWalletLease(ctx context.Context, lease service.CanonicalWalletLease) error {
	if c == nil || c.rdb == nil {
		return errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(lease.PlatformUserID) == "" || strings.TrimSpace(lease.LeaseID) == "" || lease.BudgetUnits <= 0 || lease.ExpiresAt.IsZero() {
		return errors.New("invalid canonical wallet lease")
	}
	if lease.ConsumedUnits < 0 || lease.ConsumedUnits > lease.BudgetUnits {
		return errors.New("invalid canonical wallet lease consumption")
	}
	if err := ensureRedisLuaSafeInt64(lease.BudgetUnits, lease.ConsumedUnits); err != nil {
		return err
	}
	currency, err := service.RequireCNYBillingCurrency(lease.Currency)
	if err != nil {
		return err
	}
	return installCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), canonicalWalletCurrentKey(lease.PlatformUserID)},
		lease.LeaseID, strings.TrimSpace(lease.PlatformUserID), currency,
		lease.ConsumedUnits, lease.ExpiresAt.UnixMilli(), lease.BudgetUnits, canonicalWalletLeaseKeyPrefix(lease.PlatformUserID),
	).Err()
}

func (c *gatewayCache) GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*service.CanonicalWalletLease, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	currentID, err := c.rdb.Get(ctx, canonicalWalletCurrentKey(platformUserID)).Result()
	if err == redis.Nil {
		return nil, service.ErrCanonicalWalletLeaseMissing
	}
	if err != nil {
		return nil, err
	}
	return c.GetCanonicalWalletLeaseByID(ctx, platformUserID, currentID)
}

func (c *gatewayCache) GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*service.CanonicalWalletLease, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	values, err := c.rdb.HGetAll(ctx, canonicalWalletLeaseKey(platformUserID, leaseID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, service.ErrCanonicalWalletLeaseMissing
	}
	return parseCanonicalWalletLease(values)
}

func (c *gatewayCache) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*service.CanonicalWalletReservation, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(eventID) == "" || amountUnits <= 0 || strings.TrimSpace(leaseID) == "" {
		return nil, errors.New("canonical wallet reservation requires a lease id, positive amount, and event id")
	}
	if err := ensureRedisLuaSafeInt64(amountUnits); err != nil {
		return nil, err
	}
	strictCurrency, err := service.RequireCNYBillingCurrency(currency)
	if err != nil {
		return nil, err
	}
	result, err := reserveCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(platformUserID, leaseID), canonicalWalletReservationKey(platformUserID, eventID)},
		strictCurrency, amountUnits, eventID, now.UnixMilli(),
	).Slice()
	if err != nil {
		return nil, err
	}
	return decodeCanonicalWalletReservationResult(result, platformUserID)
}

// decodeCanonicalWalletReservationResult turns the reservation Lua script's
// raw reply into a reservation or a typed error. It is deliberately a pure
// function over the raw []any and not a method: every defensive branch here
// guards against a reply shape the shipped script cannot actually produce, so
// the ONLY honest way to test them is to hand the decoder those shapes
// directly. Inlined into the Redis call, they were untestable without faking
// a driver — which is not a thing this repository does.
func decodeCanonicalWalletReservationResult(result []any, platformUserID string) (*service.CanonicalWalletReservation, error) {
	if len(result) == 0 {
		return nil, errors.New("canonical wallet reservation returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case 1:
		return nil, service.ErrCanonicalWalletLeaseMissing
	case 2:
		return nil, service.ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return nil, service.ErrCanonicalWalletLeaseExpired
	case 4:
		return nil, service.ErrCanonicalWalletLeaseExhausted
	case 6:
		return nil, service.ErrCanonicalWalletReservationConflict
	case 0, 5:
		if len(result) != 6 {
			return nil, errors.New("canonical wallet reservation returned an invalid snapshot")
		}
		budget, err := redisResultInt64(result[3])
		if err != nil {
			return nil, err
		}
		consumed, err := redisResultInt64(result[4])
		if err != nil {
			return nil, err
		}
		expiresAtMS, err := redisResultInt64(result[5])
		if err != nil {
			return nil, err
		}
		return &service.CanonicalWalletReservation{Lease: service.CanonicalWalletLease{
			LeaseID: fmt.Sprint(result[1]), PlatformUserID: strings.TrimSpace(platformUserID), Currency: fmt.Sprint(result[2]),
			BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
		}, Duplicate: code == 5}, nil
	default:
		return nil, fmt.Errorf("unknown canonical wallet reservation code %d", code)
	}
}

func parseCanonicalWalletLease(values map[string]string) (*service.CanonicalWalletLease, error) {
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
	return &service.CanonicalWalletLease{
		LeaseID: values["lease_id"], PlatformUserID: values["platform_user_id"], Currency: values["currency"],
		BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
	}, nil
}

func redisResultInt64(value any) (int64, error) {
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

var _ service.CanonicalWalletLeaseStore = (*gatewayCache)(nil)
