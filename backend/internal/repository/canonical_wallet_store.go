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

var installCanonicalWalletLeaseScript = redis.NewScript(`
	local incoming_consumed = tonumber(ARGV[5])
	local incoming_expires = tonumber(ARGV[6])
	if redis.call('EXISTS', KEYS[1]) == 1 then
		local current_id = redis.call('HGET', KEYS[1], 'lease_id')
		local current_consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_micros') or '0')
		local current_expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
		if current_id == ARGV[1] and current_consumed > incoming_consumed then
			incoming_consumed = current_consumed
		elseif current_id ~= ARGV[1] and current_expires > incoming_expires then
			return 0
		end
	end
	redis.call('HSET', KEYS[1],
		'lease_id', ARGV[1], 'platform_user_id', ARGV[2], 'currency', ARGV[3],
		'budget_micros', ARGV[4], 'consumed_micros', incoming_consumed, 'expires_at_ms', ARGV[6])
	redis.call('PEXPIREAT', KEYS[1], ARGV[6])
	return 1
`)

var reserveCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local currency = redis.call('HGET', KEYS[1], 'currency')
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_micros') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_micros') or '0')
	local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
	if currency ~= ARGV[1] then return {2} end
	if expires_at <= tonumber(ARGV[4]) then return {3} end
	local reservation_lease_id = redis.call('GET', KEYS[2])
	if reservation_lease_id ~= false then
		if reservation_lease_id ~= lease_id then return {6} end
		return {5, lease_id, currency, budget, consumed, expires_at}
	end
	local amount = tonumber(ARGV[2])
	if amount <= 0 or consumed + amount > budget then return {4} end
	local updated = consumed + amount
	redis.call('HSET', KEYS[1], 'consumed_micros', updated)
	local ttl = expires_at - tonumber(ARGV[4])
	redis.call('SET', KEYS[2], lease_id, 'PX', ttl)
	return {0, lease_id, currency, budget, updated, expires_at}
`)

func canonicalWalletLeaseKey(platformUserID string) string {
	return canonicalWalletLeasePrefix + "{" + canonicalWalletUserHash(platformUserID) + "}"
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
	if strings.TrimSpace(lease.PlatformUserID) == "" || strings.TrimSpace(lease.LeaseID) == "" || lease.BudgetMicros <= 0 || lease.ExpiresAt.IsZero() {
		return errors.New("invalid canonical wallet lease")
	}
	if lease.ConsumedMicros < 0 || lease.ConsumedMicros > lease.BudgetMicros {
		return errors.New("invalid canonical wallet lease consumption")
	}
	return installCanonicalWalletLeaseScript.Run(ctx, c.rdb, []string{canonicalWalletLeaseKey(lease.PlatformUserID)},
		lease.LeaseID, strings.TrimSpace(lease.PlatformUserID), service.NormalizeUserBillingCurrency(lease.Currency),
		lease.BudgetMicros, lease.ConsumedMicros, lease.ExpiresAt.UnixMilli(),
	).Err()
}

func (c *gatewayCache) GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*service.CanonicalWalletLease, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	values, err := c.rdb.HGetAll(ctx, canonicalWalletLeaseKey(platformUserID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, service.ErrCanonicalWalletLeaseMissing
	}
	return parseCanonicalWalletLease(values)
}

func (c *gatewayCache) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, currency, eventID string, amountMicros int64, now time.Time) (*service.CanonicalWalletReservation, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(eventID) == "" || amountMicros <= 0 {
		return nil, errors.New("canonical wallet reservation requires a positive amount and event id")
	}
	result, err := reserveCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(platformUserID), canonicalWalletReservationKey(platformUserID, eventID)},
		service.NormalizeUserBillingCurrency(currency), amountMicros, eventID, now.UnixMilli(),
	).Slice()
	if err != nil {
		return nil, err
	}
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
			BudgetMicros: budget, ConsumedMicros: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
		}, Duplicate: code == 5}, nil
	default:
		return nil, fmt.Errorf("unknown canonical wallet reservation code %d", code)
	}
}

func parseCanonicalWalletLease(values map[string]string) (*service.CanonicalWalletLease, error) {
	budget, err := strconv.ParseInt(values["budget_micros"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet budget: %w", err)
	}
	consumed, err := strconv.ParseInt(values["consumed_micros"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet consumption: %w", err)
	}
	expiresAtMS, err := strconv.ParseInt(values["expires_at_ms"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet expiry: %w", err)
	}
	return &service.CanonicalWalletLease{
		LeaseID: values["lease_id"], PlatformUserID: values["platform_user_id"], Currency: values["currency"],
		BudgetMicros: budget, ConsumedMicros: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
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
