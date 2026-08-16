package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const stickySessionPrefix = "sticky_session:"
const liveCallPrefix = "live:call:"
const liveFinalizationQueueKey = "live:pending_finalization"

type gatewayCache struct {
	rdb *redis.Client
}

func NewGatewayCache(rdb *redis.Client) service.GatewayCache {
	return &gatewayCache{rdb: rdb}
}

// buildSessionKey 构建 session key，包含 groupID 实现分组隔离
// 格式: sticky_session:{groupID}:{sessionHash}
func buildSessionKey(groupID int64, sessionHash string) string {
	return fmt.Sprintf("%s%d:%s", stickySessionPrefix, groupID, sessionHash)
}

func (c *gatewayCache) GetSessionAccountID(ctx context.Context, groupID int64, sessionHash string) (int64, error) {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Get(ctx, key).Int64()
}

func (c *gatewayCache) SetSessionAccountID(ctx context.Context, groupID int64, sessionHash string, accountID int64, ttl time.Duration) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Set(ctx, key, accountID, ttl).Err()
}

func (c *gatewayCache) RefreshSessionTTL(ctx context.Context, groupID int64, sessionHash string, ttl time.Duration) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Expire(ctx, key, ttl).Err()
}

// DeleteSessionAccountID 删除粘性会话与账号的绑定关系。
// 当检测到绑定的账号不可用（如状态错误、禁用、不可调度等）时调用，
// 以便下次请求能够重新选择可用账号。
//
// DeleteSessionAccountID removes the sticky session binding for the given session.
// Called when the bound account becomes unavailable (e.g., error status, disabled,
// or unschedulable), allowing subsequent requests to select a new available account.
func (c *gatewayCache) DeleteSessionAccountID(ctx context.Context, groupID int64, sessionHash string) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Del(ctx, key).Err()
}

// Compile-time assertion: gatewayCache must implement CyberSessionBlockStore.
var _ service.CyberSessionBlockStore = (*gatewayCache)(nil)
var _ service.LiveCallStore = (*gatewayCache)(nil)

const cyberSessionBlockPrefix = "cyber_session_block:"

// SetCyberSessionBlocked 把被 cyber_policy 命中的会话写入屏蔽表（TTL 自动过期）。
// 存储值 "1" 作为存在标记（IsCyberSessionBlocked 只检查 key 是否存在，不读值）。
func (c *gatewayCache) SetCyberSessionBlocked(ctx context.Context, key string, ttl time.Duration) error {
	return c.rdb.Set(ctx, cyberSessionBlockPrefix+key, "1", ttl).Err()
}

// IsCyberSessionBlocked 查询会话是否在屏蔽表中。
func (c *gatewayCache) IsCyberSessionBlocked(ctx context.Context, key string) (bool, error) {
	n, err := c.rdb.Exists(ctx, cyberSessionBlockPrefix+key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

var claimLiveControllerScript = redis.NewScript(`
	local key = KEYS[1]
	local target = ARGV[1]
	local owner = ARGV[2]
	local current = redis.call('HGET', key, 'controller')
	if current == false or current == 'closed' then
		return 0
	end
	if target == 'observer' and current ~= 'pending' then
		return 0
	end
	if target == 'proxy' and current ~= 'pending' and current ~= 'observer' and
		(current ~= 'proxy' or redis.call('HGET', key, 'controller_owner') ~= owner) then
		return 0
	end
	redis.call('HSET', key, 'controller', target, 'controller_owner', owner)
	return 1
`)

var markLiveCallClosedScript = redis.NewScript(`
	local key = KEYS[1]
	if redis.call('EXISTS', key) == 0 then
		return 0
	end
	if redis.call('HGET', key, 'controller') == 'closed' then
		return 0
	end
	redis.call('HSET', key, 'controller', 'closed', 'controller_owner', '')
	redis.call('EXPIRE', key, ARGV[1])
	return 1
`)

var releaseLiveControllerScript = redis.NewScript(`
	local key = KEYS[1]
	if redis.call('HGET', key, 'controller') ~= 'proxy' or
		redis.call('HGET', key, 'controller_owner') ~= ARGV[1] then
		return 0
	end
	redis.call('HSET', key, 'controller', 'pending', 'controller_owner', '')
	return 1
`)

var accumulateLiveUsageScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then
		return -1
	end
	if redis.call('HSETNX', KEYS[1], ARGV[1], '1') == 0 then
		return 0
	end
	redis.call('HINCRBY', KEYS[1], 'input_tokens', ARGV[2])
	redis.call('HINCRBY', KEYS[1], 'output_tokens', ARGV[3])
	redis.call('HINCRBY', KEYS[1], 'cache_read_tokens', ARGV[4])
	return 1
`)

func liveCallKey(callHash string) string {
	return liveCallPrefix + callHash
}

func HashLiveCallID(callID string) string {
	sum := sha256.Sum256([]byte(callID))
	return hex.EncodeToString(sum[:])
}

func (c *gatewayCache) SaveLiveCall(ctx context.Context, record *service.LiveCallRecord, ttl time.Duration) error {
	if record == nil || record.CallHash == "" || record.CallID == "" {
		return fmt.Errorf("invalid live call record")
	}
	values := map[string]any{
		"call_id":                    record.CallID,
		"account_id":                 record.AccountID,
		"api_key_id":                 record.APIKeyID,
		"user_id":                    record.UserID,
		"group_id":                   record.GroupID,
		"subscription_id":            record.SubscriptionID,
		"lease_id":                   record.LeaseID,
		"model":                      record.Model,
		"input_tokens":               record.InputTokens,
		"output_tokens":              record.OutputTokens,
		"cache_read_tokens":          record.CacheReadTokens,
		"billing_currency":           record.BillingCurrency,
		"rate_multiplier":            record.RateMultiplier,
		"group_rate_multiplier":      record.GroupRateMultiplier,
		"account_rate_multiplier":    record.AccountRateMultiplier,
		"exchange_rate":              record.ExchangeRate,
		"exchange_rate_source":       record.ExchangeRateSource,
		"exchange_rate_as_of":        record.ExchangeRateAsOf.UnixMilli(),
		"api_key_quota":              record.APIKeyQuota,
		"rate_limit_5h":              record.RateLimit5h,
		"rate_limit_1d":              record.RateLimit1d,
		"rate_limit_7d":              record.RateLimit7d,
		"subscription_billing":       record.SubscriptionBilling,
		"input_price_per_token":      record.InputPricePerToken,
		"output_price_per_token":     record.OutputPricePerToken,
		"cache_read_price_per_token": record.CacheReadPricePerToken,
		"created_at":                 record.CreatedAt.UnixMilli(),
		"expires_at":                 record.ExpiresAt.UnixMilli(),
		"controller":                 record.Controller,
		"controller_owner":           record.ControllerOwner,
		"user_agent":                 record.UserAgent,
		"ip_address":                 record.IPAddress,
		"inbound_endpoint":           record.InboundEndpoint,
		"attestation":                record.AttestationCiphertext,
	}
	key := liveCallKey(record.CallHash)
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, key, values)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *gatewayCache) GetLiveCall(ctx context.Context, callHash string) (*service.LiveCallRecord, error) {
	values, err := c.rdb.HGetAll(ctx, liveCallKey(callHash)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, service.ErrLiveCallNotFound
	}
	parseInt := func(field string) int64 {
		value, _ := strconv.ParseInt(values[field], 10, 64)
		return value
	}
	parseFloat := func(field string) float64 {
		value, _ := strconv.ParseFloat(values[field], 64)
		return value
	}
	createdAt := time.UnixMilli(parseInt("created_at"))
	expiresAt := time.UnixMilli(parseInt("expires_at"))
	return &service.LiveCallRecord{
		CallID:                 values["call_id"],
		CallHash:               callHash,
		AccountID:              parseInt("account_id"),
		APIKeyID:               parseInt("api_key_id"),
		UserID:                 parseInt("user_id"),
		GroupID:                parseInt("group_id"),
		SubscriptionID:         parseInt("subscription_id"),
		LeaseID:                values["lease_id"],
		Model:                  values["model"],
		InputTokens:            int(parseInt("input_tokens")),
		OutputTokens:           int(parseInt("output_tokens")),
		CacheReadTokens:        int(parseInt("cache_read_tokens")),
		BillingCurrency:        values["billing_currency"],
		RateMultiplier:         parseFloat("rate_multiplier"),
		GroupRateMultiplier:    parseFloat("group_rate_multiplier"),
		AccountRateMultiplier:  parseFloat("account_rate_multiplier"),
		ExchangeRate:           parseFloat("exchange_rate"),
		ExchangeRateSource:     values["exchange_rate_source"],
		ExchangeRateAsOf:       time.UnixMilli(parseInt("exchange_rate_as_of")),
		APIKeyQuota:            parseFloat("api_key_quota"),
		RateLimit5h:            parseFloat("rate_limit_5h"),
		RateLimit1d:            parseFloat("rate_limit_1d"),
		RateLimit7d:            parseFloat("rate_limit_7d"),
		SubscriptionBilling:    values["subscription_billing"] == "1" || values["subscription_billing"] == "true",
		InputPricePerToken:     parseFloat("input_price_per_token"),
		OutputPricePerToken:    parseFloat("output_price_per_token"),
		CacheReadPricePerToken: parseFloat("cache_read_price_per_token"),
		CreatedAt:              createdAt,
		ExpiresAt:              expiresAt,
		Controller:             values["controller"],
		ControllerOwner:        values["controller_owner"],
		UserAgent:              values["user_agent"],
		IPAddress:              values["ip_address"],
		InboundEndpoint:        values["inbound_endpoint"],
		AttestationCiphertext:  values["attestation"],
	}, nil
}

func (c *gatewayCache) AccumulateLiveUsage(ctx context.Context, callHash, responseKey string, inputTokens, outputTokens, cacheReadTokens int) (bool, error) {
	result, err := accumulateLiveUsageScript.Run(
		ctx,
		c.rdb,
		[]string{liveCallKey(callHash)},
		"usage_seen:"+responseKey,
		inputTokens,
		outputTokens,
		cacheReadTokens,
	).Int()
	if err != nil {
		return false, err
	}
	if result < 0 {
		return false, service.ErrLiveCallNotFound
	}
	return result == 1, nil
}

func (c *gatewayCache) ClaimLiveController(ctx context.Context, callHash, controller, owner string) (bool, error) {
	result, err := claimLiveControllerScript.Run(ctx, c.rdb, []string{liveCallKey(callHash)}, controller, owner).Int()
	return result == 1, err
}

func (c *gatewayCache) GetLiveController(ctx context.Context, callHash string) (string, error) {
	value, err := c.rdb.HGet(ctx, liveCallKey(callHash), "controller").Result()
	if err == redis.Nil {
		return "", service.ErrLiveCallNotFound
	}
	return value, err
}

func (c *gatewayCache) ReleaseLiveController(ctx context.Context, callHash, owner string) (bool, error) {
	result, err := releaseLiveControllerScript.Run(ctx, c.rdb, []string{liveCallKey(callHash)}, owner).Int()
	return result == 1, err
}

func (c *gatewayCache) MarkLiveCallClosed(ctx context.Context, callHash string, ttl time.Duration) (bool, error) {
	result, err := markLiveCallClosedScript.Run(ctx, c.rdb, []string{liveCallKey(callHash)}, int64(ttl.Seconds())).Int()
	return result == 1, err
}

func (c *gatewayCache) QueueLiveFinalization(ctx context.Context, callHash string) error {
	_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(ctx, liveFinalizationQueueKey, redis.Z{Score: float64(time.Now().Unix()), Member: callHash})
		// Pending settlement is financial state. Keep the source record until
		// billing succeeds; MarkLiveCallClosed restores the normal closed TTL.
		pipe.Persist(ctx, liveCallKey(callHash))
		return nil
	})
	return err
}

func (c *gatewayCache) RemoveLiveFinalization(ctx context.Context, callHash string) error {
	return c.rdb.ZRem(ctx, liveFinalizationQueueKey, callHash).Err()
}

func (c *gatewayCache) ListLiveFinalizations(ctx context.Context, limit int64) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}
	return c.rdb.ZRange(ctx, liveFinalizationQueueKey, 0, limit-1).Result()
}
