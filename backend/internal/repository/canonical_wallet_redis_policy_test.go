package repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

type fakeRedisCmdable struct {
	redis.Cmdable
	configGetFn func(ctx context.Context, parameter string) *redis.MapStringStringCmd
}

func (f *fakeRedisCmdable) ConfigGet(ctx context.Context, parameter string) *redis.MapStringStringCmd {
	if f.configGetFn != nil {
		return f.configGetFn(ctx, parameter)
	}
	return redis.NewMapStringStringResult(nil, errors.New("unimplemented"))
}

func TestCheckCanonicalWalletRedisPolicy_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("Noeviction_AnyMaxmemory_Passes", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				if parameter == "maxmemory-policy" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "noeviction"}, nil)
				}
				if parameter == "maxmemory" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory": "104857600"}, nil)
				}
				return redis.NewMapStringStringResult(nil, fmt.Errorf("unexpected arg: %s", parameter))
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err)
	})

	t.Run("AllkeysLRU_Maxmemory0_Passes_Inert", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				if parameter == "maxmemory-policy" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "allkeys-lru"}, nil)
				}
				if parameter == "maxmemory" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory": "0"}, nil)
				}
				return redis.NewMapStringStringResult(nil, fmt.Errorf("unexpected arg: %s", parameter))
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}
		beforeInert := CanonicalWalletRedisPolicyMetricsSnapshot().InertNoMaxmemory
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err)
		require.Equal(t, beforeInert+1, CanonicalWalletRedisPolicyMetricsSnapshot().InertNoMaxmemory)
	})

	t.Run("AllkeysLRU_Maxmemory100mb_Enforce_Refuses", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				if parameter == "maxmemory-policy" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "allkeys-lru"}, nil)
				}
				if parameter == "maxmemory" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory": "104857600"}, nil)
				}
				return redis.NewMapStringStringResult(nil, fmt.Errorf("unexpected arg: %s", parameter))
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRedisPolicyNotNoeviction)
		require.Contains(t, err.Error(), "allkeys-lru")
		require.Contains(t, err.Error(), "104857600")
		require.Contains(t, err.Error(), "canonical_wallet.mode=enforce but the wallet Redis maxmemory-policy is \"allkeys-lru\" with maxmemory=104857600 — held money could be evicted; set canonical_wallet.redis_policy_check=warn to override")
	})

	t.Run("AllkeysLRU_Maxmemory100mb_Warn_Passes", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				if parameter == "maxmemory-policy" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "allkeys-lru"}, nil)
				}
				if parameter == "maxmemory" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory": "104857600"}, nil)
				}
				return redis.NewMapStringStringResult(nil, fmt.Errorf("unexpected arg: %s", parameter))
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "warn",
			},
		}
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err)
	})

	t.Run("AllkeysLRU_Maxmemory100mb_ShadowOrDisabled_Passes", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				if parameter == "maxmemory-policy" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "allkeys-lru"}, nil)
				}
				if parameter == "maxmemory" {
					return redis.NewMapStringStringResult(map[string]string{"maxmemory": "104857600"}, nil)
				}
				return redis.NewMapStringStringResult(nil, fmt.Errorf("unexpected arg: %s", parameter))
			},
		}
		for _, mode := range []string{config.CanonicalWalletModeShadow, config.CanonicalWalletModeDisabled} {
			cfg := &config.Config{
				CanonicalWallet: config.CanonicalWalletConfig{
					Mode:             mode,
					RedisPolicyCheck: "enforce",
				},
			}
			err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
			require.NoError(t, err, "mode %s must not refuse", mode)
		}
	})

	t.Run("SettingOff_ConfigNeverCalled", func(t *testing.T) {
		called := false
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				called = true
				return redis.NewMapStringStringResult(map[string]string{"maxmemory-policy": "allkeys-lru"}, nil)
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "off",
			},
		}
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err)
		require.False(t, called, "CONFIG must never be called when setting=off")
	})

	t.Run("CommandError_ConfigRefused_AlertsAndContinues", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				return redis.NewMapStringStringResult(nil, errors.New("ERR unknown command 'CONFIG'"))
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}
		before := CanonicalWalletRedisPolicyMetricsSnapshot().UnverifiedConfigRefused
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err, "command error must alert and continue in every mode")
		require.Equal(t, before+1, CanonicalWalletRedisPolicyMetricsSnapshot().UnverifiedConfigRefused)
	})

	t.Run("TransportError_RedisUnreachable_LogsAndContinues_NotChecked", func(t *testing.T) {
		fake := &fakeRedisCmdable{
			configGetFn: func(ctx context.Context, parameter string) *redis.MapStringStringCmd {
				return redis.NewMapStringStringResult(nil, &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: errors.New("connect: connection refused"),
				})
			},
		}
		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}
		beforeChecked := CanonicalWalletRedisPolicyMetricsSnapshot().Checked
		beforeUnreachable := CanonicalWalletRedisPolicyMetricsSnapshot().UncheckedUnreachable
		err := CheckCanonicalWalletRedisPolicy(ctx, fake, cfg)
		require.NoError(t, err, "transport error must log and continue")
		require.Equal(t, beforeChecked, CanonicalWalletRedisPolicyMetricsSnapshot().Checked, "unreachable must NOT record pin as checked")
		require.Equal(t, beforeUnreachable+1, CanonicalWalletRedisPolicyMetricsSnapshot().UncheckedUnreachable)
	})

	t.Run("ProvideRedis_CallsCheckExactlyOnce", func(t *testing.T) {
		cfg := &config.Config{
			Redis: config.RedisConfig{
				Host: "127.0.0.1",
				Port: 1, // closed port -> transport error, will not panic
			},
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeDisabled,
				RedisPolicyCheck: "off",
			},
		}
		beforeCalls := CanonicalWalletRedisPolicyMetricsSnapshot().Calls
		client := ProvideRedis(cfg)
		require.NotNil(t, client)
		_ = client.Close()
		// If RedisPolicyCheck is off, CheckCanonicalWalletRedisPolicy returns before calling CONFIG
		// When we enable it:
		cfg.CanonicalWallet.RedisPolicyCheck = "enforce"
		client2 := ProvideRedis(cfg)
		require.NotNil(t, client2)
		_ = client2.Close()
		require.Equal(t, beforeCalls+1, CanonicalWalletRedisPolicyMetricsSnapshot().Calls)
	})
}

func TestCheckCanonicalWalletRedisPolicy_Integration(t *testing.T) {
	if testing.Short() || os.Getenv("CI_SKIP_INTEGRATION") != "" {
		t.Skip("skipping testcontainers integration test in short mode")
	}

	ctx := context.Background()

	t.Run("EvictingWithBound_RefusesUnderEnforce", func(t *testing.T) {
		node, err := tcredis.Run(ctx, "redis:8.4-alpine",
			testcontainers.WithCmd("redis-server", "--maxmemory", "64mb", "--maxmemory-policy", "allkeys-lru"),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = node.Terminate(ctx) })

		connStr, err := node.ConnectionString(ctx)
		require.NoError(t, err)
		opt, err := redis.ParseURL(connStr)
		require.NoError(t, err)
		rdb := redis.NewClient(opt)
		t.Cleanup(func() { _ = rdb.Close() })

		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}

		err = CheckCanonicalWalletRedisPolicy(ctx, rdb, cfg)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRedisPolicyNotNoeviction)
		require.Contains(t, err.Error(), "allkeys-lru")
		require.Contains(t, err.Error(), "67108864") // 64MB in bytes as returned by redis
	})

	t.Run("Noeviction_StartsUnderEnforce", func(t *testing.T) {
		node, err := tcredis.Run(ctx, "redis:8.4-alpine",
			testcontainers.WithCmd("redis-server", "--maxmemory", "64mb", "--maxmemory-policy", "noeviction"),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = node.Terminate(ctx) })

		connStr, err := node.ConnectionString(ctx)
		require.NoError(t, err)
		opt, err := redis.ParseURL(connStr)
		require.NoError(t, err)
		rdb := redis.NewClient(opt)
		t.Cleanup(func() { _ = rdb.Close() })

		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}

		err = CheckCanonicalWalletRedisPolicy(ctx, rdb, cfg)
		require.NoError(t, err)
	})

	t.Run("ConfigDisabled_ManagedRedis_AlertsAndStarts", func(t *testing.T) {
		node, err := tcredis.Run(ctx, "redis:8.4-alpine")
		require.NoError(t, err)
		t.Cleanup(func() { _ = node.Terminate(ctx) })

		connStr, err := node.ConnectionString(ctx)
		require.NoError(t, err)
		opt, err := redis.ParseURL(connStr)
		require.NoError(t, err)
		rdb := redis.NewClient(opt)
		t.Cleanup(func() { _ = rdb.Close() })

		// Simulate managed Redis permissions (CONFIG command disabled / forbidden via ACL)
		require.NoError(t, rdb.Do(ctx, "ACL", "SETUSER", "default", "-config").Err())

		cfg := &config.Config{
			CanonicalWallet: config.CanonicalWalletConfig{
				Mode:             config.CanonicalWalletModeEnforce,
				RedisPolicyCheck: "enforce",
			},
		}

		err = CheckCanonicalWalletRedisPolicy(ctx, rdb, cfg)
		require.NoError(t, err, "managed Redis with disabled CONFIG must alert and continue under enforce")
	})
}
