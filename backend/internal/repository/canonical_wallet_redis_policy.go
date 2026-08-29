package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
)

// ErrRedisPolicyNotNoeviction is returned when canonical_wallet.mode=enforce
// and canonical_wallet.redis_policy_check=enforce and the Redis maxmemory-policy
// is not noeviction with a non-zero maxmemory bound.
var ErrRedisPolicyNotNoeviction = errors.New("canonical_wallet: Redis maxmemory-policy is not noeviction")

var (
	canonicalWalletRedisPolicyCalls                   atomic.Int64
	canonicalWalletRedisPolicyChecked                 atomic.Int64
	canonicalWalletRedisPolicyUnverifiedConfigRefused atomic.Int64
	canonicalWalletRedisPolicyUncheckedUnreachable    atomic.Int64
	canonicalWalletRedisPolicyInertNoMaxmemory        atomic.Int64
)

// CanonicalWalletRedisPolicyMetrics captures process-lifetime counters for the
// startup Redis policy check.
type CanonicalWalletRedisPolicyMetrics struct {
	Calls                   int64
	Checked                 int64
	UnverifiedConfigRefused int64
	UncheckedUnreachable    int64
	InertNoMaxmemory        int64
}

// CanonicalWalletRedisPolicyMetricsSnapshot returns a snapshot of policy check metrics.
func CanonicalWalletRedisPolicyMetricsSnapshot() CanonicalWalletRedisPolicyMetrics {
	return CanonicalWalletRedisPolicyMetrics{
		Calls:                   canonicalWalletRedisPolicyCalls.Load(),
		Checked:                 canonicalWalletRedisPolicyChecked.Load(),
		UnverifiedConfigRefused: canonicalWalletRedisPolicyUnverifiedConfigRefused.Load(),
		UncheckedUnreachable:    canonicalWalletRedisPolicyUncheckedUnreachable.Load(),
		InertNoMaxmemory:        canonicalWalletRedisPolicyInertNoMaxmemory.Load(),
	}
}

func isRedisCommandError(err error) bool {
	if err == nil {
		return false
	}
	var rErr redis.Error
	if errors.As(err, &rErr) {
		return true
	}
	msg := err.Error()
	return strings.HasPrefix(msg, "ERR ") ||
		strings.HasPrefix(msg, "NOPERM ") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unknown subcommand") ||
		strings.Contains(msg, "unknown or disabled command")
}

// CheckCanonicalWalletRedisPolicy (Phase 4.3-G Task 1, redesign §15.4 / §15 Q3)
// validates that the canonical-wallet Redis instance will not evict active holds
// or leases.
//
// Behavior:
//   - setting=off: returns nil without issuing CONFIG commands.
//   - policy == "noeviction": returns nil.
//   - policy != "noeviction" && maxmemory == 0: returns nil + alert
//     `policy_inert_no_maxmemory` (Redis cannot evict without a memory bound).
//   - policy != "noeviction" && maxmemory != 0:
//   - setting=enforce && mode=enforce: returns ErrRedisPolicyNotNoeviction naming
//     both values.
//   - setting=warn: returns nil + warning alert.
//   - mode=shadow|disabled: returns nil + warning alert (never fails startup).
//   - CONFIG command refused (e.g. rename-command CONFIG ""): returns nil + alert
//     `policy_unverified_config_refused`, incrementing UnverifiedConfigRefused.
//   - Transport error (Redis unreachable): returns nil + log
//     `policy_unchecked_redis_unreachable`, incrementing UncheckedUnreachable
//     (the pin is NOT recorded as checked).
func CheckCanonicalWalletRedisPolicy(ctx context.Context, rdb redis.Cmdable, cfg *config.Config) error {
	if cfg == nil || rdb == nil {
		return nil
	}
	checkSetting := cfg.CanonicalWallet.RedisPolicyCheck
	if checkSetting == "" {
		checkSetting = "enforce"
	}
	if checkSetting == "off" {
		return nil
	}

	canonicalWalletRedisPolicyCalls.Add(1)

	policyRes, policyErr := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if policyErr != nil {
		if isRedisCommandError(policyErr) {
			canonicalWalletRedisPolicyUnverifiedConfigRefused.Add(1)
			slog.Warn("canonical_wallet: policy unverified — CONFIG refused", "error", policyErr)
			return nil
		}
		canonicalWalletRedisPolicyUncheckedUnreachable.Add(1)
		slog.Warn("canonical_wallet: policy unchecked — Redis unreachable at startup", "error", policyErr)
		return nil
	}

	maxmemRes, maxmemErr := rdb.ConfigGet(ctx, "maxmemory").Result()
	if maxmemErr != nil {
		if isRedisCommandError(maxmemErr) {
			canonicalWalletRedisPolicyUnverifiedConfigRefused.Add(1)
			slog.Warn("canonical_wallet: policy unverified — CONFIG refused", "error", maxmemErr)
			return nil
		}
		canonicalWalletRedisPolicyUncheckedUnreachable.Add(1)
		slog.Warn("canonical_wallet: policy unchecked — Redis unreachable at startup", "error", maxmemErr)
		return nil
	}

	canonicalWalletRedisPolicyChecked.Add(1)

	policy := strings.TrimSpace(policyRes["maxmemory-policy"])
	maxmem := strings.TrimSpace(maxmemRes["maxmemory"])

	if policy == "noeviction" || policy == "" {
		return nil
	}

	if maxmem == "0" || maxmem == "" {
		canonicalWalletRedisPolicyInertNoMaxmemory.Add(1)
		slog.Warn("canonical_wallet: policy_inert_no_maxmemory — Redis maxmemory-policy is not noeviction but maxmemory is 0 (inert)", "policy", policy, "maxmemory", maxmem)
		return nil
	}

	if checkSetting == "warn" {
		slog.Warn("canonical_wallet.redis_policy_check=warn: wallet Redis maxmemory-policy is not noeviction with bounded maxmemory — held money could be evicted", "policy", policy, "maxmemory", maxmem)
		return nil
	}

	if cfg.CanonicalWallet.Mode != config.CanonicalWalletModeEnforce {
		slog.Warn("canonical_wallet: wallet Redis maxmemory-policy is not noeviction with bounded maxmemory", "policy", policy, "maxmemory", maxmem, "mode", cfg.CanonicalWallet.Mode)
		return nil
	}

	return fmt.Errorf("%w: canonical_wallet.mode=enforce but the wallet Redis maxmemory-policy is %q with maxmemory=%s — held money could be evicted; set canonical_wallet.redis_policy_check=warn to override", ErrRedisPolicyNotNoeviction, policy, maxmem)
}
