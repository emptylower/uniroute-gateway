package service

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

type WalletImmediateReleaseRecoveryStats struct {
	PendingAcknowledgements      int64
	OldestAcknowledgementSeconds float64
	PendingCleanup               int64
	OldestCleanupSeconds         float64
	PendingFee                   int64
	OldestFeeSeconds             float64
}

var walletImmediateReleaseLastAlert atomic.Int64

// ObserveImmediateWalletRecovery measures immutable clocks. Scheduler rotation
// of updated_at cannot hide old ACK, cleanup or trusted-fee recovery work.
func (b *CanonicalWalletBridge) ObserveImmediateWalletRecovery(ctx context.Context) (WalletImmediateReleaseRecoveryStats, error) {
	var stats WalletImmediateReleaseRecoveryStats
	if b == nil || b.outboxDB == nil {
		return stats, errors.New("wallet immediate recovery database unavailable")
	}
	err := b.outboxDB.QueryRowContext(ctx, `SELECT
 count(*) FILTER (WHERE expiry_intent_version=2 AND expiry_ack_at IS NULL),
 COALESCE(EXTRACT(epoch FROM now()-min(terminal_sealed_at) FILTER (WHERE expiry_intent_version=2 AND expiry_ack_at IS NULL)),0),
 count(*) FILTER (WHERE expiry_intent_version=2 AND expiry_ack_at IS NOT NULL AND expiry_cleanup_at IS NULL),
 COALESCE(EXTRACT(epoch FROM now()-min(expiry_ack_at) FILTER (WHERE expiry_intent_version=2 AND expiry_ack_at IS NOT NULL AND expiry_cleanup_at IS NULL)),0),
 count(*) FILTER (WHERE fee_pending),
 COALESCE(EXTRACT(epoch FROM now()-min(COALESCE(terminal_sealed_at,first_write_at,created_at)) FILTER (WHERE fee_pending)),0)
 FROM wallet_authorization_segment`).Scan(&stats.PendingAcknowledgements, &stats.OldestAcknowledgementSeconds, &stats.PendingCleanup, &stats.OldestCleanupSeconds, &stats.PendingFee, &stats.OldestFeeSeconds)
	if err != nil {
		return stats, err
	}
	now := time.Now().Unix()
	last := walletImmediateReleaseLastAlert.Load()
	if now-last < 60 || !walletImmediateReleaseLastAlert.CompareAndSwap(last, now) {
		return stats, nil
	}
	for _, pending := range []struct {
		reason string
		count  int64
		age    float64
	}{
		{"ack_lag", stats.PendingAcknowledgements, stats.OldestAcknowledgementSeconds},
		{"cleanup_stalled", stats.PendingCleanup, stats.OldestCleanupSeconds},
		{"fee_recovery_stalled", stats.PendingFee, stats.OldestFeeSeconds},
	} {
		if pending.count > 0 && pending.age >= 30 {
			slog.Warn("wallet immediate recovery delayed", "reason", pending.reason, "pending_count", pending.count, "oldest_age_seconds", pending.age, "policy_version", WalletImmediateReleasePolicyVersion)
		}
	}
	return stats, nil
}
