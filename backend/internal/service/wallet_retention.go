package service

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// walletOutboxPruneStore is the outbox's prune interface implemented by
// repository.WalletOutboxStore (and outboxStoreForTest in tests).
type walletOutboxPruneStore interface {
	PruneDeliveredOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error)
}

// walletHoldOutcomePruneStore is the hold outcome table's prune interface.
type walletHoldOutcomePruneStore interface {
	PruneResolvedOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error)
}

// walletLiveProvisionalPruneStore is the live provisional table's prune interface.
type walletLiveProvisionalPruneStore interface {
	PruneTerminalOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error)
}

type walletRetentionMetrics struct {
	prunedHoldOutcomes    atomic.Int64
	prunedLiveProvisional atomic.Int64
	prunedOutboxDelivered atomic.Int64
	passes                atomic.Int64
	errors                atomic.Int64
}

var canonicalWalletRetentionMetrics walletRetentionMetrics

// WalletRetentionPrunedTotal returns the cumulative count of pruned rows for
// the specified table ("wallet_hold_outcome", "wallet_live_provisional",
// "wallet_settlement_outbox").
func WalletRetentionPrunedTotal(table string) int64 {
	switch table {
	case "wallet_hold_outcome":
		return canonicalWalletRetentionMetrics.prunedHoldOutcomes.Load()
	case "wallet_live_provisional":
		return canonicalWalletRetentionMetrics.prunedLiveProvisional.Load()
	case "wallet_settlement_outbox":
		return canonicalWalletRetentionMetrics.prunedOutboxDelivered.Load()
	default:
		return 0
	}
}

// WalletRetentionStats returns a map of all retention metrics.
func WalletRetentionStats() map[string]int64 {
	return map[string]int64{
		"retention_pruned_hold_outcomes":    canonicalWalletRetentionMetrics.prunedHoldOutcomes.Load(),
		"retention_pruned_live_provisional": canonicalWalletRetentionMetrics.prunedLiveProvisional.Load(),
		"retention_pruned_outbox_delivered": canonicalWalletRetentionMetrics.prunedOutboxDelivered.Load(),
		"retention_passes":                  canonicalWalletRetentionMetrics.passes.Load(),
		"retention_errors":                  canonicalWalletRetentionMetrics.errors.Load(),
	}
}

const (
	walletRetentionStartupDelay  = 30 * time.Second
	walletRetentionCheckInterval = 24 * time.Hour
	walletRetentionBatchSize     = 5000
	walletRetentionFloorDays     = 38
)

// WalletRetentionService (Phase 4.2-G Task 3, redesign §15.4) is the
// singleton retention pruner for the three wallet tables: resolved hold
// outcomes, terminal Live records, and delivered outbox rows.
//
// Hard invariants:
//  1. Never touches dead-letters (the receivable is money owed).
//  2. Never touches rows inside the open reconciliation window.
//  3. Retention floor is 38 days = the 31-day reconciliation window + the
//     7-day soak (§15 Q1).
//  4. Batched deletions use SKIP LOCKED so concurrent dispatcher claims are
//     unaffected.
//  5. Snapshot pruning is deferred to Phase 4.4 (usage_logs referent).
type WalletRetentionService struct {
	cfg          config.CanonicalWalletConfig
	db           *sql.DB
	holds        walletHoldOutcomePruneStore
	live         walletLiveProvisionalPruneStore
	outbox       walletOutboxPruneStore
	clock        func() time.Time
	startupDelay time.Duration
	interval     time.Duration
	batchSize    int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWalletRetentionService creates a new WalletRetentionService singleton.
func NewWalletRetentionService(
	cfg config.CanonicalWalletConfig,
	db *sql.DB,
	outbox any,
) *WalletRetentionService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &WalletRetentionService{
		cfg:          cfg,
		db:           db,
		holds:        &walletHoldOutcomeStore{db: db},
		live:         &liveProvisionalStore{db: db},
		clock:        func() time.Time { return time.Now().UTC() },
		startupDelay: walletRetentionStartupDelay,
		interval:     walletRetentionCheckInterval,
		batchSize:    walletRetentionBatchSize,
		ctx:          ctx,
		cancel:       cancel,
	}
	if pruner, ok := outbox.(walletOutboxPruneStore); ok {
		s.outbox = pruner
	}
	return s
}

// Start launches the background retention loop.
func (s *WalletRetentionService) Start() {
	if s == nil {
		return
	}
	s.wg.Add(1)
	go s.runRetentionLoop()
}

// Close stops the retention loop and waits for any in-flight pass to finish.
func (s *WalletRetentionService) Close() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

// Stop is an alias for Close.
func (s *WalletRetentionService) Stop() {
	s.Close()
}

func (s *WalletRetentionService) runRetentionLoop() {
	defer s.wg.Done()

	startupTimer := time.NewTimer(s.startupDelay)
	defer startupTimer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-startupTimer.C:
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.runRetentionOnce()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.runRetentionOnce()
		}
	}
}

func (s *WalletRetentionService) runRetentionOnce() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	defer cancel()
	if _, _, _, err := s.PruneOnce(ctx); err != nil {
		slog.Warn("canonical wallet retention run failed", "error", err)
	}
}

// PruneOnce executes one full retention pruning pass across the three wallet
// tables. Returns the per-table deleted row counts.
func (s *WalletRetentionService) PruneOnce(ctx context.Context) (holdPruned, livePruned, outboxPruned int64, err error) {
	if s == nil {
		return 0, 0, 0, nil
	}
	retentionDays := s.cfg.RetentionDays
	if retentionDays <= 0 {
		retentionDays = 45
	}
	if retentionDays < walletRetentionFloorDays {
		return 0, 0, 0, fmt.Errorf("canonical_wallet.retention_days (%d) is below floor %d (the 31-day reconciliation window + the 7-day soak)", retentionDays, walletRetentionFloorDays)
	}

	now := s.clock()
	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour)
	batch := s.batchSize
	if batch <= 0 {
		batch = walletRetentionBatchSize
	}

	// 1. Prune resolved hold outcomes (resolution IS NOT NULL AND resolved_at < cutoff)
	if s.holds != nil {
		for {
			deleted, pErr := s.holds.PruneResolvedOlderThan(ctx, cutoff, batch)
			if pErr != nil {
				canonicalWalletRetentionMetrics.errors.Add(1)
				return holdPruned, livePruned, outboxPruned, fmt.Errorf("prune hold outcomes: %w", pErr)
			}
			holdPruned += deleted
			if deleted == 0 {
				break
			}
		}
	}

	// 2. Prune terminal live provisional records (terminal_at IS NOT NULL AND terminal_at < cutoff)
	if s.live != nil {
		for {
			deleted, pErr := s.live.PruneTerminalOlderThan(ctx, cutoff, batch)
			if pErr != nil {
				canonicalWalletRetentionMetrics.errors.Add(1)
				return holdPruned, livePruned, outboxPruned, fmt.Errorf("prune live provisionals: %w", pErr)
			}
			livePruned += deleted
			if deleted == 0 {
				break
			}
		}
	}

	// 3. Prune delivered outbox rows (status = 'delivered' AND occurred_at < cutoff)
	if s.outbox != nil {
		for {
			deleted, pErr := s.outbox.PruneDeliveredOlderThan(ctx, cutoff, batch)
			if pErr != nil {
				canonicalWalletRetentionMetrics.errors.Add(1)
				return holdPruned, livePruned, outboxPruned, fmt.Errorf("prune outbox delivered: %w", pErr)
			}
			outboxPruned += deleted
			if deleted == 0 {
				break
			}
		}
	}

	canonicalWalletRetentionMetrics.prunedHoldOutcomes.Add(holdPruned)
	canonicalWalletRetentionMetrics.prunedLiveProvisional.Add(livePruned)
	canonicalWalletRetentionMetrics.prunedOutboxDelivered.Add(outboxPruned)
	canonicalWalletRetentionMetrics.passes.Add(1)

	slog.Info("canonical wallet retention prune completed",
		"hold_outcomes_pruned", holdPruned,
		"live_provisional_pruned", livePruned,
		"outbox_delivered_pruned", outboxPruned,
		"retention_days", retentionDays,
		"cutoff", cutoff,
	)

	return holdPruned, livePruned, outboxPruned, nil
}
