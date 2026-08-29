//go:build integration

package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Test 71 (Phase 4.2-G Task 3, redesign §15.4's retention pruners):
//   - Prunes old resolved hold outcomes (resolution set), old terminal live
//     records (terminal_at set), and old delivered outbox rows.
//   - NEVER prunes dead-letters (the receivable is money owed).
//   - NEVER touches open holds, active live records, or pending outbox rows.
//   - NEVER touches anything inside the open reconciliation window.
//   - Per-table counts and metric retention_pruned_total{table}.
//   - A second pass deletes nothing.
//   - Retention floor leg: with retention_days = 38, a 31-day-old delivered row
//     survives and a 39-day-old delivered row is pruned.
//   - Validate() refuses retention_days = 37.
func TestWalletRetentionPruners(t *testing.T) {
	ctx := context.Background()
	db := startWalletReconciliationTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	holds := &walletHoldOutcomeStore{db: db}
	live := &liveProvisionalStore{db: db}

	now := time.Now().UTC()
	user := "shipany-user-" + uuid.NewString()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.RetentionDays = 45
	retentionSvc := NewWalletRetentionService(cfg, db, outbox)
	retentionSvc.clock = func() time.Time { return now }
	t.Cleanup(retentionSvc.Close)

	// --- Seed tables ---
	// 1. wallet_hold_outcome
	// Old eligible (resolved_at = now - 50d, older than 45d retention):
	require.NoError(t, holds.InsertAbandoned(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_old_abandoned", LeaseID: "srv-l1", HeldUnits: 1000, Class: "c1", ArmedAt: now.Add(-50 * 24 * time.Hour),
	}, user, now.Add(-50*24*time.Hour)))
	require.NoError(t, holds.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_old_settled", LeaseID: "srv-l1", HeldUnits: 2000, Class: "c1", ArmedAt: now.Add(-50 * 24 * time.Hour),
	}, user, now.Add(-50*24*time.Hour)))
	require.NoError(t, holds.MarkSettled(ctx, "auth_71_old_settled", "gwusg_71_old_s", now.Add(-50*24*time.Hour)))
	require.NoError(t, holds.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_old_expired", LeaseID: "srv-l1", HeldUnits: 3000, Class: "c1", ArmedAt: now.Add(-50 * 24 * time.Hour),
	}, user, now.Add(-50*24*time.Hour)))
	_, err := holds.MarkExpiredOlderThan(ctx, now.Add(-49*24*time.Hour), now.Add(-50*24*time.Hour))
	require.NoError(t, err)

	// Old open (armed_at = now - 50d, resolution = NULL -> MUST SURVIVE):
	require.NoError(t, holds.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_old_open", LeaseID: "srv-l1", HeldUnits: 4000, Class: "c1", ArmedAt: now.Add(-50 * 24 * time.Hour),
	}, user, now.Add(-50*24*time.Hour)))

	// Recent in-window (armed_at = now - 10d -> MUST SURVIVE):
	require.NoError(t, holds.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_rec_settled", LeaseID: "srv-l1", HeldUnits: 5000, Class: "c1", ArmedAt: now.Add(-10 * 24 * time.Hour),
	}, user, now.Add(-10*24*time.Hour)))
	require.NoError(t, holds.MarkSettled(ctx, "auth_71_rec_settled", "gwusg_71_rec_s", now.Add(-10*24*time.Hour)))
	require.NoError(t, holds.InsertIndeterminate(ctx, CanonicalWalletHold{
		AuthorizationID: "auth_71_rec_open", LeaseID: "srv-l1", HeldUnits: 6000, Class: "c1", ArmedAt: now.Add(-10 * 24 * time.Hour),
	}, user, now.Add(-10*24*time.Hour)))

	// 2. wallet_live_provisional
	// Old eligible (terminal_at = now - 50d):
	termOld := now.Add(-50 * 24 * time.Hour)
	require.NoError(t, live.Save(ctx, &LiveProvisionalRecord{
		Token: "live_71_old_aborted", AuthorizationID: "auth_live_old_ab", PlatformUserID: user,
		UserID: 1, APIKeyID: 1, AccountID: 1, BillingCurrency: "CNY", Status: LiveProvisionalStatusAborted,
		CreatedAt: termOld, TerminalAt: &termOld,
	}))
	require.NoError(t, live.Save(ctx, &LiveProvisionalRecord{
		Token: "live_71_old_finalized", AuthorizationID: "auth_live_old_fin", PlatformUserID: user,
		UserID: 1, APIKeyID: 1, AccountID: 1, BillingCurrency: "CNY", Status: LiveProvisionalStatusFinalized,
		CreatedAt: termOld, TerminalAt: &termOld, SettlementEventID: "gwusg_live_old",
	}))
	// Old active (created_at = now - 50d, terminal_at = NULL -> MUST SURVIVE):
	require.NoError(t, live.Save(ctx, &LiveProvisionalRecord{
		Token: "live_71_old_active", AuthorizationID: "auth_live_old_act", PlatformUserID: user,
		UserID: 1, APIKeyID: 1, AccountID: 1, BillingCurrency: "CNY", Status: LiveProvisionalStatusActive,
		CreatedAt: termOld,
	}))
	// Recent in-window (terminal_at = now - 10d -> MUST SURVIVE):
	termRec := now.Add(-10 * 24 * time.Hour)
	require.NoError(t, live.Save(ctx, &LiveProvisionalRecord{
		Token: "live_71_rec_finalized", AuthorizationID: "auth_live_rec_fin", PlatformUserID: user,
		UserID: 1, APIKeyID: 1, AccountID: 1, BillingCurrency: "CNY", Status: LiveProvisionalStatusFinalized,
		CreatedAt: termRec, TerminalAt: &termRec,
	}))

	// 3. wallet_settlement_outbox
	// Helper to insert outbox rows:
	insertOutboxRow := func(eventID, status, dlReason string, occurred time.Time, delivered *time.Time) {
		t.Helper()
		var dVal any
		if delivered != nil {
			dVal = *delivered
		}
		var rVal any
		if dlReason != "" {
			rVal = dlReason
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO wallet_settlement_outbox
				(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, dead_letter_reason, occurred_at, delivered_at)
			VALUES ($1, $2, $3, 'CNY', 1000, $4, $5, $6, $7, $8)`,
			eventID, user, "req-"+eventID, "hash-"+eventID, status, rVal, occurred, dVal)
		require.NoError(t, err)
	}

	delOld := now.Add(-50 * 24 * time.Hour)
	delRec := now.Add(-10 * 24 * time.Hour)
	insertOutboxRow("gwusg_71_old_delivered", "delivered", "", delOld, &delOld)
	insertOutboxRow("gwusg_71_old_pending", "pending", "", delOld, nil)
	insertOutboxRow("gwusg_71_old_in_flight", "in_flight", "", delOld, nil)
	insertOutboxRow("gwusg_71_old_dead_shortfall", "dead_letter", "balance_shortfall", delOld, nil)
	insertOutboxRow("gwusg_71_old_dead_exhausted", "dead_letter", "attempts_exhausted", delOld, nil)
	insertOutboxRow("gwusg_71_rec_delivered", "delivered", "", delRec, &delRec)
	insertOutboxRow("gwusg_71_rec_pending", "pending", "", delRec, nil)
	insertOutboxRow("gwusg_71_rec_dead_letter", "dead_letter", "balance_shortfall", delRec, nil)

	// 4. wallet_billing_snapshot & usage_logs (Phase 4.4-G Task 2)
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS usage_logs (
			id BIGSERIAL PRIMARY KEY,
			billing_snapshot_id TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS idx_usage_logs_billing_snapshot_id
			ON usage_logs (billing_snapshot_id)
			WHERE billing_snapshot_id IS NOT NULL;
	`)
	require.NoError(t, err)

	insertSnapshot := func(id string, createdAt time.Time) {
		t.Helper()
		_, err := db.ExecContext(ctx, `
			INSERT INTO wallet_billing_snapshot
				(id, version, user_id, api_key_id, account_id, billing_model, pricing_mode, payload, created_at)
			VALUES ($1, 1, 1, 1, 1, 'gpt-4o', 'standard', '{"mode":"standard"}'::jsonb, $2)`,
			id, createdAt)
		require.NoError(t, err)
	}
	insertUsageLog := func(snapshotID string, createdAt time.Time) {
		t.Helper()
		_, err := db.ExecContext(ctx, `
			INSERT INTO usage_logs (billing_snapshot_id, created_at)
			VALUES ($1, $2)`,
			snapshotID, createdAt)
		require.NoError(t, err)
	}

	snapOldUnref := "snap_71_old_unref"
	snap60dUnref := "snap_71_60d_unref"
	snapRecUnref := "snap_71_rec_unref"
	snapLiveUsage := "snap_71_live_usage"
	snapLiveOutbox := "snap_71_live_outbox"
	snap120dLiveUsage := "snap_71_120d_live_usage"

	insertSnapshot(snapOldUnref, now.Add(-100*24*time.Hour))
	insertSnapshot(snap60dUnref, now.Add(-60*24*time.Hour))
	insertSnapshot(snapRecUnref, now.Add(-10*24*time.Hour))
	insertSnapshot(snapLiveUsage, now.Add(-50*24*time.Hour))
	insertUsageLog(snapLiveUsage, now.Add(-50*24*time.Hour))
	insertSnapshot(snapLiveOutbox, now.Add(-50*24*time.Hour))
	_, err = db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET billing_snapshot_id = $1 WHERE event_id = 'gwusg_71_rec_delivered'`, snapLiveOutbox)
	require.NoError(t, err)
	insertSnapshot(snap120dLiveUsage, now.Add(-120*24*time.Hour))
	insertUsageLog(snap120dLiveUsage, now.Add(-120*24*time.Hour))

	// --- Execute first pruner pass ---
	holdBase := WalletRetentionPrunedTotal("wallet_hold_outcome")
	liveBase := WalletRetentionPrunedTotal("wallet_live_provisional")
	outboxBase := WalletRetentionPrunedTotal("wallet_settlement_outbox")
	snapshotBase := WalletRetentionPrunedTotal("wallet_billing_snapshot")

	holdPruned, livePruned, outboxPruned, snapshotPruned, err := retentionSvc.PruneOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), holdPruned, "exactly 3 old resolved hold outcomes pruned")
	require.Equal(t, int64(2), livePruned, "exactly 2 old terminal live provisional records pruned")
	require.Equal(t, int64(1), outboxPruned, "exactly 1 old delivered outbox row pruned")
	require.Equal(t, int64(1), snapshotPruned, "exactly 1 old unreferenced snapshot pruned (older than 90d default cutoff)")

	require.Equal(t, int64(3), WalletRetentionPrunedTotal("wallet_hold_outcome")-holdBase)
	require.Equal(t, int64(2), WalletRetentionPrunedTotal("wallet_live_provisional")-liveBase)
	require.Equal(t, int64(1), WalletRetentionPrunedTotal("wallet_settlement_outbox")-outboxBase)
	require.Equal(t, int64(1), WalletRetentionPrunedTotal("wallet_billing_snapshot")-snapshotBase)

	// Verify hold outcomes
	checkHoldExists := func(authID string) bool {
		var exists bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_hold_outcome WHERE authorization_id = $1)`, authID).Scan(&exists)
		require.NoError(t, err)
		return exists
	}
	require.False(t, checkHoldExists("auth_71_old_abandoned"), "old abandoned pruned")
	require.False(t, checkHoldExists("auth_71_old_settled"), "old settled pruned")
	require.False(t, checkHoldExists("auth_71_old_expired"), "old expired pruned")
	require.True(t, checkHoldExists("auth_71_old_open"), "old open survives")
	require.True(t, checkHoldExists("auth_71_rec_settled"), "recent settled survives")
	require.True(t, checkHoldExists("auth_71_rec_open"), "recent open survives")

	// Verify live provisionals
	checkLiveExists := func(token string) bool {
		var exists bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_live_provisional WHERE token = $1)`, token).Scan(&exists)
		require.NoError(t, err)
		return exists
	}
	require.False(t, checkLiveExists("live_71_old_aborted"), "old aborted pruned")
	require.False(t, checkLiveExists("live_71_old_finalized"), "old finalized pruned")
	require.True(t, checkLiveExists("live_71_old_active"), "old active survives")
	require.True(t, checkLiveExists("live_71_rec_finalized"), "recent finalized survives")

	// Verify outbox rows
	checkOutboxExists := func(eventID string) bool {
		var exists bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id = $1)`, eventID).Scan(&exists)
		require.NoError(t, err)
		return exists
	}
	require.False(t, checkOutboxExists("gwusg_71_old_delivered"), "old delivered pruned")
	require.True(t, checkOutboxExists("gwusg_71_old_pending"), "old pending survives")
	require.True(t, checkOutboxExists("gwusg_71_old_in_flight"), "old in_flight survives")
	require.True(t, checkOutboxExists("gwusg_71_old_dead_shortfall"), "old dead-letter shortfall survives (the receivable)")
	require.True(t, checkOutboxExists("gwusg_71_old_dead_exhausted"), "old dead-letter exhausted survives")
	require.True(t, checkOutboxExists("gwusg_71_rec_delivered"), "recent delivered survives")
	require.True(t, checkOutboxExists("gwusg_71_rec_pending"), "recent pending survives")
	require.True(t, checkOutboxExists("gwusg_71_rec_dead_letter"), "recent dead-letter survives")

	// Verify snapshots
	checkSnapshotExists := func(id string) bool {
		var exists bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_billing_snapshot WHERE id = $1)`, id).Scan(&exists)
		require.NoError(t, err)
		return exists
	}
	require.False(t, checkSnapshotExists(snapOldUnref), "100d unreferenced snapshot pruned")
	require.True(t, checkSnapshotExists(snap60dUnref), "60d unreferenced snapshot survives at 90d default cutoff (not 45d retention)")
	require.True(t, checkSnapshotExists(snapRecUnref), "recent snapshot survives")
	require.True(t, checkSnapshotExists(snapLiveUsage), "snapshot referenced by live usage_logs survives")
	require.True(t, checkSnapshotExists(snapLiveOutbox), "snapshot referenced by delivered outbox row survives")
	require.True(t, checkSnapshotExists(snap120dLiveUsage), "120d snapshot referenced by live usage_logs survives (pins 3-way NOT EXISTS safety property)")

	// --- Second pruner pass deletes nothing ---
	holdPruned2, livePruned2, outboxPruned2, snapshotPruned2, err := retentionSvc.PruneOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), holdPruned2)
	require.Equal(t, int64(0), livePruned2)
	require.Equal(t, int64(0), outboxPruned2)
	require.Equal(t, int64(0), snapshotPruned2)

	// --- Disabled leg: usage_logs_days == 0 deletes nothing ---
	snapZero := "snap_71_zero_mode"
	insertSnapshot(snapZero, now.Add(-100*24*time.Hour))
	svcZero := NewWalletRetentionService(cfg, db, outbox, 0)
	svcZero.clock = func() time.Time { return now }
	t.Cleanup(svcZero.Close)

	_, _, _, snapshotPrunedZero, err := svcZero.PruneOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), snapshotPrunedZero, "pruner deletes nothing when usage_logs_days == 0 (disabled)")
	require.True(t, checkSnapshotExists(snapZero), "100d unreferenced snapshot survives when snapshot pruner is disabled")

	// --- Floor leg: retention_days = 38 ---
	cfg38 := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg38.RetentionDays = 38

	svc38 := NewWalletRetentionService(cfg38, db, outbox)
	svc38.clock = func() time.Time { return now }
	t.Cleanup(svc38.Close)

	del31 := now.Add(-31 * 24 * time.Hour)
	del39 := now.Add(-39 * 24 * time.Hour)
	insertOutboxRow("gwusg_71_floor_31d", "delivered", "", del31, &del31)
	insertOutboxRow("gwusg_71_floor_39d", "delivered", "", del39, &del39)

	_, _, outboxPruned38, _, err := svc38.PruneOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), outboxPruned38, "only the 39d-old row is pruned; the 31d-old row survives")
	require.True(t, checkOutboxExists("gwusg_71_floor_31d"), "31-day-old delivered row survives the 38-day retention floor")
	require.False(t, checkOutboxExists("gwusg_71_floor_39d"), "39-day-old delivered row is pruned under 38-day retention")

	// Validate() floor check
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("PLATFORM_IDENTITY_SECRET", strings.Repeat("p", 32))
	validConfig := func(retentionDays int) *config.Config {
		cfg, err := config.Load()
		if err != nil {
			panic(err)
		}
		cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
		cfg.CanonicalWallet.RetentionDays = retentionDays
		return cfg
	}

	require.NoError(t, validConfig(38).Validate(), "38 days is the retention floor (31d window + 7d soak)")
	err = validConfig(37).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "canonical_wallet.retention_days must be at least 38")

	// Summary over reconciliation window still functions and matches direct sums
	readSvc := &WalletReconciliationReadService{
		outbox: outbox,
		holds:  holds,
		live:   live,
		leases: store,
	}
	summary, err := readSvc.Summary(ctx, user, now.Add(-30*24*time.Hour), now.Add(time.Hour), 0)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, int64(1000), summary.Receivable.BalanceShortfallUnits)
}
