//go:build integration

package service

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Test 70 (Phase 4.2-G Task 2, redesign §15.4's receivable): a
// balance_shortfall dead-letter is VISIBLE with its parent through the
// reconciliation read, and funding the user lets the collector's re-drive +
// the ordinary dispatcher collect the debt. The collector (§11.3/§11.4's
// "uncollectable until the user funds", made collectable):
//   - (a) an unfunded user's settlement dead-letters balance_shortfall; the
//     row is visible through ListOutboxEventsByUser and the summary's
//     per-user receivable; FUND the user, tick the collector once (leader
//     held) → the row is pending with attempt_count=0, redrive_count=1,
//     next_attempt_at ≤ now; the dispatcher delivers it → delivered, the
//     receivable drops to zero;
//   - (b) a dead-letter older than retention_days is NOT re-driven;
//   - (c) a re-driven delivery refused again for balance re-dead-letters
//     balance_shortfall through the UNMODIFIED production path with
//     redrive_count preserved; the negative: a re-driven row that fails on
//     TRANSPORT dead-letters attempts_exhausted, never balance_shortfall;
//   - (d) at redrive_count == receivable_redrive_max_attempts the row is
//     terminal, stays in the receivable, and receivable_redrive_exhausted
//     counts it;
//   - (e) a second bridge without the leader key does nothing on its tick.
//
// ONE shared-Redis claim for the whole test; every bridge closed at
// t.Cleanup; the fixed PAST clock (the 37a idiom) keeps the failure
// backoffs behind real time so attempts_exhausted needs no sleeps. The
// shortfall is forced with an amount beyond every covering lease's headroom
// AND beyond the fake's balance, so the settle-purpose ensure is refused
// insufficient_balance whichever path it takes.
func TestWalletReceivableCollector(t *testing.T) {
	ctx := context.Background()
	db := startWalletReconciliationTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	readSvc := &WalletReconciliationReadService{
		outbox: outbox, holds: &walletHoldOutcomeStore{db: db},
		live: &liveProvisionalStore{db: db}, leases: store,
	}

	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })
	user := "shipany-user-" + uuid.NewString()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.RequestTimeoutMS = 50
	cfg.ReceivableRedriveIntervalSeconds = 60
	cfg.ReceivableRedriveMaxAttempts = 2
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return time.Now().UTC() }
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, func() time.Time { return time.Now().UTC() })
	t.Cleanup(bridge.Close)
	require.NotNil(t, bridge)

	pollStatus := func(eventID, want string, timeout time.Duration) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			var status string
			err := db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status)
			if err == nil && status == want {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		var status string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status))
		t.Fatalf("event %s never reached %s (now %s)", eventID, want, status)
	}
	rowState := func(eventID string) (status string, attempts, redrives int, nextAttempt time.Time) {
		t.Helper()
		var na sql.NullTime
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT status, attempt_count, redrive_count, next_attempt_at FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).
			Scan(&status, &attempts, &redrives, &na))
		if na.Valid {
			nextAttempt = na.Time
		}
		return
	}
	deadLetterReason := func(eventID string) string {
		t.Helper()
		var reason sql.NullString
		require.NoError(t, db.QueryRowContext(ctx, `SELECT dead_letter_reason FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&reason))
		return reason.String
	}
	tick := func(b *CanonicalWalletBridge) {
		t.Helper()
		collectCtx, collectCancel := context.WithTimeout(ctx, 5*time.Second)
		b.collectReceivableOnce(collectCtx)
		collectCancel()
	}
	// tickAsLeader simulates the leader key's TTL expiry before the tick —
	// the reaper's SETNX holds the key for 2 × the interval, so SEQUENTIAL
	// manual ticks on one bridge would otherwise alternate leader/follower
	// (in production the 60 s ticker naturally spaces ticks past the TTL
	// window). The leg that must NOT lead (bridge B) uses plain tick.
	tickAsLeader := func(b *CanonicalWalletBridge) {
		t.Helper()
		require.NoError(t, rdb.Del(context.Background(), "canonical_wallet:receivable_collector:tick").Err())
		tick(b)
	}
	// seedShortfall installs a terminal balance_shortfall row directly (the
	// collector's input shape, the same seeding the dispatcher's own path
	// produces).
	seedShortfall := func(eventID string, occurred time.Time, redriveCount int) {
		t.Helper()
		_, err := db.ExecContext(ctx, `
			INSERT INTO wallet_settlement_outbox
				(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, dead_letter_reason, occurred_at, redrive_count)
			VALUES ($1, $2, $3, 'CNY', 1000, $4, 'dead_letter', 'balance_shortfall', $5, $6)`,
			eventID, user, "req-"+eventID, "hash-"+eventID, occurred, redriveCount)
		require.NoError(t, err)
	}

	// --- (a) the shortfall, the visibility, the re-drive, the delivery ---
	// 80 CNY: unfunded the settle ensure is clamped to a zero budget and
	// refused (the fake's perLeaseMax is 100 CNY, so a funded re-drive
	// still fits one lease).
	const debtUnits = int64(8_000000_000)
	const debtReq = "req-70-debt"
	debtEvent := CanonicalWalletSettlementEventID(debtReq, user, "CNY")
	require.True(t, bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: debtReq, PlatformUserID: user, Currency: "CNY", AmountUnits: debtUnits,
		BillingSnapshotID: "wbs_70_debt",
	}))
	pollStatus(debtEvent, "dead_letter", 10*time.Second)
	require.Equal(t, "balance_shortfall", deadLetterReason(debtEvent))

	// Visibility: the row through ListOutboxEventsByUser (the summary's own
	// read) and the summary's per-user receivable.
	since, until := time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(time.Hour)
	rows, truncated, err := outbox.ListOutboxEventsByUser(ctx, user, since, until, 0, 100)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, rows, 1)
	require.Equal(t, debtEvent, rows[0].EventID)
	require.Equal(t, "dead_letter", rows[0].Status)
	require.Equal(t, "balance_shortfall", rows[0].DeadLetterReason)
	require.Equal(t, "wbs_70_debt", rows[0].BillingSnapshotID, "the collector's row keeps its snapshot through the cycle")
	summary, err := readSvc.Summary(ctx, user, since, until, 0)
	require.NoError(t, err)
	require.Equal(t, debtUnits, summary.Receivable.BalanceShortfallUnits)
	require.Equal(t, 1, summary.Receivable.Rows)

	// Fund the user past the debt, then ONE collector tick (leader held by
	// this bridge).
	fake.fund(user, 10_000000_000)
	redrivenBefore := canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tickAsLeader(bridge)
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "one re-drive")

	status, attempts, redrives, nextAttempt := rowState(debtEvent)
	require.Equal(t, "pending", status, "the re-queued row is pending")
	require.Equal(t, 0, attempts, "the transport budget starts fresh")
	require.Equal(t, 1, redrives, "the re-drive is counted in its own column")
	require.False(t, nextAttempt.After(time.Now().UTC().Add(5*time.Second)), "next_attempt_at is immediate")

	// The ordinary dispatcher collects the funded debt.
	pollStatus(debtEvent, "delivered", 15*time.Second)
	summary, err = readSvc.Summary(ctx, user, since, until, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), summary.Receivable.BalanceShortfallUnits, "the receivable drops to zero")
	require.Equal(t, 0, summary.Receivable.Rows)

	// --- (b) the retention bound: older than retention_days (45) is never
	// re-driven. The row also carries a parent_event_id, read back through
	// the summary's own query.
	const oldEvent = "gwusg_70_old"
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, dead_letter_reason, occurred_at, parent_event_id, split_depth)
		VALUES ($1, $2, 'req-70-old', 'CNY', 1000, 'hash-70-old', 'dead_letter', 'balance_shortfall', $3, 'gwusg_70_parent', 1)`,
		oldEvent, user, now.Add(-60*24*time.Hour))
	require.NoError(t, err)
	redrivenBefore = canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tickAsLeader(bridge)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "an out-of-window dead-letter is not re-driven")
	status, _, redrives, _ = rowState(oldEvent)
	require.Equal(t, "dead_letter", status)
	require.Equal(t, 0, redrives)
	rows, _, err = outbox.ListOutboxEventsByUser(ctx, user, now.Add(-61*24*time.Hour), until, 0, 100)
	require.NoError(t, err)
	var sawOld bool
	for _, r := range rows {
		if r.EventID == oldEvent {
			sawOld = true
			require.Equal(t, "gwusg_70_parent", r.ParentEventID, "the receivable row surfaces its parent_event_id")
		}
	}
	require.True(t, sawOld, "the out-of-window row is still readable over a wide window")

	// --- (d) the attempt bound: at redrive_count == max the row is terminal
	// and counted by receivable_redrive_exhausted. Seeded AT the bound.
	const maxedEvent = "gwusg_70_maxed"
	seedShortfall(maxedEvent, now, 2)
	exhaustedBefore := canonicalWalletBridgeMetrics.receivableRedriveExhausted.Load()
	redrivenBefore = canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tickAsLeader(bridge)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "a maxed row is not re-driven")
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.receivableRedriveExhausted.Load()-exhaustedBefore, "the exhausted metric counts it once per pass")
	status, _, redrives, _ = rowState(maxedEvent)
	require.Equal(t, "dead_letter", status, "the maxed row is terminal")
	require.Equal(t, 2, redrives)
	units, err := outbox.SumDeadLetterUnits(ctx, "balance_shortfall")
	require.NoError(t, err)
	require.Equal(t, int64(2000), units, "the terminal rows stay in the receivable")

	// --- (e) the leader: a second bridge without the leader key does
	// nothing on its tick. Bridge A's ticks above still hold the SETNX key
	// (TTL 2 × the 60 s interval), so bridge B — built on the SAME stores —
	// must not re-drive the fresh, in-window, under-bound candidate seeded
	// here.
	const candidateEvent = "gwusg_70_candidate"
	seedShortfall(candidateEvent, time.Now().UTC(), 0)
	bridgeB := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, func() time.Time { return time.Now().UTC() })
	t.Cleanup(bridgeB.Close)
	redrivenBefore = canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tick(bridgeB)
	require.Equal(t, int64(0), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "the follower bridge re-drove nothing")
	status, _, redrives, _ = rowState(candidateEvent)
	require.Equal(t, "dead_letter", status)
	require.Equal(t, 0, redrives)

	// --- (c) the refused-again class and the transport negative ---
	// (c1) zero the balance, then the LEADER's tick takes the candidate;
	// its re-driven delivery is refused again and the UNMODIFIED path
	// re-dead-letters balance_shortfall with redrive_count preserved.
	fake.mu.Lock()
	fake.balance[user] = 0
	fake.mu.Unlock()
	redrivenBefore = canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tickAsLeader(bridge)
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "the leader's tick takes the candidate")
	status, _, redrives, _ = rowState(candidateEvent)
	require.Equal(t, "pending", status)
	require.Equal(t, 1, redrives)
	pollStatus(candidateEvent, "dead_letter", 15*time.Second)
	require.Equal(t, "balance_shortfall", deadLetterReason(candidateEvent), "the refused-again row keeps its class through the unmodified path")
	status, _, redrives, _ = rowState(candidateEvent)
	require.Equal(t, "dead_letter", status)
	require.Equal(t, 1, redrives, "redrive_count survives the re-dead-letter")

	// (c2) the transport negative: fund past a SMALL row's amount, re-drive
	// it, then answer every settlements call with 500 — a fault. The row
	// must burn its TRANSPORT budget and dead-letter attempts_exhausted,
	// never balance_shortfall. The (c1) row is pinned AT the bound first so
	// exactly ONE row (the transport row) is re-drivable on this tick.
	_, err = db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET redrive_count = 2 WHERE event_id = $1`, candidateEvent)
	require.NoError(t, err)
	outbox.maxAttempts = 2
	outbox.backoff = func(attempts int, simulatedNow time.Time) time.Time { return time.Now().UTC() }
	fake.fund(user, 100_000000)
	const transportEvent = "gwusg_70_transport"
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, dead_letter_reason, occurred_at)
		VALUES ($1, $2, 'req-70-transport', 'CNY', 1000, 'hash-70-t', 'dead_letter', 'balance_shortfall', $3)`,
		transportEvent, user, time.Now().UTC())
	require.NoError(t, err)
	fake.respondWith("/api/internal/v2/wallet/settlements", 500, `{"code":-1}`, -1)
	redrivenBefore = canonicalWalletBridgeMetrics.receivableRedriven.Load()
	tickAsLeader(bridge)
	require.Equal(t, int64(1), canonicalWalletBridgeMetrics.receivableRedriven.Load()-redrivenBefore, "the transport row is re-driven")
	pollStatus(transportEvent, "dead_letter", 15*time.Second)
	fake.clearResponse("/api/internal/v2/wallet/settlements")
	require.Equal(t, "attempts_exhausted", deadLetterReason(transportEvent), "a transport fault dead-letters attempts_exhausted, not balance_shortfall")
	status, _, redrives, _ = rowState(transportEvent)
	require.Equal(t, "dead_letter", status)
	require.Equal(t, 1, redrives, "the transport budget burned without touching the re-drive bound")
}
