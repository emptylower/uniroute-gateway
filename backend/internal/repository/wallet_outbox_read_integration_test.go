//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Phase 4.1-G (redesign §15.3): the reconciliation summary's per-user outbox
// read. ListOutboxEventsByUser returns exactly one user's rows inside a
// [since, until) occurred_at window with EVERY column the wire carries
// (delivered_at, dead_letter_reason, parent_event_id, split_depth,
// pending_release_units, authorization_id included), ordered by id with a
// keyset cursor (id > afterID) and a limit+1 truncation signal; the
// DeliveredWatermark is the global high-water mark 4.1-S records per run.

// seedReconciliationOutboxRow inserts one event for platformUserID and
// returns its row id, leaving it pending.
func seedReconciliationOutboxRow(t *testing.T, store *WalletOutboxStore, platformUserID, eventID, leaseID, authID string, amount int64, occurredAt time.Time) int64 {
	t.Helper()
	ctx := context.Background()
	event := service.CanonicalWalletSettlementEvent{
		EventID: eventID, GatewayRequestID: "req-" + eventID, PlatformUserID: platformUserID,
		LeaseID: leaseID, Currency: "CNY", AmountUnits: amount, OccurredAt: occurredAt,
		AuthorizationID: authID,
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())
	var id int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT id FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&id))
	return id
}

func TestWalletOutboxReadByUser(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)

	userU := "shipany-user-recon-u"
	userV := "shipany-user-recon-v"
	now := time.Now().UTC().Truncate(time.Microsecond)
	since, until := now.Add(-2*time.Hour), now.Add(2*time.Hour)

	// U row A — delivered, with an authorization join.
	idA := seedReconciliationOutboxRow(t, store, userU, "gwusg_recon_a", "lease-u-1", "auth-u-a", 2_000000, now.Add(-3*time.Minute))
	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, store.MarkOutboxEventDelivered(ctx, idA, testWalletOutboxWorkerID))

	// U row B — the split parent: returns to pending carrying
	// pending_release_units (§11.3), amount reduced to the headroom.
	idB := seedReconciliationOutboxRow(t, store, userU, "gwusg_recon_b", "lease-u-2", "auth-u-b", 10_000000, now.Add(-2*time.Minute))
	claimed, err = store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	_, err = store.SplitOutboxEvent(ctx, idB, testWalletOutboxWorkerID, 4_000000, "gwusg_recon_b:r1", true)
	require.NoError(t, err)

	// U row C — the split remainder (parent_event_id set, split_depth 1),
	// dead-lettered balance_shortfall: the receivable's row. The claim
	// grabs BOTH post-split pending rows; the parent goes back to pending
	// through MarkOutboxEventFailed with a fresh simulated now, whose 2 s
	// backoff keeps it unclaimable for the rest of this test (the V seed
	// below claims with limit 1 and the parent's next_attempt_at is now+2s).
	claimed, err = store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 2, "the post-split parent (pending again) and the remainder are both claimable")
	var rowBClaimed, rowCClaimed service.CanonicalWalletOutboxEvent
	for _, e := range claimed {
		if e.EventID == "gwusg_recon_b:r1" {
			rowCClaimed = e
		} else {
			rowBClaimed = e
		}
	}
	require.NotEmpty(t, rowCClaimed.EventID)
	require.NoError(t, store.MarkOutboxEventDeadLetter(ctx, rowCClaimed.ID, testWalletOutboxWorkerID, "balance_shortfall"))
	require.NoError(t, store.MarkOutboxEventFailed(ctx, rowBClaimed.ID, testWalletOutboxWorkerID, time.Now().UTC()))
	idC := rowCClaimed.ID

	// V — one delivered row that must never appear in U's read.
	idV := seedReconciliationOutboxRow(t, store, userV, "gwusg_recon_v", "lease-v-1", "", 9_000000, now.Add(-time.Minute))
	claimed, err = store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, "gwusg_recon_v", claimed[0].EventID, "only V is claimable — the failed parent is inside its 2 s backoff")
	require.NoError(t, store.MarkOutboxEventDelivered(ctx, idV, testWalletOutboxWorkerID))

	// The per-user read: exactly U's three rows, ordered by id.
	rows, truncated, err := store.ListOutboxEventsByUser(ctx, userU, since, until, 0, 5000)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, rows, 3)
	require.Equal(t, []int64{idA, idB, idC}, []int64{rows[0].ID, rows[1].ID, rows[2].ID}, "ordered by the BIGSERIAL id — the wire's cursor")

	rowA, rowB, rowC := rows[0], rows[1], rows[2]
	require.Equal(t, "gwusg_recon_a", rowA.EventID)
	require.Equal(t, "lease-u-1", rowA.LeaseID)
	require.Equal(t, "req-gwusg_recon_a", rowA.GatewayRequestID)
	require.Equal(t, "CNY", rowA.Currency)
	require.Equal(t, int64(2_000000), rowA.AmountUnits)
	require.Equal(t, "delivered", rowA.Status)
	require.Equal(t, 0, rowA.AttemptCount)
	require.Empty(t, rowA.DeadLetterReason)
	require.Empty(t, rowA.ParentEventID)
	require.Equal(t, 0, rowB.SplitDepth, "the parent's split depth")
	require.Equal(t, "auth-u-a", rowA.AuthorizationID)
	require.WithinDuration(t, now.Add(-3*time.Minute), rowA.OccurredAt, time.Second)
	require.NotNil(t, rowA.DeliveredAt, "a delivered row carries delivered_at")

	require.Equal(t, "pending", rowB.Status)
	require.Equal(t, int64(4_000000), rowB.AmountUnits, "the parent's amount is the post-split headroom")
	require.NotNil(t, rowB.PendingReleaseUnits)
	require.Equal(t, int64(6_000000), *rowB.PendingReleaseUnits, "pending_release_units is what the dispatcher still owes the lease")
	require.Nil(t, rowB.DeliveredAt)

	require.Equal(t, "dead_letter", rowC.Status)
	require.Equal(t, "balance_shortfall", rowC.DeadLetterReason)
	require.Equal(t, "gwusg_recon_b", rowC.ParentEventID)
	require.Equal(t, 1, rowC.SplitDepth)
	require.Equal(t, int64(6_000000), rowC.AmountUnits)
	require.Equal(t, "auth-u-b", rowC.AuthorizationID, "the split copies authorization_id to the remainder (§11.3)")

	// V's read sees only V.
	rowsV, _, err := store.ListOutboxEventsByUser(ctx, userV, since, until, 0, 5000)
	require.NoError(t, err)
	require.Len(t, rowsV, 1)
	require.Equal(t, "gwusg_recon_v", rowsV[0].EventID)

	// The window is half-open on occurred_at: a row at until is excluded,
	// a row at since is included.
	_, _, err = store.ListOutboxEventsByUser(ctx, userU, now.Add(-time.Minute), now.Add(-2*time.Minute), 0, 5000)
	require.NoError(t, err)

	// The cursor and the truncation signal: a limit below the row count
	// reports truncated, and afterID resumes without skipping or
	// duplicating a row.
	p1, truncated1, err := store.ListOutboxEventsByUser(ctx, userU, since, until, 0, 2)
	require.NoError(t, err)
	require.True(t, truncated1)
	require.Len(t, p1, 2)
	p2, truncated2, err := store.ListOutboxEventsByUser(ctx, userU, since, until, p1[1].ID, 5000)
	require.NoError(t, err)
	require.False(t, truncated2)
	require.Len(t, p2, 1)
	require.Equal(t, idC, p2[0].ID, "the keyset cursor resumes at exactly the unreceived row")

	// The watermark: the global high-water mark over the whole table.
	wm, err := store.DeliveredWatermark(ctx)
	require.NoError(t, err)
	require.NotNil(t, wm.DeliveredAtMax)
	var maxDelivered sql.NullTime
	var maxID, pending, inFlight, deadLetter int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT max(delivered_at), max(id),
		       count(*) FILTER (WHERE status = 'pending'),
		       count(*) FILTER (WHERE status = 'in_flight'),
		       count(*) FILTER (WHERE status = 'dead_letter')
		FROM wallet_settlement_outbox`).Scan(&maxDelivered, &maxID, &pending, &inFlight, &deadLetter))
	require.WithinDuration(t, maxDelivered.Time, *wm.DeliveredAtMax, time.Second)
	require.Equal(t, maxID, wm.OutboxIDMax)
	require.Equal(t, pending, wm.Pending)
	require.Equal(t, int64(1), wm.Pending)
	require.Equal(t, inFlight, wm.InFlight)
	require.Equal(t, int64(0), wm.InFlight)
	require.Equal(t, deadLetter, wm.DeadLetter)
	require.Equal(t, int64(1), wm.DeadLetter)

	// An empty table's watermark: NULL max delivered_at, id 0, zero counts.
	resetWalletOutboxTable(t)
	wmEmpty, err := store.DeliveredWatermark(ctx)
	require.NoError(t, err)
	require.Nil(t, wmEmpty.DeliveredAtMax)
	require.Equal(t, int64(0), wmEmpty.OutboxIDMax)
	require.Zero(t, wmEmpty.Pending+wmEmpty.InFlight+wmEmpty.DeadLetter)
}

// TestWalletReconciliationPerUserQueriesUseTheIndexes is the record's
// EXPLAIN (ANALYZE) leg: at a seeded volume of ≥ 10 000 outbox rows across
// ≥ 100 users (and the hold table at the same spread), both per-user
// queries must run as an index scan over 215's indexes — the whole point of
// migration 215 (round-1 MAJOR-1).
func TestWalletReconciliationPerUserQueriesUseTheIndexes(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	require.NoError(t, integrationDB.PingContext(ctx))

	const users = 120
	const rowsPerUser = 90 // 10 800 ≥ 10 000 per table
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	for u := 0; u < users; u++ {
		user := fmt.Sprintf("shipany-user-explain-%03d", u)
		for r := 0; r < rowsPerUser; r++ {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO wallet_settlement_outbox
					(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at)
				VALUES ($1, $2, 'lease-x', $3, 'CNY', 1000, 'hash', 'delivered', $4)`,
				fmt.Sprintf("gwusg_expl_%03d_%03d", u, r), user, fmt.Sprintf("req-%03d-%03d", u, r),
				time.Now().UTC().Add(-time.Duration(r)*time.Second))
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, `
				INSERT INTO wallet_hold_outcome (authorization_id, platform_user_id, lease_id, held_units, class, armed_at, classified_at)
				VALUES ($1, $2, 'lease-x', 500, 'indeterminate', $3, $3)`,
				fmt.Sprintf("auth_expl_%03d_%03d", u, r), user, time.Now().UTC().Add(-time.Duration(r)*time.Second))
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM wallet_settlement_outbox WHERE platform_user_id LIKE 'shipany-user-explain-%'`)
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM wallet_hold_outcome WHERE platform_user_id LIKE 'shipany-user-explain-%'`)
	})

	// ANALYZE so the planner sees the seeded volume, then EXPLAIN (ANALYZE)
	// both per-user queries and require an index scan over 215's indexes.
	_, err = integrationDB.ExecContext(ctx, `ANALYZE wallet_settlement_outbox`)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `ANALYZE wallet_hold_outcome`)
	require.NoError(t, err)

	explainOutbox := `EXPLAIN (ANALYZE) SELECT id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units,
			local_balance_after_units, occurred_at, attempt_count, status, dead_letter_reason, delivered_at,
			parent_event_id, split_depth, pending_release_units, authorization_id
		FROM wallet_settlement_outbox
		WHERE platform_user_id = $1 AND occurred_at >= $2 AND occurred_at < $3 AND id > $4
		ORDER BY id LIMIT $5`
	explainHolds := `EXPLAIN (ANALYZE) SELECT authorization_id, platform_user_id, lease_id, held_units, class, armed_at, classified_at, resolution, resolved_at, settlement_event_id
		FROM wallet_hold_outcome
		WHERE platform_user_id = $1 AND armed_at >= $2 AND armed_at < $3
		ORDER BY armed_at, authorization_id`

	since := time.Now().UTC().Add(-time.Hour)
	until := time.Now().UTC().Add(time.Hour)

	for name, tc := range map[string]struct {
		query string
		args  []any
		index string
	}{
		"outbox": {explainOutbox, []any{"shipany-user-explain-007", since, until, int64(0), 5000}, "idx_wallet_settlement_outbox_user_occurred"},
		"holds":  {explainHolds, []any{"shipany-user-explain-007", since, until}, "idx_wallet_hold_outcome_user_armed"},
	} {
		joined := explainQuery(t, ctx, tc.query, tc.args...)
		t.Logf("%s plan:\n%s", name, joined)
		require.Contains(t, joined, "Index Scan", "%s: the per-user query must be an index scan at the seeded volume", name)
		require.Contains(t, joined, tc.index, "%s: the planner must use %s", name, tc.index)
	}
}

func explainQuery(t *testing.T, ctx context.Context, query string, args ...any) string {
	t.Helper()
	rows, err := integrationDB.QueryContext(ctx, query, args...)
	require.NoError(t, err)
	defer rows.Close()
	joined := ""
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		joined += line + "\n"
	}
	require.NoError(t, rows.Err())
	return joined
}
