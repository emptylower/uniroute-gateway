//go:build integration

package repository

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Test 76 (Phase 4.2-G Task 1, plan constraint 3): the outbox payload hash is
// INDIFFERENT to the billing snapshot — a pre-4.2 row's payload_hash must not
// change when the same event is re-observed carrying one. The mechanism is
// walletOutboxHashPayload's fixed seven fields (the 3.5 AuthorizationID
// precedent): BillingSnapshotID lives on the event and in its own column, and
// never enters the hashed encoding. The reflection leg pins the struct's
// exact shape so a future field addition fails HERE instead of silently
// invalidating every stored hash (round-1 MAJOR-2).
func TestWalletOutboxPayloadHashIgnoresBillingSnapshotID(t *testing.T) {
	occurred := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	base := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_76a_base", GatewayRequestID: "req-76a", PlatformUserID: "shipany-user-76a",
		Currency: "CNY", AmountUnits: 12_340000, OccurredAt: occurred,
	}
	withSnapshot := base
	withSnapshot.BillingSnapshotID = "wbs_76a_snapshot"
	require.Equal(t, walletOutboxPayloadHash(base), walletOutboxPayloadHash(withSnapshot),
		"the hash must not change when the event carries a billing snapshot id")
	withSnapshot.BillingSnapshotID = "wbs_76a_different"
	require.Equal(t, walletOutboxPayloadHash(base), walletOutboxPayloadHash(withSnapshot),
		"the hash must not change for any snapshot id value")

	// The struct is the contract: exactly its seven fields, by name, in
	// order. A field added here changes every stored payload_hash's
	// meaning — this assertion is the loud failure that prevents it.
	rt := reflect.TypeOf(walletOutboxHashPayload{})
	require.Equal(t, 7, rt.NumField(), "walletOutboxHashPayload must have exactly its seven fields")
	want := []string{
		"PlatformUserID", "LeaseID", "Currency", "GatewayRequestID",
		"AmountUnits", "LocalBalanceAfterUnits", "OccurredAt",
	}
	got := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		got = append(got, rt.Field(i).Name)
	}
	require.Equal(t, want, got)
}

// TestWalletOutboxBaselineRowRedeliversWithSnapshot (test 76's durability
// leg): a row inserted at BASELINE's shape — the pre-4.2 column set, hash
// computed over the seven-field payload — accepts the post-4.2 re-observation
// of the same event now carrying a billing snapshot id as the IDENTICAL
// event (no payload conflict), and redelivers through the normal
// claim→deliver path.
func TestWalletOutboxBaselineRowRedeliversWithSnapshot(t *testing.T) {
	resetWalletOutboxTable(t)
	ctx := context.Background()
	store := NewWalletOutboxStore(integrationDB)

	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_76b_redeliver", GatewayRequestID: "req-76b", PlatformUserID: "shipany-user-76b",
		Currency: "CNY", AmountUnits: 5_670000, OccurredAt: time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute),
	}
	// Baseline's INSERT: exactly the pre-4.2 column list, the hash the
	// seven-field payload produces.
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at)
		VALUES ($1, $2, $3, 'CNY', $4, $5, 'pending', $6)`,
		event.EventID, event.PlatformUserID, event.GatewayRequestID, event.AmountUnits,
		walletOutboxPayloadHash(event), event.OccurredAt)
	require.NoError(t, err)

	// The post-4.2 re-observation: the same event, now carrying the
	// snapshot id the callers hold. Same hash → the identical no-op.
	withSnapshot := event
	withSnapshot.BillingSnapshotID = "wbs_76b_backfilled"
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, withSnapshot))
	require.NoError(t, tx.Commit())

	var n int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT count(*) FROM wallet_settlement_outbox WHERE event_id = $1`, event.EventID).Scan(&n))
	require.Equal(t, 1, n, "the identical re-observation is a no-op, never a second row")

	// A FRESH post-4.2 observation writes its snapshot id into the column.
	fresh := event
	fresh.EventID, fresh.GatewayRequestID = "gwusg_76b_fresh", "req-76b-fresh"
	fresh.BillingSnapshotID = "wbs_76b_fresh"
	tx2, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx2, fresh))
	require.NoError(t, tx2.Commit())
	var snapID sql.NullString
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE event_id = $1`, fresh.EventID).Scan(&snapID))
	require.True(t, snapID.Valid, "a fresh insert persists the event's billing snapshot id")
	require.Equal(t, "wbs_76b_fresh", snapID.String)
	var baselineSnapID sql.NullString
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE event_id = $1`, event.EventID).Scan(&baselineSnapID))
	require.False(t, baselineSnapID.Valid, "the identical re-observation does not rewrite the baseline row's snapshot column")

	// Redelivery through the normal path: claim both pending rows and
	// deliver them — never payload_conflict.
	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	for _, e := range claimed {
		require.NoError(t, store.MarkOutboxEventDelivered(ctx, e.ID, testWalletOutboxWorkerID))
	}
	for _, id := range []string{event.EventID, fresh.EventID} {
		var status string
		require.NoError(t, integrationDB.QueryRowContext(ctx,
			`SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, id).Scan(&status))
		require.Equal(t, "delivered", status)
	}
}
