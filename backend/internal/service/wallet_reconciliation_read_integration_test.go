//go:build integration

package service

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startWalletReconciliationTestPostgres (Phase 4.1-G) builds the summary's
// full read surface on one fresh database of the shared container by
// reading the REAL migration files — the outbox chain (208/212/214), the
// billing snapshot (209), Live provisional (211), hold outcomes (213), and
// 215's indexes last. Unlike startCanonicalWalletTestPostgres's inline DDL
// (which must be kept in lockstep with 215 by hand — see its comment),
// this helper executes the shipped SQL itself, so test 69's tables always
// carry exactly what production carries, indexes included.
func startWalletReconciliationTestPostgres(t testing.TB, ctx context.Context) *sql.DB {
	t.Helper()
	db := SharedTestPostgresDBForTest(t)
	for _, migration := range []string{
		"208_wallet_settlement_outbox.sql",
		"209_wallet_billing_snapshot.sql",
		"211_wallet_live_provisional.sql",
		"212_wallet_outbox_dead_letter_reason.sql",
		"213_wallet_hold_outcome.sql",
		"214_wallet_outbox_split_and_authorization.sql",
		"215_wallet_reconciliation_indexes.sql",
	} {
		sqlContent, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(sqlContent))
		require.NoError(t, err)
	}
	return db
}

// TestWalletHoldOutcomeListByUser (Phase 4.1-G Task 1b): one row per
// resolution of migration 213's documented domain — settled (with its
// settlement_event_id), abandoned, expired, and NULL (open) — seeded
// through the store's own writers, all returned by ListHoldOutcomesByUser
// with resolution as *string.
func TestWalletHoldOutcomeListByUser(t *testing.T) {
	ctx := context.Background()
	db := startWalletHoldOutcomeTestPostgres(t, ctx)
	store := &walletHoldOutcomeStore{db: db}
	now := time.Now().UTC().Truncate(time.Microsecond)
	user := "shipany-user-hold-read"
	other := "shipany-user-hold-other"

	// settled: open row, then resolved by the settlement's event id.
	settledHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_settled", LeaseID: "lease-h1",
		HeldUnits: 11_000000, ArmedAt: now.Add(-10 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, settledHold, user, now.Add(-9*time.Minute)))
	require.NoError(t, store.MarkSettled(ctx, settledHold.AuthorizationID, "gwusg_hold_settled", now.Add(-8*time.Minute)))

	// abandoned: the reaper's release.
	abandonedHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_abandoned", LeaseID: "lease-h2",
		HeldUnits: 22_000000, ArmedAt: now.Add(-7 * time.Minute), Class: "", State: "armed",
	}
	require.NoError(t, store.InsertAbandoned(ctx, abandonedHold, user, now.Add(-6*time.Minute)))

	// expired: an open row older than the reaper's cutoff.
	expiredHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_expired", LeaseID: "lease-h3",
		HeldUnits: 33_000000, ArmedAt: now.Add(-5 * time.Minute), Class: "not_written", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, expiredHold, user, now.Add(-4*time.Minute)))
	n, err := store.MarkExpiredOlderThan(ctx, now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// open: resolution stays NULL.
	openHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_open", LeaseID: "lease-h4",
		HeldUnits: 44_000000, ArmedAt: now.Add(-1 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, openHold, user, now))

	// another user's row: never in this user's list.
	otherHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_other", LeaseID: "lease-h5",
		HeldUnits: 55_000000, ArmedAt: now.Add(-3 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, otherHold, other, now))

	rows, err := store.ListHoldOutcomesByUser(ctx, user, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 4, "exactly this user's four resolutions")

	byAuth := map[string]WalletHoldOutcome{}
	for _, r := range rows {
		byAuth[r.AuthorizationID] = r
	}
	require.Contains(t, byAuth, "auth_hold_settled")
	require.Contains(t, byAuth, "auth_hold_abandoned")
	require.Contains(t, byAuth, "auth_hold_expired")
	require.Contains(t, byAuth, "auth_hold_open")

	s := byAuth["auth_hold_settled"]
	require.Equal(t, user, s.PlatformUserID)
	require.Equal(t, "lease-h1", s.LeaseID)
	require.Equal(t, int64(11_000000), s.HeldUnits)
	require.Equal(t, "indeterminate", s.Class)
	require.NotNil(t, s.Resolution)
	require.Equal(t, "settled", *s.Resolution)
	require.NotNil(t, s.ResolvedAt)
	require.NotNil(t, s.SettlementEventID)
	require.Equal(t, "gwusg_hold_settled", *s.SettlementEventID)
	require.WithinDuration(t, now.Add(-10*time.Minute), s.ArmedAt, time.Second)
	require.WithinDuration(t, now.Add(-9*time.Minute), s.ClassifiedAt, time.Second)

	a := byAuth["auth_hold_abandoned"]
	require.NotNil(t, a.Resolution)
	require.Equal(t, "abandoned", *a.Resolution)
	require.NotNil(t, a.ResolvedAt)
	require.Nil(t, a.SettlementEventID)
	require.Empty(t, a.Class, "the unclassified orphan's class is stored as it was")

	e := byAuth["auth_hold_expired"]
	require.NotNil(t, e.Resolution)
	require.Equal(t, "expired", *e.Resolution)
	require.Nil(t, e.SettlementEventID)

	o := byAuth["auth_hold_open"]
	require.Nil(t, o.Resolution, "an open hold's resolution is NULL")
	require.Nil(t, o.ResolvedAt)
	require.Nil(t, o.SettlementEventID)

	// the window is over armed_at, half-open
	onlyLate, err := store.ListHoldOutcomesByUser(ctx, user, now.Add(-90*time.Second), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, onlyLate, 1)
	require.Equal(t, "auth_hold_open", onlyLate[0].AuthorizationID)

	otherRows, err := store.ListHoldOutcomesByUser(ctx, other, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, otherRows, 1)
	require.Equal(t, "auth_hold_other", otherRows[0].AuthorizationID)
}

// TestLiveProvisionalListByUser (Phase 4.1-G Task 1c): a record with two
// windows seeded by a direct INSERT mirroring migration 211's shape
// (saveLiveProvisional is a gateway-service method needing an
// authorization handle — not a seeding path), returned with both windows
// decoded and the fx LEFT JOIN degrading to nil for an empty
// billing_snapshot_id (round-3 fx fold; the snapshot-backed leg is test
// 69's).
func TestLiveProvisionalListByUser(t *testing.T) {
	ctx := context.Background()
	db := startWalletReconciliationTestPostgres(t, ctx)
	// the concrete store, not the LiveProvisionalStore port: the read
	// method is part of the reconciliation read model, not the gateway's
	// durable-port surface (the port's callers never list by user).
	store := &liveProvisionalStore{db: db}
	require.NotNil(t, store)

	user := "shipany-user-live-read"
	callHash := "live_callhash_read01"
	now := time.Now().UTC().Truncate(time.Microsecond)

	_, err := db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at, activated_at)
		VALUES ($1, $2, $3, $4, 101, 202, 303, 'CNY', '', 7_000000, 'active', $5::jsonb, '', $6, $6)`,
		"auth_live_read", "auth_live_read", callHash, user,
		`[{"window_seq":1,"lease_id":"lease-l1","token":"auth_live_read","pending_units":0,"settled_units":3000000,"opened_at_ms":1000},
		   {"window_seq":2,"lease_id":"lease-l2","token":"auth_live_read","pending_units":5,"settled_units":4000000,"opened_at_ms":2000}]`,
		now,
	)
	require.NoError(t, err)

	// another user's record stays out.
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at)
		VALUES ('auth_live_other', 'auth_live_other', 'live_callhash_other', $1, 1, 2, 3, 'CNY', '', 100, 'finalized', '[]'::jsonb, 'gwusg_other', now())`,
		"shipany-user-live-other",
	)
	require.NoError(t, err)

	rows, err := store.ListLiveProvisionalByUser(ctx, user, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	rec := rows[0]
	require.Equal(t, "auth_live_read", rec.Token)
	require.Equal(t, "auth_live_read", rec.AuthorizationID)
	require.Equal(t, callHash, rec.CallHash)
	require.Equal(t, user, rec.PlatformUserID)
	require.Equal(t, "CNY", rec.BillingCurrency)
	require.Equal(t, "", rec.BillingSnapshotID)
	require.Nil(t, rec.BillingFX, "an empty billing_snapshot_id degrades to null fx — never drops the record")
	require.Equal(t, "active", rec.Status)
	require.Equal(t, int64(7_000000), rec.EstimatedUnits)
	require.Empty(t, rec.SettlementEventID)
	require.WithinDuration(t, now, rec.CreatedAt, time.Second)
	require.Len(t, rec.Windows, 2, "both windows decoded from the JSON column")
	require.Equal(t, 1, rec.Windows[0].WindowSeq)
	require.Equal(t, "lease-l1", rec.Windows[0].LeaseID)
	require.Equal(t, int64(3_000000), rec.Windows[0].SettledUnits)
	require.Equal(t, int64(0), rec.Windows[0].PendingUnits)
	require.Equal(t, int64(1000), rec.Windows[0].OpenedAtMS)
	require.Equal(t, 2, rec.Windows[1].WindowSeq)
	require.Equal(t, "lease-l2", rec.Windows[1].LeaseID)
	require.Equal(t, int64(4_000000), rec.Windows[1].SettledUnits)
	require.Equal(t, int64(5), rec.Windows[1].PendingUnits)
	require.Equal(t, int64(2000), rec.Windows[1].OpenedAtMS)
}
