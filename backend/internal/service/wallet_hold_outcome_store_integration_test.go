//go:build integration

package service

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// startWalletHoldOutcomeTestPostgres applies the wallet migrations by
// reading the files directly (the live_provisional_store_integration_test.go
// pattern): ApplyMigrations lives in internal/repository, which imports
// internal/service — calling it from here would be an import cycle.
// Phase 3.7c: a database on the one shared container per run; the
// migration is unchanged.
//
// Phase 4.1-G (round-2 MAJOR-1): the helper now reads the full wallet
// chain — 208/212/214 build wallet_settlement_outbox (213 alone is no
// longer enough, because 215's index migration also touches the outbox),
// then 213, then 215 LAST so this database carries exactly the indexes
// 215 ships (the inline DDL of startCanonicalWalletTestPostgres and the
// migration file must stay in lockstep; this helper reads the real file,
// so it always does).
func startWalletHoldOutcomeTestPostgres(t testing.TB, ctx context.Context) *sql.DB {
	t.Helper()
	db := SharedTestPostgresDBForTest(t)
	for _, migration := range []string{
		"208_wallet_settlement_outbox.sql",
		"209_wallet_billing_snapshot.sql", // 218's foreign-key target
		"212_wallet_outbox_dead_letter_reason.sql",
		"213_wallet_hold_outcome.sql",
		"214_wallet_outbox_split_and_authorization.sql",
		"215_wallet_reconciliation_indexes.sql",
		"218_wallet_authorization_segments.sql", // the hold-outcome store reads it
		"219_wallet_attempt_protection.sql",
	} {
		sqlContent, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(sqlContent))
		require.NoError(t, err)
	}
	createWalletMediaTaskTableForTest(t, ctx, db) // see its comment: 217 itself needs users/api_keys
	return db
}
func TestWalletHoldOutcomeStoreWriters(t *testing.T) {
	ctx := context.Background()
	db := startWalletHoldOutcomeTestPostgres(t, ctx)
	store := &walletHoldOutcomeStore{db: db}
	now := time.Now().UTC().Truncate(time.Microsecond)

	hold := CanonicalWalletHold{
		AuthorizationID: "auth_" + uuid.NewString(), LeaseID: "srv-lease-1",
		HeldUnits: 12_345, ArmedAt: now.Add(-time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, hold, "shipany-user-1", now))
	// a second insert is a no-op (overlapping sweeps must not error)
	require.NoError(t, store.InsertIndeterminate(ctx, hold, "shipany-user-1", now.Add(time.Second)))

	var resolution *string
	var class string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT resolution, class FROM wallet_hold_outcome WHERE authorization_id = $1`, hold.AuthorizationID).Scan(&resolution, &class))
	require.Nil(t, resolution, "an indeterminate row is open")
	require.Equal(t, "indeterminate", class)

	// MarkSettled sets the settled resolution with the event id
	require.NoError(t, store.MarkSettled(ctx, hold.AuthorizationID, "gwusg_settled", now.Add(time.Minute)))
	var settled, eventID string
	var resolvedAt time.Time
	require.NoError(t, db.QueryRowContext(ctx, `SELECT resolution, settlement_event_id, resolved_at FROM wallet_hold_outcome WHERE authorization_id = $1`, hold.AuthorizationID).Scan(&settled, &eventID, &resolvedAt))
	require.Equal(t, "settled", settled)
	require.Equal(t, "gwusg_settled", eventID)
	require.WithinDuration(t, now.Add(time.Minute), resolvedAt, time.Second)
	// a second MarkSettled is a no-op (resolution IS NULL guard)
	require.NoError(t, store.MarkSettled(ctx, hold.AuthorizationID, "gwusg_other", now.Add(2*time.Minute)))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT settlement_event_id FROM wallet_hold_outcome WHERE authorization_id = $1`, hold.AuthorizationID).Scan(&eventID))
	require.Equal(t, "gwusg_settled", eventID, "an already-resolved row is never rewritten")

	// InsertAbandoned writes the abandoned resolution, twice idempotent
	orphan := CanonicalWalletHold{
		AuthorizationID: "auth_" + uuid.NewString(), LeaseID: "srv-lease-2",
		HeldUnits: 999, ArmedAt: now.Add(-2 * time.Minute), Class: "", State: "armed",
	}
	require.NoError(t, store.InsertAbandoned(ctx, orphan, "shipany-user-2", now))
	require.NoError(t, store.InsertAbandoned(ctx, orphan, "shipany-user-2", now))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT resolution, class FROM wallet_hold_outcome WHERE authorization_id = $1`, orphan.AuthorizationID).Scan(&resolution, &class))
	require.NotNil(t, resolution)
	require.Equal(t, "abandoned", *resolution)
	require.Equal(t, "", class, "the unclassified orphan's class is stored as it was")

	// MarkExpiredOlderThan marks ONLY open rows older than the cutoff
	oldOpen := CanonicalWalletHold{
		AuthorizationID: "auth_" + uuid.NewString(), LeaseID: "srv-lease-3",
		HeldUnits: 1, ArmedAt: now.Add(-3 * time.Hour), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, oldOpen, "shipany-user-3", now))
	n, err := store.MarkExpiredOlderThan(ctx, now.Add(-time.Hour), now)
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "the settled and abandoned rows are untouched; only the old open row expires")
	require.NoError(t, db.QueryRowContext(ctx, `SELECT resolution FROM wallet_hold_outcome WHERE authorization_id = $1`, oldOpen.AuthorizationID).Scan(&resolution))
	require.NotNil(t, resolution)
	require.Equal(t, "expired", *resolution)

	// a nil store is a no-op on every method
	var nilStore *walletHoldOutcomeStore
	require.NoError(t, nilStore.InsertIndeterminate(ctx, hold, "u", now))
	require.NoError(t, nilStore.InsertAbandoned(ctx, hold, "u", now))
	require.NoError(t, nilStore.MarkSettled(ctx, hold.AuthorizationID, "e", now))
	n, err = nilStore.MarkExpiredOlderThan(ctx, now, now)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}
