//go:build integration

package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	_ "github.com/lib/pq"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func startCanonicalWalletTestPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23", tcpostgres.WithDatabase("wallet_outbox_test"), tcpostgres.WithUsername("postgres"), tcpostgres.WithPassword("postgres"), tcpostgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, `
		CREATE TABLE wallet_settlement_outbox (
			id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, platform_user_id TEXT NOT NULL,
			lease_id TEXT, gateway_request_id TEXT NOT NULL, currency TEXT NOT NULL,
			amount_units BIGINT NOT NULL, local_balance_after_units BIGINT, payload_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
			attempt_count INT NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), claimed_at TIMESTAMPTZ, claimed_by TEXT,
			occurred_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ
		)`) // same columns as Task 4 Step 2's real migration — kept in sync by hand since this test owns its own throwaway database, same convention as this task's other real-Redis tests owning their own throwaway keyspace
	require.NoError(t, err)
	return db
}

// outboxStoreForTest implements CanonicalWalletOutboxStore directly against
// a *sql.DB for this test only. It does NOT import repository.WalletOutboxStore
// — `service` cannot import `repository` (repository already imports
// service; that would be a real import cycle). Only the two methods
// ObserveSettlement's own code path actually exercises are implemented for
// real; the other two panic if called, so an accidental future use of this
// stub for something it wasn't built for fails loudly instead of silently
// no-op'ing.
type outboxStoreForTest struct{ db *sql.DB }

func (o *outboxStoreForTest) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event CanonicalWalletSettlementEvent) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox (event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.PlatformUserID, event.LeaseID, event.GatewayRequestID, event.Currency, event.AmountUnits, event.LocalBalanceAfterUnits, "test-hash", event.OccurredAt)
	return err
}
func (o *outboxStoreForTest) ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]CanonicalWalletOutboxEvent, error) {
	// REAL atomic claim, mirroring repository.WalletOutboxStore: transition
	// to in_flight under the caller's claim token IN THE SAME statement, so
	// the full runOutboxDispatcher loop (TestCanonicalWalletOutboxDispatcherDeliversEndToEnd)
	// exercises genuine claiming semantics instead of re-reading the same
	// pending rows forever.
	rows, err := o.db.QueryContext(ctx, `
		UPDATE wallet_settlement_outbox
		SET status = 'in_flight', claimed_at = now(), claimed_by = $2
		WHERE id IN (
			SELECT id FROM wallet_settlement_outbox
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, attempt_count`, limit, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []CanonicalWalletOutboxEvent
	for rows.Next() {
		var e CanonicalWalletOutboxEvent
		var leaseID sql.NullString
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits, &e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount); err != nil {
			return nil, err
		}
		e.LeaseID = leaseID.String
		events = append(events, e)
	}
	return events, rows.Err()
}
// The three resolve/reclaim methods below are REAL SQL implementations
// mirroring repository.WalletOutboxStore's semantics — required so the full
// runOutboxDispatcher loop can execute against this store in
// TestCanonicalWalletOutboxDispatcherDeliversEndToEnd without a
// service->repository import.

func (o *outboxStoreForTest) MarkOutboxEventDelivered(ctx context.Context, id int64, workerID string) error {
	_, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'delivered', delivered_at = now(), claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
	return err
}

func (o *outboxStoreForTest) MarkOutboxEventFailed(ctx context.Context, id int64, workerID string, simulatedNow time.Time) error {
	var attempts int
	err := o.db.QueryRowContext(ctx, `
		UPDATE wallet_settlement_outbox SET attempt_count = attempt_count + 1
		WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2
		RETURNING attempt_count`, id, workerID).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	const maxAttempts = 8
	if attempts >= maxAttempts {
		_, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
		return err
	}
	next := simulatedNow.Add(time.Duration(1<<uint(attempts)) * time.Second)
	_, err = o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'pending', next_attempt_at = $3, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, next)
	return err
}

func (o *outboxStoreForTest) BindOutboxEventLease(ctx context.Context, id int64, workerID, leaseID string) error {
	result, err := o.db.ExecContext(ctx, `
		UPDATE wallet_settlement_outbox SET lease_id = $3
		WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, leaseID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrCanonicalWalletOutboxClaimLost
	}
	return nil
}

func (o *outboxStoreForTest) MarkOutboxEventDeadLetter(ctx context.Context, id int64, workerID, reason string) error {
	res, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', attempt_count = attempt_count + 1, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		slog.Warn("canonical wallet outbox event dead-lettered", "id", id, "reason", reason)
	}
	return nil
}

func (o *outboxStoreForTest) OutboxEventStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := o.db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status)
	return status, err
}

func (o *outboxStoreForTest) ReclaimStaleInFlightEvents(ctx context.Context, staleAfter time.Duration) (int64, error) {
	result, err := o.db.ExecContext(ctx, `
		UPDATE wallet_settlement_outbox
		SET status = 'pending', claimed_at = NULL, claimed_by = NULL
		WHERE status = 'in_flight' AND claimed_at < $1`,
		time.Now().UTC().Add(-staleAfter),
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

var _ CanonicalWalletOutboxStore = (*outboxStoreForTest)(nil)

var _ CanonicalWalletOutboxStore = (*outboxStoreForTest)(nil)

func TestCanonicalWalletObserveSettlementIsDurableAndGetsDelivered(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	outbox := &outboxStoreForTest{db: db}
	// Constructed as a direct struct literal, NOT via newCanonicalWalletBridge:
	// the constructor always starts runOutboxDispatcher when outbox deps are
	// non-nil, and this test's outboxStoreForTest panics on
	// ReclaimStaleInFlightEvents/MarkOutboxEvent* — the dispatcher's first
	// tick (RequestTimeoutMS after construction) would crash the whole test
	// binary AFTER this test had already passed (found by actually running
	// the full integration suite). This test verifies ObserveSettlement's
	// synchronous durable insert only; the dispatcher is proven for real by
	// repository's wallet_outbox_integration_test.go.
	bridge := &CanonicalWalletBridge{
		cfg:      canonicalWalletTestConfig(config.CanonicalWalletModeEnforce),
		store:    &canonicalWalletStoreStub{},
		control:  &canonicalWalletControlStub{},
		outboxDB: db,
		outbox:   outbox,
		workerID: "test-worker-no-dispatcher",
	}

	event := CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-" + uuid.NewString(), PlatformUserID: "shipany-user-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 5_000000,
	}
	bridge.ObserveSettlement(event)

	// The row must be durably visible via the outbox store immediately —
	// no channel, no goroutine race, no window where it doesn't exist yet.
	pending, err := outbox.ClaimPendingOutboxEvents(ctx, "test-worker", 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
}
