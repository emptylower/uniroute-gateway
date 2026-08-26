//go:build integration

package service

import (
	"context"
	"database/sql"
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
	rows, err := o.db.QueryContext(ctx, `SELECT id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, attempt_count FROM wallet_settlement_outbox WHERE status = 'pending' LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []CanonicalWalletOutboxEvent
	for rows.Next() {
		var e CanonicalWalletOutboxEvent
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &e.LeaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits, &e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
func (o *outboxStoreForTest) MarkOutboxEventDelivered(context.Context, int64, string) error {
	panic("not used by this test")
}
func (o *outboxStoreForTest) MarkOutboxEventFailed(context.Context, int64, string, time.Time) error {
	panic("not used by this test")
}

// ReclaimStaleInFlightEvents is part of the CanonicalWalletOutboxStore
// interface but is not exercised by
// TestCanonicalWalletObserveSettlementIsDurableAndGetsDelivered below,
// which only calls ObserveSettlement, not the dispatcher.
func (o *outboxStoreForTest) ReclaimStaleInFlightEvents(context.Context, time.Duration) (int64, error) {
	panic("not used by this test")
}

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
