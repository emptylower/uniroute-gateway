//go:build integration

package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	_ "github.com/lib/pq"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func startCanonicalWalletTestPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	// Phase 3.7c: a database on the one shared container per run (the DDL
	// below is unchanged — this helper's contract is a fresh *sql.DB with
	// exactly the outbox table).
	db := SharedTestPostgresDBForTest(t)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE wallet_settlement_outbox (
			id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, platform_user_id TEXT NOT NULL,
			lease_id TEXT, gateway_request_id TEXT NOT NULL, currency TEXT NOT NULL,
			amount_units BIGINT NOT NULL, local_balance_after_units BIGINT, payload_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
			attempt_count INT NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), claimed_at TIMESTAMPTZ, claimed_by TEXT,
			occurred_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ, dead_letter_reason TEXT,
			authorization_id TEXT, parent_event_id TEXT, split_depth INTEGER NOT NULL DEFAULT 0, pending_release_units BIGINT,
			billing_snapshot_id TEXT, redrive_count INT NOT NULL DEFAULT 0
		)`) // same columns as Task 4 Step 2's real migration (+ 212's dead_letter_reason + 214's split/authorization columns + 216's billing_snapshot_id/redrive_count — lockstep by hand, the 215 rule) — kept in sync by hand since this test owns its own throwaway database, same convention as this task's other real-Redis tests owning their own throwaway keyspace
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE INDEX idx_wallet_settlement_outbox_parent ON wallet_settlement_outbox (parent_event_id)`)
	require.NoError(t, err)
	// Phase 4.1-G (round-2 MAJOR-1): the per-user read index of migration
	// 215, INLINE here in lockstep with the migration file — this helper
	// builds the outbox by hand, so 215 is NOT read here; when 215 changes
	// this DDL must change with it (Task 4 greps both for the index name).
	// 215's wallet_hold_outcome index lands in the helpers that own that
	// table (startWalletHoldOutcomeTestPostgres reads 215 after 213;
	// startWalletReconciliationTestPostgres reads every migration).
	_, err = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_user_occurred ON wallet_settlement_outbox (platform_user_id, occurred_at, id)`)
	require.NoError(t, err)
	// Migrations 218 + 219: ObserveSettlement reads wallet_authorization_segment before it
	// settles and fails closed when the table is missing. Inline here, in lockstep with the
	// two migration files, for the same reason as the outbox above. The billing_snapshot_id
	// foreign key of 218 is left out: this helper does not own wallet_billing_snapshot, and
	// tests that need it apply migration 209 on top.
	_, err = db.ExecContext(ctx, `
		CREATE TABLE wallet_authorization_segment (
			parent_authorization_id TEXT NOT NULL,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			authorization_id TEXT NOT NULL UNIQUE,
			platform_user_id TEXT NOT NULL,
			billing_snapshot_id TEXT NOT NULL,
			lease_id TEXT NOT NULL,
			held_units BIGINT NOT NULL CHECK (held_units > 0),
			lease_basis JSONB NOT NULL,
			event_id TEXT UNIQUE,
			actual_units BIGINT NOT NULL DEFAULT 0 CHECK (actual_units >= 0 AND actual_units <= held_units),
			pin_state TEXT NOT NULL DEFAULT 'none' CHECK (pin_state IN ('none','active','finished')),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			kind TEXT NOT NULL DEFAULT 'llm' CHECK (kind IN ('llm','live','media')),
			state TEXT NOT NULL DEFAULT 'prepared' CHECK (state IN ('prepared','held','indeterminate','settling','released','finished')),
			authorization_token TEXT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			settlement_payload JSONB,
			remainder_payload JSONB,
			PRIMARY KEY (parent_authorization_id, ordinal),
			CHECK ((ordinal = 0 AND authorization_id = parent_authorization_id) OR ordinal > 0)
		)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE INDEX idx_wallet_authorization_segment_lease ON wallet_authorization_segment (platform_user_id, lease_id)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE INDEX idx_wallet_authorization_segment_pending ON wallet_authorization_segment (updated_at) WHERE state <> 'finished'`)
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
type outboxStoreForTest struct {
	db          *sql.DB
	maxAttempts int
	backoff     func(attempts int, simulatedNow time.Time) time.Time
}

func (o *outboxStoreForTest) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event CanonicalWalletSettlementEvent) error {
	// Mirrors repository.WalletOutboxStore's identity contract since Phase
	// 3.5 (test 40): a retry of the identical event is a no-op; a repriced
	// resubmission under the same event id is a payload conflict. The hash
	// is the real algorithm (testWalletOutboxPayloadHash) so the split's
	// remainder rows compare consistently.
	hash := testWalletOutboxPayloadHash(event)
	var authorizationID any
	if event.AuthorizationID != "" {
		authorizationID = event.AuthorizationID
	}
	var billingSnapshotID any
	if event.BillingSnapshotID != "" {
		billingSnapshotID = event.BillingSnapshotID
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT wallet_outbox_insert`); err != nil {
		return err
	}
	res, insertErr := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox (event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, occurred_at, authorization_id, billing_snapshot_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.PlatformUserID, event.LeaseID, event.GatewayRequestID, event.Currency, event.AmountUnits, event.LocalBalanceAfterUnits, hash, event.OccurredAt, authorizationID, billingSnapshotID)
	if insertErr != nil {
		return insertErr
	}
	if n, _ := res.RowsAffected(); n == 1 {
		_, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT wallet_outbox_insert`)
		return err
	}
	// The event id exists — identical payload: no-op; different: conflict.
	var existingHash string
	if err := tx.QueryRowContext(ctx, `SELECT payload_hash FROM wallet_settlement_outbox WHERE event_id = $1`, event.EventID).Scan(&existingHash); err != nil {
		return err
	}
	if existingHash != hash {
		return ErrCanonicalWalletOutboxPayloadConflict
	}
	return nil
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
		RETURNING id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, attempt_count, parent_event_id, split_depth, pending_release_units, authorization_id`, limit, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []CanonicalWalletOutboxEvent
	for rows.Next() {
		var e CanonicalWalletOutboxEvent
		var leaseID sql.NullString
		var parentEventID, authorizationID sql.NullString
		var pendingRelease sql.NullInt64
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits, &e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount, &parentEventID, &e.SplitDepth, &pendingRelease, &authorizationID); err != nil {
			return nil, err
		}
		e.LeaseID = leaseID.String
		e.ParentEventID = parentEventID.String
		e.AuthorizationID = authorizationID.String
		if pendingRelease.Valid {
			v := pendingRelease.Int64
			e.PendingReleaseUnits = &v
		}
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
	maxAttempts := 8
	if o.maxAttempts > 0 {
		maxAttempts = o.maxAttempts
	}
	if attempts >= maxAttempts {
		_, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', dead_letter_reason = 'attempts_exhausted', claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
		return err
	}
	next := simulatedNow.Add(time.Duration(1<<uint(attempts)) * time.Second)
	if o.backoff != nil {
		next = o.backoff(attempts, simulatedNow)
	}
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
	res, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', dead_letter_reason = $3, attempt_count = attempt_count + 1, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, reason)
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

// testWalletOutboxPayloadHash is the service-package twin of
// repository.walletOutboxPayloadHash (service cannot import repository):
// the same fixed-shape struct, the same field order — identical hashes for
// identical events. The struct's shape is §11.3's contract; if either copy
// changes, both must.
type testWalletOutboxHashPayload struct {
	PlatformUserID         string
	LeaseID                string
	Currency               string
	GatewayRequestID       string
	AmountUnits            int64
	LocalBalanceAfterUnits *int64
	OccurredAt             string
}

func testWalletOutboxPayloadHash(event CanonicalWalletSettlementEvent) string {
	payload := testWalletOutboxHashPayload{
		PlatformUserID: event.PlatformUserID, LeaseID: event.LeaseID, Currency: event.Currency,
		GatewayRequestID: event.GatewayRequestID, AmountUnits: event.AmountUnits,
		LocalBalanceAfterUnits: event.LocalBalanceAfterUnits, OccurredAt: event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// SplitOutboxEvent / ClearPendingRelease / SumDeadLetterUnits mirror
// repository.WalletOutboxStore's implementations (Phase 3.5, §11.3/§11.4) —
// the Phase 35 dispatcher tests drive the real split through this store.
func (o *outboxStoreForTest) SplitOutboxEvent(ctx context.Context, id int64, workerID string, capturedUnits int64, remainderEventID string, reserved bool) (int64, error) {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var (
		eventID, platformUserID, gatewayRequestID, currency string
		amountUnits                                         int64
		localBalance                                        sql.NullInt64
		occurredAt                                          time.Time
		splitDepth                                          int
		billingSnapshotID                                   sql.NullString
		authorizationID                                     sql.NullString
	)
	err = tx.QueryRowContext(ctx, `
		SELECT event_id, platform_user_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, split_depth, billing_snapshot_id, authorization_id
		FROM wallet_settlement_outbox
		WHERE id = $1 AND claimed_by = $2 AND status = 'in_flight'
		FOR UPDATE`, id, workerID,
	).Scan(&eventID, &platformUserID, &gatewayRequestID, &currency, &amountUnits, &localBalance, &occurredAt, &splitDepth, &billingSnapshotID, &authorizationID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrCanonicalWalletOutboxClaimLost
	}
	if err != nil {
		return 0, err
	}
	if capturedUnits < 0 || capturedUnits > amountUnits {
		return 0, fmt.Errorf("wallet outbox split captured units %d out of range for amount %d", capturedUnits, amountUnits)
	}
	remainderUnits := amountUnits - capturedUnits

	remainderEvent := CanonicalWalletSettlementEvent{
		EventID: remainderEventID, GatewayRequestID: gatewayRequestID, PlatformUserID: platformUserID,
		LeaseID: "", Currency: currency, AmountUnits: remainderUnits, OccurredAt: occurredAt,
	}
	if localBalance.Valid {
		v := localBalance.Int64
		remainderEvent.LocalBalanceAfterUnits = &v
	}
	if authorizationID.Valid {
		remainderEvent.AuthorizationID = authorizationID.String
	}
	if billingSnapshotID.Valid {
		remainderEvent.BillingSnapshotID = billingSnapshotID.String
	}
	var authorizationArg any
	if remainderEvent.AuthorizationID != "" {
		authorizationArg = remainderEvent.AuthorizationID
	}
	var billingSnapshotArg any
	if remainderEvent.BillingSnapshotID != "" {
		billingSnapshotArg = remainderEvent.BillingSnapshotID
	}
	inserted, err := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, status, attempt_count, occurred_at, authorization_id, parent_event_id, split_depth, billing_snapshot_id)
		VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, 'pending', 0, $8, $9, $10, $11, $12)
		ON CONFLICT (event_id) DO NOTHING`,
		remainderEventID, platformUserID, gatewayRequestID, currency, remainderUnits, remainderEvent.LocalBalanceAfterUnits,
		testWalletOutboxPayloadHash(remainderEvent), occurredAt, authorizationArg, eventID, splitDepth+1, billingSnapshotArg,
	)
	if err != nil {
		return 0, err
	}
	if n, _ := inserted.RowsAffected(); n == 0 {
		var existing int64
		if err := tx.QueryRowContext(ctx, `SELECT amount_units FROM wallet_settlement_outbox WHERE event_id = $1`, remainderEventID).Scan(&existing); err != nil {
			return 0, err
		}
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return existing, nil
	}

	var pendingRelease any
	if reserved {
		pendingRelease = remainderUnits
	}
	if capturedUnits == 0 {
		_, err = tx.ExecContext(ctx, `
			UPDATE wallet_settlement_outbox
			SET amount_units = 0, pending_release_units = $2, status = 'delivered', delivered_at = now(), dead_letter_reason = NULL, claimed_at = NULL, claimed_by = NULL
			WHERE id = $1`, id, pendingRelease)
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE wallet_settlement_outbox
			SET amount_units = $2, pending_release_units = $3, status = 'pending', next_attempt_at = now(), claimed_at = NULL, claimed_by = NULL
			WHERE id = $1`, id, capturedUnits, pendingRelease)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return remainderUnits, nil
}

func (o *outboxStoreForTest) ClearPendingRelease(ctx context.Context, id int64) error {
	_, err := o.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET pending_release_units = NULL WHERE id = $1`, id)
	return err
}

func (o *outboxStoreForTest) SumDeadLetterUnits(ctx context.Context, reason string) (int64, error) {
	var sum sql.NullInt64
	if err := o.db.QueryRowContext(ctx, `SELECT SUM(amount_units) FROM wallet_settlement_outbox WHERE status = 'dead_letter' AND dead_letter_reason = $1`, reason).Scan(&sum); err != nil {
		return 0, err
	}
	return sum.Int64, nil
}

// Phase 4.1-G: the two READ methods of repository.WalletOutboxStore,
// mirrored here byte-for-byte (test 69's service-level legs run through
// this mirror — a service-package test cannot import repository, which
// imports service; the repository's own read/cursor proof is Task 1a's
// repository test, and test 69's handler file exercises the REAL store).
// The 4.2-G columns ride along: the fx LEFT JOIN and redrive_count.
func (o *outboxStoreForTest) ListOutboxEventsByUser(ctx context.Context, platformUserID string, since, until time.Time, afterID int64, limit int) ([]CanonicalWalletOutboxEvent, bool, error) {
	if limit < 1 {
		limit = 1
	}
	rows, err := o.db.QueryContext(ctx, `
		SELECT o.id, o.event_id, o.platform_user_id, o.lease_id, o.gateway_request_id, o.currency, o.amount_units,
			o.local_balance_after_units, o.occurred_at, o.attempt_count, o.status, o.dead_letter_reason, o.delivered_at,
			o.parent_event_id, o.split_depth, o.pending_release_units, o.authorization_id, o.redrive_count,
			o.billing_snapshot_id, b.payload->'fx'->>'rate'
		FROM wallet_settlement_outbox o
		LEFT JOIN wallet_billing_snapshot b ON b.id = o.billing_snapshot_id
		WHERE o.platform_user_id = $1 AND o.occurred_at >= $2 AND o.occurred_at < $3 AND o.id > $4
		ORDER BY o.id
		LIMIT $5`, platformUserID, since, until, afterID, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	events := make([]CanonicalWalletOutboxEvent, 0, limit)
	for rows.Next() {
		var e CanonicalWalletOutboxEvent
		var leaseID sql.NullString
		var parentEventID, authorizationID, deadLetterReason sql.NullString
		var pendingRelease sql.NullInt64
		var deliveredAt sql.NullTime
		var billingSnapshotID, billingFX sql.NullString
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits,
			&e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount, &e.Status, &deadLetterReason, &deliveredAt,
			&parentEventID, &e.SplitDepth, &pendingRelease, &authorizationID, &e.RedriveCount,
			&billingSnapshotID, &billingFX); err != nil {
			return nil, false, err
		}
		e.LeaseID = leaseID.String
		e.ParentEventID = parentEventID.String
		e.AuthorizationID = authorizationID.String
		e.DeadLetterReason = deadLetterReason.String
		e.BillingSnapshotID = billingSnapshotID.String
		if billingFX.Valid {
			v := billingFX.String
			e.BillingFX = &v
		}
		if pendingRelease.Valid {
			v := pendingRelease.Int64
			e.PendingReleaseUnits = &v
		}
		if deliveredAt.Valid {
			v := deliveredAt.Time
			e.DeliveredAt = &v
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	truncated := len(events) > limit
	if truncated {
		events = events[:limit]
	}
	return events, truncated, nil
}

func (o *outboxStoreForTest) DeliveredWatermark(ctx context.Context) (OutboxWatermark, error) {
	var wm OutboxWatermark
	var deliveredMax sql.NullTime
	var idMax sql.NullInt64
	err := o.db.QueryRowContext(ctx, `
		SELECT max(delivered_at), max(id),
		       count(*) FILTER (WHERE status = 'pending'),
		       count(*) FILTER (WHERE status = 'in_flight'),
		       count(*) FILTER (WHERE status = 'dead_letter')
		FROM wallet_settlement_outbox`).Scan(&deliveredMax, &idMax, &wm.Pending, &wm.InFlight, &wm.DeadLetter)
	if err != nil {
		return wm, err
	}
	if deliveredMax.Valid {
		v := deliveredMax.Time
		wm.DeliveredAtMax = &v
	}
	wm.OutboxIDMax = idMax.Int64
	return wm, nil
}

var _ WalletReconciliationOutboxRead = (*outboxStoreForTest)(nil)

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
		Currency: "USD", AmountUnits: 5_000000,
	}
	bridge.ObserveSettlement(event)

	// The row must be durably visible via the outbox store immediately —
	// no channel, no goroutine race, no window where it doesn't exist yet.
	pending, err := outbox.ClaimPendingOutboxEvents(ctx, "test-worker", 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

// ListReceivableRedriveCandidates / RequeueDeadLetter (Phase 4.2-G Task 2)
// mirror repository.WalletOutboxStore's implementations for the collector
// tests. The candidate listing deliberately returns rows AT the bound too
// (`<=`, deviation from the plan's `<`): the collector counts them as
// receivable_redrive_exhausted — terminal, staying in the receivable —
// which a strict `<` filter would hide from the only pass that observes
// them.
func (o *outboxStoreForTest) ListReceivableRedriveCandidates(ctx context.Context, notBefore time.Time, maxRedrives, limit int) ([]CanonicalWalletOutboxEvent, error) {
	if limit < 1 {
		limit = 1
	}
	if maxRedrives < 1 {
		maxRedrives = 1
	}
	rows, err := o.db.QueryContext(ctx, `
		SELECT id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units,
			local_balance_after_units, occurred_at, attempt_count, redrive_count, parent_event_id, split_depth, authorization_id
		FROM wallet_settlement_outbox
		WHERE status = 'dead_letter' AND dead_letter_reason = 'balance_shortfall'
		  AND occurred_at >= $1 AND redrive_count <= $2
		ORDER BY id
		LIMIT $3`, notBefore, maxRedrives, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]CanonicalWalletOutboxEvent, 0, limit)
	for rows.Next() {
		var e CanonicalWalletOutboxEvent
		var leaseID sql.NullString
		var parentEventID, authorizationID sql.NullString
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits,
			&e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount, &e.RedriveCount, &parentEventID, &e.SplitDepth, &authorizationID); err != nil {
			return nil, err
		}
		e.LeaseID = leaseID.String
		e.ParentEventID = parentEventID.String
		e.AuthorizationID = authorizationID.String
		events = append(events, e)
	}
	return events, rows.Err()
}

func (o *outboxStoreForTest) RequeueDeadLetter(ctx context.Context, id int64, workerID string) error {
	_, err := o.db.ExecContext(ctx, `
		UPDATE wallet_settlement_outbox
		SET status = 'pending', attempt_count = 0, redrive_count = redrive_count + 1,
		    next_attempt_at = now(), dead_letter_reason = NULL, claimed_at = NULL, claimed_by = NULL
		WHERE id = $1 AND status = 'dead_letter' AND dead_letter_reason = 'balance_shortfall'`, id)
	return err
}

func (o *outboxStoreForTest) PruneDeliveredOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	if batch <= 0 {
		batch = 5000
	}
	res, err := o.db.ExecContext(ctx, `
		DELETE FROM wallet_settlement_outbox
		WHERE id IN (
			SELECT id FROM wallet_settlement_outbox
			WHERE status = 'delivered' AND occurred_at < $1
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff, batch)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
