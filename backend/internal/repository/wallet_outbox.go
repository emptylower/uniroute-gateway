package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const walletOutboxMaxAttempts = 8

var ErrWalletOutboxPayloadConflict = errors.New("wallet outbox event id already used with a different payload")

type WalletOutboxStore struct {
	db *sql.DB
}

func NewWalletOutboxStore(db *sql.DB) *WalletOutboxStore {
	return &WalletOutboxStore{db: db}
}

// ProvideWalletOutboxStore is the Wire-facing provider: it returns the
// INTERFACE, so Wire can inject it into the two gateway constructors
// without `service` ever importing `repository`. Tests keep using the
// concrete NewWalletOutboxStore (and therefore keep access to
// OutboxEventStatus); only the DI graph goes through this wrapper.
func ProvideWalletOutboxStore(db *sql.DB) service.CanonicalWalletOutboxStore {
	return NewWalletOutboxStore(db)
}

// walletOutboxHashPayload is the canonical, unambiguous encoding this hash is
// computed over. Every field of the payload EXCEPT event_id is included —
// that's the whole contract this hash exists for (event_id is the identity
// being checked FOR conflicts, so it can't also be part of what's compared).
// Hashed as a JSON encoding of a fixed-shape struct, NOT a hand-built
// delimited string — `encoding/json` escapes every `"` and `\` inside string
// field values, so two different field values can never serialize to the
// same bytes the way naive delimiter-joining can. Field order is fixed by
// the struct definition (not a map), so this is deterministic across calls
// and across process restarts, which is required since a payload hash
// written by one process must compare equal when read back by another.
type walletOutboxHashPayload struct {
	PlatformUserID         string
	LeaseID                string
	Currency               string
	GatewayRequestID       string
	AmountUnits            int64
	LocalBalanceAfterUnits *int64
	OccurredAt             string
}

func walletOutboxPayloadHash(event service.CanonicalWalletSettlementEvent) string {
	payload := walletOutboxHashPayload{
		PlatformUserID: event.PlatformUserID, LeaseID: event.LeaseID, Currency: event.Currency,
		GatewayRequestID: event.GatewayRequestID, AmountUnits: event.AmountUnits,
		LocalBalanceAfterUnits: event.LocalBalanceAfterUnits, OccurredAt: event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	// json.Marshal on a fixed struct of only string/int64/*int64 fields
	// (no channels, funcs, or cyclic references) cannot fail — every field
	// type here is always representable, so the error return isn't worth
	// threading through every caller of this function for a branch that can
	// never execute.
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// InsertOutboxEventTx runs inside its own transaction, opened and committed
// by ObserveSettlement (Task 4 Step 6) — NOT the same transaction as any
// local capture record (no open transaction is available at the real call
// site to share; see Task 4 Step 6's Scope correction). A retry of the
// identical event is a no-op. A different payload under the same event_id
// is rejected.
//
// The INSERT runs inside a real Postgres SAVEPOINT: a unique-violation
// error on PostgreSQL aborts the ENTIRE current transaction until a
// ROLLBACK or ROLLBACK TO SAVEPOINT runs — any further statement on that
// same `*sql.Tx`, including the fallback SELECT, would itself error with
// "current transaction is aborted." The savepoint means a unique-violation
// only unwinds back to the savepoint (not the whole caller-owned
// transaction), leaving the fallback SELECT able to run normally on the
// same `tx` afterward.
func (s *WalletOutboxStore) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event service.CanonicalWalletSettlementEvent) error {
	hash := walletOutboxPayloadHash(event)
	if _, err := tx.ExecContext(ctx, `SAVEPOINT wallet_outbox_insert`); err != nil {
		return err
	}
	_, insertErr := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		event.EventID, event.PlatformUserID, event.LeaseID, event.GatewayRequestID, event.Currency, event.AmountUnits, event.LocalBalanceAfterUnits, hash, event.OccurredAt,
	)
	if insertErr == nil {
		_, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT wallet_outbox_insert`)
		return err
	}
	if !isUniqueViolation(insertErr) {
		return insertErr
	}
	if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT wallet_outbox_insert`); err != nil {
		return err
	}
	var existingHash string
	if err := tx.QueryRowContext(ctx, `SELECT payload_hash FROM wallet_settlement_outbox WHERE event_id = $1`, event.EventID).Scan(&existingHash); err != nil {
		return err
	}
	if existingHash != hash {
		return ErrWalletOutboxPayloadConflict
	}
	return nil // identical retry, no-op
}

// ClaimPendingOutboxEvents atomically claims up to `limit` pending rows,
// transitioning them to `in_flight` in the SAME statement that selects
// them, so two concurrent dispatchers can never both claim the same row.
// `FOR UPDATE SKIP LOCKED` lets concurrent claimers each grab a disjoint
// set of rows instead of blocking on each other, and the status transition
// to `in_flight` means a row already claimed by another dispatcher is no
// longer `pending` and will not be selected again.
func (s *WalletOutboxStore) ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]service.CanonicalWalletOutboxEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
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
	var events []service.CanonicalWalletOutboxEvent
	for rows.Next() {
		var e service.CanonicalWalletOutboxEvent
		// Scan a SQL NULL lease_id into sql.NullString — scanning it
		// directly into a plain Go string fails with "converting NULL to
		// string is unsupported". A NULL becomes "" — the type stays a
		// plain string field, since "" already means "no lease id yet"
		// everywhere else this type is used.
		var leaseID sql.NullString
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits, &e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount); err != nil {
			return nil, err
		}
		e.LeaseID = leaseID.String
		events = append(events, e)
	}
	return events, rows.Err()
}

// ReclaimStaleInFlightEvents recovers rows a previous dispatcher process
// claimed (moved to `in_flight`) but then crashed before ever calling
// MarkOutboxEventDelivered or MarkOutboxEventFailed — without this, such a
// row is `in_flight` forever, since ClaimPendingOutboxEvents only ever
// selects `status = 'pending'`. `staleAfter` bounds how long a legitimate
// in-progress delivery attempt is given before being treated as abandoned;
// it must comfortably exceed the dispatcher's per-attempt timeout, or a
// slow but still-legitimate in-flight delivery could be reclaimed and
// delivered twice. Same `claimed_at`/timeout-based recovery pattern as
// `auth_cache_invalidation_outbox_repo.go`'s `Claim` method.
func (s *WalletOutboxStore) ReclaimStaleInFlightEvents(ctx context.Context, staleAfter time.Duration) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
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

// MarkOutboxEventDelivered only transitions a row FROM `in_flight` AND only
// while still owned by `workerID` — a row already resolved by another path
// (or reclaimed by another dispatcher) makes this a no-op (0 rows affected
// is not an error — it just means another outcome already won).
func (s *WalletOutboxStore) MarkOutboxEventDelivered(ctx context.Context, id int64, workerID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'delivered', delivered_at = now(), claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
	return err
}

// MarkOutboxEventFailed increments the attempt count and schedules the next
// attempt at exactly `simulatedNow + exponential backoff` (2s, 4s, 8s, ...),
// or moves the event to dead_letter once walletOutboxMaxAttempts is reached.
// Production callers pass `time.Now().UTC()` for simulatedNow; tests pass a
// timestamp far enough in the past (comfortably more than
// walletOutboxMaxAttempts's largest possible backoff, ~4.3 minutes at 8
// attempts) that `simulatedNow + backoff` still lands before the real
// wall-clock time `ClaimPendingOutboxEvents` compares against — letting a
// test observe "already claimable again" without a real sleep. Only acts on
// a row still `in_flight` and still owned by `workerID`; transitions back
// to `pending` on an ordinary retry, or to `dead_letter` once exhausted.
func (s *WalletOutboxStore) MarkOutboxEventFailed(ctx context.Context, id int64, workerID string, simulatedNow time.Time) error {
	var attempts int
	err := s.db.QueryRowContext(ctx, `
		UPDATE wallet_settlement_outbox SET attempt_count = attempt_count + 1
		WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2
		RETURNING attempt_count`, id, workerID).Scan(&attempts)
	if err == sql.ErrNoRows {
		return nil // already resolved by another path (delivered/dead-lettered), or this claim was reclaimed by another dispatcher — either way not this call's outcome to report
	}
	if err != nil {
		return err
	}
	if attempts >= walletOutboxMaxAttempts {
		_, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
		return err
	}
	backoff := time.Duration(1<<uint(attempts)) * time.Second // 2s, 4s, 8s, ...
	next := simulatedNow.Add(backoff)
	_, err = s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'pending', next_attempt_at = $3, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, next)
	return err
}

func (s *WalletOutboxStore) MarkOutboxEventDeadLetter(ctx context.Context, id int64, workerID, reason string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', attempt_count = attempt_count + 1, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		slog.Warn("canonical wallet outbox event dead-lettered", "id", id, "reason", reason)
	}
	return nil
}

// BindOutboxEventLease durably anchors this event to `leaseID` before its
// reservation is attempted, so a retry reserves against the SAME lease the
// first attempt used rather than whatever lease is current at retry time.
//
// It deliberately does NOT touch payload_hash. That hash is the identity
// check for a RE-INSERT of the same event_id under a different payload
// (repricing), and it was computed from the event exactly as
// ObserveSettlement saw it. Rewriting it here would make a genuine repricing
// conflict compare equal and silently double-settle — the precise failure
// Task 2 exists to prevent.
//
// Ownership-guarded like every other resolve method. Unlike them, a zero-row
// result is reported rather than swallowed: the caller is about to reserve
// real money against this lease, and must not do so on a row it no longer
// owns.
func (s *WalletOutboxStore) BindOutboxEventLease(ctx context.Context, id int64, workerID, leaseID string) error {
	result, err := s.db.ExecContext(ctx, `
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
		return service.ErrCanonicalWalletOutboxClaimLost
	}
	return nil
}

func (s *WalletOutboxStore) OutboxEventStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status)
	return status, err
}
