package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const walletOutboxMaxAttempts = 8

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
	// authorization_id (Phase 3.5, §11.9) is the token's durable join; NULL
	// when the event carries none (pre-3.3 rows and token-less paths).
	var authorizationID any
	if event.AuthorizationID != "" {
		authorizationID = event.AuthorizationID
	}
	_, insertErr := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, occurred_at, authorization_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		event.EventID, event.PlatformUserID, event.LeaseID, event.GatewayRequestID, event.Currency, event.AmountUnits, event.LocalBalanceAfterUnits, hash, event.OccurredAt, authorizationID,
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
		return service.ErrCanonicalWalletOutboxPayloadConflict
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
		RETURNING id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, attempt_count, parent_event_id, split_depth, pending_release_units, authorization_id`, limit, workerID)
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
		_, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', dead_letter_reason = 'attempts_exhausted', claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID)
		return err
	}
	backoff := time.Duration(1<<uint(attempts)) * time.Second // 2s, 4s, 8s, ...
	next := simulatedNow.Add(backoff)
	_, err = s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'pending', next_attempt_at = $3, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, next)
	return err
}

func (s *WalletOutboxStore) MarkOutboxEventDeadLetter(ctx context.Context, id int64, workerID, reason string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'dead_letter', dead_letter_reason = $3, attempt_count = attempt_count + 1, claimed_at = NULL, claimed_by = NULL WHERE id = $1 AND status = 'in_flight' AND claimed_by = $2`, id, workerID, reason)
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

// SplitOutboxEvent (Phase 3.5, redesign §11.3) is ONE Postgres transaction:
// it inserts the remainder row (its own event_id, parent_event_id = the
// parent's event_id, split_depth = parent+1, amount A−H, unbound, attempt
// budget of its own), rewrites the parent (amount H back to pending with
// next_attempt_at = now — deliberately no attempt increment and no backoff;
// or delivered with delivered_at when H = 0) and sets pending_release_units
// to what the dispatcher still owes the bound lease (A−H; NULL when
// reserved = false — the proactive split reserved nothing). It NEVER touches
// the parent's payload_hash: that hash is the identity of the event as first
// observed, and its only reader is InsertOutboxEventTx's unique-violation
// fallback — recomputing it here would turn a later in-process re-observation
// of the same event into a payload conflict (§11.3). A re-run with the same
// remainderEventID is a no-op (ON CONFLICT DO NOTHING; the parent is left
// untouched — a second rewrite would subtract H twice). Ownership-guarded
// like every resolve method.
func (s *WalletOutboxStore) SplitOutboxEvent(ctx context.Context, id int64, workerID string, capturedUnits int64, remainderEventID string, reserved bool) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
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
		authorizationID                                     sql.NullString
	)
	err = tx.QueryRowContext(ctx, `
		SELECT event_id, platform_user_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, split_depth, authorization_id
		FROM wallet_settlement_outbox
		WHERE id = $1 AND claimed_by = $2 AND status = 'in_flight'
		FOR UPDATE`, id, workerID,
	).Scan(&eventID, &platformUserID, &gatewayRequestID, &currency, &amountUnits, &localBalance, &occurredAt, &splitDepth, &authorizationID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, service.ErrCanonicalWalletOutboxClaimLost
	}
	if err != nil {
		return 0, err
	}
	if capturedUnits < 0 || capturedUnits > amountUnits {
		return 0, fmt.Errorf("wallet outbox split captured units %d out of range for amount %d", capturedUnits, amountUnits)
	}
	remainderUnits := amountUnits - capturedUnits

	remainderEvent := service.CanonicalWalletSettlementEvent{
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
	var authorizationArg any
	if remainderEvent.AuthorizationID != "" {
		authorizationArg = remainderEvent.AuthorizationID
	}
	inserted, err := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, status, attempt_count, occurred_at, authorization_id, parent_event_id, split_depth)
		VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, 'pending', 0, $8, $9, $10, $11)
		ON CONFLICT (event_id) DO NOTHING`,
		remainderEventID, platformUserID, gatewayRequestID, currency, remainderUnits, remainderEvent.LocalBalanceAfterUnits,
		walletOutboxPayloadHash(remainderEvent), occurredAt, authorizationArg, eventID, splitDepth+1,
	)
	if err != nil {
		return 0, err
	}
	if n, _ := inserted.RowsAffected(); n == 0 {
		// The remainder row already exists (a re-run of the same split): the
		// no-op — the parent is NOT rewritten a second time. Return the
		// existing remainder's amount so a caller's release stays consistent.
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
		// §11.3 split_full: nothing is capturable on the refusing lease; the
		// parent is resolved in this same transaction and the whole amount
		// moves to the remainder.
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

// ClearPendingRelease nulls pending_release_units after the owed release
// landed (§11.3). Not claim-guarded: the release itself is gated on the
// per-event release marker in Redis, which is the idempotency key — this
// column only records THAT a release is owed.
func (s *WalletOutboxStore) ClearPendingRelease(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET pending_release_units = NULL WHERE id = $1`, id)
	return err
}

// SumDeadLetterUnits is the receivable figure (§11.4): the sum of
// amount_units over dead-letter rows carrying the named reason.
func (s *WalletOutboxStore) SumDeadLetterUnits(ctx context.Context, reason string) (int64, error) {
	var sum sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT SUM(amount_units) FROM wallet_settlement_outbox WHERE status = 'dead_letter' AND dead_letter_reason = $1`, reason).Scan(&sum); err != nil {
		return 0, err
	}
	return sum.Int64, nil
}

func (s *WalletOutboxStore) OutboxEventStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status)
	return status, err
}
