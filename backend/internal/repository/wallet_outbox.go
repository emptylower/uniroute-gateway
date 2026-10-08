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
	// billing_snapshot_id (Phase 4.2-G) is the frozen pricing basis of the
	// settlement's usage — the event's own field, its own column, never in
	// the hash (constraint 3). NULL when the caller held no snapshot.
	var authorizationID, billingSnapshotID any
	if event.AuthorizationID != "" {
		authorizationID = event.AuthorizationID
	}
	if event.BillingSnapshotID != "" {
		billingSnapshotID = event.BillingSnapshotID
	}
	_, insertErr := tx.ExecContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, payload_hash, occurred_at, authorization_id, billing_snapshot_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		event.EventID, event.PlatformUserID, event.LeaseID, event.GatewayRequestID, event.Currency, event.AmountUnits, event.LocalBalanceAfterUnits, hash, event.OccurredAt, authorizationID, billingSnapshotID,
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
		RETURNING id, event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, local_balance_after_units, occurred_at, attempt_count, parent_event_id, split_depth, pending_release_units, authorization_id, billing_snapshot_id`, limit, workerID)
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
		var parentEventID, authorizationID, snapshotID sql.NullString
		var pendingRelease sql.NullInt64
		if err := rows.Scan(&e.ID, &e.EventID, &e.PlatformUserID, &leaseID, &e.GatewayRequestID, &e.Currency, &e.AmountUnits, &e.LocalBalanceAfterUnits, &e.OccurredAt, &e.AttemptCount, &parentEventID, &e.SplitDepth, &pendingRelease, &authorizationID, &snapshotID); err != nil {
			return nil, err
		}
		e.LeaseID = leaseID.String
		e.ParentEventID = parentEventID.String
		e.AuthorizationID = authorizationID.String
		e.BillingSnapshotID = snapshotID.String
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
	// Phase 4.2-G: the remainder derives from the SAME usage as its parent —
	// it inherits the parent's billing snapshot so leg 4's fx covers split
	// children without the backfill join. The hash is untouched (the event's
	// BillingSnapshotID never enters walletOutboxHashPayload — test 76).
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
		walletOutboxPayloadHash(remainderEvent), occurredAt, authorizationArg, eventID, splitDepth+1, billingSnapshotArg,
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

// ListOutboxEventsByUser (Phase 4.1-G, redesign §15.3 leg 2) is the
// reconciliation summary's per-user outbox read: every row of ONE platform
// user inside the half-open [since, until) occurred_at window, ordered by
// the BIGSERIAL id, with a keyset cursor (id > afterID) and a limit+1
// truncation signal. The cursor is the primary key, so it matches the
// ORDER BY exactly and can neither skip nor duplicate a row — including
// rows sharing an occurred_at across a page boundary, the leg that breaks
// a time cursor. Runs on 215's (platform_user_id, occurred_at, id) index,
// which keeps the predicate AND the ordering index-resident. Read-only.
//
// Phase 4.2-G (4.1-G's hand-on): the SAME LEFT JOIN the Live rows use folds
// the billing snapshot's fx in — b.payload->'fx'->>'rate', NULL when the
// row's billing_snapshot_id is empty or the snapshot row is absent (the
// leg-4 unverified tail; a missing snapshot never drops the record).
func (s *WalletOutboxStore) ListOutboxEventsByUser(ctx context.Context, platformUserID string, since, until time.Time, afterID int64, limit int) ([]service.CanonicalWalletOutboxEvent, bool, error) {
	if limit < 1 {
		limit = 1
	}
	rows, err := s.db.QueryContext(ctx, `
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
	events := make([]service.CanonicalWalletOutboxEvent, 0, limit)
	for rows.Next() {
		var e service.CanonicalWalletOutboxEvent
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

// DeliveredWatermark (Phase 4.1-G) is the global high-water mark 4.1-S
// records per reconciliation run: the outbox's max delivered_at (the
// parent plan's "settlement high-water mark" successor), the max row id,
// and the per-status counts. Read-only.
func (s *WalletOutboxStore) DeliveredWatermark(ctx context.Context) (service.OutboxWatermark, error) {
	var wm service.OutboxWatermark
	var deliveredMax sql.NullTime
	var idMax sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
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

// ListReceivableRedriveCandidates (Phase 4.2-G Task 2, redesign §15.4/§11.3)
// is the receivable collector's candidate set: balance_shortfall
// dead-letters young enough to still be collectable (occurred_at ≥
// notBefore — the retention bound), at OR under the re-drive bound
// (redrive_count <= maxRedrives — the AT-bound rows are returned so the
// collector can count them as receivable_redrive_exhausted; it re-queues
// only the under-bound ones), oldest first, capped. The receivable is
// money owed: this query NEVER deletes, and the collector never
// reclassifies — re-queueing is the only mutation (RequeueDeadLetter).
// Read-only.
func (s *WalletOutboxStore) ListReceivableRedriveCandidates(ctx context.Context, notBefore time.Time, maxRedrives, limit int) ([]service.CanonicalWalletOutboxEvent, error) {
	if limit < 1 {
		limit = 1
	}
	if maxRedrives < 1 {
		maxRedrives = 1
	}
	rows, err := s.db.QueryContext(ctx, `
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
	events := make([]service.CanonicalWalletOutboxEvent, 0, limit)
	for rows.Next() {
		var e service.CanonicalWalletOutboxEvent
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

// RequeueDeadLetter (Phase 4.2-G Task 2) moves ONE balance_shortfall
// dead-letter back to pending under the collector's re-drive: the transport
// budget starts fresh (attempt_count = 0), the re-drive is counted in its
// OWN column (redrive_count + 1), and the row is immediately claimable.
// ONE statement, idempotent by its status guard (a row already re-driven or
// resolved by another path updates zero rows — not this caller's outcome).
// The dead-letter class itself is written ONLY by the dispatcher's
// unmodified path; this method never reclassifies anything.
func (s *WalletOutboxStore) RequeueDeadLetter(ctx context.Context, id int64, workerID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE wallet_settlement_outbox
		SET status = 'pending', attempt_count = 0, redrive_count = redrive_count + 1,
		    next_attempt_at = now(), dead_letter_reason = NULL, claimed_at = NULL, claimed_by = NULL
		WHERE id = $1 AND status = 'dead_letter' AND dead_letter_reason = 'balance_shortfall'`, id)
	return err
}

// PruneDeliveredOlderThan (Phase 4.2-G Task 3, redesign §15.4) deletes
// delivered outbox rows strictly older than cutoff in batches using
// SKIP LOCKED. It never touches dead-letters (the receivable is money
// owed) and never touches rows inside the retention window.
func (s *WalletOutboxStore) PruneDeliveredOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	if batch <= 0 {
		batch = 5000
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM wallet_settlement_outbox
		WHERE id IN (
			SELECT id FROM wallet_settlement_outbox
			WHERE status = 'delivered' AND occurred_at < $1
            AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment a WHERE a.event_id=wallet_settlement_outbox.event_id AND a.state<>'finished')
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff, batch)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *WalletOutboxStore) PruneDelivered(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	return s.PruneDeliveredOlderThan(ctx, cutoff, batch)
}
