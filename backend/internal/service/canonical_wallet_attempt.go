package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"
)

// Every competing group transition locks all shares in the same order. The
// decision to release zero must be made after a concurrent positive settlement.
func lockWalletAttempt(ctx context.Context, tx *sql.Tx, parent string) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT authorization_id FROM wallet_authorization_segment WHERE parent_authorization_id=$1 ORDER BY ordinal FOR UPDATE`, parent)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return 0, err
		}
		n++
	}
	return n, rows.Err()
}

func (b *CanonicalWalletBridge) protectPoolAttempt(ctx context.Context, parent, user, snapshot string, segment *AuthorizationSegment, finish bool) error {
	svc := &MediaTaskService{bridge: b, db: b.outboxDB}
	actual := segment.ActualUnits
	record := &mediaTaskRecord{AuthorizationKind: segment.Kind, ID: parent, PlatformUserID: user, SnapshotID: snapshot, AuthorizationID: segment.AuthorizationID, LeaseID: segment.LeaseID, LeaseBasis: &segment.Basis, HeldUnits: segment.HeldUnits, EventID: segment.EventID, PinState: segment.PinState, Status: "settling", ActualUnits: &actual, CreatedAt: time.Now().UTC()}
	if finish {

		if err := svc.pinSingle(ctx, record, true); err != nil {
			return err
		}
		segment.PinState = "finished"
		return nil
	}
	if err := svc.pinSingle(ctx, record, false); err != nil {
		return err
	}
	segment.PinState = "active"
	segment.Basis = *record.LeaseBasis
	return svc.protectSingle(ctx, record)
}
func (b *CanonicalWalletBridge) preparePoolAttempt(ctx context.Context, h *AuthorizationHandle, user string) error {
	b.installImmediateEvidence(h, user)
	for i := range h.Segments {
		segment := &h.Segments[i]
		basis, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, segment.LeaseID)
		if err != nil {
			return err
		}
		segment.Basis = *basis
		if err = b.protectPoolAttempt(ctx, h.ID, user, h.SnapshotID, segment, false); err != nil {
			return err
		}
		if err = b.saveAuthorizationSegments(ctx, h.ID, h.Segments); err != nil {
			return err
		}
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if count, e := lockWalletAttempt(ctx, tx, h.ID); e != nil || count != len(h.Segments) {
		return fmt.Errorf("wallet attempt group unavailable: %v", e)
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='held',updated_at=now() WHERE parent_authorization_id=$1 AND state IN ('prepared','held')`, h.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != int64(len(h.Segments)) {
		return errors.New("wallet attempt no longer authorizable")
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	h.beforeWrite = func(ctx context.Context, token string) error {
		if err := b.startWalletReaderJournal(ctx, h, token); err != nil {
			return err
		}
		tx, err := b.outboxDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if count, e := lockWalletAttempt(ctx, tx, h.ID); e != nil || count != len(h.Segments) {
			return fmt.Errorf("wallet attempt group unavailable: %v", e)
		}
		owner, host := "", ""
		var normalization any
		h.mu.Lock()
		if h.readerNormalization != nil {
			raw, marshalErr := json.Marshal(h.readerNormalization)
			if marshalErr != nil {
				h.mu.Unlock()
				return marshalErr
			}
			normalization = string(raw)
		}
		h.mu.Unlock()
		if h.readerJournal != nil {
			owner = h.readerJournal.record.OwnerID
			host = h.readerJournal.record.Host
		}
		result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,updated_at=now(),reader_owner_id=NULLIF($4,''),reader_journal_host=NULLIF($5,''),reader_fee_normalization=$6::jsonb,first_write_at=CASE WHEN kind='llm' THEN now() ELSE first_write_at END,write_active_until=CASE WHEN kind='llm' THEN now()+interval '90 seconds' ELSE NULL END,expiry_deadline=CASE WHEN kind='llm' THEN date_trunc('milliseconds',(lease_basis->>'expires_at')::timestamptz)+($3 * interval '1 second') ELSE NULL END WHERE parent_authorization_id=$1 AND state='held' AND authorization_token IS NULL`, h.ID, token, int64(b.poolExpiryGrace()/time.Second), owner, host, normalization)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != int64(len(h.Segments)) {
			return &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "attempt already crossed the upstream write boundary"}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		if b.ImmediateWalletReleaseMode("llm") != "off" && h.AttemptKind == "llm" {
			if _, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET evidence_pending=true WHERE parent_authorization_id=$1 AND authorization_token=$2`, h.ID, token); err != nil {
				return err
			}
		}
		b.startPoolWriteOwner(ctx, h, token)
		return nil
	}
	// Only explicit validation/auth rejection or proven pre-application-byte
	// errors resolve as zero. Uncertain writes never resolve through a clock.
	h.onOutcome = func(token string, outcome AuthorizationOutcome, err error) {
		if outcome == AuthorizationOutcomeResult {
			return
		}
		h.completeWrite()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		defer cancel()
		// A failed TCP dial or DNS resolution cannot have written application
		// bytes. Missing httptrace callbacks alone are insufficient evidence.
		var dial *net.OpError
		var dns *net.DNSError
		reliableZero := outcome == AuthorizationOutcomeRejected || (outcome == AuthorizationOutcomeNotWritten &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
			((errors.As(err, &dial) && dial.Op == "dial" && !dial.Timeout()) || (errors.As(err, &dns) && !dns.Timeout())))
		if reliableZero {
			if b.ImmediateWalletReleaseMode("llm") != "off" && h.AttemptKind == "llm" {
				if e := b.sealPoolEvidence(ctx, h, "proven_not_written"); e != nil {
					return
				}
				if _, e := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET known_fee_units=0 WHERE parent_authorization_id=$1 AND authorization_token=$2 AND NOT fee_pending`, h.ID, token); e != nil {
					return
				}
			}
			if e := b.releasePoolAttempt(ctx, h.ID, user, h.Segments, token); e != nil {
				slog.Warn("wallet pre-write zero release deferred", "authorization_id", h.ID, "error", e)
			} else if b.ImmediateWalletReleaseMode("llm") != "off" {
				h.mu.Lock()
				h.zeroAcknowledged = true
				h.mu.Unlock()
			}
			return
		}
		for _, segment := range h.Segments {
			b.markHoldIndeterminate(ctx, user, segment.AuthorizationID)
		}
	}
	return nil
}
func (b *CanonicalWalletBridge) poolAttemptActive(ctx context.Context, auth string) bool {
	if b.outboxDB == nil {
		return false
	}
	var active bool
	err := b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE authorization_id=$1 AND kind<>'media' AND state<>'finished' AND (expiry_ack_at IS NULL OR expiry_cleanup_at IS NULL))`, auth).Scan(&active)
	return err != nil || active
}

// All operations are idempotent under their authorization/event IDs. This runs
// regardless of the new-media feature flag and preserves unknown cost evidence.
func (b *CanonicalWalletBridge) recoverPoolAttempts(ctx context.Context) {
	if b == nil || b.outboxDB == nil || !b.HoldsEnabled() {
		return
	}
	b.recoverBillingEvidence(ctx)
	_, _ = b.ObserveImmediateWalletRecovery(ctx)
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT parent_authorization_id,platform_user_id,billing_snapshot_id FROM wallet_authorization_segment WHERE kind<>'media' AND state<>'finished' AND (state<>'expired_unknown' OR expiry_cleanup_at IS NULL) GROUP BY parent_authorization_id,platform_user_id,billing_snapshot_id ORDER BY min(updated_at) LIMIT 32`)
	if err != nil {
		return
	}
	type group struct{ parent, user, snapshot string }
	groups := []group{}
	for rows.Next() {
		var g group
		if err = rows.Scan(&g.parent, &g.user, &g.snapshot); err != nil {
			break
		}
		groups = append(groups, g)
	}
	rows.Close()
	if err != nil {
		return
	}
	for _, g := range groups {
		// Rotate every inspected group, including a failed/partial expiry ACK.
		// Mutable scheduling time never extends the immutable cost deadline.
		_, _ = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET updated_at=now() WHERE parent_authorization_id=$1 AND state<>'finished'`, g.parent)
		handled, expiryErr := b.recoverExpiredPoolGroup(ctx, g.parent, g.user, g.snapshot)
		if expiryErr != nil {
			slog.Warn("wallet unknown expiry recovery deferred", "parent_authorization_id", g.parent, "error_class", "expiry_protocol")
			continue
		}
		if handled {
			continue
		}
		segments, e := b.authorizationSegments(ctx, g.parent)
		if e != nil {
			continue
		}
		for i := range segments {
			segment := &segments[i]
			if segment.State == "finished" {
				continue
			}
			if segment.State == "prepared" || segment.State == "held" {
				// A tokenless plan cannot have crossed the durable write boundary.
				// CAS prevents a still-running authorizer from subsequently using it.
				result, e := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment a SET state='released',updated_at=now() WHERE authorization_id=$1 AND state IN ('prepared','held') AND authorization_token IS NULL AND created_at<now()-interval '90 seconds' AND (kind<>'live' OR NOT EXISTS(SELECT 1 FROM wallet_live_provisional p WHERE p.platform_user_id=a.platform_user_id AND p.status IN ('provisional','active','finalizing') AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.windows) w WHERE w->>'token'=a.parent_authorization_id)))`, segment.AuthorizationID)
				if e != nil {
					continue
				}
				n, _ := result.RowsAffected()
				if n == 0 {
					if segment.State == "prepared" {
						continue
					}
				} else {
					segment.State = "released"
				}
			}
			if segment.State == "settling" {
				var status string
				e = b.outboxDB.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id=$1`, segment.EventID).Scan(&status)
				if e == nil && status == "delivered" {
					e = b.protectPoolAttempt(ctx, g.parent, g.user, g.snapshot, segment, true)
					if e == nil {
						e = b.finishPoolSegment(ctx, g.user, *segment)
					}
					if e != nil {
						slog.Warn("wallet attempt captured pin deferred", "error", e)
					}
					continue
				}
				if e != nil && !errors.Is(e, sql.ErrNoRows) {
					continue
				}
			}
			if segment.State == "released" {
				// The signed zero finalizer is for an attempt whose every share is
				// zero. A zero share beside a charged sibling moves no money: only
				// its capacity hold and its D1 pin end, as for any released share.
				groupCharged := false
				for _, sibling := range segments {
					if sibling.ActualUnits > 0 || (sibling.Payload != nil && sibling.Payload.AmountUnits > 0) || sibling.Remainder != nil {
						groupCharged = true
						break
					}
				}
				if segment.Kind == "llm" && !groupCharged {
					owned, ownershipErr := b.hasDurableLLMFinancialOwnership(ctx, g.parent)
					if ownershipErr != nil {
						break
					}
					if owned {
						_ = b.finishZeroPoolAttempt(ctx, g.parent, g.user, segments, "")
						break
					}
				}
				if _, e = b.store.ReleaseCanonicalWalletHold(ctx, g.user, segment.AuthorizationID, "released", "zero_cost"); e != nil && !isHoldNotArmed(e) && !errors.Is(e, ErrCanonicalWalletHoldMissing) {
					continue
				}
				segment.ActualUnits = 0
				e = b.protectPoolAttempt(ctx, g.parent, g.user, g.snapshot, segment, true)
				if e == nil {
					e = b.finishPoolSegment(ctx, g.user, *segment)
				}
				if e != nil {
					slog.Warn("wallet zero attempt pin deferred", "error", e)
				}
				continue
			}
			if segment.State == "settling" && segment.Payload != nil && segment.Payload.LeaseID == "" {
				continue
			}
			e = b.protectPoolAttempt(ctx, g.parent, g.user, g.snapshot, segment, false)
			if e == nil && segment.State == "settling" {
				conv, convertErr := b.store.ConvertCanonicalWalletHold(ctx, g.user, segment.AuthorizationID, segment.EventID, segment.ActualUnits, b.clock())
				if convertErr != nil || (conv.Code != 0 && !(conv.Code == 7 && conv.EventID == segment.EventID)) {
					e = fmt.Errorf("restore attempt conversion: %v", convertErr)
				}
			}
			if e != nil {
				slog.Warn("wallet pending attempt protection deferred", "authorization_id", segment.AuthorizationID, "error", e)
				continue
			}
			_ = b.saveAuthorizationSegments(ctx, g.parent, segments)
		}
		// A failed original PG outbox transaction is retried as one group.
		pendingInsert := false
		for _, segment := range segments {
			if segment.State == "settling" && segment.Payload != nil {
				var exists bool
				if b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id=$1)`, segment.EventID).Scan(&exists) == nil && !exists {
					pendingInsert = true
				}
			}
		}
		if pendingInsert {
			if e = b.enqueuePoolSettlements(ctx, g.user, g.snapshot, segments); e != nil {
				slog.Warn("wallet attempt outbox recovery deferred", "error", e)
			}
		}

	}
}
func (b *CanonicalWalletBridge) finishPoolSegment(ctx context.Context, user string, segment AuthorizationSegment) error {
	result, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='finished',pin_state='finished',updated_at=now() WHERE authorization_id=$1 AND state IN ('released','settling')`, segment.AuthorizationID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		// PG finished may have committed before an interrupted Redis cleanup.
		// Resume only that exact original identity, never an unrelated row.
		var finished bool
		if err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE authorization_id=$1 AND platform_user_id=$2 AND lease_id=$3 AND state='finished' AND pin_state='finished')`, segment.AuthorizationID, user, segment.LeaseID).Scan(&finished); err != nil {
			return err
		}
		if !finished {
			return nil
		}
	}
	store, ok := b.store.(MediaWalletStateStore)
	if !ok {
		return errors.New("wallet retention unavailable")
	}
	var pending bool
	err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND ((kind<>'media' AND state<>'finished' AND expiry_ack_at IS NULL) OR (kind='media' AND pin_state<>'finished')))`, user, segment.LeaseID).Scan(&pending)
	if err != nil {
		return err
	}
	err = store.UnprotectMediaLease(ctx, user, segment.LeaseID, segment.AuthorizationID, segment.EventID, pending)
	if err == nil {
		_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET funding_terminal_cleanup_at=COALESCE(funding_terminal_cleanup_at,now()) WHERE authorization_id=$1 AND platform_user_id=$2 AND lease_id=$3 AND state='finished' AND pin_state='finished'`, segment.AuthorizationID, user, segment.LeaseID)
		b.wakeFundingTerminalRecovery()
	}
	return err
}
func (b *CanonicalWalletBridge) releasePoolAttempt(ctx context.Context, parent, user string, segments []AuthorizationSegment, writeToken ...string) error {
	if len(segments) > 0 && segments[0].Kind == "llm" {
		owned, err := b.hasDurableLLMFinancialOwnership(ctx, parent)
		if err != nil {
			return err
		}
		if owned || b.ImmediateWalletReleaseMode("llm") == "enabled" {
			token := ""
			if len(writeToken) > 0 {
				token = writeToken[0]
			}
			return b.finishZeroPoolAttempt(ctx, parent, user, segments, token)
		}
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	count, err := lockWalletAttempt(ctx, tx, parent)
	if err != nil {
		return err
	}
	if count != len(segments) {
		return errors.New("wallet zero release group mismatch")
	}
	token := ""
	if len(writeToken) > 0 {
		token = writeToken[0]
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='released',actual_units=0,updated_at=now() WHERE parent_authorization_id=$1 AND kind<>'media' AND state IN ('prepared','held','indeterminate','released') AND ($2='' OR authorization_token=$2) AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment positive WHERE positive.parent_authorization_id=$1 AND (positive.actual_units>0 OR positive.remainder_payload IS NOT NULL))`, parent, token)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != int64(count) {
		return nil
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, segment := range segments {
		_, err = b.store.ReleaseCanonicalWalletHold(ctx, user, segment.AuthorizationID, "released", "zero_cost")
		if err != nil && !isHoldNotArmed(err) {
			return err
		}
	}
	return nil
}
