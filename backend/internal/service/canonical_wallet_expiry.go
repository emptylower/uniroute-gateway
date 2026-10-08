package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type walletExpiryRequest struct {
	mediaPinRequest
	ExpiryVersion         int       `json:"expiry_version"`
	ExpiryDeadline        time.Time `json:"expiry_deadline"`
	LegacyCompletionProof string    `json:"legacy_completion_proof,omitempty"`
}

type walletExpiryReceipt struct {
	GatewayJobID          string                      `json:"gateway_job_id"`
	AuthorizationID       string                      `json:"authorization_id"`
	PlatformUserID        string                      `json:"platform_user_id"`
	LeaseID               string                      `json:"lease_id"`
	BillingSnapshotID     string                      `json:"billing_snapshot_id"`
	SettlementEventID     string                      `json:"settlement_event_id"`
	Held                  canonicalWalletAmountObject `json:"held"`
	USDPolicyVersion      string                      `json:"usd_wallet_policy_version"`
	AuthorizationKind     string                      `json:"authorization_kind"`
	Status                string                      `json:"status"`
	ExpiryVersion         int                         `json:"expiry_version"`
	ExpiryDeadline        time.Time                   `json:"expiry_deadline"`
	ExpiryReceiptID       string                      `json:"expiry_receipt_id"`
	LegacyCompletionProof string                      `json:"legacy_completion_proof,omitempty"`
}

func (b *CanonicalWalletBridge) expirePoolPin(ctx context.Context, parent, user, snapshot string, s AuthorizationSegment, deadline time.Time, proof string) (string, error) {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return "", errors.New("wallet expiry control unavailable")
	}
	req := walletExpiryRequest{mediaPinRequest: mediaPinRequest{AuthorizationKind: "llm", AuthorizationID: s.AuthorizationID, GatewayJobID: parent, PlatformUserID: user, LeaseID: s.LeaseID, Held: newCanonicalWalletAmountObject(s.HeldUnits), BillingSnapshotID: snapshot, SettlementEventID: s.EventID, USDPolicyVersion: "usd-wallet-v1"}, ExpiryVersion: 1, ExpiryDeadline: deadline, LegacyCompletionProof: proof}
	var receipt walletExpiryReceipt
	if err := client.doJSON(ctx, http.MethodPost, "/api/internal/v2/wallet/task-pins/expire", "wallet:task-pin", s.AuthorizationID+":expiry:1", req, &receipt); err != nil {
		return "", err
	}
	held, err := parseCanonicalWalletAmountObject("held", receipt.Held)
	expected := s.AuthorizationID + ":expiry:1"
	if err != nil || held != s.HeldUnits || receipt.GatewayJobID != parent || receipt.AuthorizationID != s.AuthorizationID || receipt.PlatformUserID != user || receipt.LeaseID != s.LeaseID || receipt.BillingSnapshotID != snapshot || receipt.SettlementEventID != s.EventID || receipt.USDPolicyVersion != "usd-wallet-v1" || receipt.AuthorizationKind != "llm" || receipt.Status != "expired_unknown" || receipt.ExpiryVersion != 1 || !receipt.ExpiryDeadline.Equal(deadline) || receipt.ExpiryReceiptID != expected || receipt.LegacyCompletionProof != proof {
		return "", errors.New("wallet expiry acknowledgement mismatch")
	}
	return expected, nil
}

// Claiming expiry is one committed whole-group transition before the signed
// control RPC. A competing settlement must persist behind this pending fence.
func (b *CanonicalWalletBridge) claimPoolExpiry(ctx context.Context, parent string) (bool, error) {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, parent)
	if err != nil || count == 0 {
		return false, err
	}
	var pending, eligible bool
	var heldTotal int64
	err = tx.QueryRowContext(ctx, `SELECT bool_or(state IN ('expiry_pending','expired_unknown')),bool_and(kind='llm' AND state='indeterminate' AND expiry_deadline IS NOT NULL AND expiry_deadline<=now() AND ((write_ended_at IS NOT NULL AND (first_write_at IS NOT NULL OR legacy_completion_proof IS NOT NULL)) OR (first_write_at IS NOT NULL AND write_active_until IS NOT NULL AND write_active_until<=now())) AND (write_active_until IS NULL OR write_active_until<=now()) AND actual_units=0 AND settlement_payload IS NULL AND remainder_payload IS NULL AND NOT EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.event_id)) ,sum(held_units)::bigint FROM wallet_authorization_segment a WHERE parent_authorization_id=$1`, parent).Scan(&pending, &eligible, &heldTotal)
	if err != nil {
		return false, err
	}
	if pending {
		return true, tx.Commit()
	}
	if !eligible {
		return false, nil
	}
	if b.cfg.PoolExpiryMode != "enabled" {
		if b.cfg.PoolExpiryMode != "off" {
			slog.Info("wallet unknown expiry shadow candidate", "parent_authorization_id", parent, "segment_count", count, "held_units_total", heldTotal)
		}
		return false, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='expiry_pending',expiry_intent_version=1,write_active_until=NULL,updated_at=now() WHERE parent_authorization_id=$1 AND kind='llm' AND state='indeterminate'`, parent)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != int64(count) {
		return false, errors.New("wallet expiry group claim mismatch")
	}
	return true, tx.Commit()
}

func (b *CanonicalWalletBridge) recoverExpiredPoolGroup(ctx context.Context, parent, user, snapshot string) (bool, error) {
	claimed, err := b.claimPoolExpiry(ctx, parent)
	if err != nil || !claimed {
		return false, err
	}
	segments, err := b.authorizationSegments(ctx, parent)
	if err != nil {
		return true, err
	}
	for _, s := range segments {
		if s.State != "expiry_pending" {
			continue
		}
		var deadline time.Time
		var proof, receipt string
		var ack sql.NullTime
		err = b.outboxDB.QueryRowContext(ctx, `SELECT expiry_deadline,COALESCE(legacy_completion_proof,''),COALESCE(expiry_receipt_id,''),expiry_ack_at FROM wallet_authorization_segment WHERE authorization_id=$1 AND state='expiry_pending' AND expiry_intent_version=1`, s.AuthorizationID).Scan(&deadline, &proof, &receipt, &ack)
		if err != nil {
			return true, err
		}
		if ack.Valid {
			continue
		}
		receipt, err = b.expirePoolPin(ctx, parent, user, snapshot, s, deadline, proof)
		if err != nil {
			return true, err
		}
		// RPC may have committed even when this write fails. Intent is already
		// durable, so replay returns the same receipt rather than reactivating pin.
		_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET expiry_ack_at=now(),expiry_receipt_id=$2 WHERE authorization_id=$1 AND state='expiry_pending' AND expiry_intent_version=1 AND expiry_ack_at IS NULL`, s.AuthorizationID, receipt)
		if err != nil {
			return true, err
		}
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return true, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, tx, parent); err != nil {
		return true, err
	}
	var missing bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expiry_pending' AND expiry_ack_at IS NULL)`, parent).Scan(&missing); err != nil {
		return true, err
	}
	if missing {
		return true, errors.New("wallet expiry group acknowledgements incomplete")
	}
	_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state=CASE WHEN actual_units>0 THEN 'settling' ELSE 'expired_unknown' END,updated_at=now() WHERE parent_authorization_id=$1 AND state='expiry_pending' AND expiry_ack_at IS NOT NULL`, parent)
	if err != nil {
		return true, err
	}
	if err = tx.Commit(); err != nil {
		return true, err
	}
	segments, err = b.authorizationSegments(ctx, parent)
	if err != nil {
		return true, err
	}
	for _, s := range segments {
		if s.State != "expired_unknown" && (s.State != "settling" || s.Payload == nil || s.Payload.LeaseID != "") {
			continue
		}
		if err = b.cleanExpiredPoolHold(ctx, user, s); err != nil {
			return true, err
		}
	}
	if err = b.enqueuePoolSettlements(ctx, user, snapshot, segments); err != nil {
		return true, err
	}
	for i := range segments {
		s := &segments[i]
		if s.State != "settling" {
			continue
		}
		var status string
		if b.outboxDB.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id=$1`, s.EventID).Scan(&status) == nil && status == "delivered" {
			if err = b.protectPoolAttempt(ctx, parent, user, snapshot, s, true); err != nil {
				return true, err
			}
			if err = b.finishPoolSegment(ctx, user, *s); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

func (b *CanonicalWalletBridge) cleanExpiredPoolHold(ctx context.Context, user string, s AuthorizationSegment) error {
	// This is reservation expiry, not confirmed-zero cost. The durable expiry
	// receipt and original event/snapshot preserve future proven cost liability.
	_, err := b.store.ReleaseCanonicalWalletHold(ctx, user, s.AuthorizationID, "released", "expiry_unknown")
	if err != nil {
		var terminal *CanonicalWalletHoldNotArmedError
		replayReleased := errors.As(err, &terminal) && terminal.State == "released" && terminal.EventID == ""
		if !replayReleased && !errors.Is(err, ErrCanonicalWalletHoldMissing) && !errors.Is(err, ErrCanonicalWalletLeaseMissing) {
			return err
		}
	}
	store, ok := b.store.(MediaWalletStateStore)
	if !ok {
		return errors.New("wallet expiry retention unavailable")
	}
	var pending bool
	err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND ((kind<>'media' AND state<>'finished' AND expiry_ack_at IS NULL) OR (kind='media' AND pin_state<>'finished')))`, user, s.LeaseID).Scan(&pending)
	if err != nil {
		return err
	}
	if err = store.UnprotectMediaLease(ctx, user, s.LeaseID, s.AuthorizationID, s.EventID, pending); err != nil {
		return err
	}
	_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET expiry_cleanup_at=COALESCE(expiry_cleanup_at,now()) WHERE authorization_id=$1 AND expiry_ack_at IS NOT NULL`, s.AuthorizationID)
	return err
}
