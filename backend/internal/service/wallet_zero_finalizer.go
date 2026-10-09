package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

func (b *CanonicalWalletBridge) hasDurableLLMFinancialOwnership(ctx context.Context, parent string) (bool, error) {
	var owned bool
	err := b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment a WHERE a.parent_authorization_id=$1 AND a.kind='llm' AND (a.reader_owner_id IS NOT NULL OR a.reader_journal_host IS NOT NULL OR a.zero_intent_at IS NOT NULL OR a.zero_ack_at IS NOT NULL OR a.expiry_intent_version=2 OR EXISTS(SELECT 1 FROM wallet_risk_admission r WHERE r.parent_authorization_id=a.parent_authorization_id AND r.kind='llm' AND r.policy_version='wallet-immediate-v5') OR EXISTS(SELECT 1 FROM wallet_billing_pending p WHERE p.parent_authorization_id=a.parent_authorization_id AND p.policy_version='wallet-immediate-v5') OR EXISTS(SELECT 1 FROM wallet_billing_snapshot s WHERE s.id=a.billing_snapshot_id AND s.payload->'flags'->>'wallet_immediate_release_policy_version'='wallet-immediate-v5')))`, parent).Scan(&owned)
	return owned, err
}

// finishZeroPoolAttempt keeps the signed pin finish, durable acknowledgement
// and Redis cleanup in that order, including partial ACK and restart recovery.
func (b *CanonicalWalletBridge) finishZeroPoolAttempt(ctx context.Context, parent, user string, segments []AuthorizationSegment, token string) error {
	if token == "" {
		if err := b.outboxDB.QueryRowContext(ctx, `SELECT COALESCE(authorization_token,'') FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, parent).Scan(&token); err != nil {
			return err
		}
	}
	if token == "" {
		return errors.New("wallet zero lacks a fenced write identity")
	}
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return errors.New("wallet signed zero control unavailable")
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, parent)
	if err != nil {
		return err
	}
	if count != len(segments) {
		return errors.New("wallet signed zero group mismatch")
	}
	var snapshot string
	if err = tx.QueryRowContext(ctx, `SELECT billing_snapshot_id FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, parent).Scan(&snapshot); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state=CASE WHEN state='finished' THEN state ELSE 'released' END,actual_units=0,known_fee_units=0,zero_intent_at=COALESCE(zero_intent_at,now()),updated_at=now() WHERE parent_authorization_id=$1 AND kind='llm' AND authorization_token=$2 AND state IN ('held','indeterminate','released','finished') AND terminal_sealed_at IS NOT NULL AND reader_handoff_at IS NOT NULL AND write_ended_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment positive WHERE positive.parent_authorization_id=$1 AND (positive.actual_units>0 OR positive.remainder_payload IS NOT NULL OR positive.known_fee_units>0 OR positive.evidence_pending OR (positive.fee_pending AND positive.known_fee_units IS NULL)))`, parent, token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != int64(count) {
		return errors.New("wallet zero conflicts with pending positive evidence")
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, segment := range segments {
		expected := WalletTaskPinReceiptExpected{GatewayJobID: parent, AuthorizationID: segment.AuthorizationID, PlatformUserID: user, LeaseID: segment.LeaseID, BillingSnapshotID: snapshot, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: token, Status: "released"}
		request := struct {
			mediaPinRequest
			AuthorizationToken string `json:"authorization_token"`
		}{mediaPinRequest: mediaPinRequest{AuthorizationKind: "llm", AuthorizationID: segment.AuthorizationID, GatewayJobID: parent, PlatformUserID: user, LeaseID: segment.LeaseID, Held: newCanonicalWalletAmountObject(segment.HeldUnits), BillingSnapshotID: snapshot, SettlementEventID: segment.EventID, USDPolicyVersion: "usd-wallet-v1", Resolution: "released", SettlementEventIDs: []string{}}, AuthorizationToken: token}
		actual := newCanonicalWalletAmountObject(0)
		request.Actual = &actual
		var receipt WalletTaskPinReceipt
		if err = client.doJSON(ctx, http.MethodPost, "/api/internal/v2/wallet/task-pins/finish", "wallet:task-pin", parent+":"+segment.AuthorizationID+":zero", request, &receipt); err != nil {
			return err
		}
		releasedAt, verifyErr := VerifyWalletTaskPinReceipt(b.cfg.Secret, expected, receipt)
		if verifyErr != nil {
			return verifyErr
		}
		raw, marshalErr := json.Marshal(receipt)
		if marshalErr != nil {
			return marshalErr
		}
		ackTx, beginErr := b.outboxDB.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		if _, err = lockWalletAttempt(ctx, ackTx, parent); err != nil {
			_ = ackTx.Rollback()
			return err
		}
		ack, ackErr := ackTx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET zero_receipt=COALESCE(zero_receipt,$3::jsonb),zero_ack_at=COALESCE(zero_ack_at,now()),zero_receipt_id=COALESCE(zero_receipt_id,$4),zero_released_at=COALESCE(zero_released_at,$5),zero_receipt_signature=COALESCE(zero_receipt_signature,$6),pin_state='finished' WHERE authorization_id=$1 AND parent_authorization_id=$2 AND state IN ('released','finished') AND known_fee_units=0 AND NOT evidence_pending AND (zero_receipt IS NULL OR zero_receipt=$3::jsonb)`, segment.AuthorizationID, parent, string(raw), receipt.FinishReceiptID, releasedAt, receipt.ReceiptSignature)
		if ackErr != nil {
			_ = ackTx.Rollback()
			return ackErr
		}
		n, ackErr = ack.RowsAffected()
		if ackErr != nil || n != 1 {
			_ = ackTx.Rollback()
			return errors.New("wallet zero receipt acknowledgement conflict")
		}
		if err = ackTx.Commit(); err != nil {
			return err
		}
	}
	var allAck bool
	if err = b.outboxDB.QueryRowContext(ctx, `SELECT bool_and(zero_ack_at IS NOT NULL AND known_fee_units=0) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, parent).Scan(&allAck); err != nil {
		return err
	}
	if !allAck {
		return errors.New("wallet zero group acknowledgement incomplete")
	}
	for _, segment := range segments {
		_, err = b.store.ReleaseCanonicalWalletHold(ctx, user, segment.AuthorizationID, "released", "zero_cost")
		if err != nil && !isHoldNotArmed(err) && !errors.Is(err, ErrCanonicalWalletHoldMissing) {
			return err
		}
		segment.ActualUnits = 0
		segment.State = "released"
		segment.PinState = "finished"
		if err = b.finishPoolSegment(ctx, user, segment); err != nil {
			return err
		}
	}
	return nil
}
