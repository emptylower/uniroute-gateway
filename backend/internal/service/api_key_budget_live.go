package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const liveBudgetZeroSource = "live_final_usage_zero"

// The final usage row counts only amounts not owned by durable window budgets.
// Configuration alone cannot prove that historical windows were budgeted.
func (s *OpenAIGatewayService) liveAPIKeyQuotaRemainder(ctx context.Context, record *LiveCallRecord, total int64) (*float64, error) {
	if s.canonicalWallet == nil || s.canonicalWallet.outboxDB == nil || record.AuthorizationID == "" {
		return nil, nil
	}
	db := s.canonicalWallet.outboxDB
	var available bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('api_key_budget_authorization') IS NOT NULL AND to_regclass('wallet_live_provisional') IS NOT NULL`).Scan(&available); err != nil {
		return nil, err
	}
	if !available {
		return nil, nil
	}
	var raw []byte
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT windows,api_key_id=$2 AND user_id=$3 AND billing_snapshot_id=$4 FROM wallet_live_provisional WHERE token=$1`, record.AuthorizationID, record.APIKeyID, record.UserID, record.BillingSnapshotID).Scan(&raw, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, ErrAPIKeyBudgetConflict
	}
	var windows []LiveWindow
	if err = json.Unmarshal(raw, &windows); err != nil {
		return nil, err
	}
	if len(windows) == 0 {
		return nil, nil
	}
	captured := int64(0)
	currentBudget := false
	for i, w := range windows {
		var state string
		var actual sql.NullInt64
		err = db.QueryRowContext(ctx, `SELECT state,captured_units FROM api_key_budget_authorization WHERE authorization_id=$1 AND api_key_id=$2 AND user_id=$3 AND snapshot_id=$4 AND kind='live'`, w.Token, record.APIKeyID, record.UserID, record.BillingSnapshotID).Scan(&state, &actual)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if i == len(windows)-1 {
			currentBudget = true
		}
		if state == "captured" && actual.Valid {
			captured, err = AddUnits(captured, actual.Int64)
			if err != nil {
				return nil, err
			}
		}
	}
	if !currentBudget && captured == 0 {
		return nil, nil
	}
	remaining := int64(0)
	if !currentBudget && total > captured {
		remaining = total - captured
	}
	usd := float64(remaining) / canonicalWalletUnitsPerUSD
	return &usd, nil
}

// Sealing is durable before RPCs. Every segment and the provisional row are
// locked before the Key/allowance; a failed ACK leaves recoverable zero proof.
func (b *CanonicalWalletBridge) finalizeLiveWindowZero(ctx context.Context, record *LiveCallRecord, seq int, total, settled int64, auth string) error {
	if b == nil || b.outboxDB == nil {
		return nil
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	n, err := lockWalletAttempt(ctx, tx, auth)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	var raw []byte
	var valid bool
	err = tx.QueryRowContext(ctx, `SELECT windows,status='finalizing' AND api_key_id=$2 AND user_id=$3 AND billing_snapshot_id=$4 FROM wallet_live_provisional WHERE token=$1 FOR UPDATE`, record.AuthorizationID, record.APIKeyID, record.UserID, record.BillingSnapshotID).Scan(&raw, &valid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrAPIKeyBudgetConflict
	}
	var windows []LiveWindow
	if err = json.Unmarshal(raw, &windows); err != nil {
		return err
	}
	if seq != len(windows) || seq < 1 || windows[seq-1].Token != auth || windows[seq-1].PendingUnits != 0 || total > settled {
		return ErrAPIKeyBudgetConflict
	}
	sum := int64(0)
	for _, w := range windows[:seq-1] {
		sum, err = AddUnits(sum, w.SettledUnits)
		if err != nil {
			return err
		}
	}
	if sum != settled {
		return ErrAPIKeyBudgetConflict
	}
	proof, _ := json.Marshal(map[string]any{"source": liveBudgetZeroSource, "session_authorization_id": record.AuthorizationID, "window_authorization_id": auth, "call_hash": record.CallHash, "window_seq": seq, "total_units": total, "previous_settled_units": settled})
	res, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state=CASE WHEN state='finished' THEN state ELSE 'released' END,known_fee_units=0,terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,$5::jsonb),write_ended_at=COALESCE(write_ended_at,now()),updated_at=now() WHERE parent_authorization_id=$1 AND kind='live' AND platform_user_id=$2 AND billing_snapshot_id=$3 AND actual_units=0 AND (known_fee_units IS NULL OR known_fee_units=0) AND NOT fee_pending AND NOT evidence_pending AND remainder_payload IS NULL AND (settlement_payload IS NULL OR COALESCE((settlement_payload->>'amount_units')::bigint,0)=0) AND (terminal_evidence IS NULL OR terminal_evidence=$5::jsonb) AND state IN ('prepared','held','indeterminate','released','finished') AND $4::bigint=0`, auth, record.PlatformUserID, record.BillingSnapshotID, int64(0), string(proof))
	if err != nil {
		return err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if changed != int64(n) {
		return fmt.Errorf("live zero evidence conflicts with financial obligation")
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	segments, err := b.authorizationSegments(ctx, auth)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		segment.ActualUnits = 0
		if err = b.protectPoolAttempt(ctx, auth, record.PlatformUserID, record.BillingSnapshotID, &segment, true); err != nil {
			return err
		}
		if _, err = b.store.ReleaseCanonicalWalletHold(ctx, record.PlatformUserID, segment.AuthorizationID, "released", "zero_cost"); err != nil && !isHoldNotArmed(err) && !errors.Is(err, ErrCanonicalWalletHoldMissing) {
			return err
		}
		if err = b.finishPoolSegment(ctx, record.PlatformUserID, segment); err != nil {
			return err
		}
	}
	budgetTx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = budgetTx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, budgetTx, auth); err != nil {
		return err
	}
	if err = releaseAPIKeyBudgetIfPresentTx(ctx, budgetTx, auth); err != nil {
		return err
	}
	return budgetTx.Commit()
}

func (b *CanonicalWalletBridge) completeRecoveredLiveZeroFinalizations(ctx context.Context) error {
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT p.token,jsonb_array_length(p.windows),p.call_hash,p.platform_user_id,p.billing_currency,COALESCE(a.captured_units,0)
 FROM wallet_live_provisional p JOIN api_key_budget_authorization a ON a.authorization_id=p.windows->-1->>'token'
 WHERE p.status='finalizing' AND jsonb_array_length(p.windows)>0 AND a.kind='live' AND a.api_key_id=p.api_key_id AND a.user_id=p.user_id AND a.snapshot_id=p.billing_snapshot_id
 AND ((a.state='released' AND EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.authorization_id AND s.terminal_evidence->>'source'=$1)) OR a.state='captured')
 AND EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.authorization_id)
 AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.authorization_id AND (s.state<>'finished' OR s.pin_state<>'finished')) LIMIT 32`, liveBudgetZeroSource)
	if err != nil {
		return err
	}
	type entry struct {
		token, callHash, user, currency string
		seq                             int
		units                           int64
	}
	list := []entry{}
	for rows.Next() {
		var e entry
		if err = rows.Scan(&e.token, &e.seq, &e.callHash, &e.user, &e.currency, &e.units); err != nil {
			break
		}
		list = append(list, e)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, e := range list {
		event := ""
		if e.units > 0 {
			event = LiveWindowSettlementEventID(e.callHash, e.seq, e.user, e.currency)
		}
		if err = b.liveProvisional.CompleteFinalization(ctx, e.token, event, e.units, e.seq, time.Now().UTC()); err != nil && !errors.Is(err, ErrLiveProvisionalNotFound) {
			return err
		}
	}
	return nil
}
