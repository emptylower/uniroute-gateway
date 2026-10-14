package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func (s *MediaTaskService) mediaSupplierPolicy(ctx context.Context, r *mediaTaskRecord) (*FrozenMediaSupplierPricing, error) {
	snap, err := s.snapshots.store.GetBillingSnapshot(ctx, r.SnapshotID)
	if err != nil {
		return nil, err
	}
	if snap.Pricing.MediaSupplierPricing != nil && (snap.RequestedModel != r.Model || snap.BillingModel != r.Model || snap.AccountID != s.accountID) {
		return nil, errors.New("media frozen supplier identity conflict")
	}
	return snap.Pricing.MediaSupplierPricing, nil
}

func (s *MediaTaskService) finishMediaFreeSuccess(ctx context.Context, r *mediaTaskRecord) error {
	actual, loss, policy, err := s.mediaSuccessAmount(ctx, r, r.Result)
	if err != nil || policy == nil || actual != 0 || r.Result.Status != "success" {
		return errors.Join(err, errors.New("media free success requires authoritative zero usage"))
	}
	evidence, err := mediaFeeEvidence(r, actual, loss, policy)
	if err != nil {
		return err
	}
	result, err := json.Marshal(r.Result)
	if err != nil {
		return err
	}
	if err = s.closeAcknowledgedMediaFunding(ctx, r); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, tx, r.AuthorizationID); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET status='completed',financial_state='released_zero',actual_units=0,pin_state='finished',result=$3::jsonb,fee_evidence=COALESCE(fee_evidence,$4::jsonb),fee_pending_at=COALESCE(fee_pending_at,now()),financial_released_at=COALESCE(financial_released_at,(SELECT min(zero_released_at) FROM wallet_authorization_segment WHERE parent_authorization_id=$2)),media_journal_pending=false,settled_at=COALESCE(settled_at,now()),claimed_by=NULL,claim_until=NULL WHERE id=$1 AND authorization_id=$2 AND financial_state IN ('zero_pending','released_unknown','released_zero') AND financial_terminal_proof IS NOT NULL AND (fee_evidence IS NULL OR fee_evidence=$4::jsonb) AND EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$2) AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$2 AND (pin_state<>'finished' OR (zero_ack_at IS NULL AND (expiry_ack_at IS NULL OR expiry_cleanup_at IS NULL))))`, r.ID, r.AuthorizationID, string(result), string(evidence))
	if err != nil {
		return err
	}
	if n, e := res.RowsAffected(); e != nil || n != 1 {
		return errors.Join(e, errors.New("media free success acknowledgement incomplete"))
	}
	zero := int64(0)
	r.ActualUnits = &zero
	if err = s.recordMediaUsageTx(ctx, tx, r, zero); err != nil {
		return err
	}
	if err = ReleaseAPIKeyBudgetTx(ctx, tx, r.AuthorizationID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.unprotect(ctx, r)
}

func (s *MediaTaskService) recordMediaUsageTx(ctx context.Context, tx *sql.Tx, r *mediaTaskRecord, actual int64) error {
	usd := mediaUSD(actual)
	_, err := tx.ExecContext(ctx, `INSERT INTO usage_logs(user_id,api_key_id,account_id,request_id,model,requested_model,billing_snapshot_id,billing_mode,media_type,total_cost,actual_cost,source_currency,settlement_currency,exchange_rate,exchange_rate_source,exchange_rate_as_of,source_cost,base_cost,rate_multiplier,image_count,video_count,video_duration_seconds,image_size,created_at)
 SELECT $1,$2,$3,$4::text,$5::text,$5::text,$6,'per_request',$7,$8::numeric,$8::numeric,'USD','USD',1,'usd-e8-v1',$9,$8::numeric,$8::numeric,1,$10,$11,$12,$13,$9 WHERE NOT EXISTS(SELECT 1 FROM usage_logs WHERE request_id=$4::text AND api_key_id=$2)`, r.UserID, r.APIKeyID, s.accountID, r.ID, r.Model, r.SnapshotID, r.MediaType, usd, r.CreatedAt, mediaImageCount(r), mediaVideoCount(r), mediaDuration(r), mediaImageSize(r))
	return err
}

func (s *MediaTaskService) mediaSuccessAmount(ctx context.Context, r *mediaTaskRecord, result MediaProviderResult) (int64, int64, *FrozenMediaSupplierPricing, error) {
	policy, err := s.mediaSupplierPolicy(ctx, r)
	if err != nil {
		return 0, 0, nil, err
	}
	if policy != nil && (!mediaTrustedResult(r, result) || result.ProviderModel != mediaUpstreamModel(r.Model)) {
		return 0, 0, policy, errors.New("media supplier usage model identity conflict")
	}
	actual, loss, err := mediaSupplierActualUnits(policy, result.CreditsConsumed, r.QuotedUnits)
	return actual, loss, policy, err
}

func mediaFeeEvidence(r *mediaTaskRecord, actual, loss int64, policy *FrozenMediaSupplierPricing) ([]byte, error) {
	evidence := map[string]any{"source": "kie_authoritative_get", "provider_task_id": r.ProviderTaskID, "snapshot_id": r.SnapshotID, "quoted_units": r.QuotedUnits, "result": r.Result}
	if policy != nil {
		evidence["supplier_pricing"] = policy
		evidence["actual_units"] = actual
		evidence["over_cap_loss_units"] = loss
	}
	return json.Marshal(evidence)
}
