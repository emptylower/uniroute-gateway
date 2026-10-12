package service

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Old unlimited LLM Keys did not increment quota_used. Only a durable applied
// charge receipt proves both the exact USD amount and which counter effects ran.
// Never infer a fee from token counts or add an aggregate usage-log total.
func reconcileHistoricalLLMKeyBudgetTx(ctx context.Context, tx *sql.Tx, key int64, exclude string) error {
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('wallet_billing_charge_receipt') IS NOT NULL`).Scan(&present); err != nil {
		return err
	}
	if !present {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.receipt FROM wallet_billing_charge_receipt r LEFT JOIN api_key_budget_authorization a ON a.authorization_id=r.receipt->'binding'->>'parent_authorization_id' WHERE r.api_key_id=$1 AND r.receipt->'binding'->>'parent_authorization_id'<>$2 AND (a.authorization_id IS NULL OR a.state NOT IN ('captured','released')) ORDER BY r.created_at,r.request_id`, key, exclude)
	if err != nil {
		return err
	}
	receipts := []WalletBillingChargeReceipt{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var r WalletBillingChargeReceipt
		if err = json.Unmarshal(raw, &r); err != nil {
			break
		}
		receipts = append(receipts, r)
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
	for _, r := range receipts {
		cmd, binding := r.Command, r.Binding
		if cmd.APIKeyID != key || cmd.UserID <= 0 || binding.ParentAuthorizationID == "" || binding.FeeUnits < 0 || cmd.SettlementCurrency != CurrencyUSD || (binding.Source != "llm_http_usage" && binding.Source != "llm_ws_usage") || binding.BillingSnapshotID == "" || cmd.WalletBinding == nil || *cmd.WalletBinding != binding || binding.PolicyVersion != WalletImmediateReleasePolicyVersion {
			return ErrAPIKeyBudgetReconciliation
		}
		var valid bool
		if err = tx.QueryRowContext(ctx, `SELECT api_key_id=$2 AND user_id=$3 FROM wallet_billing_snapshot WHERE id=$1`, binding.BillingSnapshotID, key, cmd.UserID).Scan(&valid); err != nil {
			return err
		}
		if !valid {
			return ErrAPIKeyBudgetReconciliation
		}
		var state string
		validLedger := true
		err = tx.QueryRowContext(ctx, `SELECT state,api_key_id=$2 AND user_id=$3 AND snapshot_id=$4 AND kind='llm' FROM api_key_budget_authorization WHERE authorization_id=$1`, binding.ParentAuthorizationID, key, cmd.UserID, binding.BillingSnapshotID).Scan(&state, &validLedger)
		if err == sql.ErrNoRows {
			_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind) VALUES($1,$2,$3,$4,$5,'llm')`, binding.ParentAuthorizationID, key, cmd.UserID, binding.BillingSnapshotID, binding.FeeUnits)
		}
		if err != nil {
			return err
		}
		if !validLedger {
			return ErrAPIKeyBudgetReconciliation
		}
		if state == "captured" || state == "released" {
			continue
		}
		// A positive old APIKeyQuotaCost proves that quota_used already includes
		// this fee. Old window counters retain their original limit/bucket policy.
		if cmd.APIKeyQuotaCost == 0 && binding.FeeUnits > 0 {
			_, err = tx.ExecContext(ctx, `UPDATE api_keys SET quota_used=quota_used+$2::numeric/100000000,status=CASE WHEN status='active' AND quota>0 AND quota_used+$2::numeric/100000000>=quota THEN 'quota_exhausted' ELSE status END,updated_at=now() WHERE id=$1`, key, binding.FeeUnits)
			if err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='captured',captured_units=$2,updated_at=now() WHERE authorization_id=$1 AND api_key_id=$3 AND user_id=$4 AND snapshot_id=$5 AND kind='llm'`, binding.ParentAuthorizationID, binding.FeeUnits, key, cmd.UserID, binding.BillingSnapshotID)
		if err != nil {
			return err
		}
	}
	return nil
}
