package service

import (
	"context"
	"encoding/json"
	"errors"
)

// An invalid or changed late fee can never become a new user charge. Serialize
// its platform-only evidence with the same attempt lock as the signed zero.
func (b *CanonicalWalletBridge) quarantineLateWalletUsage(ctx context.Context, parent, token, snapshot, user string, cmd *UsageBillingCommand, evidence WalletReaderEvidence) (bool, error) {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, parent)
	if err != nil || count == 0 {
		return false, errors.New("wallet late evidence group unavailable")
	}
	var valid, zero bool
	if err = tx.QueryRowContext(ctx, `SELECT bool_and(authorization_token=$2 AND billing_snapshot_id=$3 AND platform_user_id=$4 AND kind='llm'),bool_or(zero_intent_at IS NOT NULL OR zero_ack_at IS NOT NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, parent, token, snapshot, user).Scan(&valid, &zero); err != nil {
		return false, err
	}
	if !valid {
		return false, ErrUsageBillingRequestConflict
	}
	if !zero {
		return false, nil
	}
	raw, err := json.Marshal(struct {
		Source   string               `json:"source"`
		Command  *UsageBillingCommand `json:"candidate_command"`
		Evidence WalletReaderEvidence `json:"selected_reader_evidence"`
	}{"platform_anomaly_excluded_from_wallet", cmd, evidence})
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO wallet_billing_anomaly(parent_authorization_id,authorization_token,reason,evidence) VALUES($1,$2,'positive_after_zero',$3::jsonb) ON CONFLICT DO NOTHING`, parent, token, string(raw)); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
