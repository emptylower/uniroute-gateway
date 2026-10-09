package service

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
)

type WalletBillingBinding struct {
	PendingID             string `json:"pending_id"`
	ParentAuthorizationID string `json:"parent_authorization_id"`
	AuthorizationToken    string `json:"authorization_token"`
	PlatformUserID        string `json:"platform_user_id"`
	BillingSnapshotID     string `json:"billing_snapshot_id"`
	EventID               string `json:"event_id"`
	ProviderAccountID     int64  `json:"provider_account_id"`
	Source                string `json:"source"`
	PolicyVersion         string `json:"policy_version"`
	FeeUnits              int64  `json:"fee_units"`
}

// The original command and amount survive Apply success followed by an observe
// failure. Applied=false means replay this receipt, never a zero-cost release.
type WalletBillingChargeReceipt struct {
	ReceiptID string               `json:"receipt_id"`
	Binding   WalletBillingBinding `json:"binding"`
	Command   UsageBillingCommand  `json:"command"`
}

type walletUsageStageKey struct{}
type walletUsageStage struct {
	bridge  *CanonicalWalletBridge
	handle  *AuthorizationHandle
	ids     []string
	zeroIDs []string
	seen    bool
	err     error
}

func (b *CanonicalWalletBridge) SetBillingEvidenceRepository(repo UsageBillingRepository) {
	if b == nil {
		return
	}
	b.billingEvidenceMu.Lock()
	b.billingEvidenceRepo = repo
	b.billingEvidenceMu.Unlock()
}

func (b *CanonicalWalletBridge) SetBillingEvidenceDependencies(logs UsageLogRepository, deps *billingDeps) {
	if b == nil {
		return
	}
	b.billingEvidenceMu.Lock()
	b.billingEvidenceLogs = logs
	b.billingEvidenceDeps = deps
	b.billingEvidenceMu.Unlock()
}

// PrepareUsageTask runs only pricing/normalization on the reader-owning thread.
// The submitted closure thereafter retains the dispatcher and durable IDs, not
// result pointers, live gin contexts, credentials or mutable pricing objects.
func (h *AuthorizationHandle) PrepareUsageTask(ctx context.Context, task UsageRecordTask) (UsageRecordTask, error) {
	h = h.activeAttempt()
	if h == nil || h.stageUsage == nil || task == nil {
		return task, nil
	}
	return h.stageUsage(ctx, task)
}

// SealFinancialEvidence is called by the reader owner after its stop/join or
// per-turn actor handoff. A cancelled request context alone is not this proof.
func (h *AuthorizationHandle) SealFinancialEvidence(ctx context.Context, reason string) error {
	h = h.activeAttempt()
	if h == nil || h.sealEvidence == nil {
		return nil
	}
	return h.sealEvidence(ctx, reason)
}

func (h *AuthorizationHandle) DispatchFinancialEvidence(ctx context.Context) error {
	h = h.activeAttempt()
	if h == nil || h.dispatchEvidence == nil {
		return nil
	}
	return h.dispatchEvidence(ctx)
}

func (b *CanonicalWalletBridge) prepareDurableUsage(ctx context.Context, h *AuthorizationHandle, task UsageRecordTask) (next UsageRecordTask, err error) {
	stage := &walletUsageStage{bridge: b, handle: h}
	defer func() {
		if recovered := recover(); recovered != nil {
			h.MarkAbandoned("usage_stage_panic")
			err = errors.New("wallet usage handoff interrupted")
		}
	}()
	stageCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	task(context.WithValue(stageCtx, walletUsageStageKey{}, stage))
	if stage.err != nil {
		return nil, stage.err
	}
	if !stage.seen {
		h.MarkAbandoned("usage_stage_incomplete")
		return nil, errors.New("wallet usage evidence has not reached durable billing")
	}
	if err = b.sealPoolEvidence(stageCtx, h, "usage_handoff"); err != nil {
		return nil, err
	}
	// Known zero has no financial work to put behind the best-effort queue.
	// Finish the signed Worker -> PG ACK -> Redis release synchronously;
	// durable pending remains available to recovery if any step fails.
	for _, id := range stage.zeroIDs {
		if err = b.ApplyPendingWalletBilling(stageCtx, id); err != nil {
			return nil, err
		}
	}
	if len(stage.zeroIDs) > 0 {
		h.mu.Lock()
		h.zeroAcknowledged = true
		h.mu.Unlock()
	}
	ids := append([]string(nil), stage.ids...)
	return func(runCtx context.Context) {
		for _, id := range ids {
			if applyErr := b.ApplyPendingWalletBilling(runCtx, id); applyErr != nil {
				slog.Warn("wallet durable billing deferred", "pending_id", id, "error_class", "billing_handoff")
			}
		}
	}, nil
}

func (b *CanonicalWalletBridge) stageWalletBilling(ctx context.Context, cmd *UsageBillingCommand) (string, error) {
	if cmd == nil || cmd.WalletBinding == nil || b == nil || b.outboxDB == nil {
		return "", errors.New("wallet billing evidence unavailable")
	}
	binding := cmd.WalletBinding
	if binding.PolicyVersion != WalletImmediateReleasePolicyVersion || binding.AuthorizationToken == "" || binding.ParentAuthorizationID == "" || binding.PlatformUserID == "" || binding.BillingSnapshotID == "" || binding.ProviderAccountID != cmd.AccountID || (binding.Source != "llm_http_usage" && binding.Source != "llm_ws_usage") || binding.FeeUnits < 0 {
		return "", errors.New("wallet billing evidence identity invalid")
	}
	cmd.Normalize()
	raw, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, binding.ParentAuthorizationID)
	if err != nil || count == 0 {
		return "", fmt.Errorf("wallet evidence group unavailable: %w", err)
	}
	var matches bool
	err = tx.QueryRowContext(ctx, `SELECT bool_and(platform_user_id=$2 AND billing_snapshot_id=$3 AND authorization_token=$4 AND kind='llm') FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, binding.ParentAuthorizationID, binding.PlatformUserID, binding.BillingSnapshotID, binding.AuthorizationToken).Scan(&matches)
	if err != nil || !matches {
		return "", errors.New("wallet billing identity conflict")
	}
	var snapshotRaw []byte
	if err = tx.QueryRowContext(ctx, `SELECT payload FROM wallet_billing_snapshot WHERE id=$1`, binding.BillingSnapshotID).Scan(&snapshotRaw); err != nil {
		return "", err
	}
	snapshot, err := UnmarshalBillingSnapshotPayload(snapshotRaw)
	if err != nil || snapshot.AccountID != cmd.AccountID || snapshot.UserID != cmd.UserID || snapshot.APIKeyID != cmd.APIKeyID {
		return "", errors.New("wallet billing frozen provider identity conflict")
	}
	var knownZero bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND (zero_ack_at IS NOT NULL OR zero_intent_at IS NOT NULL))`, binding.ParentAuthorizationID).Scan(&knownZero); err != nil {
		return "", err
	}
	if knownZero && binding.FeeUnits > 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO wallet_billing_anomaly(parent_authorization_id,authorization_token,reason,evidence) VALUES($1,$2,'positive_after_zero',$3::jsonb) ON CONFLICT DO NOTHING`, binding.ParentAuthorizationID, binding.AuthorizationToken, string(raw))
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			return "", err
		}
		return "", ErrWalletPositiveAfterZero
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO wallet_billing_pending(id,parent_authorization_id,authorization_token,platform_user_id,billing_snapshot_id,event_id,provider_account_id,evidence_source,policy_version,fee_units,command) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) ON CONFLICT(id) DO NOTHING`, binding.PendingID, binding.ParentAuthorizationID, binding.AuthorizationToken, binding.PlatformUserID, binding.BillingSnapshotID, binding.EventID, binding.ProviderAccountID, binding.Source, binding.PolicyVersion, binding.FeeUnits, string(raw))
	if err != nil {
		return "", err
	}
	var original []byte
	if err = tx.QueryRowContext(ctx, `SELECT command FROM wallet_billing_pending WHERE id=$1`, binding.PendingID).Scan(&original); err != nil {
		return "", err
	}
	var stored UsageBillingCommand
	if err = json.Unmarshal(original, &stored); err != nil {
		return "", err
	}
	if stored.WalletBinding == nil || *stored.WalletBinding != *binding {
		return "", ErrUsageBillingRequestConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET known_fee_units=$2,fee_pending=true,evidence_pending=false WHERE parent_authorization_id=$1 AND authorization_token=$3 AND zero_ack_at IS NULL`, binding.ParentAuthorizationID, binding.FeeUnits, binding.AuthorizationToken)
	if err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return binding.PendingID, nil
}

var ErrWalletPositiveAfterZero = errors.New("wallet positive evidence quarantined after signed zero")

func (b *CanonicalWalletBridge) ApplyPendingWalletBilling(ctx context.Context, id string) error {
	b.billingEvidenceMu.RLock()
	repo := b.billingEvidenceRepo
	b.billingEvidenceMu.RUnlock()
	if repo == nil {
		return errors.New("wallet billing repository recovery unavailable")
	}
	var raw []byte
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT p.command FROM wallet_billing_pending p WHERE p.id=$1 AND p.canonical_ack_at IS NULL AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment a WHERE a.parent_authorization_id=p.parent_authorization_id AND (a.terminal_sealed_at IS NULL OR a.reader_handoff_at IS NULL OR a.evidence_pending))`, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	var cmd UsageBillingCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return err
	}
	result, err := repo.Apply(ctx, &cmd)
	if err != nil {
		return err
	}
	if result == nil || result.OriginalCharge == nil {
		return errors.New("wallet original charge receipt missing")
	}
	if result.Quarantined {
		return ErrWalletPositiveAfterZero
	}
	receipt := result.OriginalCharge
	if receipt.Binding.PendingID != id || receipt.Command.RequestFingerprint != cmd.RequestFingerprint {
		return errors.New("wallet original receipt identity mismatch")
	}
	binding := receipt.Binding
	b.billingEvidenceMu.RLock()
	logs := b.billingEvidenceLogs
	deps := b.billingEvidenceDeps
	invalidator := b.billingEvidenceInvalidator
	b.billingEvidenceMu.RUnlock()
	if binding.FeeUnits == 0 {
		segments, readErr := b.authorizationSegments(ctx, binding.ParentAuthorizationID)
		if readErr != nil {
			return readErr
		}
		if err = b.finishZeroPoolAttempt(ctx, binding.ParentAuthorizationID, binding.PlatformUserID, segments, binding.AuthorizationToken); err != nil {
			return err
		}
	} else if !b.ObserveSettlement(CanonicalWalletSettlementEvent{EventID: binding.EventID, GatewayRequestID: receipt.Command.RequestID, PlatformUserID: binding.PlatformUserID, Currency: "USD", AmountUnits: binding.FeeUnits, OccurredAt: time.Now().UTC(), AuthorizationToken: binding.AuthorizationToken, AuthorizationID: binding.ParentAuthorizationID, BillingSnapshotID: binding.BillingSnapshotID}) {
		return errors.New("wallet original charge canonical handoff deferred")
	}
	// Diagnostics and caches must not delay the financial acknowledgement.
	// Leave canonical pending until these best-effort writes return so recovery
	// can retry the idempotent usage log after a crash in this interval.
	if receipt.Command.WalletUsageLog != nil {
		writeUsageLogBestEffort(ctx, logs, receipt.Command.WalletUsageLog, "service.wallet.billing")
	}
	if result.Applied && deps != nil {
		if deps.deferredService != nil {
			deps.deferredService.ScheduleLastUsedUpdate(receipt.Command.AccountID)
		}
		if deps.billingCacheService != nil && receipt.Command.APIKeyRateLimitCost > 0 {
			deps.billingCacheService.QueueUpdateAPIKeyRateLimitUsage(receipt.Command.APIKeyID, receipt.Command.APIKeyRateLimitCost)
		}
		if result.APIKeyQuotaExhausted && invalidator != nil {
			var key string
			if b.outboxDB.QueryRowContext(ctx, `SELECT key FROM api_keys WHERE id=$1`, receipt.Command.APIKeyID).Scan(&key) == nil && key != "" {
				invalidator.InvalidateAuthCacheByKey(ctx, key)
			}
		}
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `UPDATE wallet_billing_pending SET canonical_ack_at=COALESCE(canonical_ack_at,now()),updated_at=now() WHERE id=$1 AND apply_ack_at IS NOT NULL`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET fee_pending=false WHERE parent_authorization_id=$1 AND authorization_token=$2`, binding.ParentAuthorizationID, binding.AuthorizationToken); err != nil {
		return err
	}
	return tx.Commit()
}

func (b *CanonicalWalletBridge) recoverBillingEvidence(ctx context.Context) {
	b.recoverWalletReaderJournals(ctx)
	b.recoverReaderBillingEvidence(ctx)
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT id FROM wallet_billing_pending WHERE canonical_ack_at IS NULL ORDER BY updated_at,id LIMIT 32`)
	if err != nil {
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			break
		}
		ids = append(ids, id)
	}
	_ = rows.Close()
	for _, id := range ids {
		_, _ = b.outboxDB.ExecContext(ctx, `UPDATE wallet_billing_pending SET updated_at=now() WHERE id=$1`, id)
		_ = b.ApplyPendingWalletBilling(ctx, id)
	}
}

func (b *CanonicalWalletBridge) sealPoolEvidence(ctx context.Context, h *AuthorizationHandle, reason string) error {
	h.mu.Lock()
	handoffErr := h.readerEvidenceErr
	h.mu.Unlock()
	if handoffErr != nil {
		return handoffErr
	}
	h.completeWrite()
	var durableEvidence []byte
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT COALESCE(reader_evidence,'null'::jsonb) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0`, h.ID, h.LastWriteToken()).Scan(&durableEvidence); err != nil {
		return err
	}
	proof := struct {
		AuthorizationID, Token, Snapshot, Reason string
		Evidence                                 json.RawMessage
	}{h.ID, h.LastWriteToken(), h.SnapshotID, reason, durableEvidence}
	raw, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,$3::jsonb),expiry_terminal_proof=COALESCE(expiry_terminal_proof,$4),evidence_pending=false,reader_handoff_at=CASE WHEN $5 THEN COALESCE(reader_handoff_at,now()) ELSE reader_handoff_at END,known_fee_units=CASE WHEN $5 THEN 0 ELSE known_fee_units END WHERE parent_authorization_id=$1 AND authorization_token=$2 AND write_ended_at IS NOT NULL AND (reader_handoff_at IS NOT NULL OR $5)`, h.ID, h.LastWriteToken(), string(raw), hex.EncodeToString(sum[:]), reason == "proven_not_written")
	if err != nil {
		return err
	}
	var deadline time.Time
	var terminalProof string
	var eligible bool
	err = b.outboxDB.QueryRowContext(ctx, `SELECT terminal_sealed_at,expiry_terminal_proof,known_fee_units IS NULL AND NOT fee_pending AND NOT evidence_pending AND legacy_zero_candidate IS NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0 AND terminal_sealed_at IS NOT NULL`, h.ID, h.LastWriteToken()).Scan(&deadline, &terminalProof, &eligible)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("wallet terminal reader ownership is not stopped")
	}
	if err != nil {
		return err
	}
	if eligible {
		claimed, claimErr := b.ClaimImmediateWalletExpiry(ctx, h.ID, deadline.UTC().Truncate(time.Millisecond), terminalProof)
		if claimErr != nil {
			return claimErr
		}
		if claimed {
			_, err = b.RecoverImmediateWalletExpiry(ctx, h.ID)
		}
	}
	return err
}
