package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
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
	err = tx.QueryRowContext(ctx, `SELECT bool_or(state IN ('expiry_pending','expired_unknown')),bool_and(kind='llm' AND state='indeterminate' AND ((terminal_sealed_at IS NULL AND NOT reader_started) OR reader_handoff_at IS NOT NULL) AND NOT evidence_pending AND NOT fee_pending AND known_fee_units IS NULL AND NOT EXISTS(SELECT 1 FROM wallet_risk_admission risk WHERE risk.parent_authorization_id=a.parent_authorization_id) AND expiry_deadline IS NOT NULL AND expiry_deadline<=now() AND ((write_ended_at IS NOT NULL AND (first_write_at IS NOT NULL OR legacy_completion_proof IS NOT NULL)) OR (first_write_at IS NOT NULL AND write_active_until IS NOT NULL AND write_active_until<=now())) AND (write_active_until IS NULL OR write_active_until<=now()) AND actual_units=0 AND settlement_payload IS NULL AND remainder_payload IS NULL AND NOT EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.event_id)) ,sum(held_units)::bigint FROM wallet_authorization_segment a WHERE parent_authorization_id=$1`, parent).Scan(&pending, &eligible, &heldTotal)
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
	var immediate bool
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND expiry_intent_version=2)`, parent).Scan(&immediate); err != nil {
		return false, err
	}
	if immediate {
		return b.RecoverImmediateWalletExpiry(ctx, parent)
	}
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
	err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND expiry_ack_at IS NULL AND ((kind<>'media' AND state<>'finished') OR (kind='media' AND pin_state<>'finished')))`, user, s.LeaseID).Scan(&pending)
	if err != nil {
		return err
	}
	if err = store.UnprotectMediaLease(ctx, user, s.LeaseID, s.AuthorizationID, s.EventID, pending); err != nil {
		return err
	}
	_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET expiry_cleanup_at=COALESCE(expiry_cleanup_at,now()),pin_state=CASE WHEN expiry_intent_version=2 THEN 'finished' ELSE pin_state END,funding_terminal_cleanup_at=CASE WHEN expiry_intent_version=2 THEN COALESCE(funding_terminal_cleanup_at,now()) ELSE funding_terminal_cleanup_at END WHERE authorization_id=$1 AND expiry_ack_at IS NOT NULL`, s.AuthorizationID)
	if err == nil {
		b.wakeFundingTerminalRecovery()
	}
	return err
}

type walletImmediateExpiryRequest struct {
	mediaPinRequest
	AuthorizationToken    string `json:"authorization_token"`
	ExpiryVersion         int    `json:"expiry_version"`
	ExpiryDeadline        string `json:"expiry_deadline"`
	ExpiryPolicyVersion   string `json:"expiry_policy_version"`
	ExpiryTerminalProof   string `json:"expiry_terminal_proof"`
	LegacyCompletionProof string `json:"legacy_completion_proof,omitempty"`
}

// ClaimImmediateWalletExpiry commits the entire authorization's intent before
// contacting the Worker. The immutable terminal evidence belongs to the reader
// or task owner; expiry cannot manufacture it from a stale claim or a clock.
func (b *CanonicalWalletBridge) ClaimImmediateWalletExpiry(ctx context.Context, parent string, deadline time.Time, proof string) (bool, error) {
	if b == nil || b.outboxDB == nil || !walletProofHex(proof) || deadline.IsZero() {
		return false, errors.New("wallet immediate expiry terminal evidence unavailable")
	}
	deadline = deadline.UTC().Truncate(time.Millisecond)
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, parent)
	if err != nil || count == 0 {
		return false, err
	}
	var kind string
	var eligible, existing, conflicting bool
	err = tx.QueryRowContext(ctx, `SELECT min(kind),bool_and(kind IN ('llm','media') AND kind=(SELECT kind FROM wallet_authorization_segment WHERE authorization_id=$1) AND state='indeterminate' AND expiry_intent_version=0 AND terminal_sealed_at IS NOT NULL AND terminal_evidence IS NOT NULL AND NOT evidence_pending AND NOT fee_pending AND known_fee_units IS NULL AND actual_units=0 AND settlement_payload IS NULL AND remainder_payload IS NULL AND authorization_token IS NOT NULL AND authorization_token<>'' AND (write_active_until IS NULL OR write_active_until<=now()) AND NOT EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.event_id)),bool_and(expiry_intent_version=2),bool_or(expiry_intent_version=1 OR (expiry_intent_version=2 AND (expiry_v2_deadline IS DISTINCT FROM $2 OR expiry_terminal_proof IS DISTINCT FROM $3 OR expiry_policy_version IS DISTINCT FROM $4))) FROM wallet_authorization_segment a WHERE parent_authorization_id=$1`, parent, deadline, proof, WalletImmediateReleasePolicyVersion).Scan(&kind, &eligible, &existing, &conflicting)
	if err != nil {
		return false, err
	}
	if conflicting {
		return false, errors.New("wallet immediate expiry immutable intent conflict")
	}
	if existing {
		return true, tx.Commit()
	}
	if !eligible {
		return false, nil
	}
	mode := b.ImmediateWalletReleaseMode(kind)
	if mode != "enabled" {
		if mode == "shadow" {
			slog.Info("wallet immediate expiry shadow candidate", "parent_authorization_id", parent, "kind", kind, "segment_count", count)
		}
		return false, nil
	}
	var due bool
	if err = tx.QueryRowContext(ctx, `SELECT $1::timestamptz<=now()`, deadline).Scan(&due); err != nil || !due {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='expiry_pending',expiry_intent_version=2,expiry_v2_deadline=$2,expiry_policy_version=$3,expiry_terminal_proof=$4,write_active_until=NULL,updated_at=now() WHERE parent_authorization_id=$1 AND expiry_intent_version=0 AND state='indeterminate'`, parent, deadline, WalletImmediateReleasePolicyVersion, proof)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != int64(count) {
		return false, errors.New("wallet immediate expiry whole-group claim mismatch")
	}
	return true, tx.Commit()
}

func walletExpiryGatewayJobID(ctx context.Context, db walletRiskQueryer, parent, user, snapshot, kind string) (string, error) {
	if kind == "llm" {
		return parent, nil
	}
	if kind != "media" {
		return "", errors.New("wallet immediate expiry kind unavailable")
	}
	var job string
	if err := db.QueryRowContext(ctx, `SELECT id FROM gateway_media_task WHERE authorization_id=$1 AND platform_user_id=$2 AND billing_snapshot_id=$3`, parent, user, snapshot).Scan(&job); err != nil {
		return "", err
	}
	return job, nil
}

func walletLegacyMappingProof(parent, job, user, snapshot, token, proof string, segment AuthorizationSegment) (string, error) {
	parts := []string{"wallet-task-pin-legacy-mapping-v2", segment.AuthorizationID, job, user, segment.LeaseID, snapshot, segment.EventID, strconv.FormatInt(segment.HeldUnits, 10), segment.Kind, token, proof}
	// Every input comes from the primary PG segment/task identity, never from
	// provider-returned strings or an RPC caller's choice of kind.
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(parts); err != nil {
		return "", err
	}
	digest := sha256.Sum256(bytes.TrimSuffix(buffer.Bytes(), []byte("\n")))
	return hex.EncodeToString(digest[:]), nil
}

func (b *CanonicalWalletBridge) expireImmediateWalletPin(ctx context.Context, job, user, snapshot, token, proof, legacy string, segment AuthorizationSegment, deadline time.Time) (WalletTaskPinReceipt, time.Time, error) {
	var receipt WalletTaskPinReceipt
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return receipt, time.Time{}, errors.New("wallet immediate expiry control unavailable")
	}
	request := walletImmediateExpiryRequest{mediaPinRequest: mediaPinRequest{
		AuthorizationKind: segment.Kind, AuthorizationID: segment.AuthorizationID,
		GatewayJobID: job, PlatformUserID: user, LeaseID: segment.LeaseID,
		Held: newCanonicalWalletAmountObject(segment.HeldUnits), BillingSnapshotID: snapshot,
		SettlementEventID: segment.EventID, USDPolicyVersion: "usd-wallet-v1",
	}, AuthorizationToken: token, ExpiryVersion: 2, ExpiryDeadline: walletWireTimestamp(deadline), ExpiryPolicyVersion: WalletImmediateReleasePolicyVersion, ExpiryTerminalProof: proof, LegacyCompletionProof: legacy}
	if err := client.doJSON(ctx, http.MethodPost, "/api/internal/v2/wallet/task-pins/expire", "wallet:task-pin", segment.AuthorizationID+":expiry:2", request, &receipt); err != nil {
		return receipt, time.Time{}, err
	}
	releasedAt, err := VerifyWalletTaskPinReceipt(client.cfg.Secret, WalletTaskPinReceiptExpected{
		GatewayJobID: job, AuthorizationID: segment.AuthorizationID,
		PlatformUserID: user, LeaseID: segment.LeaseID, BillingSnapshotID: snapshot,
		SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits,
		AuthorizationKind: segment.Kind, AuthorizationToken: token, Status: "expired_unknown",
		ExpiryDeadline: deadline, ExpiryTerminalProof: proof, LegacyCompletionProof: legacy,
	}, receipt)
	return receipt, releasedAt, err
}

func (b *CanonicalWalletBridge) acknowledgeImmediateWalletExpiry(ctx context.Context, parent, user, snapshot, token string, segment AuthorizationSegment, receipt WalletTaskPinReceipt, releasedAt time.Time) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, tx, parent); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET expiry_ack_at=COALESCE(expiry_ack_at,now()),expiry_receipt_id=$2,expiry_released_at=$3,expiry_receipt_signature=$4,expiry_receipt=$5::jsonb WHERE authorization_id=$1 AND parent_authorization_id=$6 AND platform_user_id=$7 AND billing_snapshot_id=$8 AND authorization_token=$9 AND expiry_intent_version=2 AND (expiry_receipt_id IS NULL OR (expiry_receipt_id=$2 AND expiry_released_at=$3 AND expiry_receipt_signature=$4 AND expiry_receipt=$5::jsonb))`, segment.AuthorizationID, receipt.ExpiryReceiptID, releasedAt, receipt.ReceiptSignature, string(raw), parent, user, snapshot, token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("wallet immediate expiry acknowledgement identity conflict")
	}
	// Unique per segment, with the signed first D1 release time. PG ACK latency
	// never shifts the rolling window; Redis cleanup is an independent saga.
	_, err = tx.ExecContext(ctx, `INSERT INTO wallet_unknown_release_counter(authorization_id,parent_authorization_id,platform_user_id,authorization_token,billing_snapshot_id,kind,held_units,expiry_version,receipt_id,receipt_signature,released_at,receipt) VALUES($1,$2,$3,$4,$5,$6,$7,2,$8,$9,$10,$11::jsonb) ON CONFLICT(authorization_id) DO NOTHING`, segment.AuthorizationID, parent, user, token, snapshot, segment.Kind, segment.HeldUnits, receipt.ExpiryReceiptID, receipt.ReceiptSignature, releasedAt, string(raw))
	if err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverImmediateWalletExpiry resumes a durable v2 intent even after either
// rollout flag is off/shadow. RPC errors and 409 conflicts never downgrade it
// to v1. Every acknowledged share records its counter before cleanup.
func (b *CanonicalWalletBridge) RecoverImmediateWalletExpiry(ctx context.Context, parent string) (bool, error) {
	if b == nil || b.outboxDB == nil {
		return false, errors.New("wallet immediate expiry database unavailable")
	}
	segments, err := b.authorizationSegments(ctx, parent)
	if err != nil {
		return false, err
	}
	if len(segments) == 0 {
		return false, nil
	}
	var user, snapshot string
	var found bool
	var deferred error
	for _, segment := range segments {
		var token, policy, proof, legacy string
		var version int
		var deadline, released sql.NullTime
		var ack sql.NullTime
		err = b.outboxDB.QueryRowContext(ctx, `SELECT platform_user_id,billing_snapshot_id,COALESCE(authorization_token,''),expiry_intent_version,expiry_v2_deadline,COALESCE(expiry_policy_version,''),COALESCE(expiry_terminal_proof,''),COALESCE(expiry_legacy_mapping_proof,''),expiry_ack_at,expiry_released_at FROM wallet_authorization_segment WHERE authorization_id=$1 AND parent_authorization_id=$2`, segment.AuthorizationID, parent).Scan(&user, &snapshot, &token, &version, &deadline, &policy, &proof, &legacy, &ack, &released)
		if err != nil {
			return found, err
		}
		if version != 2 {
			continue
		}
		found = true
		if !deadline.Valid || policy != WalletImmediateReleasePolicyVersion || !walletProofHex(proof) || token == "" {
			return true, errors.New("wallet immediate expiry persisted intent incomplete")
		}
		if !ack.Valid {
			job, identityErr := walletExpiryGatewayJobID(ctx, b.outboxDB, parent, user, snapshot, segment.Kind)
			if identityErr != nil {
				deferred = errors.Join(deferred, identityErr)
				continue
			}
			if legacy == "" {
				legacy, err = walletLegacyMappingProof(parent, job, user, snapshot, token, proof, segment)
				if err != nil {
					return true, err
				}
				if _, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET expiry_legacy_mapping_proof=$2 WHERE authorization_id=$1 AND expiry_intent_version=2 AND expiry_legacy_mapping_proof IS NULL`, segment.AuthorizationID, legacy); err != nil {
					return true, err
				}
			}
			receipt, releasedAt, rpcErr := b.expireImmediateWalletPin(ctx, job, user, snapshot, token, proof, legacy, segment, deadline.Time)
			if rpcErr != nil {
				deferred = errors.Join(deferred, rpcErr)
				continue
			}
			if err = b.acknowledgeImmediateWalletExpiry(ctx, parent, user, snapshot, token, segment, receipt, releasedAt); err != nil {
				deferred = errors.Join(deferred, err)
				continue
			}
		}
		// The cleanup record does not participate in the counter's eligibility.
		if err = b.cleanExpiredPoolHold(ctx, user, segment); err != nil {
			deferred = errors.Join(deferred, err)
		}
	}
	if !found {
		return false, nil
	}
	// Positive evidence arriving behind the expiry fence keeps its persisted
	// original payload and proceeds using new settlement funding after all ACKs.
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return true, errors.Join(deferred, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, tx, parent); err != nil {
		return true, errors.Join(deferred, err)
	}
	var missing bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND expiry_intent_version=2 AND expiry_ack_at IS NULL)`, parent).Scan(&missing); err != nil {
		return true, errors.Join(deferred, err)
	}
	if missing {
		return true, errors.Join(deferred, errors.New("wallet immediate expiry acknowledgements incomplete"))
	}
	if _, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state=CASE WHEN actual_units>0 THEN 'settling' ELSE 'expired_unknown' END,updated_at=now() WHERE parent_authorization_id=$1 AND expiry_intent_version=2 AND state='expiry_pending' AND expiry_ack_at IS NOT NULL`, parent); err != nil {
		return true, errors.Join(deferred, err)
	}
	if err = tx.Commit(); err != nil {
		return true, errors.Join(deferred, err)
	}
	segments, err = b.authorizationSegments(ctx, parent)
	if err != nil {
		return true, errors.Join(deferred, err)
	}
	if err = b.enqueuePoolSettlements(ctx, user, snapshot, segments); err != nil {
		deferred = errors.Join(deferred, err)
	}
	return true, deferred
}
