package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"
)

// walletBarrierReleaseGrace is how long past a segment's expiry deadline an
// unresolved fee/evidence barrier may keep its hold armed. A barrier is set when
// a positive usage cannot be proven strictly. If the reader's own evidence can
// never be priced, no trusted fee will arrive after the deadline either, so the
// hold is released as an unknown cost that the platform bears instead of staying
// frozen forever. Evidence that CAN be priced is never released this way: it is
// staged and charged by the normal recovery.
const walletBarrierReleaseGrace = 30 * time.Minute

// walletBarrierDue reports whether every segment of the attempt is past its
// expiry deadline plus walletBarrierReleaseGrace. It is a cheap pre-check; the
// release itself re-checks everything under the attempt lock.
func (b *CanonicalWalletBridge) walletBarrierDue(ctx context.Context, parent, token string) bool {
	var due sql.NullBool
	if b.outboxDB.QueryRowContext(ctx, `SELECT bool_and(expiry_deadline IS NOT NULL AND expiry_deadline+make_interval(secs=>$3::float8)<=now()) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2`, parent, token, walletBarrierReleaseGrace.Seconds()).Scan(&due) != nil {
		return false
	}
	return due.Valid && due.Bool
}

// alertFeeBarrier logs a barrier condition that needs an operator, at most once a
// minute per process and kind, so a persistent condition cannot flood the log or
// hide a different kind of alert.
func (b *CanonicalWalletBridge) alertFeeBarrier(kind, msg, parent string, args ...any) {
	b.feeBarrierMu.Lock()
	if b.feeBarrierLastAlert == nil {
		b.feeBarrierLastAlert = map[string]time.Time{}
	}
	if time.Since(b.feeBarrierLastAlert[kind]) < time.Minute {
		b.feeBarrierMu.Unlock()
		return
	}
	b.feeBarrierLastAlert[kind] = time.Now()
	b.feeBarrierMu.Unlock()
	slog.Warn(msg, append([]any{"parent_authorization_id", parent, "reason_code", kind}, args...)...)
}

// releaseExpiredFeeBarrier releases an unresolvable fee/evidence barrier whose
// whole attempt is past its deadline plus grace, as an unknown cost that the
// platform bears. Everything happens in ONE transaction under the attempt lock,
// all-or-nothing across the attempt's segments: it re-checks that no fee is known
// or staged, that the reader finished and that the stored evidence is still the
// evidence the decision was based on, then seals the whole attempt and clears the
// barrier. A sealed attempt can no longer be re-latched by the reader journal
// recovery. The normal immediate unknown release (signed Worker ACK, Redis
// release, unknown counter) follows. Only with the LLM release mode enabled.
func (b *CanonicalWalletBridge) releaseExpiredFeeBarrier(ctx context.Context, h *AuthorizationHandle, user string, rawEvidence []byte, evidence WalletReaderEvidence) bool {
	if b == nil || b.outboxDB == nil || h == nil {
		return false
	}
	token := h.LastWriteToken()
	if token == "" || !b.walletBarrierDue(ctx, h.ID, token) {
		return false
	}
	if mode := b.ImmediateWalletReleaseMode("llm"); mode != "enabled" {
		if mode == "shadow" {
			b.alertFeeBarrier("fee_barrier_overdue_shadow_candidate", "wallet unresolved fee barrier is past its deadline; it would be released as an unknown cost in enabled mode", h.ID, "platform_user_id", user)
		}
		return false
	}
	var pinned any
	if len(rawEvidence) > 0 {
		pinned = string(rawEvidence)
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, h.ID)
	if err != nil || count == 0 {
		return false
	}
	var allGuards, barrier bool
	var held int64
	// A safe-4xx label is neither a fee nor a zero proof. It must not keep an
	// unpriceable positive barrier forever; known/staged fees still fence release.
	err = tx.QueryRowContext(ctx, `SELECT bool_and(a.kind='llm' AND a.state='indeterminate' AND a.authorization_token=$2 AND a.write_ended_at IS NOT NULL AND a.reader_handoff_at IS NOT NULL AND a.known_fee_units IS NULL AND a.zero_intent_at IS NULL AND a.expiry_intent_version=0 AND a.actual_units=0 AND a.settlement_payload IS NULL AND a.remainder_payload IS NULL AND (a.write_active_until IS NULL OR a.write_active_until<=now()) AND a.expiry_deadline IS NOT NULL AND a.expiry_deadline+make_interval(secs=>$3::float8)<=now() AND (a.ordinal<>0 OR a.reader_evidence IS NOT DISTINCT FROM $4::jsonb) AND NOT EXISTS(SELECT 1 FROM wallet_billing_pending p WHERE p.parent_authorization_id=a.parent_authorization_id) AND NOT EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.event_id)),COALESCE(bool_or(a.fee_pending OR a.evidence_pending),false),COALESCE(sum(a.held_units),0)::bigint FROM wallet_authorization_segment a WHERE a.parent_authorization_id=$1`, h.ID, token, walletBarrierReleaseGrace.Seconds(), pinned).Scan(&allGuards, &barrier, &held)
	if err != nil || !allGuards || !barrier {
		return false
	}
	var durable []byte
	if tx.QueryRowContext(ctx, `SELECT COALESCE(reader_evidence,'null'::jsonb) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0`, h.ID, token).Scan(&durable) != nil {
		return false
	}
	const reason = "barrier_timeout_unknown"
	rawProof, err := json.Marshal(struct {
		AuthorizationID, Token, Snapshot, Reason string
		Evidence                                 json.RawMessage
	}{h.ID, token, h.SnapshotID, reason, durable})
	if err != nil {
		return false
	}
	sum := sha256.Sum256(rawProof)
	result, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,$3::jsonb),expiry_terminal_proof=COALESCE(expiry_terminal_proof,$4),evidence_pending=false,fee_pending=false WHERE parent_authorization_id=$1 AND authorization_token=$2`, h.ID, token, string(rawProof), hex.EncodeToString(sum[:]))
	if err != nil {
		return false
	}
	if n, rowsErr := result.RowsAffected(); rowsErr != nil || n != int64(count) {
		return false
	}
	// The immediate-expiry claim (intent version 2) is part of the SAME transaction,
	// exactly as ClaimImmediateWalletExpiry would write it: once this commits, the
	// existing intent-2 recovery always finishes the release (signed Worker ACK,
	// Redis release, unknown counter) even if this process dies right after, and no
	// sealed, barrier-free attempt is ever left without an owner.
	var sealedAt time.Time
	var proof string
	if tx.QueryRowContext(ctx, `SELECT terminal_sealed_at,expiry_terminal_proof FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0`, h.ID, token).Scan(&sealedAt, &proof) != nil || proof == "" {
		return false
	}
	claim, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='expiry_pending',expiry_intent_version=2,expiry_v2_deadline=$2,expiry_policy_version=$3,expiry_terminal_proof=$4,write_active_until=NULL,updated_at=now() WHERE parent_authorization_id=$1 AND expiry_intent_version=0 AND state='indeterminate'`, h.ID, sealedAt.UTC().Truncate(time.Millisecond), WalletImmediateReleasePolicyVersion, proof)
	if err != nil {
		return false
	}
	if n, rowsErr := claim.RowsAffected(); rowsErr != nil || n != int64(count) {
		return false
	}
	if tx.Commit() != nil {
		return false
	}
	h.completeWrite()
	slog.Warn("wallet unresolved fee barrier released after its deadline; the platform bears the unknown cost",
		"parent_authorization_id", h.ID, "platform_user_id", user, "reason_code", "fee_barrier_timeout_release",
		"held_units", held, "segment_count", count, "grace_seconds", int(walletBarrierReleaseGrace.Seconds()),
		"evidence_complete", evidence.Complete, "evidence_valid", evidence.Valid, "evidence_malformed", evidence.Malformed, "evidence_observed_positive", evidence.ObservedPositive)
	// Best effort now; the periodic intent-2 recovery completes it otherwise.
	_, _ = b.RecoverImmediateWalletExpiry(ctx, h.ID)
	return true
}
