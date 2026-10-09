package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Optional extension of the shared Redis store. Ordinary callers and their
// test doubles keep the existing canonical wallet interface unchanged.
type MediaWalletStateStore interface {
	ProtectMediaHold(context.Context, CanonicalWalletLease, CanonicalWalletHold) error
	UnprotectMediaLease(context.Context, string, string, string, string, bool) error
	ReadMediaWalletState(context.Context, string, []string, []string) (MediaWalletRawState, error)
}
type MediaWalletRawLease struct {
	FundedUnits    int64  `json:"funded_units"`
	ReturnedUnits  int64  `json:"returned_units"`
	ReturnRevision int64  `json:"return_revision"`
	BudgetRevision int64  `json:"budget_revision"`
	FundingFrozen  bool   `json:"funding_frozen"`
	LeaseID        string `json:"lease_id"`
	BudgetUnits    int64  `json:"budget_units"`
	ConsumedUnits  int64  `json:"consumed_units"`
	ReleasedUnits  int64  `json:"released_units"`
	ExpiresAtMS    int64  `json:"expires_at_ms"`
	Sealed         bool   `json:"sealed"`
	Recovered      bool   `json:"recovered"`
	Present        bool   `json:"present"`
}
type MediaWalletRawState struct {
	Leases       []MediaWalletRawLease
	Holds        []CanonicalWalletHold
	Reservations map[string]string
	Fingerprint  string
}

func mediaFinancialDeadline(r *mediaTaskRecord) *time.Time {
	deadline := r.FinancialRuntimeDeadline
	if r.QueryUncertaintyDeadline != nil && (deadline == nil || r.QueryUncertaintyDeadline.Before(*deadline)) {
		deadline = r.QueryUncertaintyDeadline
	}
	return deadline
}
func mediaTrustedResult(r *mediaTaskRecord, result MediaProviderResult) bool {
	return result.ProviderTaskID == r.ProviderTaskID && r.ProviderTaskID != "" &&
		(result.Status == "pending" || result.Status == "processing" || result.Status == "success" || result.Status == "failed")
}
func mediaTerminalEvidence(r *mediaTaskRecord, reason string, deadline time.Time) ([]byte, string, error) {
	raw, err := json.Marshal(struct {
		Policy, Source, Task, Parent, User, Snapshot, Token, ProviderID, Reason, Deadline string
	}{WalletImmediateReleasePolicyVersion, "kie_task_owner", r.ID, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.AuthorizationToken, r.ProviderTaskID, reason, walletWireTimestamp(deadline)})
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(raw)
	return raw, hex.EncodeToString(hash[:]), nil
}

// This callback runs at the decorated HTTP boundary, after any checkpoint pause.
// An active write cannot be financially terminated using an expired worker claim.
func (s *MediaTaskService) bindMediaWriteOwner(r *mediaTaskRecord, handle *AuthorizationHandle) {
	previous := handle.beforeWrite
	handle.beforeWrite = func(ctx context.Context, token string) error {
		r.WriteOwner, r.AuthorizationToken = r.ClaimedBy, token
		if err := s.beginMediaJournal(ctx, r, "write"); err != nil {
			return err
		}
		j := r.journal
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			j.record.NotSent = true
			return err
		}
		defer func() { _ = tx.Rollback() }()
		count, err := lockWalletAttempt(ctx, tx, r.AuthorizationID)
		if err != nil || count == 0 || count != len(r.Segments) {
			j.record.NotSent = true
			return errors.New("media write funding segment group unavailable")
		}
		res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET write_owner=$2,authorization_token=$3,write_started_at=now(),accepted_at=now(),financial_runtime_deadline=now()+CASE WHEN media_type='image' THEN interval '30 minutes' ELSE interval '2 hours' END,media_journal_volume=$4,media_journal_owner=$5,media_journal_pending=true,next_journal_recovery_at=now() WHERE id=$1 AND claimed_by=$2 AND claim_until>now() AND status='submitting' AND financial_state='held' AND write_started_at IS NULL AND provider_task_id IS NULL AND NOT media_journal_pending AND authorization_id=$6 AND platform_user_id=$7 AND billing_snapshot_id=$8 AND quoted_units=$9 AND held_units=$10`, r.ID, r.ClaimedBy, token, j.record.Volume, j.record.Owner, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits, r.HeldUnits)
		if err != nil {
			j.record.NotSent = true
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			_ = tx.Rollback()
			j.record.NotSent = true
			// The decorated callback refused before send; unlike a stale claim clock,
			// this live owner has direct proof that this POST never started.
			r.AuthorizationToken = token
			proofCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, e := s.prepareMediaFinancialTerminal(proofCtx, r, true, true); e != nil {
				return e
			}
			return errors.New("media write owner fence refused")
		}
		res, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,first_write_at=now(),write_active_until=NULL,updated_at=now() WHERE parent_authorization_id=$1 AND platform_user_id=$3 AND billing_snapshot_id=$4 AND kind='media' AND state IN ('prepared','held') AND pin_state='active' AND authorization_token IS NULL AND first_write_at IS NULL AND write_ended_at IS NULL AND zero_intent_at IS NULL AND expiry_intent_version=0 AND actual_units=0 AND settlement_payload IS NULL AND remainder_payload IS NULL`, r.AuthorizationID, token, r.PlatformUserID, r.SnapshotID)
		if err != nil {
			j.record.NotSent = true
			return err
		}
		n, err = res.RowsAffected()
		if err != nil || n != int64(count) {
			j.record.NotSent = true
			return errors.New("media write funding segment identity conflict")
		}
		if err = tx.Commit(); err != nil {
			j.record.NotSent = true
			return err
		}
		if previous != nil {
			err = previous(ctx, token)
			if err != nil {
				j.record.NotSent = true
			}
			return err
		}
		return nil
	}
}

// Return from Create (including stack unwind after panic) is the actual owner
// handoff. It binds a trustworthy provider ID even if the polling claim expired.
func (s *MediaTaskService) completeMediaWrite(ctx context.Context, r *mediaTaskRecord, providerID string) error {
	if r.AuthorizationToken == "" {
		return nil
	}
	j := r.journal
	if j == nil {
		return errors.New("media write durable owner unavailable")
	}
	j.record.Joined = true
	at := time.Now().UTC()
	j.record.WriteEndedAt = &at
	j.record.ProviderID = providerID
	if err := j.save(); err != nil {
		return err
	}
	return s.persistMediaWrite(ctx, r, j)
}

func (s *mediaTaskStore) queryFailed(ctx context.Context, r *mediaTaskRecord) error {
	res, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET query_episode_first_failed_at=COALESCE(query_episode_first_failed_at,now()),query_uncertainty_deadline=COALESCE(query_uncertainty_deadline,now()+interval '15 minutes') WHERE id=$1 AND claimed_by=$2 AND claim_until>now() AND provider_task_id=$3 AND financial_state IN ('held','unknown_pending','released_unknown')`, r.ID, r.ClaimedBy, r.ProviderTaskID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("media query owner lost")
	}
	return nil
}
func (s *mediaTaskStore) queryConfirmed(ctx context.Context, r *mediaTaskRecord) error {
	res, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET query_episode_first_failed_at=NULL,query_uncertainty_deadline=NULL WHERE id=$1 AND claimed_by=$2 AND claim_until>now() AND provider_task_id=$3`, r.ID, r.ClaimedBy, r.ProviderTaskID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("media query owner lost")
	}
	return nil
}

func (s *MediaTaskService) prepareMediaFinancialTerminal(ctx context.Context, r *mediaTaskRecord, zero bool, notWritten ...bool) (bool, error) {
	provenNotWritten := len(notWritten) > 0 && notWritten[0]
	durableZero := zero && r.journal != nil && r.journal.record.Joined && r.journal.record.Failure != nil && mediaTrustedResult(r, *r.journal.record.Failure)
	journalOwner := ""
	if durableZero {
		journalOwner = r.journal.record.Owner
	}
	if r.FeePendingAt != nil || r.ActualUnits != nil && *r.ActualUnits > 0 {
		return false, nil
	}
	if !zero && s.bridge.ImmediateWalletReleaseMode("media") != "enabled" {
		if s.bridge.ImmediateWalletReleaseMode("media") == "shadow" && r.WriteEndedAt != nil {
			if r.ProviderTaskID == "" || mediaFinancialDeadline(r) != nil && !mediaFinancialDeadline(r).After(time.Now()) {
				slog.Info("media immediate financial release shadow candidate", "task_id", r.ID, "policy", WalletImmediateReleasePolicyVersion)
			}
		}
		return false, nil
	}
	if !zero && r.WriteEndedAt == nil {
		return false, nil
	}
	deadline := time.Now().UTC().Truncate(time.Millisecond)
	reason := "provider_confirmed_zero"
	if provenNotWritten {
		reason = "confirmed_not_written"
	}
	if !zero {
		reason = "create_owner_ended_unknown"
		if r.ProviderTaskID != "" {
			d := mediaFinancialDeadline(r)
			if d == nil || d.After(time.Now()) {
				return false, nil
			}
			deadline = d.UTC().Truncate(time.Millisecond)
			reason = "provider_financial_deadline"
		}
	}
	if r.FinancialTerminalAt != nil {
		deadline = *r.FinancialTerminalAt
	}
	if r.AuthorizationToken == "" {
		r.AuthorizationToken = r.AuthorizationID + ".media-not-written"
	}
	evidence, proof, err := mediaTerminalEvidence(r, reason, deadline)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, r.AuthorizationID)
	if err != nil {
		return false, err
	}
	if provenNotWritten && r.WriteStartedAt == nil {
		var unstarted bool
		if count == 0 || tx.QueryRowContext(ctx, `SELECT bool_and(kind='media' AND platform_user_id=$3 AND billing_snapshot_id=$4 AND (authorization_token IS NULL OR authorization_token=$2) AND first_write_at IS NULL AND write_ended_at IS NULL AND state IN ('prepared','held') AND terminal_sealed_at IS NULL AND known_fee_units IS NULL AND zero_intent_at IS NULL AND expiry_intent_version=0 AND actual_units=0 AND NOT evidence_pending AND NOT fee_pending AND settlement_payload IS NULL AND remainder_payload IS NULL) AND sum(held_units)=$5 FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, r.AuthorizationID, r.AuthorizationToken, r.PlatformUserID, r.SnapshotID, r.HeldUnits).Scan(&unstarted) != nil || !unstarted {
			return false, errors.New("media not-written funding owner identity conflict")
		}
	}
	state := "unknown_pending"
	if zero {
		state = "zero_pending"
	}
	res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET financial_state=$3,authorization_token=$4,financial_terminal_proof=COALESCE(financial_terminal_proof,$5),financial_terminal_at=COALESCE(financial_terminal_at,$6),media_journal_pending=CASE WHEN $9 THEN false ELSE media_journal_pending END,claimed_by=CASE WHEN $8 THEN NULL ELSE claimed_by END,claim_until=CASE WHEN $8 THEN NULL ELSE claim_until END,next_poll_at=CASE WHEN $8 THEN now() ELSE next_poll_at END,next_financial_recovery_at=CASE WHEN $8 THEN now() ELSE next_financial_recovery_at END WHERE id=$1 AND ((claimed_by=$2 AND claim_until>now()) OR ($8 AND write_started_at IS NULL AND authorization_token IS NULL AND status IN ('submitting','indeterminate')) OR ($9 AND media_journal_pending AND media_journal_owner=$10)) AND (NOT media_journal_pending OR $9) AND fee_pending_at IS NULL AND (actual_units IS NULL OR actual_units=0) AND financial_state IN ('held',$3) AND ($7 OR write_ended_at IS NOT NULL) AND authorization_id=$11 AND platform_user_id=$12 AND billing_snapshot_id=$13 AND quoted_units=$14 AND provider_task_id IS NOT DISTINCT FROM NULLIF($15,'')`, r.ID, r.ClaimedBy, state, r.AuthorizationToken, proof, deadline, zero, provenNotWritten, durableZero, journalOwner, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits, r.ProviderTaskID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if zero {
		res, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='released',authorization_token=COALESCE(authorization_token,$2),terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,$3::jsonb),reader_handoff_at=COALESCE(reader_handoff_at,now()),write_ended_at=COALESCE(write_ended_at,now()),write_active_until=NULL,known_fee_units=0,zero_intent_at=COALESCE(zero_intent_at,now()),evidence_pending=false,fee_pending=false WHERE parent_authorization_id=$1 AND kind='media' AND platform_user_id=$4 AND billing_snapshot_id=$5 AND (authorization_token IS NULL OR authorization_token=$2) AND expiry_intent_version=0 AND actual_units=0 AND settlement_payload IS NULL AND remainder_payload IS NULL AND known_fee_units IS NULL AND NOT fee_pending`, r.AuthorizationID, r.AuthorizationToken, string(evidence), r.PlatformUserID, r.SnapshotID)
		if err == nil && provenNotWritten {
			var affected int64
			affected, err = res.RowsAffected()
			if err == nil && affected != int64(count) {
				err = errors.New("media not-written terminal segment identity conflict")
			}
		}
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='indeterminate',terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,$3::jsonb),write_ended_at=COALESCE(write_ended_at,now()),write_active_until=NULL WHERE parent_authorization_id=$1 AND kind='media' AND authorization_token=$2 AND expiry_intent_version=0 AND actual_units=0 AND known_fee_units IS NULL AND NOT evidence_pending AND NOT fee_pending AND settlement_payload IS NULL AND remainder_payload IS NULL`, r.AuthorizationID, r.AuthorizationToken, string(evidence))
	}
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	r.FinancialState, r.FinancialTerminalProof, r.FinancialTerminalAt = state, proof, &deadline
	return true, nil
}

func (s *MediaTaskService) finishMediaZero(ctx context.Context, r *mediaTaskRecord) error {
	client, ok := s.bridge.control.(*canonicalWalletHTTPClient)
	if !ok {
		return errors.New("media signed zero control unavailable")
	}
	pinless := map[string]bool{}
	for _, segment := range r.Segments {
		var acknowledged bool
		if err := s.db.QueryRowContext(ctx, `SELECT zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE authorization_id=$1 AND parent_authorization_id=$2`, segment.AuthorizationID, r.AuthorizationID).Scan(&acknowledged); err != nil {
			return err
		}
		if acknowledged {
			continue
		}
		if segment.State == "finished" && segment.PinState == "finished" && segment.ActualUnits == 0 {
			// A pinless share completed on an earlier pass (see below).
			continue
		}
		// An unsubmitted armed share may never have had its first create pin ACK.
		leaf := *r
		leaf.Segments = nil
		leaf.AuthorizationID = segment.AuthorizationID
		leaf.LeaseID = segment.LeaseID
		leaf.LeaseBasis = &segment.Basis
		leaf.HeldUnits = segment.HeldUnits
		leaf.EventID = segment.EventID
		if segment.PinState == "none" {
			if err := s.pinSingle(ctx, &leaf, false); err != nil {
				var refusal *canonicalWalletStatusError
				if r.WriteStartedAt == nil && r.ProviderTaskID == "" && errors.As(err, &refusal) && refusal.Status == http.StatusConflict {
					// The Worker refused to create this share's pin with a protocol
					// conflict and the task never wrote to the provider. Treat the share
					// as pinless: no signed zero receipt can be obtained for a pin that
					// was not created. This is NOT proof of absence (a 409 can also come
					// from a pin that exists), so the share is only finished after the
					// signed close of its isolated lease succeeds below; the Worker
					// refuses that close while any pin or hold survives, and the task then
					// stays zero_pending and is retried. 401/403/404/405 are never treated
					// as a refusal: they are authentication, routing or rollout faults.
					pinless[segment.AuthorizationID] = true
					continue
				}
				return err
			}
			if _, err := s.db.ExecContext(ctx, `UPDATE wallet_authorization_segment SET pin_state='active' WHERE authorization_id=$1 AND zero_ack_at IS NULL`, segment.AuthorizationID); err != nil {
				return err
			}
		}
		request := struct {
			mediaPinRequest
			AuthorizationToken string `json:"authorization_token"`
		}{mediaPinRequest: mediaPinRequest{AuthorizationKind: "media", AuthorizationID: segment.AuthorizationID, GatewayJobID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: segment.LeaseID, Held: newCanonicalWalletAmountObject(segment.HeldUnits), BillingSnapshotID: r.SnapshotID, SettlementEventID: segment.EventID, USDPolicyVersion: "usd-wallet-v1", Resolution: "released", SettlementEventIDs: []string{}}, AuthorizationToken: r.AuthorizationToken}
		actual := newCanonicalWalletAmountObject(0)
		request.Actual = &actual
		var receipt WalletTaskPinReceipt
		if err := client.doJSON(ctx, http.MethodPost, "/api/internal/v2/wallet/task-pins/finish", "wallet:task-pin", r.ID+":"+segment.AuthorizationID+":zero", request, &receipt); err != nil {
			return err
		}
		released, err := VerifyWalletTaskPinReceipt(client.cfg.Secret, WalletTaskPinReceiptExpected{GatewayJobID: r.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: r.PlatformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: r.SnapshotID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "media", AuthorizationToken: r.AuthorizationToken, Status: "released"}, receipt)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		res, err := s.db.ExecContext(ctx, `UPDATE wallet_authorization_segment SET zero_ack_at=COALESCE(zero_ack_at,now()),zero_receipt=COALESCE(zero_receipt,$3::jsonb),zero_receipt_id=COALESCE(zero_receipt_id,$4),zero_released_at=COALESCE(zero_released_at,$5),zero_receipt_signature=COALESCE(zero_receipt_signature,$6),pin_state='finished' WHERE authorization_id=$1 AND parent_authorization_id=$2 AND known_fee_units=0 AND zero_intent_at IS NOT NULL AND (zero_receipt IS NULL OR zero_receipt=$3::jsonb)`, segment.AuthorizationID, r.AuthorizationID, string(raw), receipt.FinishReceiptID, released, receipt.ReceiptSignature)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return errors.New("media signed zero acknowledgement conflict")
		}
	}
	all, rowsSeen := true, 0
	pending, err := s.db.QueryContext(ctx, `SELECT authorization_id,zero_ack_at IS NOT NULL OR (state='finished' AND pin_state='finished' AND actual_units=0) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, r.AuthorizationID)
	if err != nil {
		return err
	}
	for pending.Next() {
		var id string
		var done bool
		if err = pending.Scan(&id, &done); err != nil {
			_ = pending.Close()
			return err
		}
		rowsSeen++
		all = all && (done || pinless[id])
	}
	err = pending.Err()
	_ = pending.Close()
	if err != nil {
		return err
	}
	if !all || rowsSeen == 0 || rowsSeen != len(r.Segments) {
		return errors.New("media zero acknowledgement incomplete")
	}
	for _, segment := range r.Segments {
		_, err := s.bridge.store.ReleaseCanonicalWalletHold(ctx, r.PlatformUserID, segment.AuthorizationID, "released", "zero_cost")
		if err != nil && !isHoldNotArmed(err) && !errors.Is(err, ErrCanonicalWalletHoldMissing) && !errors.Is(err, ErrCanonicalWalletLeaseMissing) {
			return err
		}
		if pinless[segment.AuthorizationID] && segment.Basis.FundingScope == "media" {
			// The signed close is the only arbiter for a share whose pin was never
			// created. It is refused while any pin or hold exists, and then this share
			// is not finished and the task is not marked released: it retries.
			if err = s.bridge.closePinlessMediaLease(ctx, r.PlatformUserID, segment); err != nil {
				return err
			}
		}
		if err = s.bridge.finishPoolSegment(ctx, r.PlatformUserID, segment); err != nil {
			return err
		}
	}
	_, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET financial_state='released_zero',financial_released_at=COALESCE(financial_released_at,(SELECT min(zero_released_at) FROM wallet_authorization_segment WHERE parent_authorization_id=$2)),actual_units=0,pin_state='finished',status='failed',settled_at=COALESCE(settled_at,now()),claimed_by=NULL,claim_until=NULL WHERE id=$1 AND financial_state='zero_pending'`, r.ID, r.AuthorizationID)
	if err != nil {
		return err
	}
	return s.unprotect(ctx, r)
}

func (s *MediaTaskService) recoverMediaFinancial(ctx context.Context, r *mediaTaskRecord) error {
	if r.FinancialState == "zero_pending" {
		return s.finishMediaZero(ctx, r)
	}
	if r.FinancialState == "held" {
		var notSent, rejected bool
		if err := s.db.QueryRowContext(ctx, `SELECT write_proven_not_sent,write_proven_zero FROM gateway_media_task WHERE id=$1`, r.ID).Scan(&notSent, &rejected); err != nil {
			return err
		}
		claimed, err := s.prepareMediaFinancialTerminal(ctx, r, notSent || rejected, notSent)
		if err != nil {
			return err
		}
		if !claimed {
			return s.store.save(ctx, r, 5*time.Second)
		}
		if notSent || rejected {
			return s.finishMediaZero(ctx, r)
		}
	}
	if r.FinancialState != "unknown_pending" {
		if r.FinancialState == "released_unknown" && mediaClaimLane(r) == "query" {
			return s.store.save(ctx, r, 5*time.Second)
		}
		return nil
	}
	claimed, err := s.bridge.ClaimImmediateWalletExpiry(ctx, r.AuthorizationID, *r.FinancialTerminalAt, r.FinancialTerminalProof)
	if err != nil {
		return err
	}
	if !claimed {
		return s.store.save(ctx, r, 5*time.Second)
	}
	if _, err = s.bridge.RecoverImmediateWalletExpiry(ctx, r.AuthorizationID); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET financial_state='released_unknown',financial_released_at=COALESCE(financial_released_at,(SELECT min(expiry_released_at) FROM wallet_authorization_segment WHERE parent_authorization_id=$2)),pin_state='finished',claimed_by=NULL,claim_until=NULL,next_poll_at=now(),updated_at=now() WHERE id=$1 AND financial_state='unknown_pending' AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$2 AND (expiry_ack_at IS NULL OR expiry_cleanup_at IS NULL))`, r.ID, r.AuthorizationID)
	if err != nil {
		return err
	}
	return s.closeAcknowledgedMediaFunding(ctx, r)
}
