package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func (s *MediaTaskService) run() {
	defer s.loops.Done()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	for _, lane := range []string{"fee", "finance", "query", "journal", "pool"} {
		workers.Add(1)
		go func(lane string) { defer workers.Done(); s.runMediaLane(runCtx, lane) }(lane)
	}
	<-s.stop
	cancel()
	workers.Wait()
}

func (s *MediaTaskService) runMediaLane(runCtx context.Context, lane string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(runCtx, 45*time.Second)
			if lane == "journal" {
				s.recoverMediaJournals(ctx)
				cancel()
				continue
			}
			if lane == "pool" {
				s.bridge.recoverPoolAttempts(ctx)
				s.observeMediaRecovery(ctx)
				cancel()
				continue
			}
			// Lanes run independently. A slow RPC consumes only its own item's
			// context and cannot exhaust another lane's deadline.
			for i := 0; i < 8; i++ {
				select {
				case <-s.stop:
					cancel()
					return
				default:
				}
				if ctx.Err() != nil {
					break
				}
				r, err := s.store.claimLane(ctx, "media-"+uuid.NewString(), lane)
				if errors.Is(err, sql.ErrNoRows) {
					break
				}
				if err != nil {
					slog.Warn("media task claim failed", "lane", lane, "error", err)
					break
				}
				opCtx, opCancel := context.WithTimeout(ctx, 30*time.Second)
				err = s.process(opCtx, r)
				opCancel()
				if err != nil {
					slog.Warn("media task processing deferred", "task_id", r.ID, "lane", lane, "error", err)
					retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = s.store.reschedule(retryCtx, r)
					retryCancel()
				}
			}
			cancel()
		}
	}
}
func (s *MediaTaskService) process(ctx context.Context, r *mediaTaskRecord) error {
	if r.FinancialPolicyVersion != WalletImmediateReleasePolicyVersion {
		return errors.New("media financial policy version unavailable")
	}
	segments, err := s.bridge.authorizationSegments(ctx, r.AuthorizationID)
	if err != nil {
		return err
	}
	r.Segments = segments

	if r.FinancialState == "zero_pending" {
		if r.FinancialTerminalProof == "" {
			claimed, e := s.prepareMediaFinancialTerminal(ctx, r, true)
			if e != nil {
				return e
			}
			if !claimed {
				return errors.New("media legacy zero evidence unavailable")
			}
		}
		return s.recoverMediaFinancial(ctx, r)
	}
	if r.FinancialState == "unknown_pending" && (r.ClaimLane != "query" || r.ProviderTaskID == "") {
		return s.recoverMediaFinancial(ctx, r)
	}
	if r.FinancialState == "fee_pending" {
		return s.recoverMediaFee(ctx, r)
	}
	delay := 5 * time.Second
	if s.cfg.MediaTasks.PollSeconds > 0 {
		delay = time.Duration(s.cfg.MediaTasks.PollSeconds) * time.Second
	}
	deferSave := func() error { return s.store.save(ctx, r, delay) }
	switch r.Status {
	case "queued", "authorizing":
		if s.provider == nil {
			return ErrMediaUnavailable
		}
		user, err := s.users.GetByID(ctx, r.UserID)
		if err != nil {
			return err
		}
		if user.Status != StatusActive || user.PlatformUserID != r.PlatformUserID {
			return s.failUnsubmitted(ctx, r, "USER_UNAVAILABLE")
		}
		snap, err := s.snapshots.store.GetBillingSnapshot(ctx, r.SnapshotID)
		if err != nil {
			return err
		}
		if err = validateCanonicalUSDWalletSnapshot(snap.FX, snap.Flags.USDWalletPolicyVersion); err != nil {
			return err
		}
		if _, err = s.ensureMediaJournal(ctx); err != nil {
			return err
		}
		r.Status = "authorizing"
		if err = s.store.checkpoint(ctx, r); err != nil {
			return err
		}
		handle, err := s.mediaAuthorization(ctx, r, snap, user)
		if err != nil {
			code, reason := "AUTHORIZATION_REFUSED", "authorization_unavailable"
			refused, typed := AsAuthorizationRefused(err)
			if typed {
				switch refused.Reason {
				case AuthorizationRefusalUnmarkedWrite, AuthorizationRefusalSnapshotMissing,
					AuthorizationRefusalEstimateFailed, AuthorizationRefusalIdentityMissing,
					AuthorizationRefusalCurrency, AuthorizationRefusalBalanceShortfall,
					AuthorizationRefusalLeaseCapReached, AuthorizationRefusalLeaseUnavailable,
					AuthorizationRefusalLiveStoreUnavailable:
					reason = string(refused.Reason)
				}
			}
			if errors.Is(err, ErrCanonicalWalletBalanceShortfall) || (typed && refused.Reason == AuthorizationRefusalBalanceShortfall) {
				code, reason = "INSUFFICIENT_BALANCE", string(AuthorizationRefusalBalanceShortfall)
			}
			// Log only an opaque task reference and a bounded reason, never the
			// cause (which can contain wallet identifiers or HTTP response bodies).
			taskRef := fmt.Sprintf("%x", sha256.Sum256([]byte(r.ID)))[:12]
			slog.Warn("media authorization refused", "task_ref", taskRef, "reason", reason)
			return s.failUnsubmitted(ctx, r, code)
		}

		if err = s.store.checkpoint(ctx, r); err != nil {
			return err
		}
		if err = s.protect(ctx, r); err != nil {
			var statusErr *canonicalWalletStatusError
			if errors.As(err, &statusErr) && statusErr.Status >= 400 && statusErr.Status < 500 && statusErr.Status != 408 && statusErr.Status != 429 {
				return s.failUnsubmitted(ctx, r, "PIN_AUTHORIZATION_REFUSED")
			}
			return err
		}
		if err = s.pin(ctx, r, false); err != nil {
			var statusErr *canonicalWalletStatusError
			if errors.As(err, &statusErr) && statusErr.Status >= 400 && statusErr.Status < 500 && statusErr.Status != 408 && statusErr.Status != 429 {
				return s.failUnsubmitted(ctx, r, "PIN_AUTHORIZATION_REFUSED")
			}
			// Transport uncertainty may mean the pin was accepted. Stay durable
			// and retry the same pin; no upstream creation can occur yet.
			return err
		}
		r.PinState = "active"
		r.Status = "submitting"
		if err = s.store.checkpoint(ctx, r); err != nil {
			return err
		}
		s.bindMediaWriteOwner(r, handle)
		providerID, rejected, createErr := s.createWithOwner(ctx, r, handle)
		if createErr != nil {
			if r.journal != nil {
				// The journal owns the completed write even when either PG
				// handoff step failed. Never save a stale empty provider ID.
				return createErr
			}
			r.ErrorCode = "PROVIDER_CREATE_UNKNOWN"
			r.ErrorMessage = "The provider submission could not be confirmed. Reconciliation continues."
			if rejected {
				r.ErrorCode = "PROVIDER_REJECTED"
				r.ErrorMessage = "The provider rejected the request."
			}
			refreshed, readErr := s.store.get(ctx, r.UserID, r.ID)
			if readErr != nil {
				return readErr
			}
			refreshed.Segments = r.Segments
			refreshed.ClaimLane = r.ClaimLane
			if refreshed.FinancialState == "zero_pending" {
				return s.finishMediaZero(ctx, refreshed)
			}
			if rejected {
				claimed, e := s.prepareMediaFinancialTerminal(ctx, refreshed, true)
				if e != nil {
					return e
				}
				if claimed {
					return s.finishMediaZero(ctx, refreshed)
				}
			}
			if refreshed.WriteEndedAt != nil {
				return s.recoverMediaFinancial(ctx, refreshed)
			}
			r.Status = "indeterminate"
			return deferSave()
		}
		r.ProviderTaskID = providerID
		return nil // the atomic owner handoff already scheduled provider polling
	case "submitting":
		// The durable pre-write checkpoint is the point of no automatic retry.
		// A crash after it may have submitted a paid provider job.
		r.Status = "indeterminate"
		r.ErrorCode = "SUBMISSION_RECOVERY_REQUIRED"
		r.ErrorMessage = "Submission was interrupted and requires reconciliation."
		return deferSave()
	case "indeterminate":
		if r.FinancialState == "held" && r.HeldUnits > 0 {
			if err := s.protect(ctx, r); err != nil {
				return err
			}
		}
		if r.ProviderTaskID != "" {
			r.Status = "processing"
			return deferSave()
		}
		if r.WriteEndedAt != nil {
			return s.recoverMediaFinancial(ctx, r)
		}
		// An expired claim is not a completed write owner. Never repost this job.
		delay = time.Minute
		return deferSave()
	case "processing":
		if r.FinancialState != "released_unknown" && r.HeldUnits > 0 {
			if err := s.protect(ctx, r); err != nil {
				return err
			}
		}
		if s.provider == nil {
			return ErrMediaUnavailable
		}
		if err := s.beginMediaQuery(ctx, r); err != nil {
			return err
		}
		defer func() { r.journal.close(); r.journal = nil }()
		readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		result, readErr := s.provider.Read(readCtx, r.ProviderTaskID, r.MediaType)
		cancel()
		if err := s.completeMediaQuery(ctx, r, result); err != nil {
			return err
		}
		if mediaTrustedResult(r, result) && (result.Status == "success" || result.Status == "failed") {
			return nil
		}
		if readErr != nil || !mediaTrustedResult(r, result) {
			if err := s.store.queryFailed(ctx, r); err != nil {
				return err
			}
			refreshed, err := s.store.get(ctx, r.UserID, r.ID)
			if err != nil {
				return err
			}
			refreshed.Segments = r.Segments
			refreshed.ClaimLane = r.ClaimLane
			return s.recoverMediaFinancial(ctx, refreshed)
		}
		if err := s.store.queryConfirmed(ctx, r); err != nil {
			return err
		}
		r.QueryEpisodeFirstFailedAt = nil
		r.QueryUncertaintyDeadline = nil
		if result.Status == "failed" {
			r.Result = result
			r.ErrorCode = result.ErrorCode
			r.ErrorMessage = result.ErrorMessage
			if r.FinancialState == "released_unknown" {
				r.Status = "failed"
				return s.store.saveReleasedFailure(ctx, r)
			}
			claimed, err := s.prepareMediaFinancialTerminal(ctx, r, true)
			if err != nil {
				return err
			}
			if claimed {
				return s.finishMediaZero(ctx, r)
			}
		}
		if r.FinancialState == "held" {
			return s.recoverMediaFinancial(ctx, r)
		}
		return deferSave()
	case "releasing":
		// A task that failed before its first provider write has proof it sent nothing.
		claimed, err := s.prepareMediaFinancialTerminal(ctx, r, true, r.WriteStartedAt == nil && r.ProviderTaskID == "")
		if err != nil {
			return err
		}
		if claimed {
			return s.finishMediaZero(ctx, r)
		}
		return deferSave()
	case "settling":
		return s.recoverMediaFee(ctx, r)
	}
	return nil
}
func (s *MediaTaskService) failUnsubmitted(ctx context.Context, r *mediaTaskRecord, code string) error {
	segments, e := s.bridge.authorizationSegments(ctx, r.AuthorizationID)
	if e != nil {
		return e
	}
	r.Segments = segments
	var held int64
	for i := range r.Segments {
		segment := &r.Segments[i]
		hold, e := s.bridge.store.GetCanonicalWalletHold(ctx, r.PlatformUserID, segment.AuthorizationID)
		if errors.Is(e, ErrCanonicalWalletHoldMissing) {
			if r.HeldUnits == 0 {
				// A plan is committed before the all-or-none Lua arm. Without
				// an actual hold or checkpointed arm ACK it is not a debit.
				segment.PinState = "finished"
				continue
			}
			// No provider write is possible before this persisted stage.
			// Preserve each share for explicit zero pin finish, even when Redis
			// disappeared and a new pin can no longer be created.
			held += segment.HeldUnits
			continue
		}
		if e != nil {
			return e
		}
		if hold.State == "armed" {
			held += hold.HeldUnits
			basis, e := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, r.PlatformUserID, hold.LeaseID)
			if e != nil && !errors.Is(e, ErrCanonicalWalletLeaseMissing) {
				return e
			}
			if e == nil {
				segment.Basis = *basis
			}
		}
	}
	if held > 0 {
		r.HeldUnits = held
		r.LeaseID = r.Segments[0].LeaseID
		r.LeaseBasis = &r.Segments[0].Basis
	}
	if e = s.bridge.saveAuthorizationSegments(ctx, r.AuthorizationID, r.Segments); e != nil {
		return e
	}

	r.ErrorCode = code
	r.ErrorMessage = "The request could not be authorized."
	if code == "INSUFFICIENT_BALANCE" {
		r.ErrorMessage = "Insufficient USD balance. Top up in the console and retry."
	}
	zero := int64(0)
	r.ActualUnits = &zero
	if r.HeldUnits > 0 {
		r.Status = "releasing"
	} else {
		r.Status = "failed"
		r.PinState = "finished"
		if _, e = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET financial_state='released_zero' WHERE id=$1 AND held_units=0`, r.ID); e != nil {
			return e
		}
	}
	return s.store.save(ctx, r, time.Second)
}
func (s *MediaTaskService) protectSingle(ctx context.Context, r *mediaTaskRecord) error {
	if r.LeaseBasis == nil || r.LeaseID == "" || r.HeldUnits <= 0 {
		return errors.New("media task has no durable lease basis")
	}
	store, ok := s.bridge.store.(MediaWalletStateStore)
	if !ok {
		return errors.New("media hold persistence is unavailable")
	}
	_, cacheErr := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, r.PlatformUserID, r.LeaseID)
	if errors.Is(cacheErr, ErrCanonicalWalletLeaseMissing) && r.PinState != "finished" {
		// An idempotent pin read supplies the current canonical lease, including
		// later top-ups/captures. A persisted job alone cannot prove that basis.
		if err := s.pinSingle(ctx, r, false); err != nil {
			return err
		}
		captured := map[string]bool{}
		for _, id := range r.CapturedEventIDs {
			captured[id] = true
		}
		rows, err := s.db.QueryContext(ctx, `SELECT a.event_id,a.held_units,a.actual_units FROM wallet_authorization_segment a WHERE a.platform_user_id=$1 AND a.lease_id=$2 AND ((a.kind='media' AND a.pin_state<>'finished') OR (a.kind<>'media' AND a.state<>'finished'))`, r.PlatformUserID, r.LeaseID)
		if err != nil {
			return err
		}
		consumed := r.LeaseBasis.ConsumedUnits
		for rows.Next() {
			var id string
			var units, actual int64
			if err = rows.Scan(&id, &units, &actual); err != nil {
				rows.Close()
				return err
			}
			if captured[id] {
				units -= actual
			}
			if units > 0 {
				consumed, err = AddUnits(consumed, units)
				if err != nil {
					rows.Close()
					return err
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		r.LeaseBasis.ConsumedUnits = consumed
		r.LeaseBasis.ReleasedUnits = 0
		if err = s.bridge.applyFundingRegistry(ctx, r.LeaseBasis); err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `UPDATE wallet_authorization_segment SET lease_basis=$2::jsonb WHERE authorization_id=$1`, r.AuthorizationID, mediaLeaseJSON(r.LeaseBasis)); err != nil {
			return err
		}
	} else if cacheErr != nil && !errors.Is(cacheErr, ErrCanonicalWalletLeaseMissing) {
		return cacheErr
	}
	return store.ProtectMediaHold(ctx, *r.LeaseBasis, CanonicalWalletHold{AuthorizationID: r.AuthorizationID, LeaseID: r.LeaseID, HeldUnits: r.HeldUnits, ArmedAt: r.CreatedAt})
}
func (s *MediaTaskService) unprotectSingle(ctx context.Context, r *mediaTaskRecord) error {
	remaining, err := s.store.leasePinned(ctx, r.PlatformUserID, r.LeaseID)
	if err != nil {
		return err
	}
	store, ok := s.bridge.store.(MediaWalletStateStore)
	if !ok {
		return errors.New("media hold persistence unavailable")
	}
	return store.UnprotectMediaLease(ctx, r.PlatformUserID, r.LeaseID, r.AuthorizationID, r.EventID, remaining)
}

type mediaPinRequest struct {
	AuthorizationID    string                       `json:"authorization_id"`
	GatewayJobID       string                       `json:"gateway_job_id"`
	PlatformUserID     string                       `json:"platform_user_id"`
	LeaseID            string                       `json:"lease_id"`
	Held               canonicalWalletAmountObject  `json:"held"`
	BillingSnapshotID  string                       `json:"billing_snapshot_id"`
	SettlementEventID  string                       `json:"settlement_event_id"`
	AuthorizationKind  string                       `json:"authorization_kind,omitempty"`
	USDPolicyVersion   string                       `json:"usd_wallet_policy_version"`
	Resolution         string                       `json:"resolution,omitempty"`
	SettlementEventIDs []string                     `json:"settlement_event_ids,omitempty"`
	Actual             *canonicalWalletAmountObject `json:"actual,omitempty"`
}

func (s *MediaTaskService) pinSingle(ctx context.Context, r *mediaTaskRecord, finish bool) error {
	client, ok := s.bridge.control.(*canonicalWalletHTTPClient)
	if !ok {
		return errors.New("media task pin control plane unavailable")
	}
	request := mediaPinRequest{AuthorizationKind: r.AuthorizationKind, AuthorizationID: r.AuthorizationID, GatewayJobID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: r.LeaseID, Held: newCanonicalWalletAmountObject(r.HeldUnits), BillingSnapshotID: r.SnapshotID, SettlementEventID: r.EventID, USDPolicyVersion: "usd-wallet-v1"}
	path := "/api/internal/v2/wallet/task-pins/create"
	if finish {
		path = "/api/internal/v2/wallet/task-pins/finish"
		actual := int64(0)
		if r.ActualUnits != nil {
			actual = *r.ActualUnits
		}
		amount := newCanonicalWalletAmountObject(actual)
		request.Actual = &amount
		request.Resolution = "released"
		request.SettlementEventIDs = []string{}
		if actual > 0 {
			request.Resolution = "settled"
			request.SettlementEventIDs = []string{r.EventID}
		}
	}
	var response struct {
		GatewayJobID    string `json:"gateway_job_id"`
		AuthorizationID string `json:"authorization_id"`
		LeaseID         string `json:"lease_id"`
		Status          string `json:"status"`
		Lease           *struct {
			LeaseID            string     `json:"lease_id"`
			FundingScope       string     `json:"funding_scope"`
			FundingOwnerID     string     `json:"funding_owner_id"`
			FundingIssuanceKey string     `json:"funding_issuance_key"`
			FundingFrozenAt    *time.Time `json:"funding_frozen_at"`
			ReturnRevision     int64      `json:"return_revision"`
			BudgetRevision     int64      `json:"budget_revision"`
			BudgetUnits        string     `json:"budget_units"`
			CapturedUnits      string     `json:"captured_units"`
			ReleasedUnits      string     `json:"released_units"`
			ReservedUnits      string     `json:"reserved_units"`
			ExpiresAt          time.Time  `json:"expires_at"`
			Status             string     `json:"status"`
		} `json:"lease"`
		CapturedEventIDs []string `json:"captured_event_ids"`
	}
	if err := client.doJSON(ctx, http.MethodPost, path, "wallet:task-pin", r.ID+":"+request.Resolution, request, &response); err != nil {

		return err
	}
	expected := "active"
	if finish {
		expected = request.Resolution
	}
	resolvedCreate := !finish && r.Status == "settling" && response.Status == "settled"
	if response.GatewayJobID != r.ID || response.AuthorizationID != r.AuthorizationID || response.LeaseID != r.LeaseID || (response.Status != expected && !resolvedCreate) {
		return errors.New("media pin acknowledgement mismatch")
	}
	if !finish {
		if response.Lease == nil || response.Lease.LeaseID != r.LeaseID || response.Lease.Status != "active" || response.Lease.ExpiresAt.IsZero() || len(response.CapturedEventIDs) > 1000 {
			return errors.New("media pin canonical lease basis missing")
		}
		budget, err := strictAvailabilityUnits(response.Lease.BudgetUnits)
		if err != nil {
			return err
		}
		captured, err := strictAvailabilityUnits(response.Lease.CapturedUnits)
		if err != nil || captured > budget {
			return errors.New("media pin canonical captured invalid")
		}
		returned, err := strictAvailabilityUnits(response.Lease.ReleasedUnits)
		if err != nil || returned > budget || captured > budget-returned || response.Lease.ReturnRevision < 0 {
			return errors.New("media pin returned backing invalid")
		}
		_, cacheErr := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, r.PlatformUserID, r.LeaseID)
		if errors.Is(cacheErr, ErrCanonicalWalletLeaseMissing) {
			expiry := response.Lease.ExpiresAt
			if r.LeaseBasis.ExpiresAt.Before(expiry) {
				expiry = r.LeaseBasis.ExpiresAt
			}
			r.LeaseBasis = &CanonicalWalletLease{LeaseID: r.LeaseID, PlatformUserID: r.PlatformUserID, Currency: CurrencyUSD, BudgetUnits: budget - returned, FundedUnits: budget, ReturnedUnits: returned, ReturnRevision: response.Lease.ReturnRevision, BudgetRevision: response.Lease.BudgetRevision, FundingScope: response.Lease.FundingScope, FundingOwnerID: response.Lease.FundingOwnerID, FundingIssuanceKey: response.Lease.FundingIssuanceKey, FundingFrozen: response.Lease.FundingFrozenAt != nil, ConsumedUnits: captured, ExpiresAt: expiry, Sealed: true}
		}
		r.CapturedEventIDs = response.CapturedEventIDs
	}
	return nil
}

// The authoritative result and the original frozen fee commit before any
// segment actual, Redis conversion or settlement dispatch is attempted.
func (s *MediaTaskService) persistMediaSuccess(ctx context.Context, r *mediaTaskRecord) error {
	if !mediaTrustedResult(r, r.Result) || r.Result.Status != "success" {
		return errors.New("media fee requires authoritative same-task success")
	}
	if r.journal == nil || r.journal.record.Success == nil || !r.journal.record.Joined {
		return errors.New("media first success requires its fsynced owner handoff")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = lockWalletAttempt(ctx, tx, r.AuthorizationID); err != nil {
		return err
	}
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT financial_state FROM gateway_media_task WHERE id=$1 AND provider_task_id=$2 AND billing_snapshot_id=$3 FOR UPDATE`, r.ID, r.ProviderTaskID, r.SnapshotID).Scan(&state); err != nil {
		return err
	}
	if state == "released_zero" {
		raw, e := json.Marshal(r.Result)
		if e != nil {
			return e
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO wallet_billing_anomaly(parent_authorization_id,authorization_token,reason,evidence) VALUES($1,$2,'positive_after_zero',$3::jsonb) ON CONFLICT DO NOTHING`, r.AuthorizationID, r.AuthorizationToken, string(raw))
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE gateway_media_task SET media_journal_pending=false,claimed_by=NULL,claim_until=NULL WHERE id=$1`, r.ID); err != nil {
			return err
		}
		return tx.Commit()
	}
	if state == "charged" {
		if _, err = tx.ExecContext(ctx, `UPDATE gateway_media_task SET media_journal_pending=false,claimed_by=NULL,claim_until=NULL WHERE id=$1`, r.ID); err != nil {
			return err
		}
		return tx.Commit()
	}
	result, err := json.Marshal(r.Result)
	if err != nil {
		return err
	}
	evidence, err := json.Marshal(map[string]any{"source": "kie_authoritative_get", "provider_task_id": r.ProviderTaskID, "snapshot_id": r.SnapshotID, "quoted_units": r.QuotedUnits, "result": r.Result})
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET status='settling',financial_state='fee_pending',actual_units=quoted_units,result=$4::jsonb,fee_evidence=COALESCE(fee_evidence,$5::jsonb),fee_pending_at=COALESCE(fee_pending_at,now()),next_fee_recovery_at=now(),media_journal_pending=false,updated_at=now(),claim_until=NULL,claimed_by=NULL WHERE id=$1 AND provider_task_id=$2 AND billing_snapshot_id=$3 AND financial_state IN ('held','unknown_pending','released_unknown','fee_pending') AND (fee_evidence IS NULL OR fee_evidence=$5::jsonb)`, r.ID, r.ProviderTaskID, r.SnapshotID, string(result), string(evidence))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("media original fee evidence conflict")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET fee_pending=true,known_fee_units=$2 WHERE parent_authorization_id=$1 AND kind='media' AND zero_intent_at IS NULL`, r.AuthorizationID, r.QuotedUnits); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MediaTaskService) recoverMediaFee(ctx context.Context, r *mediaTaskRecord) error {
	if r.ActualUnits == nil || *r.ActualUnits != r.QuotedUnits {
		return errors.New("media durable fee mismatch")
	}
	// A success raced an already committed expiry intent. Complete its signed
	// ACK first; never convert the released hold when the first fee arrives late.
	var pending bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND expiry_intent_version>0 AND expiry_ack_at IS NULL)`, r.AuthorizationID).Scan(&pending); err != nil {
		return err
	}
	if pending {
		if _, err := s.bridge.RecoverImmediateWalletExpiry(ctx, r.AuthorizationID); err != nil {
			return err
		}
	}
	if err := s.persistMediaFeePlan(ctx, r); err != nil {
		return err
	}
	segments, err := s.bridge.authorizationSegments(ctx, r.AuthorizationID)
	if err != nil {
		return err
	}
	r.Segments = segments
	needsOriginalHold := false
	for _, segment := range segments {
		if segment.Payload != nil && segment.Payload.LeaseID != "" && segment.ActualUnits > 0 {
			needsOriginalHold = true
		}
	}
	if needsOriginalHold {
		if err = s.protect(ctx, r); err != nil {
			return err
		}
	}
	if err = s.bridge.enqueuePoolSettlements(ctx, r.PlatformUserID, r.SnapshotID, segments); err != nil {
		return err
	}
	complete, dead, err := s.mediaSettled(ctx, r)
	if err != nil {
		return err
	}
	if dead {
		return errors.New("media fee requires settlement reconciliation")
	}
	if !complete {
		return s.store.save(ctx, r, time.Second)
	}
	if err = s.pin(ctx, r, true); err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='finished',pin_state='finished',fee_pending=false WHERE parent_authorization_id=$1 AND actual_units>0`, r.AuthorizationID); err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET status='completed',financial_state='charged',pin_state='finished',settled_at=COALESCE(settled_at,now()),claimed_by=NULL,claim_until=NULL,updated_at=now() WHERE id=$1 AND financial_state='fee_pending' AND actual_units=quoted_units`, r.ID); err != nil {
		return err
	}
	return s.unprotect(ctx, r)
}

func (s *MediaTaskService) persistMediaFeePlan(ctx context.Context, r *mediaTaskRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	locked, err := lockWalletAttempt(ctx, tx, r.AuthorizationID)
	if err != nil {
		return err
	}
	if locked == 0 || locked != len(r.Segments) {
		return errors.New("media fee plan segment identity conflict")
	}
	var taskParent, taskToken, taskSnapshot, taskUser string
	var taskQuote int64
	var taskCreated time.Time
	if err = tx.QueryRowContext(ctx, `SELECT authorization_id,authorization_token,billing_snapshot_id,platform_user_id,quoted_units,created_at FROM gateway_media_task WHERE id=$1 AND financial_state='fee_pending' AND fee_evidence IS NOT NULL FOR UPDATE`, r.ID).Scan(&taskParent, &taskToken, &taskSnapshot, &taskUser, &taskQuote, &taskCreated); err != nil {
		return err
	}
	if taskParent != r.AuthorizationID || taskToken != r.AuthorizationToken || taskSnapshot != r.SnapshotID || taskUser != r.PlatformUserID || taskQuote != r.QuotedUnits || !taskCreated.Equal(r.CreatedAt) {
		return errors.New("media fee plan task identity conflict")
	}
	remaining := r.QuotedUnits
	for ordinal, segment := range r.Segments {
		var version int
		var originalOrdinal int
		var existing []byte
		var parent, user, snapshot, token, kind, eventID, leaseID string
		var held int64
		if err = tx.QueryRowContext(ctx, `SELECT expiry_intent_version,settlement_payload,parent_authorization_id,platform_user_id,billing_snapshot_id,authorization_token,kind,event_id,lease_id,held_units,ordinal FROM wallet_authorization_segment WHERE authorization_id=$1 FOR UPDATE`, segment.AuthorizationID).Scan(&version, &existing, &parent, &user, &snapshot, &token, &kind, &eventID, &leaseID, &held, &originalOrdinal); err != nil {
			return err
		}
		// These ownership fields are intentionally absent from settlement JSON.
		// Validate them against the locked original segment and task instead.
		if parent != taskParent || user != taskUser || snapshot != taskSnapshot || token != taskToken || kind != "media" || eventID != segment.EventID || leaseID != segment.LeaseID || held != segment.HeldUnits || originalOrdinal != ordinal {
			return errors.New("media fee plan segment identity conflict")
		}
		actual := remaining
		if actual > segment.HeldUnits {
			actual = segment.HeldUnits
		}
		remaining -= actual
		lease := segment.LeaseID
		if version > 0 {
			lease = ""
		}
		event := CanonicalWalletSettlementEvent{EventID: segment.EventID, GatewayRequestID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: lease, Currency: CurrencyUSD, AmountUnits: actual, OccurredAt: r.CreatedAt, AuthorizationID: segment.AuthorizationID, AuthorizationToken: r.AuthorizationToken, BillingSnapshotID: r.SnapshotID}
		raw, e := json.Marshal(event)
		if e != nil {
			return e
		}
		if len(existing) > 0 {
			var original CanonicalWalletSettlementEvent
			if e = json.Unmarshal(existing, &original); e != nil {
				return e
			}
			if original.EventID != event.EventID || original.AmountUnits != actual || original.GatewayRequestID != event.GatewayRequestID || original.PlatformUserID != event.PlatformUserID || original.LeaseID != event.LeaseID || original.Currency != event.Currency || !original.OccurredAt.Equal(event.OccurredAt) || original.LocalBalanceAfterUnits != nil {
				return errors.New("media original settlement payload conflict")
			}
			raw = existing
		}
		state := "settling"
		if actual == 0 {
			state = "released"
			if version > 0 {
				state = "expired_unknown"
			}
		}
		res, updateErr := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET actual_units=$2,state=$3,settlement_payload=COALESCE(settlement_payload,$4::jsonb) WHERE authorization_id=$1 AND zero_intent_at IS NULL`, segment.AuthorizationID, actual, state, string(raw))
		if updateErr != nil {
			return updateErr
		}
		n, updateErr := res.RowsAffected()
		if updateErr != nil {
			return updateErr
		}
		if n != 1 {
			return errors.New("media fee plan segment update conflict")
		}
	}
	if remaining > 0 {
		extra := CanonicalWalletSettlementEvent{EventID: CanonicalWalletSettlementEventID(r.Segments[0].AuthorizationID+":overrun", r.PlatformUserID, CurrencyUSD), GatewayRequestID: r.ID, PlatformUserID: r.PlatformUserID, Currency: CurrencyUSD, AmountUnits: remaining, OccurredAt: r.CreatedAt, AuthorizationToken: r.AuthorizationToken, BillingSnapshotID: r.SnapshotID}
		raw, e := json.Marshal(extra)
		if e != nil {
			return e
		}
		if _, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET remainder_payload=COALESCE(remainder_payload,$2::jsonb) WHERE authorization_id=$1 AND (remainder_payload IS NULL OR remainder_payload=$2::jsonb)`, r.Segments[0].AuthorizationID, string(raw)); err != nil {
			return err
		}
	}
	usd := mediaUSD(r.QuotedUnits)
	amountUSD := strconv.FormatFloat(float64(r.QuotedUnits)/100000000, 'f', 8, 64)
	_, err = tx.ExecContext(ctx, `INSERT INTO usage_logs(user_id,api_key_id,account_id,request_id,model,requested_model,billing_snapshot_id,billing_mode,media_type,total_cost,actual_cost,source_currency,settlement_currency,exchange_rate,exchange_rate_source,exchange_rate_as_of,source_cost,base_cost,rate_multiplier,image_count,video_count,video_duration_seconds,image_size,created_at)
 SELECT $1,$2,$3,$4::text,$5::text,$5::text,$6,'per_request',$7,$8::numeric,$8::numeric,'USD','USD',1,'usd-e8-v1',$9,$10::numeric,$8::numeric,1,$11,$12,$13,$14,$9 WHERE NOT EXISTS(SELECT 1 FROM usage_logs WHERE request_id=$4::text AND api_key_id=$2)`, r.UserID, r.APIKeyID, s.accountID, r.ID, r.Model, r.SnapshotID, r.MediaType, amountUSD, r.CreatedAt, usd, mediaImageCount(r), mediaVideoCount(r), mediaDuration(r), mediaImageSize(r))
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE gateway_media_task SET fee_plan_at=COALESCE(fee_plan_at,now()) WHERE id=$1 AND financial_state='fee_pending' AND fee_evidence IS NOT NULL`, r.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MediaTaskService) createWithOwner(ctx context.Context, r *mediaTaskRecord, h *AuthorizationHandle) (id string, rejected bool, err error) {
	defer func() {
		defer func() {
			if r.journal != nil {
				r.journal.close()
			}
		}()
		if panicValue := recover(); panicValue != nil {
			err = fmt.Errorf("media create owner panic: %T", panicValue)
		}
		handoffCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if r.FinancialState == "zero_pending" {
			return
		}
		if r.journal != nil {
			r.journal.record.Rejected = rejected
		}
		if e := s.completeMediaWrite(handoffCtx, r, id); e != nil {
			err = errors.Join(err, e)
		}
	}()
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return s.provider.Create(writeCtx, r.Model, r.RequestPayload, h)
}

func (s *MediaTaskService) observeMediaRecovery(ctx context.Context) {
	var queryAge, feeAge, cleanupAge, ownerAge float64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(query_episode_first_failed_at) FILTER (WHERE provider_task_id IS NOT NULL AND status NOT IN ('completed','failed'))),0),COALESCE(EXTRACT(EPOCH FROM now()-min(fee_pending_at) FILTER (WHERE financial_state='fee_pending')),0),COALESCE(EXTRACT(EPOCH FROM now()-min(financial_terminal_at) FILTER (WHERE financial_state IN ('unknown_pending','zero_pending'))),0),COALESCE(EXTRACT(EPOCH FROM now()-min(write_started_at) FILTER (WHERE write_ended_at IS NULL AND provider_task_id IS NULL AND financial_state='held')),0) FROM gateway_media_task`).Scan(&queryAge, &feeAge, &cleanupAge, &ownerAge)
	if err != nil {
		slog.Warn("media recovery age unavailable", "error", err)
		return
	}
	if feeAge > 60 || cleanupAge > 30 || queryAge > 900 || ownerAge > 90 {
		slog.Warn("media recovery oldest age", "query_age_seconds", queryAge, "fee_age_seconds", feeAge, "financial_ack_cleanup_age_seconds", cleanupAge, "write_owner_proof_age_seconds", ownerAge)
	}
}
func mediaImageCount(r *mediaTaskRecord) int {
	if r.MediaType == "image" {
		return 1
	}
	return 0
}
func mediaVideoCount(r *mediaTaskRecord) int {
	if r.MediaType == "video" {
		return 1
	}
	return 0
}
func mediaDuration(r *mediaTaskRecord) any {
	if r.MediaType != "video" {
		return nil
	}
	return int(mediaOptionSeconds(r.Option))
}
func mediaImageSize(r *mediaTaskRecord) any {
	if r.MediaType == "image" {
		if len(r.Option) >= 2 && (r.Option[:2] == "2K" || r.Option[:2] == "4K") {
			return r.Option[:2]
		}
		return "1K"
	}
	return nil
}

func mediaLeaseJSON(basis *CanonicalWalletLease) string {
	raw, _ := json.Marshal(basis)
	return string(raw)
}
