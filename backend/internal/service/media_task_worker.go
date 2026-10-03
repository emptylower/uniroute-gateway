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
	"time"
)

func (s *MediaTaskService) run() {
	defer s.loops.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			s.bridge.recoverPoolAttempts(ctx)
			for i := 0; i < 8; i++ {
				select {
				case <-s.stop:
					cancel()
					return
				default:
				}
				r, err := s.store.claim(ctx, "media-"+uuid.NewString())
				if errors.Is(err, sql.ErrNoRows) {
					break
				}
				if err != nil {
					slog.Warn("media task claim failed", "error", err)
					break
				}
				if err = s.process(ctx, r); err != nil {
					slog.Warn("media task processing deferred", "task_id", r.ID, "error", err)
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
	segments, err := s.bridge.authorizationSegments(ctx, r.AuthorizationID)
	if err != nil {
		return err
	}
	r.Segments = segments
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
		writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		providerID, rejected, createErr := s.provider.Create(writeCtx, r.Model, r.RequestPayload, handle)
		cancel()
		r.AuthorizationToken = handle.LastWriteToken()
		if createErr != nil {
			r.ErrorCode = "PROVIDER_CREATE_UNKNOWN"
			r.ErrorMessage = "The provider submission could not be confirmed. Funds remain reserved for review."
			if rejected {
				r.Status = "releasing"
				r.ErrorCode = "PROVIDER_REJECTED"
				r.ErrorMessage = "The provider rejected the request."
				zero := int64(0)
				r.ActualUnits = &zero
			} else {
				r.Status = "indeterminate"
			}
			return deferSave()
		}
		r.ProviderTaskID = providerID
		r.Status = "processing"
		return deferSave()
	case "submitting":
		// The durable pre-write checkpoint is the point of no automatic retry.
		// A crash after it may have submitted a paid provider job.
		r.Status = "indeterminate"
		r.ErrorCode = "SUBMISSION_RECOVERY_REQUIRED"
		r.ErrorMessage = "Submission was interrupted and requires reconciliation."
		return deferSave()
	case "indeterminate":
		if r.HeldUnits > 0 {
			if err := s.protect(ctx, r); err != nil {
				return err
			}
		}
		delay = time.Minute
		return deferSave()
	case "processing":
		if err := s.protect(ctx, r); err != nil {
			return err
		}
		if s.provider == nil {
			return ErrMediaUnavailable
		}
		if time.Now().After(r.DeadlineAt) {
			r.Status = "indeterminate"
			r.ErrorCode = "PROVIDER_DEADLINE_UNKNOWN"
			r.ErrorMessage = "The provider has not confirmed a final outcome. Funds remain reserved."
			return deferSave()
		}
		readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		result, err := s.provider.Read(readCtx, r.ProviderTaskID, r.MediaType)
		cancel()
		if err != nil {
			return err
		}
		switch result.Status {
		case "success":
			r.Result = result
			return s.settle(ctx, r)
		case "failed":
			r.Result = result
			r.Status = "releasing"
			r.ErrorCode = result.ErrorCode
			r.ErrorMessage = result.ErrorMessage
			zero := int64(0)
			r.ActualUnits = &zero
		}
		return deferSave()
	case "releasing":
		if err := s.store.fence(ctx, r); err != nil {
			return err
		}
		if r.HeldUnits > 0 {
			err := s.releaseMedia(ctx, r)
			if err != nil {
				return err
			}

			if err = s.pin(ctx, r, true); err != nil {
				return err
			}
		}
		r.Status = "failed"
		r.PinState = "finished"
		now := time.Now().UTC()
		r.SettledAt = &now
		if err := deferSave(); err != nil {
			return err
		}
		return s.unprotect(ctx, r)
	case "settling":
		complete, dead, err := s.mediaSettled(ctx, r)
		if err != nil {
			return err
		}
		if dead {
			r.Status = "indeterminate"
			r.ErrorCode = "SETTLEMENT_REVIEW_REQUIRED"
			r.ErrorMessage = "Settlement requires reconciliation."
			return deferSave()
		}
		if !complete {
			if err = s.protect(ctx, r); err != nil {
				return err
			}
			if err = s.convertMedia(ctx, r); err != nil {
				return err
			}
			return deferSave()
		}

		if err = s.store.fence(ctx, r); err != nil {
			return err
		}
		if err = s.pin(ctx, r, true); err != nil {
			return err
		}
		r.Status = "completed"
		r.PinState = "finished"
		now := time.Now().UTC()
		r.SettledAt = &now
		if err = deferSave(); err != nil {
			return err
		}
		return s.unprotect(ctx, r)
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
	request := mediaPinRequest{AuthorizationID: r.AuthorizationID, GatewayJobID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: r.LeaseID, Held: newCanonicalWalletAmountObject(r.HeldUnits), BillingSnapshotID: r.SnapshotID, SettlementEventID: r.EventID, USDPolicyVersion: "usd-wallet-v1"}
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
			LeaseID       string    `json:"lease_id"`
			BudgetUnits   string    `json:"budget_units"`
			CapturedUnits string    `json:"captured_units"`
			ReleasedUnits string    `json:"released_units"`
			ReservedUnits string    `json:"reserved_units"`
			ExpiresAt     time.Time `json:"expires_at"`
			Status        string    `json:"status"`
		} `json:"lease"`
		CapturedEventIDs []string `json:"captured_event_ids"`
	}
	if err := client.doJSON(ctx, http.MethodPost, path, "wallet:task-pin", r.ID+":"+request.Resolution, request, &response); err != nil {
		var statusErr *canonicalWalletStatusError
		if finish && request.Resolution == "released" && errors.As(err, &statusErr) && statusErr.Status == http.StatusNotFound {
			return nil
		}
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
		_, cacheErr := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, r.PlatformUserID, r.LeaseID)
		if errors.Is(cacheErr, ErrCanonicalWalletLeaseMissing) {
			expiry := response.Lease.ExpiresAt
			if r.LeaseBasis.ExpiresAt.Before(expiry) {
				expiry = r.LeaseBasis.ExpiresAt
			}
			r.LeaseBasis = &CanonicalWalletLease{LeaseID: r.LeaseID, PlatformUserID: r.PlatformUserID, Currency: CurrencyUSD, BudgetUnits: budget, ConsumedUnits: captured, ExpiresAt: expiry, Sealed: true}
		}
		r.CapturedEventIDs = response.CapturedEventIDs
	}
	return nil
}

func (s *MediaTaskService) settle(ctx context.Context, r *mediaTaskRecord) error {
	if err := s.store.fence(ctx, r); err != nil {
		return err
	}
	actual := r.QuotedUnits
	if actual > r.HeldUnits {
		return errors.New("media usage exceeds frozen hold")
	}
	r.ActualUnits = &actual
	for i := range r.Segments {
		r.Segments[i].ActualUnits = r.Segments[i].HeldUnits
	}
	if err := s.bridge.saveAuthorizationSegments(ctx, r.AuthorizationID, r.Segments); err != nil {
		return err
	}
	if err := s.convertMedia(ctx, r); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, segment := range r.Segments {
		event := CanonicalWalletSettlementEvent{EventID: segment.EventID, GatewayRequestID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: segment.LeaseID, Currency: CurrencyUSD, AmountUnits: segment.ActualUnits, OccurredAt: r.CreatedAt, AuthorizationID: segment.AuthorizationID, AuthorizationToken: r.AuthorizationToken, BillingSnapshotID: r.SnapshotID}
		if err = s.bridge.outbox.InsertOutboxEventTx(ctx, tx, event); err != nil {
			return err
		}
	}

	usd := mediaUSD(actual)
	amountUSD := strconv.FormatFloat(float64(actual)/100000000, 'f', 8, 64)
	// Usage and outbox are one durable terminal transaction. No native user
	// balance or legacy credit path is involved in media settlement.
	_, err = tx.ExecContext(ctx, `INSERT INTO usage_logs(user_id,api_key_id,account_id,request_id,model,requested_model,billing_snapshot_id,billing_mode,media_type,total_cost,actual_cost,source_currency,settlement_currency,exchange_rate,exchange_rate_source,exchange_rate_as_of,source_cost,base_cost,rate_multiplier,image_count,video_count,video_duration_seconds,image_size,created_at)
 SELECT $1,$2,$3,$4::text,$5::text,$5::text,$6,'per_request',$7,$8::numeric,$8::numeric,'USD','USD',1,'usd-e8-v1',$9,$10::numeric,$8::numeric,1,$11,$12,$13,$14,$9 WHERE NOT EXISTS(SELECT 1 FROM usage_logs WHERE request_id=$4::text AND api_key_id=$2)`, r.UserID, r.APIKeyID, s.accountID, r.ID, r.Model, r.SnapshotID, r.MediaType, amountUSD, r.CreatedAt, usd, mediaImageCount(r), mediaVideoCount(r), mediaDuration(r), mediaImageSize(r))
	if err != nil {
		return err
	}
	result, err := json.Marshal(r.Result)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET status='settling',actual_units=$3,result=$4::jsonb,updated_at=now(),next_poll_at=now()+interval '1 second',claim_until=NULL,claimed_by=NULL WHERE id=$1 AND claimed_by=$2 AND claim_until>now() AND status='processing'`, r.ID, r.ClaimedBy, actual, string(result))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("media terminal claim lost")
	}
	return tx.Commit()
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
