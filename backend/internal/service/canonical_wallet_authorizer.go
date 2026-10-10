package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// CanonicalWalletAuthorizer is the authorization point (spec §2.0, §4; §6 item 1):
// after candidate selection, pricing and the 3.2 freeze, once per billable attempt.
// It mints the handle, bounds the attempt with 3.2's estimator, and ensures a lease
// whose remaining budget covers the bound. It ARMS NO HOLD (3.4b).
type CanonicalWalletAuthorizer struct {
	cfg       *config.Config
	bridge    *CanonicalWalletBridge
	snapshots *BillingSnapshotService
}

func NewCanonicalWalletAuthorizer(cfg *config.Config, bridge *CanonicalWalletBridge, snapshots *BillingSnapshotService) *CanonicalWalletAuthorizer {
	return &CanonicalWalletAuthorizer{cfg: cfg, bridge: bridge, snapshots: snapshots}
}

type AuthorizeInput struct {
	// DurableAuthorizationID is preallocated only by the persisted media job.
	// Its row exists before Redis can arm a hold, closing the crash window.
	DurableAuthorizationID string
	Snapshot               *BillingSnapshot
	Estimate               EstimateInput
	User                   *User
	// FixedEstimateUnits (Phase 3.7b, redesign §13.2.1): when > 0 it REPLACES
	// the estimation step only — Authorize skips EstimateUpperBoundUnits and
	// uses this value as the units bound. The snapshot, identity and currency
	// checks, the ensure, the hold arm and the refusal mapping are unchanged,
	// which is what lets the Live hard stop key on ErrAuthorizationRefused.
	// The estimate is a CHUNK, not a bound: each Live window re-authorizes at
	// the session's ORIGINAL estimate (the window closes when accrued usage
	// reaches it), so the estimator — which would re-bound from the request
	// body — is bypassed by design. Setting both Estimate and
	// FixedEstimateUnits is a programming error: FixedEstimateUnits wins and
	// the authorizer does not estimate. Live window re-authorization and gateway-owned fixed media quotes use this bound.
	FixedEstimateUnits int64
}

func (a *CanonicalWalletAuthorizer) mode() string {
	if a == nil || a.cfg == nil {
		return config.CanonicalWalletModeDisabled
	}
	return a.cfg.CanonicalWallet.Mode
}

func (a *CanonicalWalletAuthorizer) requestTimeout() time.Duration {
	if a == nil || a.cfg == nil || a.cfg.CanonicalWallet.RequestTimeoutMS <= 0 {
		return 300 * time.Millisecond
	}
	return time.Duration(a.cfg.CanonicalWallet.RequestTimeoutMS) * time.Millisecond
}

// Authorize returns the handle and, in enforce mode only, the refusal. In shadow
// mode every failure is counted and logged and the attempt is admitted; in
// disabled mode (or with no bridge) the handle carries only its id.
func (a *CanonicalWalletAuthorizer) Authorize(ctx context.Context, in AuthorizeInput) (*AuthorizationHandle, error) {
	ctx, diagnostic := newWalletAuthorizationDiagnostic(ctx)
	mode := a.mode()
	h, err := newAuthorizationHandle(mode)
	if err != nil {
		return nil, err
	}
	if in.DurableAuthorizationID != "" {
		if !strings.HasPrefix(in.DurableAuthorizationID, "auth_") || len(in.DurableAuthorizationID) != 37 {
			return nil, errors.New("invalid durable authorization id")
		}
		h.ID = in.DurableAuthorizationID
	}
	if a == nil || a.bridge == nil || a.snapshots == nil {
		walletAuthorizationStage(ctx, "dependencies")
		refused := &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "USD wallet authorization dependencies unavailable"}
		h.Refusal = refused
		diagnostic.log(ctx, h, in, refused.Reason, nil, mode == config.CanonicalWalletModeEnforce)
		return h, refused
	}
	if mode == "" || mode == config.CanonicalWalletModeDisabled {
		return h, nil
	}
	authorizationMetrics.minted.Add(1)
	enforce := mode == config.CanonicalWalletModeEnforce
	refuse := func(reason AuthorizationRefusalReason, detail string, cause error) (*AuthorizationHandle, error) {
		diagnostic.log(ctx, h, in, reason, cause, enforce)
		if enforce && a.bridge.outboxDB != nil && h.AttemptKind != "media" {
			abortCtx, abortCancel := context.WithTimeout(context.Background(), a.requestTimeout())
			_, _ = a.bridge.outboxDB.ExecContext(abortCtx, `UPDATE wallet_authorization_segment SET state='released',updated_at=now() WHERE parent_authorization_id=$1 AND state='prepared'`, h.ID)
			abortCancel()
		}
		e := &AuthorizationRefusedError{Reason: reason, AuthorizationID: h.ID, Detail: detail, Cause: cause}
		if enforce {
			h.Refusal = e
			authorizationMetrics.refused.Add(1)
			return h, e
		}
		return h, nil
	}
	walletAuthorizationStage(ctx, "snapshot_check")
	if in.Snapshot == nil {
		authorizationMetrics.snapshotMissing.Add(1)
		return refuse(AuthorizationRefusalSnapshotMissing, "no billing snapshot for this attempt", nil)
	}
	h.SnapshotID = in.Snapshot.ID
	h.readerBillingFamily = in.Snapshot.Family
	h.readerPlatform = in.Snapshot.ProviderPlatform
	h.readerTokenOnly = in.Snapshot.Pricing.Mode == BillingModeToken && in.Estimate.ImageCount == 0 && in.Estimate.VideoCount == 0 && in.Estimate.WebSearchCalls == 0
	switch {
	case in.Estimate.WebSearchCalls > 0:
		h.readerCountKind = "alpha_search"
	case in.Estimate.VideoCount > 0 && in.Estimate.GrokVideo:
		h.readerCountKind = "grok_video"
	case in.Snapshot.Family == BillingFamilyGeneric && (in.Estimate.ImageCount > 0 || isImageGenerationModel(in.Snapshot.RequestedModel)):
		h.readerCountKind = "gemini_image"
	case in.Estimate.ImageCount > 0:
		h.readerCountKind = "openai_images"
	case in.Snapshot.Pricing.Mode == BillingModePerRequest:
		h.readerCountKind = "request"
	case in.Snapshot.Family == BillingFamilyOpenAI:
		// Responses can emit an image call even without the request's advisory
		// estimate predicting it. Freeze the output parser, never that estimate.
		h.readerCountKind = "openai_images"
	}
	h.AttemptKind = "llm"
	if in.Snapshot.Family == BillingFamilyLive {
		h.AttemptKind = "live"
	}
	if in.DurableAuthorizationID != "" {
		h.AttemptKind = "media"
	}
	policyVersion := in.Snapshot.Flags.USDWalletPolicyVersion
	walletAuthorizationStage(ctx, "policy_check")
	_, policyEnabled, policyErr := canonicalUSDWalletSnapshot(in.User, a.cfg)
	if policyErr != nil || (policyEnabled && policyVersion == "") ||
		(policyVersion != "" && (!policyEnabled || validateCanonicalUSDWalletSnapshot(in.Snapshot.FX, policyVersion) != nil)) {
		return refuse(AuthorizationRefusalLeaseUnavailable, "USD wallet policy", ErrCanonicalUSDWalletPolicy)
	}

	// Phase 3.7b (§13.2.1): FixedEstimateUnits > 0 replaces the estimation
	// step ONLY — the bound is the caller's (the Live window's chunk).
	var units int64
	walletAuthorizationStage(ctx, "estimate")
	if in.FixedEstimateUnits > 0 {
		units = in.FixedEstimateUnits
	} else {
		estimated, estimateErr := a.snapshots.EstimateUpperBoundUnits(in.Snapshot, in.Estimate)
		if estimateErr != nil {
			authorizationMetrics.estimateFailed.Add(1)
			return refuse(AuthorizationRefusalEstimateFailed, "model "+in.Snapshot.BillingModel, estimateErr)
		}
		units = estimated
	}
	h.EstimatedUnits = units
	h.Continuation = in.Estimate.Continuation
	walletAuthorizationStage(ctx, "identity_check")
	if in.User == nil || strings.TrimSpace(in.User.PlatformUserID) == "" {
		authorizationMetrics.identityMissing.Add(1)
		return refuse(AuthorizationRefusalIdentityMissing, "no platform user id", nil)
	}
	// ObserveSettlement rejects any raw non-USD currency, so a user admitted
	// here with one would be served and never charged. Refuse before any hold.
	walletAuthorizationStage(ctx, "currency_check")
	if raw := strings.TrimSpace(in.User.BillingCurrency); raw != "" {
		if _, err := RequireUSDBillingCurrency(raw); err != nil {
			authorizationMetrics.currencyUnsupported.Add(1)
			return refuse(AuthorizationRefusalCurrency, "billing currency "+raw, err)
		}
	}
	currency := CurrencyUSD
	leaseCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	defer cancel()
	if policyVersion != "" && a.bridge.outboxDB != nil && a.bridge.HoldsEnabled() {
		if _, ok := a.bridge.control.(*canonicalWalletHTTPClient); ok {
			// The plan's segment rows reference the snapshot by foreign key, but a
			// text or live snapshot is otherwise persisted only at settlement.
			// Media persists its own in the task transaction; Persist is idempotent.
			if h.AttemptKind != "media" {
				walletAuthorizationStage(leaseCtx, "snapshot_persist")
				if err = a.snapshots.Persist(leaseCtx, in.Snapshot); err != nil {
					return refuse(AuthorizationRefusalLeaseUnavailable, "snapshot persist", err)
				}
			}
			if err = a.bridge.authorizePool(leaseCtx, h, in.User.PlatformUserID, units); err != nil {
				if _, _, riskRefused := WalletRiskRefusalDetails(err); riskRefused {
					// Preserve 429/503 and prevent an ignored admission error from
					// allowing this handle to cross the upstream write boundary.
					riskErr := err
					h.beforeWrite = func(context.Context, string) error { return riskErr }
					return h, riskErr
				}
				reason := AuthorizationRefusalLeaseUnavailable
				if errors.Is(err, ErrCanonicalWalletBalanceShortfall) {
					reason = AuthorizationRefusalBalanceShortfall
				}
				return refuse(reason, "pool", err)
			}
			if h.AttemptKind != "media" {
				walletAuthorizationStage(leaseCtx, "attempt_protection")
				if err = a.bridge.preparePoolAttempt(leaseCtx, h, in.User.PlatformUserID); err != nil {
					return refuse(AuthorizationRefusalLeaseUnavailable, "attempt protection", err)
				}
			}
			if h.AttemptKind == "llm" {
				h.retryCheck = func(ctx context.Context) error {
					segments, e := a.bridge.authorizationSegments(ctx, h.ID)
					if e != nil || len(segments) != len(h.Segments) {
						return &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "previous zero resolution unavailable", Cause: e}
					}
					if mode := a.bridge.ImmediateWalletReleaseMode("llm"); mode == "enabled" || mode == "shadow" {
						var acknowledged bool
						if e = a.bridge.outboxDB.QueryRowContext(ctx, `SELECT COALESCE(bool_and(zero_ack_at IS NOT NULL),false) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&acknowledged); e != nil || !acknowledged {
							return &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "previous signed zero acknowledgement unavailable", Cause: e}
						}
					}
					for _, segment := range segments {
						if segment.ActualUnits != 0 || segment.Remainder != nil || (segment.State != "released" && segment.State != "finished") {
							return &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "previous attempt has no reliable zero proof"}
						}
						if segment.State != "finished" {
							if _, e = a.bridge.store.ReleaseCanonicalWalletHold(ctx, in.User.PlatformUserID, segment.AuthorizationID, "released", "zero_cost"); e != nil && !isHoldNotArmed(e) && !errors.Is(e, ErrCanonicalWalletHoldMissing) {
								return e
							}
							if e = a.bridge.protectPoolAttempt(ctx, h.ID, in.User.PlatformUserID, h.SnapshotID, &segment, true); e == nil {
								e = a.bridge.finishPoolSegment(ctx, in.User.PlatformUserID, segment)
							}
							if e != nil {
								return &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: h.ID, Detail: "previous zero pin ACK unavailable", Cause: e}
							}
						}
					}
					return nil
				}
				h.renewAfterZero = func(ctx context.Context) (*AuthorizationHandle, error) {
					if err := h.retryCheck(ctx); err != nil {
						return nil, err
					}
					return a.Authorize(ctx, in)
				}
			}
			return h, nil
		}
	}
	walletAuthorizationStage(leaseCtx, "lease_ensure")
	lease, err := a.bridge.ensureLeaseWithPolicy(leaseCtx, in.User.PlatformUserID, currency, units, canonicalWalletLeasePurposeAuthorize, "", policyVersion)
	if err != nil {
		if errors.Is(err, ErrCanonicalWalletBalanceShortfall) {
			authorizationMetrics.balanceShortfall.Add(1)
			return refuse(AuthorizationRefusalBalanceShortfall, "", err)
		}
		if errors.Is(err, ErrCanonicalWalletLeaseCapReached) {
			authorizationMetrics.leaseCapReached.Add(1)
			return refuse(AuthorizationRefusalLeaseCapReached, "", err)
		}
		authorizationMetrics.leaseUnavailable.Add(1) // includes the transient ErrCanonicalWalletLeaseContention
		return refuse(AuthorizationRefusalLeaseUnavailable, "", err)
	}
	h.LeaseID = lease.LeaseID
	walletAuthorizationPool(leaseCtx, []CanonicalWalletLease{*lease})
	// Phase 3.4b (§10.4): with holds on, Authorize continues past the ensure
	// and arms this attempt's hold at the estimate E. On the guard's {4} — a
	// concurrent reservation took the headroom between ensureLease's covering
	// read and the arm — the ensure is re-run ONCE with no prefer_lease_id and
	// the arm retried on what it returns; a second refusal (or any re-ensure
	// error) is lease_unavailable (transient class; no new refusal reason —
	// the constant would have no distinct producer). Shadow admits either way.
	if a.bridge.HoldsEnabled() {
		walletAuthorizationStage(leaseCtx, "hold_arm")
		leaseID, held, _, aerr := a.bridge.store.ArmCanonicalWalletHold(leaseCtx, in.User.PlatformUserID, lease.LeaseID, currency, h.ID, units, a.bridge.graceMS(), a.bridge.clock())
		if errors.Is(aerr, ErrCanonicalWalletLeaseExhausted) || errors.Is(aerr, ErrCanonicalWalletLeaseMissing) || errors.Is(aerr, ErrCanonicalWalletLeaseExpired) {
			authorizationMetrics.holdArmRetried.Add(1)
			walletAuthorizationStage(leaseCtx, "lease_ensure_retry")
			lease, err = a.bridge.ensureLeaseWithPolicy(leaseCtx, in.User.PlatformUserID, currency, units, canonicalWalletLeasePurposeAuthorize, "", policyVersion)
			if err == nil {
				walletAuthorizationPool(leaseCtx, []CanonicalWalletLease{*lease})
				walletAuthorizationStage(leaseCtx, "hold_arm_retry")
				leaseID, held, _, aerr = a.bridge.store.ArmCanonicalWalletHold(leaseCtx, in.User.PlatformUserID, lease.LeaseID, currency, h.ID, units, a.bridge.graceMS(), a.bridge.clock())
			}
		}
		if aerr != nil || err != nil {
			authorizationMetrics.holdArmRefused.Add(1)
			cause := aerr
			if cause == nil {
				cause = err
			}
			return refuse(AuthorizationRefusalLeaseUnavailable, "hold", cause) // shadow admits inside refuse
		}
		h.LeaseID, h.HoldArmed, h.HeldUnits = leaseID, true, held
		h.onOutcome = a.bridge.holdOutcome(in.User.PlatformUserID, h.ID)
		authorizationMetrics.holdsArmed.Add(1)
	}
	return h, nil
}

// EstimateInputOptions carries the media terms only the handler knows.
type EstimateInputOptions struct {
	ImageCount, ImageInputTokensUpperBound int
	ImageSize                              string
	VideoCount, VideoDurationSeconds       int
	VideoResolution                        string
	GrokVideo                              bool
	WebSearchCalls                         int
}

// EstimateInputFromRequestBody builds the estimator's input from the bytes the
// forwarder will send. The input bound is the body length (BPE tokens are >= 1 byte
// — the rule 3.2 documents on EstimateInput); the output bound is the MAXIMUM
// over max_output_tokens / max_tokens / max_completion_tokens / Gemini's
// generationConfig.maxOutputTokens present (0 falls back to the snapshot's model maximum
// inside the estimator). HTTP attempts are never continuations (that is ws_v2, 3.3b).
func EstimateInputFromRequestBody(body []byte, opts EstimateInputOptions) EstimateInput {
	in := EstimateInput{
		InputTokensUpperBound:      len(body),
		ImageInputTokensUpperBound: opts.ImageInputTokensUpperBound,
		Continuation:               ContinuationNone,
		ImageCount:                 opts.ImageCount,
		ImageSize:                  opts.ImageSize,
		VideoCount:                 opts.VideoCount,
		VideoResolution:            opts.VideoResolution,
		VideoDurationSeconds:       opts.VideoDurationSeconds,
		GrokVideo:                  opts.GrokVideo,
		WebSearchCalls:             opts.WebSearchCalls,
	}
	// The bound is the MAX over every present key — whichever the upstream ends up
	// honouring — never the first match: {"max_tokens":16,"max_completion_tokens":100000}
	// must bound at 100000 (round-1 finding).
	for _, key := range []string{"max_output_tokens", "max_tokens", "max_completion_tokens", "generationConfig.maxOutputTokens", "generation_config.max_output_tokens"} {
		if v := gjson.GetBytes(body, key); v.Exists() && v.Int() > int64(in.MaxOutputTokens) {
			in.MaxOutputTokens = int(v.Int())
		}
	}
	in.ServiceTier = strings.TrimSpace(gjson.GetBytes(body, "service_tier").String())
	return in
}
