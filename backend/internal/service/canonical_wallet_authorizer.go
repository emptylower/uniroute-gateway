package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
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
	Snapshot *BillingSnapshot
	Estimate EstimateInput
	User     *User
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
	mode := a.mode()
	h, err := newAuthorizationHandle(mode)
	if err != nil {
		return nil, err
	}
	if a == nil || mode == "" || mode == config.CanonicalWalletModeDisabled || a.bridge == nil || a.snapshots == nil {
		return h, nil
	}
	authorizationMetrics.minted.Add(1)
	enforce := mode == config.CanonicalWalletModeEnforce
	refuse := func(reason AuthorizationRefusalReason, detail string, cause error) (*AuthorizationHandle, error) {
		e := &AuthorizationRefusedError{Reason: reason, AuthorizationID: h.ID, Detail: detail, Cause: cause}
		if enforce {
			h.Refusal = e
			authorizationMetrics.refused.Add(1)
			return h, e
		}
		logger.LegacyPrintf("service.authorization", "shadow: attempt admitted despite %s", e.Error())
		return h, nil
	}
	if in.Snapshot == nil {
		authorizationMetrics.snapshotMissing.Add(1)
		return refuse(AuthorizationRefusalSnapshotMissing, "no billing snapshot for this attempt", nil)
	}
	h.SnapshotID = in.Snapshot.ID
	units, err := a.snapshots.EstimateUpperBoundUnits(in.Snapshot, in.Estimate)
	if err != nil {
		authorizationMetrics.estimateFailed.Add(1)
		return refuse(AuthorizationRefusalEstimateFailed, "model "+in.Snapshot.BillingModel, err)
	}
	h.EstimatedUnits = units
	h.Continuation = in.Estimate.Continuation
	if in.User == nil || strings.TrimSpace(in.User.PlatformUserID) == "" {
		authorizationMetrics.identityMissing.Add(1)
		return refuse(AuthorizationRefusalIdentityMissing, "no platform user id", nil)
	}
	currency := strings.ToUpper(strings.TrimSpace(in.User.BillingCurrency))
	if currency == "" {
		currency = "CNY"
	}
	if currency != "CNY" {
		authorizationMetrics.currencyUnsupported.Add(1)
		return refuse(AuthorizationRefusalCurrency, "billing currency "+currency, nil)
	}
	leaseCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	defer cancel()
	lease, err := a.bridge.ensureLease(leaseCtx, in.User.PlatformUserID, currency, units, canonicalWalletLeasePurposeAuthorize, "")
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
