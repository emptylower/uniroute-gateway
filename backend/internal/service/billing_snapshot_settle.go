package service

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// SnapshotSettlementInput is what the settlement path knows only after the
// upstream replied: usage and media counts. Everything price-shaped comes
// from the snapshot.
type SnapshotSettlementInput struct {
	Tokens               UsageTokens
	ServiceTier          string
	ImageCount           int
	ImageSize            string
	VideoCount           int
	VideoResolution      string
	VideoDurationSeconds int
	WebSearchCalls       int
	// GrokVideo is the OpenAI path's isGrokVideoUsageResult(result, billingModels)
	// (openai_gateway_usage.go:459-472), evaluated by the caller on the SAME
	// candidate list the live path uses — it scans result.Model/UpstreamModel
	// too, which the snapshot cannot see.
	GrokVideo bool
}

// SettlementContextFromSnapshot pins the snapshot's FX into a FRESH holder so
// ResolveCostSettlement takes the pinned path and never calls the exchange
// rate service. It never mutates a holder the request already carries:
// storeBillingSettlementSnapshot writes into whatever holder is present
// (billing_settlement.go:49-56) and returns nothing. Subscription billing
// carries no FX and pins nothing; a zero rate (FX outage at freeze) pins
// nothing, so settlement degrades to the live rate.
func SettlementContextFromSnapshot(ctx context.Context, snap *BillingSnapshot) context.Context {
	if snap == nil || snap.Flags.SubscriptionBilling || (snap.FX.Rate <= 0 && snap.Flags.USDWalletPolicyVersion == "") {
		return ctx
	}
	// The holder literal matches WithBillingSettlementContext (:22-35); do not
	// call that function if it reuses an existing holder.
	ctx = context.WithValue(ctx, billingSettlementContextKey{}, &billingSettlementSnapshots{snapshots: map[string]ExchangeRateSnapshot{}, frozen: true, walletPolicyVersion: snap.Flags.USDWalletPolicyVersion, frozenCurrency: snap.FX.QuoteCurrency})
	storeBillingSettlementSnapshot(ctx, snap.FX)
	return ctx
}

// CalculateCostFromSnapshot prices a settlement from the frozen snapshot,
// dispatching exactly as calculateOpenAIRecordUsageCost /
// calculateRecordUsageCost do today, but with Resolved pre-set so
// CalculateCostUnified never calls the resolver.
func (s *BillingService) CalculateCostFromSnapshot(snap *BillingSnapshot, in SnapshotSettlementInput) (*CostBreakdown, error) {
	if s == nil || snap == nil {
		return nil, errors.New("billing snapshot is nil")
	}
	model := snap.BillingModel
	resolved := snap.resolvedPricing()

	if in.WebSearchCalls > 0 {
		return s.CalculateWebSearchCost(in.WebSearchCalls, snap.Media.WebSearchPricePerCall, snap.Multipliers.WebSearch), nil
	}
	// The live media gates are on CHANNEL pricing, not on the resolved mode:
	// resolveChannelPricing/resolveOpenAIChannelPricing return nil unless
	// Source == PricingSourceChannel (gateway_usage_billing.go:937-947), and
	// media goes the token path only when that channel pricing is token-mode
	// (openai_gateway_usage.go:416-425; gateway_usage_billing.go:868-873).
	// LiteLLM/fallback pricing is always BillingModeToken
	// (model_pricing_resolver.go:88-91), so a gate on resolved.Mode would never
	// take the media branch for any non-channel-priced model.
	channelToken := resolved.Source == PricingSourceChannel && resolved.Mode == BillingModeToken
	if in.VideoCount > 0 && in.GrokVideo && !channelToken {
		return s.videoCostFromSnapshot(snap, resolved, in), nil
	}
	if in.ImageCount > 0 && !channelToken {
		return s.imageCostFromSnapshot(snap, resolved, in)
	}

	// Gemini-style long-context opts (RecordUsageWithLongContext) are a
	// separate arithmetic path today; keep them separate here.
	if snap.Flags.LongContextThreshold > 0 && snap.Flags.LongContextMultiplier > 1 && resolved.Source != PricingSourceChannel {
		total := in.Tokens.CacheReadTokens + in.Tokens.InputTokens
		if total > snap.Flags.LongContextThreshold {
			if resolved.BasePricing == nil {
				return nil, ErrModelPricingUnavailable
			}
			// GetModelPricing lowercases and applies the model-specific policy
			// before CalculateCost sees the pricing — do the same to the frozen copy.
			pricing := s.applyModelSpecificPricingPolicy(strings.ToLower(model), clonePricing(resolved.BasePricing))
			return s.calculateCostWithLongContextPricing(pricing, in.Tokens, snap.Multipliers.Text, snap.Flags.LongContextThreshold, snap.Flags.LongContextMultiplier)
		}
	}

	var longCtx *bool
	if snap.Family == BillingFamilyOpenAI {
		v := snap.Flags.LongContextBillingEnabled
		longCtx = &v
	}
	cost, err := s.CalculateCostUnified(CostInput{
		Ctx:          context.Background(),
		Model:        model, // un-lowercased, as CalculateCostUnified's live callers pass it (:981); GetModelPricing lowercases (:807) — a latent generic-family asymmetry the policy's gpt-5.x matching never reaches, and record mode would count as drift
		GroupID:      snap.GroupID,
		Tokens:       in.Tokens,
		RequestCount: maxInt(in.ImageCount, 1), // maxInt already exists: grok_quota_service.go:543 — do NOT redeclare it
		SizeTier:     strings.TrimSpace(in.ImageSize),

		RateMultiplier:            snap.Multipliers.Text,
		ServiceTier:               in.ServiceTier,
		Resolver:                  snapshotTierResolver, // pure over `Resolved`; never resolves
		Resolved:                  resolved,
		LongContextBillingEnabled: longCtx,
	})
	if err != nil {
		return nil, err
	}
	// BillingMode must match the family's live stamping, or settle mode changes
	// usage_logs.billing_mode and the multiplier the OpenAI log picks
	// (openai_gateway_usage.go:288-291): CalculateCostUnified stamps "token"
	// (billing_service.go:960-965); the generic non-channel path
	// (calculateTokenCost → CalculateCost → computeTokenBreakdown) leaves it
	// EMPTY. Read calculateTokenCost (gateway_usage_billing.go:988-1030) and
	// calculateOpenAIRecordUsageCost (openai_gateway_usage.go:430-451 and the
	// calls at :496/:539/:586) and mirror each family exactly; costsEqual
	// compares BillingMode, so record mode proves it across the suite.
	if snap.Family == BillingFamilyGeneric && resolved.Source != PricingSourceChannel {
		cost.BillingMode = ""
	}
	return cost, nil
}

// imageCostFromSnapshot mirrors calculateImageCost (gateway_usage_billing.go:950-986)
// and calculateOpenAIImageCost (openai_gateway_usage.go:517-556) against the
// FROZEN group config and channel pricing: normalized tier; a configured group
// price for that tier wins; then channel pricing through CalculateCostUnified —
// any mode on the generic family, per_request/image only on the OpenAI family
// (its apiKeyWithFreshGroupMediaPricing refresh already happened at freeze
// time); then the default per-image price.
func (s *BillingService) imageCostFromSnapshot(snap *BillingSnapshot, resolved *ResolvedPricing, in SnapshotSettlementInput) (*CostBreakdown, error) {
	sizeTier := NormalizeImageBillingTierOrDefault(in.ImageSize)
	if snapshotHasConfiguredImagePrice(snap.Media.ImagePrice, sizeTier) {
		return s.CalculateImageCost(snap.BillingModel, sizeTier, in.ImageCount, snap.Media.ImagePrice, snap.Multipliers.Image), nil
	}
	generic := snap.Family == BillingFamilyGeneric
	channel := resolved.Source == PricingSourceChannel
	if !generic {
		channel = channel && (resolved.Mode == BillingModePerRequest || resolved.Mode == BillingModeImage)
	}
	if channel {
		tokens := UsageTokens{}
		if generic { // the generic helper passes text + image-output tokens; the OpenAI helper passes none
			tokens = UsageTokens{InputTokens: in.Tokens.InputTokens, OutputTokens: in.Tokens.OutputTokens, ImageOutputTokens: in.Tokens.ImageOutputTokens}
		}
		cost, err := s.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: snap.BillingModel, GroupID: snap.GroupID, Tokens: tokens, RequestCount: in.ImageCount, SizeTier: sizeTier, RateMultiplier: snap.Multipliers.Image, Resolver: snapshotTierResolver, Resolved: resolved})
		if err == nil {
			return cost, nil
		}
		if generic {
			return nil, err // the generic helper propagates; the OpenAI helper logs and falls through
		}
		logger.LegacyPrintf("service.billing_snapshot", "image channel cost from snapshot failed: %v", err)
	}
	return s.CalculateImageCost(snap.BillingModel, sizeTier, in.ImageCount, snap.Media.ImagePrice, snap.Multipliers.Image), nil
}

// snapshotHasConfiguredImagePrice is apiKeyHasConfiguredImagePrice
// (media_price_config.go:14 → Group.GetImagePrice(size) != nil) against the
// frozen config. Mirror Group.GetImagePrice's tier switch (grep it in group.go)
// tier for tier.
func snapshotHasConfiguredImagePrice(cfg *ImagePriceConfig, sizeTier string) bool {
	if cfg == nil {
		return false
	}
	switch sizeTier {
	case "1K":
		return cfg.Price1K != nil
	case "4K":
		return cfg.Price4K != nil
	default: // "2K" and, as Group.GetImagePrice does (group.go:160-162), any unknown tier — unreachable after NormalizeImageBillingTierOrDefault, mirrored anyway
		return cfg.Price2K != nil
	}
}

// snapshotHasConfiguredVideoPrice mirrors apiKeyHasConfiguredVideoPrice
// (media_price_config.go:29) against the frozen config, resolution for resolution.
func snapshotHasConfiguredVideoPrice(cfg *VideoPriceConfig, resolution string) bool {
	if cfg == nil {
		return false
	}
	switch resolution {
	case "720p":
		return cfg.Price720P != nil
	case "1080p":
		return cfg.Price1080P != nil
	default: // "480p" and, as Group.GetVideoPrice does (group.go:176-177), any unknown resolution
		return cfg.Price480P != nil
	}
}

// videoCostFromSnapshot mirrors calculateOpenAIVideoCost (openai_gateway_usage.go:558-600):
// count clamped to ≥ 1, resolution and duration normalized, a configured group
// price first, channel per_request/image pricing by request count (stamped
// BillingModeVideo), then the default video price.
func (s *BillingService) videoCostFromSnapshot(snap *BillingSnapshot, resolved *ResolvedPricing, in SnapshotSettlementInput) *CostBreakdown {
	videoCount := in.VideoCount
	if videoCount <= 0 {
		videoCount = 1
	}
	resolution := NormalizeVideoBillingResolutionOrDefault(in.VideoResolution)
	duration := NormalizeVideoBillingDurationSecondsOrDefault(in.VideoDurationSeconds)
	if snapshotHasConfiguredVideoPrice(snap.Media.VideoPrice, resolution) {
		return s.CalculateVideoCost(snap.BillingModel, resolution, videoCount, duration, snap.Media.VideoPrice, snap.Multipliers.Video)
	}
	if resolved.Source == PricingSourceChannel && (resolved.Mode == BillingModePerRequest || resolved.Mode == BillingModeImage) {
		cost, err := s.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: snap.BillingModel, GroupID: snap.GroupID, RequestCount: videoCount, SizeTier: resolution, RateMultiplier: snap.Multipliers.Video, Resolver: snapshotTierResolver, Resolved: resolved})
		if err == nil {
			cost.BillingMode = string(BillingModeVideo)
			return cost
		}
		logger.LegacyPrintf("service.billing_snapshot", "video channel cost from snapshot failed: %v", err)
	}
	return s.CalculateVideoCost(snap.BillingModel, resolution, videoCount, duration, snap.Media.VideoPrice, snap.Multipliers.Video)
}

// snapshotTierResolver has no channel service and no billing service; the
// only methods CalculateCostUnified calls on it with Resolved pre-set are
// GetIntervalPricing (:243) / GetRequestTierPrice (:293) /
// GetRequestTierPriceByContext (:303), which read the ResolvedPricing they
// are handed and package-level helpers — verified, none dereferences
// r.channelService or r.billingService.
var snapshotTierResolver = &ModelPricingResolver{}

// snapshotCoversModel guards against settling a model the snapshot did not
// price. Both settlement paths derive the billing model independently and
// late (gateway_usage_billing.go:759-780; openai_gateway_usage.go:182-197 with a
// six-element candidate chain walked at :430-451); under failover, alias
// rewriting or an upstream that reports a different result.Model, the frozen
// model and the settled model differ. A mismatch settles live and is counted.
func snapshotCoversModel(snap *BillingSnapshot, settledModel string) bool {
	m := strings.TrimSpace(settledModel)
	if m == "" {
		return false
	}
	if m == snap.BillingModel {
		return true
	}
	for _, c := range snap.Candidates {
		if c == m {
			return true
		}
	}
	return false
}

// snapshotCacheTTLOverride is the frozen (enabled, target) pair.
func snapshotCacheTTLOverride(snap *BillingSnapshot) cacheTTLOverride {
	if snap == nil {
		return cacheTTLOverride{}
	}
	return cacheTTLOverride{Enabled: snap.Flags.CacheTTLOverrideEnabled, Target: snap.Flags.CacheTTLOverrideTarget}
}

// snapshotSettlementInputFromClaudeUsage builds the snapshot path's input from
// the RAW usage (pre-override) and applies the SNAPSHOT's frozen cache-TTL
// override to a copy — applyCacheTTLOverride mutates in place
// (gateway_upstream_response.go:1268) and treats an empty target as 5m, exactly
// as the live path does when ok is true with an empty target — so `usage` is
// taken by value and Enabled, not the target string, decides. The UsageTokens
// literal is calculateTokenCost's (gateway_usage_billing.go:997-1005), field for field.
func snapshotSettlementInputFromClaudeUsage(usage ClaudeUsage, override cacheTTLOverride, imageCount int, imageSize string) SnapshotSettlementInput {
	if override.Enabled {
		applyCacheTTLOverride(&usage, override.Target)
	}
	return SnapshotSettlementInput{
		Tokens: UsageTokens{
			InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens,
			CacheCreationTokens: usage.CacheCreationInputTokens, CacheReadTokens: usage.CacheReadInputTokens,
			CacheCreation5mTokens: usage.CacheCreation5mTokens, CacheCreation1hTokens: usage.CacheCreation1hTokens,
			ImageOutputTokens: usage.ImageOutputTokens,
		},
		ImageCount: imageCount, ImageSize: imageSize,
	}
}

// openAIUsageTokens is RecordUsage's token conversion (openai_gateway_usage.go:146-159),
// extracted unchanged so both paths use one implementation (Task 5 switches
// RecordUsage to call it).
func openAIUsageTokens(usage OpenAIUsage) UsageTokens {
	actualInputTokens := usage.InputTokens - usage.CacheReadInputTokens - usage.CacheCreationInputTokens
	if actualInputTokens < 0 {
		actualInputTokens = 0
	}
	return UsageTokens{
		InputTokens:         actualInputTokens,
		ImageInputTokens:    usage.ImageInputTokens,
		OutputTokens:        usage.OutputTokens,
		CacheCreationTokens: usage.CacheCreationInputTokens,
		CacheReadTokens:     usage.CacheReadInputTokens,
		ImageOutputTokens:   usage.ImageOutputTokens,
	}
}

func snapshotSettlementInputFromOpenAIResult(result *OpenAIForwardResult, serviceTier string, grokVideo bool) SnapshotSettlementInput {
	if result == nil {
		return SnapshotSettlementInput{}
	}
	return SnapshotSettlementInput{
		Tokens:      openAIUsageTokens(result.Usage),
		ServiceTier: serviceTier,
		ImageCount:  result.ImageCount, ImageSize: result.ImageSize,
		VideoCount: result.VideoCount, VideoResolution: result.VideoResolution, VideoDurationSeconds: result.VideoDurationSeconds,
		WebSearchCalls: result.WebSearchCalls,
		GrokVideo:      grokVideo,
	}
}

// applyBillingSnapshotToSettlement is the single place the mode switch lives.
//
//	off:    return the live cost untouched.
//	record: compute the snapshot cost too; count drift; return the live cost.
//	settle: return the snapshot cost and a ctx with the snapshot's FX pinned.
//
// settledModel is the billing model the settlement path derived; a snapshot
// that did not price it (snapshotCoversModel) settles live and is counted.
func (s *billingSnapshotSettler) applyBillingSnapshotToSettlement(ctx context.Context, snap *BillingSnapshot, live *CostBreakdown, in SnapshotSettlementInput, settledModel string) (*CostBreakdown, context.Context) {
	if snap == nil || s.snapshots == nil || live == nil {
		return live, ctx
	}
	mode := s.snapshots.Mode()
	if mode == BillingSnapshotModeOff {
		return live, ctx
	}
	if !snapshotCoversModel(snap, settledModel) {
		billingSnapshotMetrics.modelMismatch.Add(1)
		logger.LegacyPrintf("service.billing_snapshot", "snapshot=%s frozen model %q does not cover settled model %q; settling live", snap.ID, snap.BillingModel, settledModel)
		return live, ctx
	}
	fromSnapshot, err := s.billing.CalculateCostFromSnapshot(snap, in)
	if err != nil {
		billingSnapshotMetrics.settleError.Add(1)
		return live, ctx
	}
	if mode == BillingSnapshotModeRecord {
		if !costsEqual(live, fromSnapshot) {
			billingSnapshotMetrics.drift.Add(1)
			logger.LegacyPrintf("service.billing_snapshot", "record-mode drift snapshot=%s live=%.12f/%q snapshot=%.12f/%q", snap.ID, live.ActualCost, live.BillingMode, fromSnapshot.ActualCost, fromSnapshot.BillingMode)
		}
		return live, ctx
	}
	billingSnapshotMetrics.settled.Add(1)
	return fromSnapshot, SettlementContextFromSnapshot(ctx, snap)
}

// costsEqual compares the two figures the wallet and the usage log consume AND
// BillingMode, which feeds usage_logs.billing_mode and the OpenAI log's
// multiplier choice — a silent change there is drift too.
func costsEqual(a, b *CostBreakdown) bool {
	const eps = 1e-12
	return math.Abs(a.ActualCost-b.ActualCost) <= eps && math.Abs(a.TotalCost-b.TotalCost) <= eps && a.BillingMode == b.BillingMode
}

// billingSnapshotSettler is embedded by both gateway services so the method
// above has one implementation. Fields are set in their constructors (Task 5).
type billingSnapshotSettler struct {
	snapshots *BillingSnapshotService
	billing   *BillingService
}

// persistBillingSnapshotBestEffort runs on its own detached context: the
// usage-log writes happen on the caller's ctx (writeUsageLogBestEffort,
// gateway_usage_billing.go:589-594 derives its own), and the billing context
// applyUsageBillingDetailed creates (:367) is cancelled by its defer (:368)
// before the log is written at :854 — there is no in-scope detached context to reuse.
func persistBillingSnapshotBestEffort(ctx context.Context, svc *BillingSnapshotService, snap *BillingSnapshot) {
	if svc == nil || snap == nil {
		return
	}
	pctx, cancel := detachedBillingContext(ctx)
	defer cancel()
	if err := svc.Persist(pctx, snap); err != nil {
		billingSnapshotMetrics.persistError.Add(1)
		logger.LegacyPrintf("service.billing_snapshot", "persist failed snapshot=%s err=%v", snap.ID, err)
	}
}
