package service

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/tidwall/gjson"
)

// Phase 3.2 — the conservative upper-bound estimator (spec §2.0, §8).
// A pure function over (snapshot, EstimateInput) returning cny-e8-v1 units,
// rounded UP, overflow-checked, failing closed on anything it cannot bound.
// 3.3 wires it to authorization; here it is built and proven.

type ContinuationKind string

const (
	ContinuationNone ContinuationKind = "none"
	ContinuationWarm ContinuationKind = "warm" // previous_response_id with accumulated usage in this relay
	ContinuationCold ContinuationKind = "cold" // previous_response_id with no accumulated usage (a reconnect)
)

var ErrEstimateUnbounded = errors.New("billing estimate is unbounded")

type EstimateInput struct {
	// InputTokensUpperBound is the caller's bound on this turn's own input —
	// a tokenizer count, or the request body's byte length (BPE tokens are
	// >= 1 byte, so bytes >= tokens).
	InputTokensUpperBound      int
	ImageInputTokensUpperBound int
	// MaxOutputTokens from the request (max_output_tokens / max_tokens);
	// 0 falls back to the snapshot's model maximum; both 0 = unbounded.
	MaxOutputTokens       int
	ServiceTier           string
	Continuation          ContinuationKind
	PriorTurnInputTokens  int // warm: the relay's accumulated input of the prior turn
	PriorTurnOutputTokens int // warm: … and its output
	ImageCount            int    // the caller's bound — the cost is linear in it
	ImageSize             string // advisory only — the bound is taken over every tier against the original snapshot (the upstream reports the real one after the fact)
	VideoCount            int
	VideoResolution       string // advisory only — see ImageSize
	VideoDurationSeconds  int    // advisory only — the bound uses VideoBillingMaxDurationSeconds
	GrokVideo             bool   // the same isGrokVideoUsageResult verdict the settle input carries; the two dispatchers must agree
	WebSearchCalls        int
}

// ClassifyWSContinuation implements spec §2.0's three cases for a ws_v2
// turn payload. store:false without previous_response_id is an ordinary turn —
// and so, deliberately, is store:TRUE without previous_response_id: that turn's
// input is its own payload; the unbounded-input exposure appears on the NEXT
// turn, which carries previous_response_id. isOpenAIWSStoreDisabledInRequestRaw
// (ingress:452/:798/:1665) is therefore read by 3.3 for the session's
// accumulation policy, not by this classifier.
func ClassifyWSContinuation(payload []byte, sessionHasAccumulatedUsage bool) ContinuationKind {
	prev := gjson.GetBytes(payload, "previous_response_id").String()
	if prev == "" {
		return ContinuationNone
	}
	if sessionHasAccumulatedUsage {
		return ContinuationWarm
	}
	return ContinuationCold
}

// EstimateUpperBoundUnits is a method on the snapshot service because the
// image branch reads BillingService.pricingService for default per-image
// prices (getDefaultImagePrice, billing_service.go:1551-1562) — a zero
// receiver would silently fall back to defaultImageGenerationPrice and
// under-bound.
func (s *BillingSnapshotService) EstimateUpperBoundUnits(snap *BillingSnapshot, in EstimateInput) (int64, error) {
	if s == nil || s.billing == nil {
		return 0, errors.New("billing snapshot service is unavailable")
	}
	if snap == nil {
		return 0, errors.New("nil billing snapshot")
	}
	billing := s.billing
	resolved := snap.resolvedPricing()
	channelToken := resolved.Source == PricingSourceChannel && resolved.Mode == BillingModeToken // the same gate CalculateCostFromSnapshot uses
	var cost *CostBreakdown
	switch {
	case in.WebSearchCalls > 0:
		cost = billing.CalculateWebSearchCost(in.WebSearchCalls, snap.Media.WebSearchPricePerCall, snap.Multipliers.WebSearch)
	// Media: the settle path's OWN arithmetic (imageCostFromSnapshot /
	// videoCostFromSnapshot) — deterministic over the snapshot, so est == settled,
	// a valid bound — never a second implementation that could under-bound
	// (channel per-image pricing above the default, the ≥1 video clamp, …).
	case in.VideoCount > 0 && in.GrokVideo && !channelToken:
		// The count is the caller's bound; resolution and duration are NOT trusted —
		// the upstream reports them after the fact, so the bound is the maximum over
		// every resolution at the maximum billable duration, on a snapshot whose group
		// video prices are the maximum over every configured resolution.
		// Against the ORIGINAL snapshot, tier by tier — never a "max over configured
		// tiers" transform: getImageUnitPrice/getVideoUnitPrice use the group price
		// only for the tier actually configured and fall to the DEFAULT price for every
		// other tier (billing_service.go:1506-1533, :1553-1581, :1627-1652); a group
		// with one cheap configured tier would otherwise hide a 0.134-based default
		// at the unconfigured tiers — a 200× under-bound (round-6 finding). Settlement's
		// tier is always one of exactly three values (image_billing_size.go:64-69;
		// video_billing_resolution.go:33-44), so evaluating the real settlement
		// function at all three IS the bound.
		for _, res := range []string{"480p", "720p", "1080p"} {
			c := billing.videoCostFromSnapshot(snap, worstPerRequestResolved(resolved), SnapshotSettlementInput{VideoCount: in.VideoCount, VideoResolution: res, VideoDurationSeconds: VideoBillingMaxDurationSeconds, GrokVideo: true})
			if cost == nil || c.ActualCost > cost.ActualCost {
				cost = c
			}
		}
	case in.ImageCount > 0 && !channelToken:
		// Same rule for images: the caller's ImageSize is advisory — a request that
		// omits the size normalizes to 2K while the upstream returns 4K — so the bound
		// is the maximum over every tier against the original snapshot. This branch is
		// reachable only with channel per_request/image pricing (or none):
		// calculatePerRequestCost (billing_service.go:1118-1146) reads tokens ONLY to
		// pick a request tier, so the input bound is passed for tier selection and the
		// tier price is bounded by worstPerRequestResolved.
		for _, tier := range []string{"1K", "2K", "4K"} {
			c, err := billing.imageCostFromSnapshot(snap, worstPerRequestResolved(resolved), SnapshotSettlementInput{ImageCount: in.ImageCount, ImageSize: tier, Tokens: UsageTokens{InputTokens: in.InputTokensUpperBound + in.ImageInputTokensUpperBound}})
			if err != nil {
				return 0, err
			}
			if cost == nil || c.ActualCost > cost.ActualCost {
				cost = c
			}
		}
	case snap.Pricing.Mode == BillingModePerRequest || snap.Pricing.Mode == BillingModeImage: // Resolve's per_request/image early return carries no BasePricing (model_pricing_resolver.go:75-83); routing image-mode-without-a-count to the token bound would refuse what settlement charges
		c, err := billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: snap.BillingModel, GroupID: snap.GroupID, RequestCount: maxInt(in.ImageCount, 1), SizeTier: in.ImageSize, Tokens: UsageTokens{InputTokens: in.InputTokensUpperBound + in.ImageInputTokensUpperBound}, RateMultiplier: snap.Multipliers.Text, Resolver: snapshotTierResolver, Resolved: worstPerRequestResolved(resolved)})
		if err != nil {
			return 0, err
		}
		cost = c
	default:
		c, err := estimateTokenUpperBound(billing, snap, in)
		if err != nil {
			return 0, err
		}
		cost = c
	}
	return unitsRoundUp(cost.ActualCost, snap)
}

func estimateTokenUpperBound(billing *BillingService, snap *BillingSnapshot, in EstimateInput) (*CostBreakdown, error) {
	base := snap.Pricing.Base
	if base == nil && len(snap.Pricing.Intervals) == 0 {
		return nil, ErrModelPricingUnavailable
	}
	inputBound := in.InputTokensUpperBound + in.ImageInputTokensUpperBound
	switch in.Continuation {
	case ContinuationWarm:
		inputBound += in.PriorTurnInputTokens + in.PriorTurnOutputTokens
	case ContinuationCold:
		if snap.Pricing.MaxInputTokens <= 0 {
			return nil, ErrEstimateUnbounded
		}
		inputBound = snap.Pricing.MaxInputTokens
	}
	// The context window is the ceiling on what a WARM continuation accumulates
	// (spec §2.0) — never a reason to shrink the caller's own bound: a conservative
	// bound does not shrink, cache-read plus fresh input can exceed the published
	// window, and LiteLLM's figure can be wrong.
	if in.Continuation == ContinuationWarm && snap.Pricing.MaxInputTokens > 0 && inputBound > snap.Pricing.MaxInputTokens {
		inputBound = maxInt(snap.Pricing.MaxInputTokens, in.InputTokensUpperBound+in.ImageInputTokensUpperBound)
	}
	outputBound := in.MaxOutputTokens
	if outputBound <= 0 {
		outputBound = snap.Pricing.MaxOutputTokens
	}
	if outputBound <= 0 {
		return nil, ErrEstimateUnbounded
	}

	resolved := snap.resolvedPricing()
	// NOT the single interval at the bound: settlement picks the interval containing
	// the ACTUAL total context (FindMatchingInterval, channel.go:157-165, called at
	// billing_service.go:974-976), which is ≤ the bound and may be dearer (a volume
	// discount: [0,10k) @ $10/MTok, [10k,∞) @ $1/MTok bounds at $1 but settles 5k
	// tokens at $10), or none at all (a zero-token settlement falls back to
	// BasePricing). Take the field-wise maximum over everything reachable below the bound.
	pricing := worstIntervalPricing(resolved, inputBound)
	if pricing == nil {
		return nil, ErrModelPricingUnavailable
	}
	pricing = billing.applyModelSpecificPricingPolicy(strings.ToLower(snap.BillingModel), pricing) // lowercased, unlike CalculateCostFromSnapshot's un-lowercased CalculateCostUnified call — the safe direction: it can only make the GPT-5.x long-context policy fire, raising the bound

	// Worst-case pricing for the whole context: every input token priced at the
	// dearest of input / cache-read / cache-creation / 5m / 1h / image-input AND
	// their priority variants — settlement may route tokens to any of them
	// (:1084, :1105-1117) and the split is unknown before the write.
	worst := *pricing
	dearest := worst.InputPricePerToken
	for _, p := range []float64{worst.CacheReadPricePerToken, worst.CacheCreationPricePerToken, worst.CacheCreation5mPrice, worst.CacheCreation1hPrice, worst.ImageInputPricePerToken,
		worst.InputPricePerTokenPriority, worst.CacheReadPricePerTokenPriority, worst.CacheCreationPricePerTokenPriority} {
		dearest = math.Max(dearest, p)
	}
	worst.InputPricePerToken = dearest
	worst.ImageInputPricePerToken = 0 // folded into the dearest input price
	// Output: image-output tokens are priced at ImageOutputPricePerToken when set
	// (billing_service.go:1072-1079), which is normally ABOVE the text price on
	// image models — and the text/image split is unknown before the write.
	worst.OutputPricePerToken = math.Max(worst.OutputPricePerToken, worst.ImageOutputPricePerToken)
	worst.ImageOutputPricePerToken, worst.ImageOutputPriceExplicit = 0, false
	if usePriorityServiceTierPricing(in.ServiceTier, &worst) {
		worst.InputPricePerTokenPriority = dearest
		worst.OutputPricePerTokenPriority = math.Max(worst.OutputPricePerTokenPriority, worst.OutputPricePerToken)
	}
	applyLongCtx := len(resolved.Intervals) == 0 && (snap.Family != BillingFamilyOpenAI || snap.Flags.LongContextBillingEnabled)
	tokens := UsageTokens{InputTokens: inputBound, OutputTokens: outputBound}

	// Service tier: NOT decided once. computeTokenBreakdown re-evaluates
	// usePriorityServiceTierPricing on the pricing it is handed (billing_service.go:1011):
	// true iff any *Priority price is > 0 (:124-129) → the priority-price branch at 1×;
	// false → serviceTierCostMultiplier (:131-140; priority 2×, flex 0.5×). Settlement
	// evaluates it on the SINGLE pricing GetIntervalPricing returns — BasePricing when
	// no interval matches the actual context — while `worst` merges every reachable
	// interval, and intervalToModelPricing mirrors interval prices into the *Priority
	// fields (model_pricing_resolver.go:257-269). The two sides can therefore take
	// different arms: a gapped interval set ([0,10k) with no tail) over a
	// priority-less BasePricing settles 50k tokens at base × 2.0, while an estimator
	// that decided "priority prices exist" once would price them at the interval's
	// priority price × 1.0 (round-5 finding: est 0.30 vs settled 0.45). Take the
	// dearer of BOTH arms.
	tierMul := math.Max(1.0, serviceTierCostMultiplier(in.ServiceTier)) // flex's 0.5× is a discount a bound must not assume
	cost := billing.computeTokenBreakdown(&worst, tokens, snap.Multipliers.Text, "", applyLongCtx)
	cost.TotalCost *= tierMul
	cost.ActualCost *= tierMul
	if usePriorityServiceTierPricing(in.ServiceTier, &worst) {
		pri := billing.computeTokenBreakdown(&worst, tokens, snap.Multipliers.Text, in.ServiceTier, applyLongCtx)
		if pri.ActualCost > cost.ActualCost {
			cost = pri
		}
	}
	// The Gemini opts path (CalculateCostFromSnapshot → calculateCostWithLongContextPricing)
	// multiplies the out-of-range slice by LongContextMultiplier when the total input
	// crosses the threshold; bound the WHOLE cost by it.
	if snap.Flags.LongContextThreshold > 0 && snap.Flags.LongContextMultiplier > 1 && resolved.Source != PricingSourceChannel && inputBound > snap.Flags.LongContextThreshold {
		cost.TotalCost *= snap.Flags.LongContextMultiplier
		cost.ActualCost *= snap.Flags.LongContextMultiplier
	}
	return cost, nil
}

// worstIntervalPricing is the field-wise maximum over BasePricing and every
// interval whose MinTokens <= inputBound (intervalToModelPricing is what
// GetIntervalPricing builds each interval's pricing with — same function, so the
// per-interval figures are exactly settlement's). Every price field and the
// long-context multipliers take the max; the long-context threshold takes the
// smallest non-zero value (it applies earlier); SupportsCacheBreakdown and
// ImageOutputPriceExplicit are OR-ed. CacheCreationPriceExplicit is deliberately
// left as BasePricing's: it has no effect on the bound — the estimator's token
// vector carries no cache-creation tokens, and the flag only gates the GPT-5.6 arm
// of applyModelSpecificPricingPolicy (billing_service.go:1206), which can only
// raise a price, and runs before `dearest` is folded. Returns nil only
// when there is nothing to price with; the estimator reads the context window from
// snap.Pricing, never from here.
func worstIntervalPricing(resolved *ResolvedPricing, inputBound int) *ModelPricing {
	var worst *ModelPricing
	consider := func(p *ModelPricing) {
		if p == nil {
			return
		}
		if worst == nil {
			worst = clonePricing(p)
			return
		}
		for _, f := range []struct {
			dst *float64
			src float64
		}{
			{&worst.InputPricePerToken, p.InputPricePerToken}, {&worst.OutputPricePerToken, p.OutputPricePerToken},
			{&worst.CacheCreationPricePerToken, p.CacheCreationPricePerToken}, {&worst.CacheReadPricePerToken, p.CacheReadPricePerToken},
			{&worst.CacheCreation5mPrice, p.CacheCreation5mPrice}, {&worst.CacheCreation1hPrice, p.CacheCreation1hPrice},
			{&worst.ImageInputPricePerToken, p.ImageInputPricePerToken}, {&worst.ImageOutputPricePerToken, p.ImageOutputPricePerToken},
			{&worst.InputPricePerTokenPriority, p.InputPricePerTokenPriority}, {&worst.OutputPricePerTokenPriority, p.OutputPricePerTokenPriority},
			{&worst.CacheCreationPricePerTokenPriority, p.CacheCreationPricePerTokenPriority}, {&worst.CacheReadPricePerTokenPriority, p.CacheReadPricePerTokenPriority},
			{&worst.LongContextInputMultiplier, p.LongContextInputMultiplier}, {&worst.LongContextOutputMultiplier, p.LongContextOutputMultiplier},
		} {
			*f.dst = math.Max(*f.dst, f.src)
		}
		if p.LongContextInputThreshold > 0 && (worst.LongContextInputThreshold == 0 || p.LongContextInputThreshold < worst.LongContextInputThreshold) {
			worst.LongContextInputThreshold = p.LongContextInputThreshold
		}
		worst.SupportsCacheBreakdown = worst.SupportsCacheBreakdown || p.SupportsCacheBreakdown
		worst.ImageOutputPriceExplicit = worst.ImageOutputPriceExplicit || p.ImageOutputPriceExplicit
	}
	consider(resolved.BasePricing)
	for _, iv := range resolved.Intervals {
		if iv.MinTokens <= inputBound {
			consider(intervalToModelPricing(&iv, resolved.SupportsCacheBreakdown, resolved.channelPricing)) // *PricingInterval receiver (model_pricing_resolver.go:257); Intervals is []PricingInterval (:24); go 1.22+ per-iteration loop variable, and consider copies immediately
		}
	}
	return worst
}

// worstPerRequestResolved bounds the per-request / image / video price the same
// way: GetRequestTierPrice / GetRequestTierPriceByContext pick ONE tier by size
// or context (model_pricing_resolver.go:293-311); the bound is a copy of the
// resolved pricing whose DefaultPerRequestPrice is the maximum over the default
// and every tier's PerRequestPrice, with no tiers left to select from.
func worstPerRequestResolved(resolved *ResolvedPricing) *ResolvedPricing {
	c := *resolved
	c.RequestTiers = nil
	worst := resolved.DefaultPerRequestPrice
	for _, t := range resolved.RequestTiers {
		if t.PerRequestPrice != nil && *t.PerRequestPrice > worst {
			worst = *t.PerRequestPrice
		}
	}
	c.DefaultPerRequestPrice = worst
	return &c
}

// unitsRoundUp converts a USD cost to cny-e8-v1 units through the snapshot's
// FX, rounding UP — a bound never rounds toward zero — and fails closed when
// there is no rate to convert with (a CNY user whose freeze saw an FX outage:
// treating USD as CNY would under-bound ~7×). Subscription billing has no FX.
func unitsRoundUp(costUSD float64, snap *BillingSnapshot) (int64, error) {
	if costUSD <= 0 {
		return 0, nil
	}
	rate := snap.FX.Rate
	if snap.Flags.SubscriptionBilling {
		rate = 1
	}
	if rate <= 0 {
		return 0, ErrEstimateUnbounded
	}
	units := math.Ceil(costUSD * rate * canonicalWalletUnitsPerCNY) // canonical_wallet_units.go:11 — the constant canonicalWalletUnitsFromCNY uses; never redeclare 1e8
	if math.IsNaN(units) || math.IsInf(units, 0) || units > float64(math.MaxInt64) {
		return 0, errors.New("billing estimate overflows int64")
	}
	return int64(units), nil
}
