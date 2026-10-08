//go:build unit

package service

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func estimatorSnapshot(t *testing.T) (*BillingSnapshotService, *BillingSnapshot) {
	t.Helper()
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	p := svc.billing.fallbackPrices["claude-sonnet-4"]
	p.MaxInputTokens, p.MaxOutputTokens = 200000, 64000
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyOpenAI,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	return svc, snap
}

// settledUnitsForTest is exactly what observeCanonicalWalletSettlement does:
// ResolveCostSettlement rewrites cost.ActualCost into CNY IN PLACE
// (billing_settlement.go:166-175; with fx == nil it errors before consulting
// the pin, :143-152, so the fixture's real service is passed), then
// canonicalWalletUnitsFromUSD(cost.ActualCost) (declared canonical_wallet_bridge.go:818, called at :850).
// settlement.BaseCost × ExchangeRate would apply FX twice and drop the rate
// multiplier — never compute it that way.
func settledUnitsForTest(t *testing.T, svc *BillingSnapshotService, snap *BillingSnapshot, cost *CostBreakdown) int64 {
	t.Helper()
	_, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), cost, &User{BillingCurrency: "CNY"}, false, svc.exchangeRates, svc.cfg)
	require.NoError(t, err)
	units, err := canonicalWalletUnitsFromUSD(cost.ActualCost)
	require.NoError(t, err)
	return units
}

// Property: for any usage within the declared bounds, settled units <= estimated units.
func TestEstimateUpperBoundDominatesSettlementForBoundedAttempts(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 2000; i++ {
		inBound := rng.Intn(250000) + 1 // above the fixture's 200000-token window a fifth of the time: the caller's bound is never clamped
		outBound := rng.Intn(60000) + 1
		tier := []string{"", "priority", "flex"}[rng.Intn(3)]
		est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: inBound, MaxOutputTokens: outBound, ServiceTier: tier})
		require.NoError(t, err)
		in := rng.Intn(inBound + 1)
		cr := rng.Intn(inBound - in + 1)
		cc := inBound - in - cr
		out := rng.Intn(outBound + 1)
		tokens := UsageTokens{InputTokens: in, CacheReadTokens: cr, CacheCreationTokens: cc, OutputTokens: out, ImageOutputTokens: rng.Intn(out + 1)}
		cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens, ServiceTier: tier})
		require.NoError(t, err)
		settled := settledUnitsForTest(t, svc, snap, cost)
		require.LessOrEqual(t, settled, est, "iteration %d: settled %d > estimate %d for %+v tier=%q", i, settled, est, tokens, tier)
	}
}

// The same property once the pricing entry HAS priority prices and cache
// breakdown prices — the branch computeTokenBreakdown takes then is different
// (billing_service.go:1009 onward), and dearest-input must cover
// CacheReadPricePerToken and CacheCreation5mPrice too (:1084, :1105-1117).
func TestEstimateUpperBoundDominatesWithPriorityAndCachePrices(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	b := snap.Pricing.Base
	b.InputPricePerTokenPriority, b.OutputPricePerTokenPriority, b.CacheReadPricePerTokenPriority = 6e-6, 30e-6, 0.6e-6
	b.SupportsCacheBreakdown, b.CacheCreation5mPrice, b.CacheCreation1hPrice, b.CacheReadPricePerToken = true, 3.75e-6, 6e-6, 9e-6 // cache read deliberately dearer than input
	b.ImageOutputPricePerToken, b.ImageOutputPriceExplicit = 40e-6, true                                                           // image output dearer than text output
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 1000; i++ {
		inBound := rng.Intn(50000) + 1
		outBound := rng.Intn(5000) + 1
		tier := []string{"", "priority", "flex"}[rng.Intn(3)]
		est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: inBound, MaxOutputTokens: outBound, ServiceTier: tier})
		require.NoError(t, err)
		in := rng.Intn(inBound + 1)
		cr := rng.Intn(inBound - in + 1)
		cc := inBound - in - cr
		c5 := rng.Intn(cc + 1)
		out := rng.Intn(outBound + 1)
		tokens := UsageTokens{InputTokens: in, CacheReadTokens: cr, CacheCreationTokens: cc, CacheCreation5mTokens: c5, CacheCreation1hTokens: cc - c5, OutputTokens: out, ImageOutputTokens: rng.Intn(out + 1)}
		cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens, ServiceTier: tier})
		require.NoError(t, err)
		settled := settledUnitsForTest(t, svc, snap, cost)
		require.LessOrEqual(t, settled, est, "iteration %d: settled %d > estimate %d for %+v tier=%q", i, settled, est, tokens, tier)
	}
}

// Gemini long-context opts (freeze row 9: threshold 200000, multiplier 2.0) —
// the settle path doubles the out-of-range slice; the bound must cover it.
func TestEstimateUpperBoundDominatesGeminiLongContextSettlement(t *testing.T) {
	// The GENERIC family (freeze row 9 uses the generic facade): applyLongCtx is
	// true on both sides only there, and the fallback entry gets a real threshold
	// so the pricing-level long-context branch (billing_service.go:1234-1244) fires
	// as well as the Gemini-opts branch — estimatorSnapshot's OpenAI-family snapshot
	// would make both sides no-op and prove nothing.
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	p := svc.billing.fallbackPrices["claude-sonnet-4"]
	p.MaxInputTokens, p.MaxOutputTokens = 200000, 64000
	p.LongContextInputThreshold, p.LongContextInputMultiplier, p.LongContextOutputMultiplier = 100000, 2.0, 2.0
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric, LongContextThreshold: 100000, LongContextMultiplier: 2.0,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 500; i++ {
		inBound := rng.Intn(200000) + 1
		outBound := rng.Intn(5000) + 1
		est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: inBound, MaxOutputTokens: outBound})
		require.NoError(t, err)
		in := rng.Intn(inBound + 1)
		cc := rng.Intn(inBound - in + 1) // cache-creation tokens ride the in-range slice whole (billing_service.go:1287-1300), so the pricing-level multiplier can fire on top of the split — the one double-application path
		tokens := UsageTokens{InputTokens: in, CacheReadTokens: inBound - in - cc, CacheCreationTokens: cc, OutputTokens: rng.Intn(outBound + 1)}
		cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens})
		require.NoError(t, err)
		settled := settledUnitsForTest(t, svc, snap, cost)
		require.LessOrEqual(t, settled, est, "iteration %d: %+v", i, tokens)
	}
}

// The web-search and per-request branches, each against the settle path.
func TestEstimateUpperBoundWebSearchAndPerRequestBranches(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{WebSearchCalls: 3})
	require.NoError(t, err)
	cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{WebSearchCalls: 3})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est)

	// per-request: no ImageCount, or the image branch would take it first
	perReq := *snap
	perReq.Pricing.Mode, perReq.Pricing.DefaultPerRequestPrice, perReq.Pricing.Source = BillingModePerRequest, 0.01, PricingSourceChannel
	est, err = svc.EstimateUpperBoundUnits(&perReq, EstimateInput{InputTokensUpperBound: 10, MaxOutputTokens: 10})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(&perReq, SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: 10, OutputTokens: 10}})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, &perReq, cost), est)

	// image-mode channel pricing with an image count: the image branch, through the settle arithmetic
	img := *snap
	img.Pricing.Mode, img.Pricing.DefaultPerRequestPrice, img.Pricing.Source = BillingModeImage, 0.05, PricingSourceChannel
	est, err = svc.EstimateUpperBoundUnits(&img, EstimateInput{ImageCount: 2, ImageSize: "1K", MaxOutputTokens: 10})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(&img, SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K"})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, &img, cost), est)
}

// Decreasing channel interval pricing (a volume discount) and a zero-token
// settlement — both land in a DEARER interval than the one at the bound.
func TestEstimateUpperBoundDominatesDecreasingIntervalPricing(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	hi, lo, out := 10e-6, 1e-6, 20e-6
	tenK := 10000
	seed := []ChannelModelPricing{{ID: 1, ChannelID: 1, Platform: "anthropic", Models: []string{"claude-sonnet-4"}, BillingMode: BillingModeToken, InputPrice: &lo, OutputPrice: &out,
		Intervals: []PricingInterval{{MinTokens: 0, MaxTokens: &tenK, InputPrice: &hi, OutputPrice: &out}, {MinTokens: 10000, InputPrice: &lo, OutputPrice: &out}}}} // PricingInterval fields: channel.go:104-119
	// A second seed whose intervals do NOT cover the bound ([0,10k) only, no tail):
	// a 50k-token settlement falls back to BasePricing — priority-less — and takes
	// the 2× tier multiplier, where the interval carries priority prices at 1×.
	gapped := []ChannelModelPricing{{ID: 1, ChannelID: 1, Platform: "anthropic", Models: []string{"claude-sonnet-4"}, BillingMode: BillingModeToken, InputPrice: &lo, OutputPrice: &out,
		Intervals: []PricingInterval{{MinTokens: 0, MaxTokens: &tenK, InputPrice: &hi, OutputPrice: &out}}}}
	for name, s := range map[string][]ChannelModelPricing{"decreasing": seed, "gapped": gapped} {
		svc.resolver = NewModelPricingResolver(newChannelServiceForTest(7, s), svc.billing)
		snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyOpenAI,
			ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
		require.NoError(t, err)
		require.Equal(t, PricingSourceChannel, snap.Pricing.Source, name)
		require.Len(t, snap.Pricing.Intervals, len(s[0].Intervals), "%s: the intervals must survive the freeze (filterValidIntervals, model_pricing_resolver.go:229, keeps an interval with any price set), or this test is the flat-price test again", name)
		for _, tier := range []string{"", "priority", "flex"} {
			est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 50000, MaxOutputTokens: 100, ServiceTier: tier})
			require.NoError(t, err)
			for _, tokens := range []UsageTokens{{InputTokens: 5000, OutputTokens: 100}, {InputTokens: 0, OutputTokens: 0}, {InputTokens: 50000, OutputTokens: 100}, {InputTokens: 9999, CacheReadTokens: 1, OutputTokens: 100}} {
				cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens, ServiceTier: tier})
				require.NoError(t, err)
				require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est, "%s tier=%q %+v", name, tier, tokens)
			}
		}
	}
}

// The image branch reads s.pricingService for default per-image prices
// (billing_service.go:1551-1562) — a zero BillingService would silently
// under-bound, which is why the estimator is a method on the real service.
func TestEstimateUpperBoundDominatesImageSettlement(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	in := SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K"}
	est, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{ImageCount: 2, ImageSize: "1K"})
	require.NoError(t, err)
	cost, err := svc.billing.CalculateCostFromSnapshot(snap, in)
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est)

	// A cheap CONFIGURED tier must not hide an expensive DEFAULT tier: settlement
	// at an unconfigured tier goes through getDefaultImagePrice (billing_service.go:1553-1581).
	snap.Media.ImagePrice = &ImagePriceConfig{Price4K: floatPtr(0.001)}
	est, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{ImageCount: 1, ImageSize: "4K"})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{ImageCount: 1, ImageSize: "2K"})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est, "the default 2K price must be inside the bound")
	grok := *snap
	grok.BillingModel = "grok-imagine-video-1.5" // a model with real default video prices (billing_service.go:1627-1652)
	grok.Media.VideoPrice = &VideoPriceConfig{Price1080P: floatPtr(0.01)}
	est, err = svc.EstimateUpperBoundUnits(&grok, EstimateInput{VideoCount: 1, VideoResolution: "1080p", VideoDurationSeconds: 15, GrokVideo: true})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(&grok, SnapshotSettlementInput{VideoCount: 1, VideoResolution: "480p", VideoDurationSeconds: 15, GrokVideo: true})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, &grok, cost), est, "the default 480p price must be inside the bound")

	// A group with only Price4K configured, a request that said 2K (or nothing),
	// an upstream that returned 4K: the bound must cover the settled tier.
	snap.Media.ImagePrice = &ImagePriceConfig{Price4K: floatPtr(0.20)}
	est, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{ImageCount: 2, ImageSize: "2K"})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{ImageCount: 2, ImageSize: "4K"})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est)
	// and video: resolution/duration the caller guessed low
	snap.Media.VideoPrice = &VideoPriceConfig{Price1080P: floatPtr(0.50)}
	est, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{VideoCount: 1, VideoResolution: "480p", VideoDurationSeconds: 5, GrokVideo: true})
	require.NoError(t, err)
	cost, err = svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{VideoCount: 1, VideoResolution: "1080p", VideoDurationSeconds: 15, GrokVideo: true})
	require.NoError(t, err)
	require.LessOrEqual(t, settledUnitsForTest(t, svc, snap, cost), est)
}

func TestEstimateUpperBoundRefusesUnboundedOutput(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	snap.Pricing.MaxOutputTokens = 0
	_, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 10})
	require.ErrorIs(t, err, ErrEstimateUnbounded)
	_, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 10, MaxOutputTokens: 100})
	require.NoError(t, err, "a request-supplied max_output_tokens bounds it")
}

func TestEstimateUpperBoundContinuationKinds(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	none, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1000, MaxOutputTokens: 100})
	require.NoError(t, err)
	warm, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1000, MaxOutputTokens: 100, Continuation: ContinuationWarm, PriorTurnInputTokens: 50000, PriorTurnOutputTokens: 5000})
	require.NoError(t, err)
	cold, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1000, MaxOutputTokens: 100, Continuation: ContinuationCold})
	require.NoError(t, err)
	require.Less(t, none, warm)
	require.Less(t, warm, cold, "cold is bounded by the full context window")
	snap.Pricing.MaxInputTokens = 0
	_, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1000, MaxOutputTokens: 100, Continuation: ContinuationCold})
	require.ErrorIs(t, err, ErrEstimateUnbounded, "cold continuation without a known context window is unbounded")
}

func TestEstimateUpperBoundRoundsUpAndFailsClosed(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	tiny, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1, MaxOutputTokens: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, tiny, int64(1), "a non-zero cost never estimates to zero units")

	noFX := *snap
	noFX.FX.Rate = 0 // a CNY user whose freeze saw an FX outage: USD-as-CNY would under-bound ~7×
	_, err = svc.EstimateUpperBoundUnits(&noFX, EstimateInput{InputTokensUpperBound: 1, MaxOutputTokens: 1})
	require.NoError(t, err)

	snap.Pricing.Base = nil
	_, err = svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: 1, MaxOutputTokens: 1})
	require.ErrorIs(t, err, ErrModelPricingUnavailable)
}

func TestClassifyWSContinuation(t *testing.T) {
	require.Equal(t, ContinuationNone, ClassifyWSContinuation([]byte(`{"model":"m","input":"hi"}`), false))
	require.Equal(t, ContinuationNone, ClassifyWSContinuation([]byte(`{"model":"m","input":"hi","store":false}`), false))
	require.Equal(t, ContinuationWarm, ClassifyWSContinuation([]byte(`{"model":"m","previous_response_id":"resp_1"}`), true))
	require.Equal(t, ContinuationCold, ClassifyWSContinuation([]byte(`{"model":"m","previous_response_id":"resp_1"}`), false))
	require.Equal(t, ContinuationNone, ClassifyWSContinuation([]byte(`{"model":"m","previous_response_id":""}`), true))
	require.Equal(t, ContinuationNone, ClassifyWSContinuation([]byte(`{"model":"m","input":"hi","store":true}`), false), "store on without previous_response_id: this turn's input is its own payload")
}
