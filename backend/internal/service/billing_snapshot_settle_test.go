//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func freezeForSettleTest(t *testing.T, family BillingFamily, model string) (*BillingSnapshotService, *BillingSnapshot, *APIKey, *User, *Account) {
	t.Helper()
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: model, BillingModel: model, Family: family,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	return svc, snap, apiKey, user, account
}

// Token mode: the snapshot path must reproduce CalculateCostUnified exactly
// for every service tier and long-context combination.
func TestCalculateCostFromSnapshotMatchesLivePathTokenMode(t *testing.T) {
	svc, snap, apiKey, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	gid := apiKey.Group.ID
	for _, tc := range []struct {
		name   string
		tokens UsageTokens
		tier   string
	}{
		{"plain", UsageTokens{InputTokens: 1000, OutputTokens: 500}, ""},
		{"cache", UsageTokens{InputTokens: 1000, OutputTokens: 500, CacheCreationTokens: 200, CacheReadTokens: 300}, ""},
		{"priority", UsageTokens{InputTokens: 1000, OutputTokens: 500}, "priority"},
		{"flex", UsageTokens{InputTokens: 1000, OutputTokens: 500}, "flex"},
		{"large-input", UsageTokens{InputTokens: 300000, OutputTokens: 1000}, ""}, // NOT a long-context case: claude-sonnet-4's fallback entry carries no threshold and applyModelSpecificPricingPolicy skips it (:1198-1203); long-context is covered by the drift table's row
	} {
		t.Run(tc.name, func(t *testing.T) {
			live, err := svc.billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-sonnet-4", GroupID: &gid, Tokens: tc.tokens, RequestCount: 1, RateMultiplier: snap.Multipliers.Text, ServiceTier: tc.tier, Resolver: svc.resolver})
			require.NoError(t, err)
			got, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tc.tokens, ServiceTier: tc.tier})
			require.NoError(t, err)
			require.Equal(t, live.ActualCost, got.ActualCost)
			require.Equal(t, live.TotalCost, got.TotalCost)
			require.Equal(t, live.LongContextBillingApplied, got.LongContextBillingApplied)
			// BillingMode is NOT compared here: CalculateCostUnified stamps "token"
			// (billing_service.go:960-965) while the generic family's real seam leaves
			// it empty — see TestCalculateCostFromSnapshotStampsBillingModePerFamily.
		})
	}
}

// BillingMode per family, pinned against the two live seams: the generic
// non-channel path is calculateTokenCost → CalculateCost/CalculateCostWithLongContext
// (gateway_usage_billing.go:1013-1027), which never stamps it; the OpenAI path
// goes through CalculateCostUnified (openai_gateway_usage.go:490-497), which
// stamps "token". settle mode must not flip usage_logs.billing_mode.
func TestCalculateCostFromSnapshotStampsBillingModePerFamily(t *testing.T) {
	in := SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: 1000, OutputTokens: 100}}
	svc, generic, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	got, err := svc.billing.CalculateCostFromSnapshot(generic, in)
	require.NoError(t, err)
	require.Equal(t, "", got.BillingMode)
	plain, err := svc.billing.CalculateCost("claude-sonnet-4", in.Tokens, generic.Multipliers.Text)
	require.NoError(t, err)
	require.Equal(t, plain.BillingMode, got.BillingMode)
	svc, openai, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "claude-sonnet-4")
	got, err = svc.billing.CalculateCostFromSnapshot(openai, in)
	require.NoError(t, err)
	require.Equal(t, string(BillingModeToken), got.BillingMode)
}

// The exit condition: mutate each pricing-input class between freeze and
// settlement. Every row asserts BOTH halves — the live path moved (so the
// mutation actually bites; a row whose live cost does not move proves
// nothing) and the snapshot path did not — and runs the snapshot half through
// applyBillingSnapshotToSettlement in settle mode (Task 5), the real
// settlement seam, not CalculateCostFromSnapshot alone.
type driftFixture struct {
	svc     *BillingSnapshotService
	apiKey  *APIKey
	user    *User
	account *Account
	sub     *UserSubscription
}

func (f *driftFixture) freeze(t *testing.T) *BillingSnapshot {
	t.Helper()
	snap, err := f.svc.Freeze(context.Background(), FreezeInput{APIKey: f.apiKey, User: f.user, Account: f.account, Subscription: f.sub, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	return snap
}

// liveCost is what recordUsageCore computes today for this fixture: the
// multiplier re-resolved now, the resolver consulted now, group media prices
// read now (gateway_usage_billing.go:716-780, :868-873).
func (f *driftFixture) liveCost(t *testing.T, in SnapshotSettlementInput) *CostBreakdown {
	t.Helper()
	currency := CurrencyUSD
	if f.sub == nil {
		currency = NormalizeUserBillingCurrency(f.user.BillingCurrency)
	}
	base := f.apiKey.Group.RateMultiplierForCurrency(currency) // group.go:113 — CNY reads RateMultiplierCNY when set
	text, image := computePeakAwareMultipliers(f.apiKey, base, f.svc.now())
	if in.ImageCount > 0 {
		// The REAL live helper (gateway_usage_billing.go:950-986), never a
		// re-implementation: it normalizes the tier, prefers a configured group
		// price, then channel pricing through CalculateCostUnified, then the
		// default per-image price. It reads only s.billingService and s.resolver
		// (`billingService` at gateway_service.go:691, `resolver` at :711).
		gw := &GatewayService{billingService: f.svc.billing, resolver: f.svc.resolver}
		cost, err := gw.calculateImageCost(context.Background(), &ForwardResult{ImageCount: in.ImageCount, ImageSize: in.ImageSize}, f.apiKey, "claude-sonnet-4", image)
		require.NoError(t, err)
		return cost
	}
	gid := f.apiKey.Group.ID
	cost, err := f.svc.billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-sonnet-4", GroupID: &gid, Tokens: in.Tokens, RequestCount: 1, RateMultiplier: text, ServiceTier: in.ServiceTier, Resolver: f.svc.resolver})
	require.NoError(t, err)
	return cost
}

func TestCalculateCostFromSnapshotIsImmuneToEveryDriftClass(t *testing.T) {
	tokens := UsageTokens{InputTokens: 10000, OutputTokens: 2000, CacheReadTokens: 500}
	rows := []struct {
		name   string
		in     SnapshotSettlementInput
		setup  func(f *driftFixture) // before freeze (nil = none)
		mutate func(f *driftFixture) // after freeze, before settlement
	}{
		{"model/default price", SnapshotSettlementInput{Tokens: tokens}, nil,
			func(f *driftFixture) { f.svc.billing.fallbackPrices["claude-sonnet-4"].InputPricePerToken *= 10 }},
		// usePriorityServiceTierPricing flips as soon as any *Priority price is > 0
		// (billing_service.go:124-129): before the mutation "priority" costs 2× via
		// serviceTierCostMultiplier, after it the priority price is used.
		{"service-tier policy", SnapshotSettlementInput{Tokens: tokens, ServiceTier: "priority"}, nil,
			func(f *driftFixture) { f.svc.billing.fallbackPrices["claude-sonnet-4"].InputPricePerTokenPriority = 1e-3 }},
		{"user/group multiplier", SnapshotSettlementInput{Tokens: tokens}, nil,
			func(f *driftFixture) { f.apiKey.Group.RateMultiplierCNY = floatPtr(4.5) }}, // the CNY user's multiplier source (group.go:113-125)
		// PeakMultiplierAt applies only to subscription groups (group.go:271), so
		// this row freezes under subscription billing.
		{"peak-hour multiplier", SnapshotSettlementInput{Tokens: tokens},
			func(f *driftFixture) { f.apiKey.Group.SubscriptionType = SubscriptionTypeSubscription; f.sub = &UserSubscription{ID: 5} },
			func(f *driftFixture) {
				g := f.apiKey.Group
				g.PeakRateEnabled, g.PeakRateMultiplier, g.PeakStart, g.PeakEnd = true, 5, "00:00", "23:59" // group.go:27-30
			}},
		// shouldApplySessionLongContextPricing needs threshold > 0 AND a multiplier > 1 (billing_service.go:1234-1244).
		{"long-context threshold/multiplier", SnapshotSettlementInput{Tokens: tokens}, nil,
			func(f *driftFixture) {
				p := f.svc.billing.fallbackPrices["claude-sonnet-4"]
				p.LongContextInputThreshold, p.LongContextInputMultiplier, p.LongContextOutputMultiplier = 1, 2, 2
			}},
		{"image group config / size tier", SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K"}, nil,
			func(f *driftFixture) { f.apiKey.Group.ImagePrice1K = floatPtr(99) }},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			svc, apiKey, user, account := newSnapshotTestFixture(t)
			f := &driftFixture{svc: svc, apiKey: apiKey, user: user, account: account}
			if row.setup != nil {
				row.setup(f)
			}
			snap := f.freeze(t)
			liveBefore := f.liveCost(t, row.in)
			snapBefore, err := svc.billing.CalculateCostFromSnapshot(snap, row.in)
			require.NoError(t, err)
			require.InDelta(t, liveBefore.ActualCost, snapBefore.ActualCost, 1e-12, "before any drift the two paths agree")

			row.mutate(f)

			liveAfter := f.liveCost(t, row.in)
			require.NotEqual(t, liveBefore.ActualCost, liveAfter.ActualCost, "the %s mutation must move the live path, or this row proves nothing", row.name)
			settler := &billingSnapshotSettler{snapshots: svc, billing: svc.billing}
			svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
			settled, _ := settler.applyBillingSnapshotToSettlement(context.Background(), snap, liveAfter, row.in, "claude-sonnet-4")
			require.Equal(t, snapBefore.ActualCost, settled.ActualCost, "snapshot settlement must be immune to %s", row.name)
		})
	}
}

// The account multiplier is not a drift class on the user's cost:
// Account.BillingRateMultiplier() is recorded on the usage log
// (account_rate_multiplier) and never applied to ActualCost. The snapshot
// freezes it so the LOG is stable; that is what is asserted.
func TestBillingSnapshotFreezesAccountMultiplierForTheLog(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	f := &driftFixture{svc: svc, apiKey: apiKey, user: user, account: account}
	snap := f.freeze(t)
	v := 9.0
	account.RateMultiplier = &v
	require.Equal(t, 1.25, snap.Multipliers.Account)
}

// Channel price — the resolver's first source. Seeded through
// mockChannelRepository exactly as newResolverWithChannel does
// (model_pricing_resolver_test.go:216-236), bound to the fixture's group 7;
// field names per ChannelModelPricing (channel.go:85-101). The channel table
// is cached for 10 minutes, so the mutation must invalidateCache().
func TestCalculateCostFromSnapshotIsImmuneToChannelPriceDrift(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	inPrice, outPrice := 4e-6, 20e-6
	seed := []ChannelModelPricing{{ID: 1, ChannelID: 1, Platform: "anthropic", Models: []string{"claude-sonnet-4"}, BillingMode: BillingModeToken, InputPrice: &inPrice, OutputPrice: &outPrice}}
	cs := newChannelServiceForTest(7, seed)
	svc.resolver = NewModelPricingResolver(cs, svc.billing)
	f := &driftFixture{svc: svc, apiKey: apiKey, user: user, account: account}
	snap := f.freeze(t)
	require.Equal(t, PricingSourceChannel, snap.Pricing.Source, "the fixture must actually resolve channel pricing, or this is the token-mode test again")
	in := SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: 10000, OutputTokens: 2000}}
	before, err := svc.billing.CalculateCostFromSnapshot(snap, in)
	require.NoError(t, err)
	liveBefore := f.liveCost(t, in)
	require.InDelta(t, liveBefore.ActualCost, before.ActualCost, 1e-12)

	inPrice *= 10 // the seeded pointer — the snapshot must hold its own copy (Freeze deep-copies every *float64)
	cs.invalidateCache()

	liveAfter := f.liveCost(t, in)
	require.NotEqual(t, liveBefore.ActualCost, liveAfter.ActualCost, "the channel price mutation must move the live path")
	settler := &billingSnapshotSettler{snapshots: svc, billing: svc.billing}
	svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	settled, _ := settler.applyBillingSnapshotToSettlement(context.Background(), snap, liveAfter, in, "claude-sonnet-4")
	require.Equal(t, before.ActualCost, settled.ActualCost)
}

// The video branch mirrors calculateOpenAIVideoCost's normalization
// (openai_gateway_usage.go:558-600): a zero count bills as one, resolution and
// duration are normalized before the price lookup.
func TestCalculateCostFromSnapshotVideoBranchNormalizesLikeLive(t *testing.T) {
	svc, snap, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "claude-sonnet-4")
	// The dispatch gate is `VideoCount > 0 && GrokVideo` — the live predicate
	// isGrokVideoUsageResult returns false for VideoCount <= 0 (openai_gateway_usage.go:461-463),
	// so a zero-count result never reaches the video helper on either path. Do NOT
	// weaken the gate to make a zero-count assertion pass; the helper's own ≥1 clamp
	// is exercised by the duration path below.
	got, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{VideoCount: 1, VideoResolution: "720p", VideoDurationSeconds: 0, GrokVideo: true})
	require.NoError(t, err)
	want := svc.billing.CalculateVideoCost("claude-sonnet-4", NormalizeVideoBillingResolutionOrDefault("720p"), 1, NormalizeVideoBillingDurationSecondsOrDefault(0), snap.Media.VideoPrice, snap.Multipliers.Video)
	require.Equal(t, want.ActualCost, got.ActualCost, "default duration")
	got, err = svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{VideoCount: 2, VideoResolution: "720p", VideoDurationSeconds: 30, GrokVideo: true})
	require.NoError(t, err)
	want = svc.billing.CalculateVideoCost("claude-sonnet-4", NormalizeVideoBillingResolutionOrDefault("720p"), 2, NormalizeVideoBillingDurationSecondsOrDefault(30), snap.Media.VideoPrice, snap.Multipliers.Video)
	require.Equal(t, want.ActualCost, got.ActualCost, "duration clamped to the upstream maximum (video_billing_resolution.go:20-31)")
}

// Cache-input classification. The live path rewrites result.Usage IN PLACE
// through applyCacheTTLOverride(&usage, target) (gateway_upstream_response.go:1268;
// applied at gateway_usage_billing.go:736-740) with a target resolved at
// record time; in settle mode the SNAPSHOT's frozen target is applied to the
// raw usage instead (snapshotSettlementInputFromClaudeUsage, Task 5). The
// fixture's fallback entry has no 5m/1h prices, so give it distinct ones.
func TestCalculateCostFromSnapshotIsImmuneToCacheTTLOverrideDrift(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	p := svc.billing.fallbackPrices["claude-sonnet-4"]
	p.SupportsCacheBreakdown, p.CacheCreation5mPrice, p.CacheCreation1hPrice = true, 3.75e-6, 6e-6
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric, CacheTTLOverride: cacheTTLOverride{Enabled: true, Target: "5m"},
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	raw := ClaudeUsage{InputTokens: 1000, OutputTokens: 100, CacheCreationInputTokens: 4000, CacheCreation1hTokens: 4000}

	fromSnapshot := snapshotSettlementInputFromClaudeUsage(raw, snapshotCacheTTLOverride(snap), 0, "") // the snapshot's (enabled, "5m")
	driftedLive := raw
	applyCacheTTLOverride(&driftedLive, "1h") // the account's target drifted to 1h after the freeze
	fromLive := snapshotSettlementInputFromClaudeUsage(driftedLive, cacheTTLOverride{}, 0, "")

	a, err := svc.billing.CalculateCostFromSnapshot(snap, fromSnapshot)
	require.NoError(t, err)
	b, err := svc.billing.CalculateCostFromSnapshot(snap, fromLive)
	require.NoError(t, err)
	require.NotEqual(t, a.ActualCost, b.ActualCost, "5m and 1h must price differently for this fixture, or the row is vacuous")
	settler := &billingSnapshotSettler{snapshots: svc, billing: svc.billing}
	svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	settled, _ := settler.applyBillingSnapshotToSettlement(context.Background(), snap, b, fromSnapshot, "claude-sonnet-4")
	require.Equal(t, a.ActualCost, settled.ActualCost, "settle mode classifies cache input by the frozen target")
}

func TestCalculateCostFromSnapshotFXIsPinnedFromSnapshot(t *testing.T) {
	svc, snap, _, user, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	snap.FX.Rate = 6.25
	cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: 1000}})
	require.NoError(t, err)
	settlement, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), cost, user, false, svc.exchangeRates, svc.cfg)
	require.NoError(t, err)
	require.Equal(t, 6.25, settlement.ExchangeRate)
}

func TestCalculateCostFromSnapshotWebSearchAndImageModes(t *testing.T) {
	svc, snap, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "claude-sonnet-4")
	ws, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{WebSearchCalls: 3})
	require.NoError(t, err)
	require.InDelta(t, 3*0.02*snap.Multipliers.WebSearch, ws.ActualCost, 1e-12)
	img, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K"})
	require.NoError(t, err)
	live := svc.billing.CalculateImageCost("claude-sonnet-4", "1K", 2, snap.Media.ImagePrice, snap.Multipliers.Image)
	require.Equal(t, live.ActualCost, img.ActualCost)
}

func TestCalculateCostFromSnapshotGeminiLongContextOpts(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric, LongContextThreshold: 200000, LongContextMultiplier: 2.0,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	tokens := UsageTokens{InputTokens: 250000, OutputTokens: 100}
	live, err := svc.billing.CalculateCostWithLongContext("claude-sonnet-4", tokens, snap.Multipliers.Text, 200000, 2.0)
	require.NoError(t, err)
	got, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens})
	require.NoError(t, err)
	require.Equal(t, live.ActualCost, got.ActualCost)
}

// The extracted long-context core must reproduce the public function exactly.
func TestCalculateCostWithLongContextPricingMatchesPublicFunction(t *testing.T) {
	svc := newTestBillingService()
	pricing, err := svc.GetModelPricing("claude-sonnet-4")
	require.NoError(t, err)
	for _, tokens := range []UsageTokens{
		{InputTokens: 250000, OutputTokens: 100},
		{InputTokens: 150000, CacheReadTokens: 120000, OutputTokens: 5000},
		{InputTokens: 10, OutputTokens: 10}, // below threshold: public function short-circuits to CalculateCost
	} {
		public, err := svc.CalculateCostWithLongContext("claude-sonnet-4", tokens, 1.5, 200000, 2.0)
		require.NoError(t, err)
		core, err := svc.calculateCostWithLongContextPricing(pricing, tokens, 1.5, 200000, 2.0)
		require.NoError(t, err)
		if tokens.InputTokens+tokens.CacheReadTokens <= 200000 {
			continue // the core is only reached above the threshold
		}
		require.Equal(t, public.ActualCost, core.ActualCost)
		require.Equal(t, public.TotalCost, core.TotalCost)
		require.Equal(t, public.BillingMode, core.BillingMode)
	}
}
