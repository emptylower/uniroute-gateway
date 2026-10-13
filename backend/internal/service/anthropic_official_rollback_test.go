//go:build unit

package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestAnthropicRollbackLinkedMode(t *testing.T) {
	want := os.Getenv("SEO_DAY2_EXPECT_BASELINE_MODE")
	if want == "" {
		want = "enabled"
	}
	require.Equal(t, want, anthropicOfficialBaselineMode, "link-time writer mode must match the artifact recipe")
}

func withOfficialBaselineMode(t *testing.T, mode string) {
	t.Helper()
	previous := anthropicOfficialBaselineMode
	anthropicOfficialBaselineMode = mode
	t.Cleanup(func() { anthropicOfficialBaselineMode = previous })
}

func TestAnthropicRollbackProducerRestoresDynamicFallbackAndChannelZero(t *testing.T) {
	withOfficialBaselineMode(t, "disabled")
	loaded := &LiteLLMModelPricing{InputCostPerToken: 9e-6, OutputCostPerToken: 45e-6, CacheCreationInputTokenCost: 11.25e-6, CacheCreationInputTokenCostAbove1hr: 18e-6, CacheReadInputTokenCost: .9e-6}
	billing := NewBillingService(&config.Config{}, &PricingService{pricingData: map[string]*LiteLLMModelPricing{"claude-sonnet-5-5": loaded}})
	price, err := billing.GetModelPricing("claude-sonnet-5-5")
	require.NoError(t, err)
	require.Equal(t, 9e-6, price.InputPricePerToken)
	require.Equal(t, 18e-6, price.CacheCreation1hPrice)
	require.Nil(t, price.PreservedPriorityPricing)
	resolver := NewModelPricingResolver(nil, billing)
	require.Equal(t, PricingSourceLiteLLM, resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-5-5"}).Source)

	// Missing mirror rows follow the original fallback/error path, including
	// known official IDs; rollback cannot return a nil price with nil error.
	legacy := NewBillingService(&config.Config{}, nil)
	fallback, err := legacy.GetModelPricing("claude-sonnet-4-5-20250929")
	require.NoError(t, err)
	require.Equal(t, legacy.getFallbackPricing("claude-sonnet-4-5-20250929"), fallback)
	legacy.fallbackPrices = map[string]*ModelPricing{}
	missing, err := legacy.GetModelPricing("claude-sonnet-5-5")
	require.ErrorIs(t, err, ErrModelPricingUnavailable)
	require.Nil(t, missing)

	zero := 0.0
	gid := int64(7)
	resolver = NewModelPricingResolver(newChannelServiceForTest(gid, []ChannelModelPricing{{Platform: PlatformAnthropic, Models: []string{"claude-sonnet-5-5"}, BillingMode: BillingModeToken, InputPrice: &zero, OutputPrice: &zero, CacheReadPrice: &zero, CacheWritePrice: &zero}}), billing)
	cost, err := billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-sonnet-5-5", GroupID: &gid, Tokens: UsageTokens{InputTokens: 100, OutputTokens: 100, CacheReadTokens: 100, CacheCreationTokens: 100, CacheCreation1hTokens: 100}, RateMultiplier: 1, Resolver: resolver})
	require.NoError(t, err)
	require.Zero(t, cost.ActualCost)
	require.Equal(t, PricingSourceChannel, resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-5-5", GroupID: &gid}).Source)

	public := NewPublicModelPriceSnapshot(nil, time.Now())
	require.Empty(t, public.OfficialSource)
	require.True(t, public.OfficialVerifiedAt.IsZero())
	require.Empty(t, public.OfficialSnapshotVersion)
}

func TestAnthropicRollbackReaderReplaysReleaseFrozenPriorityAndReservation(t *testing.T) {
	withOfficialBaselineMode(t, "enabled")
	loaded := &LiteLLMModelPricing{InputCostPerToken: 9e-6, OutputCostPerToken: 45e-6, CacheCreationInputTokenCost: 11.25e-6, CacheReadInputTokenCost: .9e-6, InputCostPerTokenPriority: 4e-6, OutputCostPerTokenPriority: 20e-6, CacheCreationInputTokenCostPriority: 5e-6, CacheReadInputTokenCostPriority: .2e-6, LongContextInputTokenThreshold: 200000, LongContextInputCostMultiplier: 2, LongContextOutputCostMultiplier: 1.5, MaxInputTokens: 1000000, MaxOutputTokens: 128000}
	billing := NewBillingService(&config.Config{}, &PricingService{pricingData: map[string]*LiteLLMModelPricing{"claude-sonnet-5-5": loaded}})
	price, err := billing.GetModelPricing("claude-sonnet-5-5")
	require.NoError(t, err)
	require.NotNil(t, price.PreservedPriorityPricing)
	svc, snapshot := estimatorSnapshot(t)
	svc.billing = billing
	snapshot.Family = BillingFamilyGeneric
	snapshot.BillingModel = "claude-sonnet-5-5"
	snapshot.Pricing = BillingSnapshotPricing{Mode: BillingModeToken, Source: PricingSourceOfficial, Base: clonePricing(price), SupportsCacheBreakdown: true, MaxInputTokens: 1000000, MaxOutputTokens: 128000}
	type replay struct {
		tokens UsageTokens
		cost   *CostBreakdown
		bound  int64
	}
	var cases []replay
	for _, prompt := range []int{200000, 200001} {
		for _, ttl := range []string{"5m", "1h", "aggregate"} {
			tokens := UsageTokens{InputTokens: prompt - 3, CacheReadTokens: 1, CacheCreationTokens: 2, OutputTokens: 1000}
			if ttl == "5m" {
				tokens.CacheCreation5mTokens = 2
			}
			if ttl == "1h" {
				tokens.CacheCreation1hTokens = 2
			}
			cost, err := billing.CalculateCostFromSnapshot(snapshot, SnapshotSettlementInput{Tokens: tokens, ServiceTier: "priority"})
			require.NoError(t, err)
			bound, err := svc.EstimateUpperBoundUnits(snapshot, EstimateInput{InputTokensUpperBound: prompt, MaxOutputTokens: 1000, ServiceTier: "priority"})
			require.NoError(t, err)
			cases = append(cases, replay{tokens, cost, bound})
		}
	}
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "PreservedPriorityPricing")

	anthropicOfficialBaselineMode = "disabled"
	var restored BillingSnapshot
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, PricingSourceOfficial, restored.Pricing.Source)
	require.Equal(t, snapshot.Pricing.Base.PreservedPriorityPricing, restored.Pricing.Base.PreservedPriorityPricing)
	for _, tc := range cases {
		cost, err := billing.CalculateCostFromSnapshot(&restored, SnapshotSettlementInput{Tokens: tc.tokens, ServiceTier: "priority"})
		require.NoError(t, err)
		require.Equal(t, tc.cost, cost, "reader-compatible rollback must preserve every frozen cost component")
		bound, err := svc.EstimateUpperBoundUnits(&restored, EstimateInput{InputTokensUpperBound: tc.tokens.InputTokens + tc.tokens.CacheReadTokens + tc.tokens.CacheCreationTokens, MaxOutputTokens: 1000, ServiceTier: "priority"})
		require.NoError(t, err)
		require.Equal(t, tc.bound, bound)
		require.LessOrEqual(t, settledUnitsForTest(t, svc, &restored, cost), bound)
	}
	clone := clonePricing(restored.Pricing.Base)
	clone.PreservedPriorityPricing.CacheCreationPricePerToken = 99e-6
	require.Equal(t, 5e-6, restored.Pricing.Base.PreservedPriorityPricing.CacheCreationPricePerToken)
	after, err := json.Marshal(&restored)
	require.NoError(t, err)
	require.Equal(t, encoded, after, "rollback settlement/reservation must not mutate the frozen policy")
}
