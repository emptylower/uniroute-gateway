//go:build unit

package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestAnthropicOfficialBaselineOverridesStaleDynamicReloadWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pricing.json")
	dynamic := &PricingService{}
	billing := NewBillingService(&config.Config{}, dynamic)
	for _, stale := range []string{
		`{"claude-sonnet-5-5":{"input_cost_per_token":0.000009,"output_cost_per_token":0.000045,"cache_read_input_token_cost":0.0000009,"cache_creation_input_token_cost":0.00001125,"cache_creation_input_token_cost_above_1hr":0.000018,"input_cost_per_token_priority":0.000004,"output_cost_per_token_priority":0.000020,"cache_read_input_token_cost_priority":0.0000002,"cache_creation_input_token_cost_priority":0.000005,"input_cost_per_image_token":0.0000004,"output_cost_per_image_token":0.0000008,"max_input_tokens":200000,"max_output_tokens":64000,"long_context_input_token_threshold":200000,"long_context_input_cost_multiplier":2,"long_context_output_cost_multiplier":1.5}}`,
		`{"claude-sonnet-5-5":{"input_cost_per_token":0.000007,"output_cost_per_token":0.000035,"input_cost_per_token_priority":0.000004,"output_cost_per_token_priority":0.000020,"cache_read_input_token_cost_priority":0.0000002,"cache_creation_input_token_cost_priority":0.000005,"input_cost_per_image_token":0.0000004,"output_cost_per_image_token":0.0000008,"max_input_tokens":200000,"max_output_tokens":64000}}`,
	} {
		require.NoError(t, os.WriteFile(path, []byte(stale), 0600))
		require.NoError(t, dynamic.loadPricingData(path))
		raw := dynamic.GetModelPricing("claude-sonnet-5-5")
		before := *raw
		price, err := billing.GetModelPricing("claude-sonnet-5-5")
		require.NoError(t, err)
		require.InDelta(t, 2e-6, price.InputPricePerToken, 1e-18)
		require.InDelta(t, 10e-6, price.OutputPricePerToken, 1e-18)
		require.InDelta(t, .1e-6, price.CacheReadPricePerToken, 1e-18)
		require.InDelta(t, 2.5e-6, price.CacheCreation5mPrice, 1e-18)
		require.InDelta(t, 4e-6, price.CacheCreation1hPrice, 1e-18)
		require.Equal(t, 4e-6, price.InputPricePerTokenPriority)
		require.Equal(t, 20e-6, price.OutputPricePerTokenPriority)
		require.Equal(t, .2e-6, price.CacheReadPricePerTokenPriority)
		require.Equal(t, 5e-6, price.CacheCreationPricePerTokenPriority)
		require.Equal(t, .4e-6, price.ImageInputPricePerToken)
		require.Equal(t, .8e-6, price.ImageOutputPricePerToken)
		require.Equal(t, 1000000, price.MaxInputTokens)
		require.Equal(t, 128000, price.MaxOutputTokens)
		require.Zero(t, price.LongContextInputThreshold)
		require.True(t, price.SupportsCacheBreakdown)
		require.Equal(t, before, *raw, "shared dynamic row must remain immutable")
	}
}

func TestAnthropicOfficialBaselineExactAllowlistAndOlderContextPolicy(t *testing.T) {
	loaded := &ModelPricing{InputPricePerToken: 9e-6, OutputPricePerToken: 45e-6, LongContextInputThreshold: 200000, LongContextInputMultiplier: 2, LongContextOutputMultiplier: 1.5, MaxInputTokens: 200000, MaxOutputTokens: 64000}
	for _, id := range []string{"claude-sonnet-9", "claude-sonnet-5-5-preview", "claude-haiku-5-5-20261007", "vendor/claude-opus-5-5"} {
		price, ok := officialAnthropicBasePricing(id, loaded)
		require.False(t, ok, id)
		require.Same(t, loaded, price)
	}
	older, ok := officialAnthropicBasePricing("claude-sonnet-4-5-20250929", loaded)
	require.True(t, ok)
	require.Equal(t, 200000, older.LongContextInputThreshold)
	require.Equal(t, 2.0, older.LongContextInputMultiplier)
	require.Equal(t, 64000, older.MaxOutputTokens)
	newer, ok := officialAnthropicBasePricing("claude-sonnet-4-6", loaded)
	require.True(t, ok)
	require.Zero(t, newer.LongContextInputThreshold)
	require.Equal(t, 1000000, newer.MaxInputTokens)
	dynamic := &PricingService{pricingData: map[string]*LiteLLMModelPricing{"claude-sonnet-9": {InputCostPerToken: 7e-6, OutputCostPerToken: 35e-6}}}
	billing := NewBillingService(&config.Config{}, dynamic)
	price, err := billing.GetModelPricing("claude-sonnet-9")
	require.NoError(t, err)
	require.Equal(t, 7e-6, price.InputPricePerToken)
}

func TestAnthropicOfficialBaselinesCurrentRatesAndNoPremium(t *testing.T) {
	billing := NewBillingService(&config.Config{}, nil)
	for _, test := range []struct {
		id                                    string
		input, output, read, write5m, write1h float64
	}{
		{"claude-sonnet-5-5", 2, 10, .1, 2.5, 4},
		{"claude-opus-5-5", 4, 20, .2, 5, 8},
		{"claude-fable-5-1", 10, 50, .25, 12.5, 20},
		{"claude-haiku-5-5", .1, .5, .01, .125, .2},
		{"claude-sonnet-5", 2, 10, .2, 2.5, 4},
		{"claude-fable-5", 10, 50, 1, 12.5, 20},
		{"claude-haiku-4-5", 1, 5, .1, 1.25, 2},
		{"claude-opus-4-5-20251101", 5, 25, .5, 6.25, 10},
	} {
		t.Run(test.id, func(t *testing.T) {
			price, err := billing.GetModelPricing(test.id)
			require.NoError(t, err)
			require.InDelta(t, test.input, price.InputPricePerToken*1e6, 1e-12)
			require.InDelta(t, test.output, price.OutputPricePerToken*1e6, 1e-12)
			require.InDelta(t, test.read, price.CacheReadPricePerToken*1e6, 1e-12)
			require.InDelta(t, test.write5m, price.CacheCreation5mPrice*1e6, 1e-12)
			require.InDelta(t, test.write1h, price.CacheCreation1hPrice*1e6, 1e-12)
		})
	}
	cost, err := billing.CalculateCost("claude-sonnet-4-6", UsageTokens{InputTokens: 900000, OutputTokens: 1000}, 1)
	require.NoError(t, err)
	require.False(t, cost.LongContextBillingApplied)
	require.InDelta(t, 2.7, cost.InputCost, 1e-12)
}

func TestAnthropicOfficialHaikuPromptBoundaryPricesAllTokenCategories(t *testing.T) {
	billing := NewBillingService(&config.Config{}, nil)
	for _, ttl := range []string{"5m", "1h"} {
		t.Run(ttl, func(t *testing.T) {
			tokens := UsageTokens{InputTokens: 99998, OutputTokens: 1000, CacheCreationTokens: 1, CacheReadTokens: 1}
			if ttl == "5m" {
				tokens.CacheCreation5mTokens = 1
			} else {
				tokens.CacheCreation1hTokens = 1
			}
			lower, err := billing.CalculateCost("claude-haiku-5-5", tokens, 1)
			require.NoError(t, err)
			require.False(t, lower.LongContextBillingApplied)
			tokens.InputTokens++
			higher, err := billing.CalculateCost("claude-haiku-5-5", tokens, 1)
			require.NoError(t, err)
			require.True(t, higher.LongContextBillingApplied)
			require.InDelta(t, .5e-6, higher.InputCost/float64(tokens.InputTokens), 1e-18)
			require.InDelta(t, .0005, lower.OutputCost, 1e-15)
			require.InDelta(t, lower.OutputCost*5, higher.OutputCost, 1e-15)
			require.InDelta(t, lower.CacheReadCost*5, higher.CacheReadCost, 1e-18)
			require.InDelta(t, lower.CacheCreationCost*5, higher.CacheCreationCost, 1e-18)
		})
	}
}

func TestAnthropicOfficialBaselinePreservesChannelZeroAndIntervals(t *testing.T) {
	zero, customInput, customOutput, customCache := 0.0, 7e-6, 11e-6, 3e-6
	for _, test := range []struct {
		name    string
		pricing ChannelModelPricing
	}{
		{"flat explicit zero", ChannelModelPricing{Platform: PlatformAnthropic, Models: []string{"claude-sonnet-5-5"}, BillingMode: BillingModeToken, InputPrice: &zero, OutputPrice: &zero, CacheReadPrice: &zero, CacheWritePrice: &zero}},
		{"custom interval", ChannelModelPricing{Platform: PlatformAnthropic, Models: []string{"claude-sonnet-5-5"}, BillingMode: BillingModeToken, Intervals: []PricingInterval{{MinTokens: 0, InputPrice: &customInput, OutputPrice: &customOutput, CacheReadPrice: &zero, CacheWritePrice: &customCache}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := NewModelPricingResolver(newChannelServiceForTest(7, []ChannelModelPricing{test.pricing}), NewBillingService(&config.Config{}, nil))
			gid := int64(7)
			resolved := resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-5-5", GroupID: &gid})
			require.Equal(t, PricingSourceChannel, resolved.Source)
			price := resolver.GetIntervalPricing(resolved, 1)
			if test.name == "flat explicit zero" {
				require.Zero(t, price.InputPricePerToken)
				require.Zero(t, price.OutputPricePerToken)
				require.Zero(t, price.CacheReadPricePerToken)
				require.Zero(t, price.CacheCreation5mPrice)
				require.Zero(t, price.CacheCreation1hPrice)
			} else {
				require.Equal(t, customInput, price.InputPricePerToken)
				require.Equal(t, customOutput, price.OutputPricePerToken)
				require.Equal(t, customCache, price.CacheCreation5mPrice)
				require.Equal(t, customCache, price.CacheCreation1hPrice)
				require.Zero(t, price.CacheReadPricePerToken)
			}
		})
	}
}

func TestAnthropicOfficialUpdateDoesNotRepriceOlderFrozenSnapshot(t *testing.T) {
	old := &ModelPricing{InputPricePerToken: 3e-6, OutputPricePerToken: 15e-6, CacheReadPricePerToken: .2e-6, CacheCreation5mPrice: 3.75e-6, CacheCreation1hPrice: 6e-6, SupportsCacheBreakdown: true}
	snap := &BillingSnapshot{Version: BillingSnapshotVersion, Family: BillingFamilyGeneric, BillingModel: "claude-sonnet-5-5", Pricing: BillingSnapshotPricing{Mode: BillingModeToken, Base: clonePricing(old), SupportsCacheBreakdown: true}, Multipliers: BillingSnapshotMultipliers{Text: 1}}
	billing := NewBillingService(&config.Config{}, nil)
	tokens := UsageTokens{InputTokens: 1000000, OutputTokens: 1000}
	before, err := billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens})
	require.NoError(t, err)
	updated, err := billing.GetModelPricing(snap.BillingModel)
	require.NoError(t, err)
	require.InDelta(t, 2e-6, updated.InputPricePerToken, 1e-18)
	after, err := billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens})
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.InDelta(t, 3, before.InputCost, 1e-12)
	live, err := billing.CalculateCost(snap.BillingModel, tokens, 1)
	require.NoError(t, err)
	require.InDelta(t, 2, live.InputCost, 1e-12)
}

func TestAnthropicOfficialPublicSnapshotProvenance(t *testing.T) {
	snapshot := NewPublicModelPriceSnapshot(nil, time.Now())
	require.Equal(t, "https://platform.claude.com/docs/en/about-claude/pricing", snapshot.OfficialSource)
	require.Equal(t, time.Date(2026, 10, 13, 7, 53, 44, 0, time.UTC), snapshot.OfficialVerifiedAt)
	require.Regexp(t, `^sha256:[a-f0-9]{64}$`, snapshot.OfficialSnapshotVersion)
	require.Nil(t, snapshot.EffectiveAt)
}

func TestAnthropicOfficialResolverSourceAndFallbackContext(t *testing.T) {
	billing := NewBillingService(&config.Config{}, nil)
	billing.fallbackPrices["claude-sonnet-4"] = &ModelPricing{InputPricePerToken: 3e-6, OutputPricePerToken: 15e-6, LongContextInputThreshold: 200000, LongContextInputMultiplier: 2, LongContextOutputMultiplier: 1.5, InputPricePerTokenPriority: 6e-6, ImageInputPricePerToken: .4e-6}
	resolver := NewModelPricingResolver(nil, billing)
	official := resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-5-5"})
	require.Equal(t, PricingSourceOfficial, official.Source)
	older := resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-4-5"})
	require.Equal(t, PricingSourceOfficial, older.Source)
	require.Equal(t, 200000, older.BasePricing.LongContextInputThreshold)
	require.Equal(t, 2.0, older.BasePricing.LongContextInputMultiplier)
	require.Equal(t, 6e-6, older.BasePricing.InputPricePerTokenPriority)
	require.Equal(t, .4e-6, older.BasePricing.ImageInputPricePerToken)
	ordinary := resolver.Resolve(context.Background(), PricingInput{Model: "gpt-5.4"})
	require.Equal(t, PricingSourceLiteLLM, ordinary.Source)
}

func TestAnthropicOfficialHaikuCustomIntervalDoesNotReapplyPromptPremium(t *testing.T) {
	input, output := 7e-6, 11e-6
	billing := NewBillingService(&config.Config{}, nil)
	channel := newChannelServiceForTest(7, []ChannelModelPricing{{Platform: PlatformAnthropic, Models: []string{"claude-haiku-5-5"}, BillingMode: BillingModeToken, Intervals: []PricingInterval{{MinTokens: 0, InputPrice: &input, OutputPrice: &output}}}})
	resolver := NewModelPricingResolver(channel, billing)
	gid := int64(7)
	cost, err := billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-haiku-5-5", GroupID: &gid, Tokens: UsageTokens{InputTokens: 100001, OutputTokens: 1000}, RateMultiplier: 1, Resolver: resolver})
	require.NoError(t, err)
	require.False(t, cost.LongContextBillingApplied)
	require.InDelta(t, float64(100001)*input, cost.InputCost, 1e-12)
	require.InDelta(t, .011, cost.OutputCost, 1e-12)
}

func TestAnthropicOfficialNewModelDoesNotInheritDifferentFamilyFallbackMetadata(t *testing.T) {
	billing := NewBillingService(&config.Config{}, nil)
	billing.fallbackPrices["claude-3-5-sonnet"].InputPricePerTokenPriority = 99e-6
	billing.fallbackPrices["claude-3-5-sonnet"].ImageInputPricePerToken = 88e-6
	price, err := billing.GetModelPricing("claude-sonnet-5-5")
	require.NoError(t, err)
	require.Zero(t, price.InputPricePerTokenPriority)
	require.Zero(t, price.ImageInputPricePerToken)
}

func TestAnthropicOfficialPriorityCostsRemainStableAcrossTTLAndContext(t *testing.T) {
	billing := NewBillingService(&config.Config{}, nil)
	for _, breakdown := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			loaded := &ModelPricing{InputPricePerToken: 9e-6, OutputPricePerToken: 45e-6, CacheReadPricePerToken: .9e-6, CacheCreationPricePerToken: 11.25e-6,
				InputPricePerTokenPriority: 4e-6, OutputPricePerTokenPriority: 20e-6, CacheReadPricePerTokenPriority: .2e-6, CacheCreationPricePerTokenPriority: 5e-6,
				SupportsCacheBreakdown: breakdown, LongContextInputThreshold: 200000, LongContextInputMultiplier: 2, LongContextOutputMultiplier: 1.5}
			if breakdown {
				loaded.CacheCreation5mPrice = 11.25e-6
				loaded.CacheCreation1hPrice = 18e-6
			}
			if partial {
				loaded.OutputPricePerTokenPriority = 0
				loaded.CacheReadPricePerTokenPriority = 0
				loaded.CacheCreationPricePerTokenPriority = 0
			}
			updated, ok := officialAnthropicBasePricing("claude-sonnet-5-5", loaded)
			require.True(t, ok)
			require.NotNil(t, updated.PreservedPriorityPricing)
			require.Zero(t, updated.LongContextInputThreshold)
			for _, prompt := range []int{200000, 200001} {
				for _, ttl := range []string{"5m", "1h", "aggregate"} {
					tokens := UsageTokens{InputTokens: prompt - 3, OutputTokens: 1000, CacheCreationTokens: 2, CacheReadTokens: 1}
					if ttl == "5m" {
						tokens.CacheCreation5mTokens = 2
					}
					if ttl == "1h" {
						tokens.CacheCreation1hTokens = 2
					}
					before := billing.computeTokenBreakdown(loaded, tokens, 1, "priority", true)
					after := billing.computeTokenBreakdown(updated, tokens, 1, "priority", true)
					require.Equal(t, before, after, "explicit priority cost must survive official standard overlay: breakdown=%v partial=%v prompt=%v ttl=%s", breakdown, partial, prompt, ttl)
				}
			}
		}
	}
}

func TestAnthropicOfficialPriorityPolicySurvivesFrozenSnapshotJSONAndClone(t *testing.T) {
	loaded := &ModelPricing{InputPricePerToken: 9e-6, OutputPricePerToken: 45e-6, CacheCreationPricePerToken: 11.25e-6, InputPricePerTokenPriority: 4e-6, CacheCreationPricePerTokenPriority: 5e-6, LongContextInputThreshold: 200000, LongContextInputMultiplier: 2, LongContextOutputMultiplier: 1.5}
	updated, ok := officialAnthropicBasePricing("claude-sonnet-5-5", loaded)
	require.True(t, ok)
	snap := &BillingSnapshot{Version: BillingSnapshotVersion, Family: BillingFamilyGeneric, BillingModel: "claude-sonnet-5-5", Pricing: BillingSnapshotPricing{Mode: BillingModeToken, Base: clonePricing(updated), SupportsCacheBreakdown: true}, Multipliers: BillingSnapshotMultipliers{Text: 1}}
	encoded, err := json.Marshal(snap)
	require.NoError(t, err)
	var restored BillingSnapshot
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, snap.Pricing.Base.PreservedPriorityPricing, restored.Pricing.Base.PreservedPriorityPricing)
	billing := NewBillingService(&config.Config{}, nil)
	tokens := UsageTokens{InputTokens: 200000, CacheCreationTokens: 1, CacheCreation1hTokens: 1, OutputTokens: 1000}
	before := billing.computeTokenBreakdown(loaded, tokens, 1, "priority", true)
	after, err := billing.CalculateCostFromSnapshot(&restored, SnapshotSettlementInput{Tokens: tokens, ServiceTier: "priority"})
	require.NoError(t, err)
	require.InDelta(t, before.ActualCost, after.ActualCost, 1e-12)
	updated.PreservedPriorityPricing.CacheCreationPricePerToken = 99e-6
	require.Equal(t, 5e-6, snap.Pricing.Base.PreservedPriorityPricing.CacheCreationPricePerToken, "frozen policy must not share mutable pointers")
}

func TestAnthropicOfficialPriorityPolicyRespectsExplicitFlatAndIntervalOverrides(t *testing.T) {
	zero := 0.0
	loaded := &LiteLLMModelPricing{InputCostPerToken: 9e-6, OutputCostPerToken: 45e-6, CacheCreationInputTokenCost: 11.25e-6, InputCostPerTokenPriority: 4e-6, OutputCostPerTokenPriority: 20e-6, CacheCreationInputTokenCostPriority: 5e-6, CacheReadInputTokenCostPriority: .2e-6, LongContextInputTokenThreshold: 200000, LongContextInputCostMultiplier: 2, LongContextOutputCostMultiplier: 1.5}
	billing := NewBillingService(&config.Config{}, &PricingService{pricingData: map[string]*LiteLLMModelPricing{"claude-sonnet-5-5": loaded}})
	for _, interval := range []bool{false, true} {
		config := ChannelModelPricing{Platform: PlatformAnthropic, Models: []string{"claude-sonnet-5-5"}, BillingMode: BillingModeToken, InputPrice: &zero, OutputPrice: &zero, CacheReadPrice: &zero, CacheWritePrice: &zero}
		if interval {
			config.Intervals = []PricingInterval{{MinTokens: 0, InputPrice: &zero, OutputPrice: &zero, CacheReadPrice: &zero, CacheWritePrice: &zero}}
		}
		resolver := NewModelPricingResolver(newChannelServiceForTest(7, []ChannelModelPricing{config}), billing)
		gid := int64(7)
		resolved := resolver.Resolve(context.Background(), PricingInput{Model: "claude-sonnet-5-5", GroupID: &gid})
		require.Equal(t, PricingSourceChannel, resolved.Source)
		price := resolver.GetIntervalPricing(resolved, 200001)
		if interval {
			require.Nil(t, price.PreservedPriorityPricing)
		}
		cost, err := billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-sonnet-5-5", GroupID: &gid, Tokens: UsageTokens{InputTokens: 199999, CacheCreationTokens: 1, CacheCreation1hTokens: 1, CacheReadTokens: 1, OutputTokens: 1000}, RateMultiplier: 1, ServiceTier: "priority", Resolver: resolver})
		require.NoError(t, err)
		require.Zero(t, cost.ActualCost)
	}
	// A partial cache override must beat the old priority cache policy while
	// leaving the other priority categories active.
	resolver := NewModelPricingResolver(newChannelServiceForTest(7, []ChannelModelPricing{{Platform: PlatformAnthropic, Models: []string{"claude-sonnet-5-5"}, BillingMode: BillingModeToken, CacheWritePrice: &zero}}), billing)
	gid := int64(7)
	cost, err := billing.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "claude-sonnet-5-5", GroupID: &gid, Tokens: UsageTokens{InputTokens: 200000, CacheCreationTokens: 1, CacheCreation1hTokens: 1, OutputTokens: 1000}, RateMultiplier: 1, ServiceTier: "priority", Resolver: resolver})
	require.NoError(t, err)
	require.Zero(t, cost.CacheCreationCost)
	require.Positive(t, cost.InputCost)
}

func TestAnthropicOfficialPreservedPriorityReservationBoundsCacheAndContext(t *testing.T) {
	svc, snap := estimatorSnapshot(t)
	loaded := &ModelPricing{InputPricePerToken: 9e-6, OutputPricePerToken: 45e-6, CacheReadPricePerToken: .9e-6, CacheCreationPricePerToken: 11.25e-6, InputPricePerTokenPriority: 4e-6, OutputPricePerTokenPriority: 20e-6, CacheCreationPricePerTokenPriority: 5e-6, SupportsCacheBreakdown: true, CacheCreation5mPrice: 11.25e-6, CacheCreation1hPrice: 180e-6, LongContextInputThreshold: 200000, LongContextInputMultiplier: 2, LongContextOutputMultiplier: 1.5}
	updated, ok := officialAnthropicBasePricing("claude-sonnet-5-5", loaded)
	require.True(t, ok)
	snap.Family = BillingFamilyGeneric
	snap.BillingModel = "claude-sonnet-5-5"
	snap.Pricing = BillingSnapshotPricing{Mode: BillingModeToken, Source: PricingSourceOfficial, Base: updated, SupportsCacheBreakdown: true, MaxInputTokens: 1000000, MaxOutputTokens: 128000}
	original, err := json.Marshal(snap)
	require.NoError(t, err)
	for _, tier := range []string{"", "priority", "flex"} {
		for _, prompt := range []int{100000, 200000, 200001, 900000} {
			bound, err := svc.EstimateUpperBoundUnits(snap, EstimateInput{InputTokensUpperBound: prompt, MaxOutputTokens: 1000, ServiceTier: tier})
			require.NoError(t, err)
			for _, input := range []int{0, prompt / 2, prompt} {
				cached := prompt - input
				tokens := UsageTokens{InputTokens: input, CacheCreationTokens: cached, CacheCreation1hTokens: cached, OutputTokens: 1000}
				cost, err := svc.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: tokens, ServiceTier: tier})
				require.NoError(t, err)
				settled := settledUnitsForTest(t, svc, snap, cost)
				require.LessOrEqual(t, settled, bound, "tier=%s prompt=%d cached=%d", tier, prompt, cached)
			}
		}
	}
	after, err := json.Marshal(snap)
	require.NoError(t, err)
	require.Equal(t, original, after, "reservation must not mutate frozen priority policy")
}

func TestAnthropicOfficialLegacyChannelPricingPreservesZeroPriorityCacheOverride(t *testing.T) {
	loaded := &LiteLLMModelPricing{InputCostPerToken: 9e-6, OutputCostPerToken: 45e-6, CacheCreationInputTokenCost: 11.25e-6, InputCostPerTokenPriority: 4e-6, CacheCreationInputTokenCostPriority: 5e-6}
	billing := NewBillingService(&config.Config{}, &PricingService{pricingData: map[string]*LiteLLMModelPricing{"claude-sonnet-5-5": loaded}})
	zero := 0.0
	before, err := billing.GetModelPricing("claude-sonnet-5-5")
	require.NoError(t, err)
	channel, err := billing.GetModelPricingWithChannel("claude-sonnet-5-5", &ChannelModelPricing{CacheWritePrice: &zero})
	require.NoError(t, err)
	cost := billing.computeTokenBreakdown(channel, UsageTokens{InputTokens: 10, CacheCreationTokens: 10, CacheCreation1hTokens: 10}, 1, "priority", true)
	require.Zero(t, cost.CacheCreationCost)
	require.Equal(t, 5e-6, before.PreservedPriorityPricing.CacheCreationPricePerToken, "channel override must not mutate existing base pointer")
}
