package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type modelCatalogPricingFake struct {
	byGroup  map[int64]*ResolvedPricing
	official *ResolvedPricing
}

type modelCatalogAccountFake struct {
	byGroup map[int64][]Account
}

func (f *modelCatalogAccountFake) ListSchedulableByGroupID(_ context.Context, groupID int64) ([]Account, error) {
	return f.byGroup[groupID], nil
}

func (f *modelCatalogPricingFake) Resolve(_ context.Context, input PricingInput) *ResolvedPricing {
	if input.GroupID == nil {
		return f.official
	}
	return f.byGroup[*input.GroupID]
}

func TestModelCatalogListsEachRoutingGroupWithDynamicCNYCosts(t *testing.T) {
	now := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	groups := []Group{
		{ID: 1, Name: "Claude standard", Platform: PlatformAnthropic, RateMultiplier: 1, Status: StatusActive},
		{ID: 3, Name: "Claude discount", Platform: PlatformAnthropic, RateMultiplier: 0.15, Status: StatusActive},
	}
	access := &channelRoutingAccessFake{groups: groups, rates: map[int64]float64{3: 0.2}}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, access, channelRoutingConfig(true, 3))
	pricing := &ModelPricing{
		InputPricePerToken: 3e-6, OutputPricePerToken: 15e-6,
		CacheReadPricePerToken: 0.3e-6, CacheCreationPricePerToken: 3.75e-6,
	}
	svc := &ModelCatalogService{
		channels: &channelRoutingCatalogFake{},
		selector: selector,
		pricing: &modelCatalogPricingFake{
			byGroup: map[int64]*ResolvedPricing{
				1: {Mode: BillingModeToken, BasePricing: pricing},
				3: {Mode: BillingModeToken, BasePricing: pricing},
			},
			official: &ResolvedPricing{Mode: BillingModeToken, BasePricing: pricing},
		},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{
			1: {{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5"},
			}}},
			3: {{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5"},
			}}},
		}},
		fx: &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)},
	}

	quote, err := svc.QuoteChannelCosts(context.Background(), 42, now, CurrencyCNY)

	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, quote.BaseCurrency)
	require.Equal(t, CurrencyCNY, quote.QuoteCurrency)
	require.InDelta(t, 7.2, quote.ExchangeRate, 1e-12)
	require.InDelta(t, quote.ExchangeRate, quote.Rate, 1e-12)
	require.Equal(t, "bootstrap_config", quote.RateSource)
	require.True(t, quote.RateFallback)
	require.False(t, quote.RateAsOf.IsZero())
	require.False(t, quote.RateFetchedAt.IsZero())
	require.True(t, quote.RateExpiresAt.After(quote.RateFetchedAt))
	items := quote.Groups
	require.Len(t, items, 2)
	require.Equal(t, []int64{3, 1}, []int64{items[0].GroupID, items[1].GroupID})
	require.Len(t, items[0].Models, 1)
	require.InDelta(t, 0.2, items[0].EffectiveMultiplier, 1e-12)
	require.InDelta(t, 4.32, *items[0].Models[0].InputPricePerMillion, 1e-12)
	require.InDelta(t, 21.6, *items[0].Models[0].OutputPricePerMillion, 1e-12)
	require.InDelta(t, 21.6, *items[0].Models[0].OfficialInputPricePerMillion, 1e-12)
	require.InDelta(t, 108.0, *items[0].Models[0].OfficialOutputPricePerMillion, 1e-12)
	require.Equal(t, "CNY", items[0].Models[0].Currency)
}

func TestModelCatalogQuoteReturnsRequestedCurrencyAndRateMetadata(t *testing.T) {
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	usdMultiplier := 0.5
	group := Group{ID: 1, Name: "USD", Platform: PlatformOpenAI, RateMultiplier: 1, RateMultiplierUSD: &usdMultiplier, Status: StatusActive}
	access := &channelRoutingAccessFake{groups: []Group{group}}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, access, channelRoutingConfig(true, 3))
	pricing := &ModelPricing{InputPricePerToken: 2e-6, OutputPricePerToken: 10e-6}
	svc := &ModelCatalogService{
		channels: &channelRoutingCatalogFake{}, selector: selector,
		pricing:  &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{1: {Mode: BillingModeToken, BasePricing: pricing}}, official: &ResolvedPricing{Mode: BillingModeToken, BasePricing: pricing}},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{1: {{ID: 1, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-5.4": "gpt-5.4"}}}}}},
		fx:       &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)},
	}

	quote, err := svc.QuoteChannelCosts(context.Background(), 42, now, CurrencyUSD)

	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, quote.BaseCurrency)
	require.Equal(t, CurrencyUSD, quote.QuoteCurrency)
	require.InDelta(t, 1, quote.ExchangeRate, 1e-12)
	require.Equal(t, "identity", quote.RateSource)
	require.Len(t, quote.Groups, 1)
	require.InDelta(t, 0.5, quote.Groups[0].EffectiveMultiplier, 1e-12)
	require.Equal(t, CurrencyUSD, quote.Groups[0].Models[0].Currency)
	require.InDelta(t, 1, *quote.Groups[0].Models[0].InputPricePerMillion, 1e-12)
	require.InDelta(t, 2, *quote.Groups[0].Models[0].OfficialInputPricePerMillion, 1e-12)
}

func TestModelCatalogUsesRuntimeCandidateAndEffectivePrice(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	groups := []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 0.8, SortOrder: 1, Status: StatusActive},
		{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 0.4, SortOrder: 2, Status: StatusActive},
	}
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{{
		ID: 10, Status: StatusActive,
		Groups: []AvailableGroupRef{{ID: 1, Platform: PlatformOpenAI}, {ID: 2, Platform: PlatformOpenAI}},
		SupportedModels: []SupportedModel{{
			Name: "gpt-5.4", Platform: PlatformOpenAI,
			Pricing: &ChannelModelPricing{BillingMode: BillingModeToken},
		}},
	}}}
	access := &channelRoutingAccessFake{groups: groups}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	svc := &ModelCatalogService{
		channels: catalog,
		selector: selector,
		pricing: &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{
			2: {Mode: BillingModeToken, BasePricing: &ModelPricing{
				InputPricePerToken: 2e-6, OutputPricePerToken: 10e-6,
				CacheReadPricePerToken: 0.2e-6, CacheCreationPricePerToken: 2.5e-6,
			}},
		}},
	}

	items, err := svc.ListText(context.Background(), 42, now)

	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, items[0].Available)
	require.Equal(t, 1, items[0].AvailableRouteCount)
	require.Equal(t, "openai", items[0].Provider)
	require.InDelta(t, 0.8, *items[0].InputPricePerMillion, 0.000001)
	require.InDelta(t, 4.0, *items[0].OutputPricePerMillion, 0.000001)
}

func TestModelCatalogOmitsNonTextPricingAndDeduplicatesModels(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{{
		ID: 10, Status: StatusActive,
		Groups: []AvailableGroupRef{{ID: 1, Platform: PlatformOpenAI}},
		SupportedModels: []SupportedModel{
			{Name: "GPT-5.4", Platform: PlatformOpenAI, Pricing: &ChannelModelPricing{BillingMode: BillingModeToken}},
			{Name: "gpt-5.4", Platform: PlatformOpenAI, Pricing: &ChannelModelPricing{BillingMode: BillingModeToken}},
			{Name: "gpt-image-2", Platform: PlatformOpenAI, Pricing: &ChannelModelPricing{BillingMode: BillingModeImage}},
		},
	}}}
	access := &channelRoutingAccessFake{groups: []Group{{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive}}}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	svc := &ModelCatalogService{
		channels: catalog,
		selector: selector,
		pricing: &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{
			1: {Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 1e-6}},
		}},
	}

	items, err := svc.ListText(context.Background(), 7, now)

	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "gpt-5.4", items[0].ID)
}

func TestModelCatalogUsesAccountDefaultsForUnrestrictedRoute(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{{
		ID: 10, Status: StatusActive, RestrictModels: false,
		Groups: []AvailableGroupRef{{ID: 2, Platform: PlatformOpenAI}},
	}}}
	access := &channelRoutingAccessFake{groups: []Group{{
		ID: 2, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive,
	}}}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	svc := &ModelCatalogService{
		channels: catalog,
		selector: selector,
		pricing: &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{
			2: {Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 1e-6}},
		}},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{
			2: {{ID: 1, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true}},
		}},
	}

	items, err := svc.ListText(context.Background(), 42, now)

	require.NoError(t, err)
	require.NotEmpty(t, items)
	byID := make(map[string]TextModelCatalogItem, len(items))
	for _, item := range items {
		byID[item.ID] = item
		require.NotContains(t, item.ID, "image")
	}
	require.True(t, byID["gpt-5.4"].Available)
	require.Equal(t, 1, byID["gpt-5.4"].AvailableRouteCount)
}

func TestModelCatalogMergesNewProviderIntoRouteWithExplicitModels(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{{
		ID: 10, Status: StatusActive, RestrictModels: false,
		Groups: []AvailableGroupRef{
			{ID: 1, Platform: PlatformOpenAI},
			{ID: 2, Platform: PlatformAnthropic},
		},
		SupportedModels: []SupportedModel{{
			Name: "gpt-5.4", Platform: PlatformOpenAI,
			Pricing: &ChannelModelPricing{BillingMode: BillingModeToken},
		}},
	}}}
	access := &channelRoutingAccessFake{groups: []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive},
		{ID: 2, Platform: PlatformAnthropic, RateMultiplier: 1, Status: StatusActive},
	}}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	svc := &ModelCatalogService{
		channels: catalog,
		selector: selector,
		pricing: &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{
			1: {Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 1e-6}},
			2: {Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 1e-6}},
		}},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{
			1: {{ID: 1, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true}},
			2: {{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true}},
		}},
	}

	items, err := svc.ListText(context.Background(), 42, now)

	require.NoError(t, err)
	providers := make(map[string]bool)
	for _, item := range items {
		if item.Available {
			providers[item.Provider] = true
		}
	}
	require.True(t, providers["openai"])
	require.True(t, providers["anthropic"])
}
