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

func TestModelCatalogListTextFailsClosedForOpenAIAccountWithoutDiscoveredModels(t *testing.T) {
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
	require.Empty(t, items)
}

func TestModelCatalogQuoteFailsClosedForOpenAIAccountWithoutDiscoveredModels(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	group := Group{ID: 2, Name: "OpenAI", Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive}
	access := &channelRoutingAccessFake{groups: []Group{group}}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, access, channelRoutingConfig(true, 3))
	pricing := &ModelPricing{InputPricePerToken: 1e-6, OutputPricePerToken: 2e-6}
	svc := &ModelCatalogService{
		channels: &channelRoutingCatalogFake{},
		selector: selector,
		pricing: &modelCatalogPricingFake{
			byGroup:  map[int64]*ResolvedPricing{2: {Mode: BillingModeToken, BasePricing: pricing}},
			official: &ResolvedPricing{Mode: BillingModeToken, BasePricing: pricing},
		},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{
			2: {{ID: 1, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true}},
		}},
		fx: &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)},
	}

	quote, err := svc.QuoteChannelCosts(context.Background(), 42, now, CurrencyCNY)

	require.NoError(t, err)
	require.Len(t, quote.Groups, 1)
	require.Empty(t, quote.Groups[0].Models)
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
			1: {{ID: 1, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true, Credentials: map[string]any{
				"model_mapping": map[string]any{"gpt-5.4": "gpt-5.4"},
			}}},
			// Synced account shape: catalog truth comes from the discovered
			// upstream model list, not from hardcoded platform defaults.
			2: {{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-opus-4-6": "claude-opus-4-6"},
			}}},
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

func TestCatalogAccountModelsEmptyMappingPublishesNothing(t *testing.T) {
	// Strict truth: unsynced accounts (no discovered model_mapping) must not
	// emit hardcoded platform defaults — that surfaced models the upstream
	// does not carry (fake catalog). Applies to every platform now.
	for _, platform := range []string{PlatformAnthropic, PlatformGemini, PlatformGrok, PlatformOpenAI, PlatformAntigravity} {
		account := &Account{Platform: platform, Status: StatusActive, Schedulable: true}
		require.Empty(t, catalogAccountModels(account), "platform %s must publish nothing when unsynced", platform)
		require.False(t, catalogAccountSupportsModel(account, defaultModelsListCandidateIDs(platform)[0]))
	}

	// Synced account publishes exactly its discovered models.
	synced := &Account{Platform: PlatformGrok, Status: StatusActive, Schedulable: true, Credentials: map[string]any{
		"model_mapping": map[string]any{"grok-4.6": "grok-4.6", "grok-4.5": "grok-4.5"},
	}}
	seeds := catalogAccountModels(synced)
	require.Len(t, seeds, 2)
	require.True(t, catalogAccountSupportsModel(synced, "grok-4.6"))
	require.False(t, catalogAccountSupportsModel(synced, "grok-4.3"), "not in upstream mapping → not published")
}

func TestModelCatalogProviderGrokPlatformUnifiesToXAI(t *testing.T) {
	// grok and xAI are one vendor; the catalog must never emit "grok".
	require.Equal(t, "xai", modelCatalogProvider(PlatformGrok, "grok-4.5"))
	require.Equal(t, "xai", modelCatalogProvider(PlatformGrok, "grok-composer-2.5-fast"))
	require.Equal(t, "xai", modelCatalogProvider(PlatformGrok, "anything-else-on-grok-platform"))
}

func TestModelVendorFamilyRealAggregatorCatalog(t *testing.T) {
	// Real shape: the 29-model list returned by api.aicodewith.ai /v1/models.
	cases := map[string]string{
		"claude-haiku-4-5-20251001": "anthropic", "claude-opus-4-6": "anthropic", "claude-opus-4-7": "anthropic",
		"claude-opus-4-8": "anthropic", "claude-sonnet-4-6": "anthropic", "claude-sonnet-5": "anthropic",
		"claude-opus-5": "anthropic", "claude-fable-5": "anthropic",
		"gpt-5.4": "openai", "gpt-5.5": "openai", "gpt-5.6-sol": "openai", "gpt-5.6-terra": "openai",
		"gpt-5.6-luna": "openai", "gpt-image-2": "openai", "gpt-image-2-beta": "openai",
		"gemini-2.5-pro": "google", "gemini-3-pro-preview": "google", "gemini-3.1-pro-preview": "google",
		"gemini-3.5-flash": "google",
		"grok-4.5": "xai", "grok-4.6": "xai",
		"glm-5.1": "glm", "glm-5.2": "glm",
		"deepseek-v4-flash": "deepseek", "deepseek-v4-pro": "deepseek", "deepseek-v4-flash-vision-exp": "deepseek",
		"kimi-k3": "kimi", "kimi-k2.7-code": "kimi", "kimi-k2.6": "kimi",
	}
	for model, vendor := range cases {
		require.Equal(t, vendor, modelVendorFamily(model), "model %s", model)
	}
	require.Equal(t, "qwen", modelVendorFamily("qwen3.5-397b-a17b"))
	require.Equal(t, "longcat", modelVendorFamily("longcat-flash-chat"))
	require.Equal(t, "bytedance", modelVendorFamily("seed-oss-36b-instruct"))
	require.Equal(t, "minimax", modelVendorFamily("minimax-m3"))
	require.Equal(t, "minimax", modelVendorFamily("mimo-v2-flash"))
	require.Equal(t, "", modelVendorFamily("my-custom-fine-tune"))
	require.Equal(t, "openai", modelVendorFamily("GPT-5.5"))
	require.Equal(t, "anthropic", modelVendorFamily("anthropic/claude-opus-5"))
}

func TestGroupServesModelFamilyIsolation(t *testing.T) {
	require.True(t, groupServesModel(PlatformAnthropic, "claude-opus-5"))
	require.False(t, groupServesModel(PlatformAnthropic, "glm-5"))
	require.False(t, groupServesModel(PlatformOpenAI, "claude-opus-5"))
	require.False(t, groupServesModel(PlatformOpenAI, "grok-4.5"), "grok has its own platform despite riding openai wire format")
	require.True(t, groupServesModel(PlatformGrok, "grok-4.5"))
	require.True(t, groupServesModel(PlatformGemini, "gemini-2.5-pro"))
	require.True(t, groupServesModel(PlatformAnthropic, "my-custom-fine-tune"))
	require.True(t, groupServesModel(PlatformComposite, "glm-5"))
	require.True(t, groupServesModel(PlatformAntigravity, "claude-opus-5"))
	require.True(t, groupServesModel(PlatformAntigravity, "gemini-2.5-pro"))
	require.False(t, groupServesModel(PlatformAntigravity, "gpt-5.5"))
}

func TestQuoteChannelCostsFamilyIsolationRealAccountShapes(t *testing.T) {
	// Real production shape: openai-platform account 8 carried the FULL 37-model
	// aggregator catalog (claude/gemini/grok/glm...); anthropic account 4 too.
	// The square must attribute each model to its true vendor and scope each
	// group's channels to the families that platform actually serves.
	now := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	pricing := &ModelPricing{InputPricePerToken: 1e-6, OutputPricePerToken: 2e-6}
	mixedMapping := map[string]any{
		"model_mapping": map[string]any{
			"claude-opus-5": "claude-opus-5", "gpt-5.5": "gpt-5.5", "glm-5": "glm-5",
			"grok-4.6": "grok-4.6", "gemini-2.5-pro": "gemini-2.5-pro",
		},
	}
	access := &channelRoutingAccessFake{groups: []Group{
		{ID: 1, Name: "openai订阅", Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive},
		{ID: 2, Name: "aws云厂商渠道", Platform: PlatformAnthropic, RateMultiplier: 1, Status: StatusActive},
	}}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, access, channelRoutingConfig(true, 3))
	svc := &ModelCatalogService{
		channels: &channelRoutingCatalogFake{},
		selector: selector,
		pricing: &modelCatalogPricingFake{
			byGroup:  map[int64]*ResolvedPricing{1: {Mode: BillingModeToken, BasePricing: pricing}, 2: {Mode: BillingModeToken, BasePricing: pricing}},
			official: &ResolvedPricing{Mode: BillingModeToken, BasePricing: pricing},
		},
		accounts: &modelCatalogAccountFake{byGroup: map[int64][]Account{
			1: {{ID: 8, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true, Credentials: mixedMapping}},
			2: {{ID: 4, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true, Credentials: mixedMapping}},
		}},
		fx: &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)},
	}

	quote, err := svc.QuoteChannelCosts(context.Background(), 42, now, CurrencyUSD)
	require.NoError(t, err)
	require.Len(t, quote.Groups, 2)
	byID := map[int64]RoutingGroupModelCosts{}
	for _, g := range quote.Groups {
		byID[g.GroupID] = g
	}
	openaiModels := []string{}
	for _, m := range byID[1].Models {
		openaiModels = append(openaiModels, m.ID)
		require.Equal(t, "openai", m.Provider, "openai group must only carry openai-family models")
	}
	require.Equal(t, []string{"gpt-5.5"}, openaiModels, "claude/glm/grok/gemini entries are foreign to an openai group")

	anthropicModels := []string{}
	for _, m := range byID[2].Models {
		anthropicModels = append(anthropicModels, m.ID)
		require.Equal(t, "anthropic", m.Provider)
	}
	require.Equal(t, []string{"claude-opus-5"}, anthropicModels, "glm/grok/gemini can never be served by the anthropic surface")
}

func TestGroupServesModelVendorPlatforms(t *testing.T) {
	require.True(t, groupServesModel(PlatformDeepseek, "deepseek-v4-pro"))
	require.False(t, groupServesModel(PlatformDeepseek, "gpt-5.5"))
	require.False(t, groupServesModel(PlatformDeepseek, "glm-5"))
	require.True(t, groupServesModel(PlatformGLM, "glm-5.2"))
	require.True(t, groupServesModel(PlatformKimi, "kimi-k3"))
	require.True(t, groupServesModel(PlatformQwen, "qwen3-coder"))
	require.True(t, groupServesModel(PlatformLongcat, "longcat-flash-chat"))
	require.True(t, groupServesModel(PlatformBytedance, "seed-oss-36b-instruct"))
	require.True(t, groupServesModel(PlatformMinimax, "minimax-m3"))
	require.True(t, groupServesModel(PlatformMinimax, "mimo-v2-flash"), "mimo is minimax family")
}

func TestValidGovernanceProviderVendorFamilies(t *testing.T) {
	for _, p := range []GovernanceProvider{
		GovernanceProviderDeepseek, GovernanceProviderGLM, GovernanceProviderKimi,
		GovernanceProviderQwen, GovernanceProviderLongcat, GovernanceProviderBytedance, GovernanceProviderMinimax,
	} {
		require.True(t, ValidGovernanceProvider(p), "provider %s", p)
		require.Equal(t, string(p), providerToPlatform(p))
	}
}
