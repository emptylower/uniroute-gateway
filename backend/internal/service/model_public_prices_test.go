//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type publicCatalogAccessFake struct {
	groups []Group
	err    error
}

func (f *publicCatalogAccessFake) GetPublicCatalogGroups(context.Context) ([]Group, error) {
	return f.groups, f.err
}
func (f *publicCatalogAccessFake) GetAvailableGroups(context.Context, int64) ([]Group, error) {
	panic("anonymous projection must never load a user")
}
func (f *publicCatalogAccessFake) GetUserGroupRates(context.Context, int64) (map[int64]float64, error) {
	panic("anonymous projection must never load negotiated rates")
}

func publicCatalogFixture(groups []Group) *ModelCatalogService {
	access := &publicCatalogAccessFake{groups: groups}
	pricing := &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{}}
	accounts := &modelCatalogAccountFake{byGroup: map[int64][]Account{}}
	for _, group := range groups {
		pricing.byGroup[group.ID] = &ResolvedPricing{Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 3e-6, OutputPricePerToken: 15e-6, CacheReadPricePerToken: 0.3e-6, CacheCreationPricePerToken: 3.75e-6, CacheCreation5mPrice: 3.75e-6, CacheCreation1hPrice: 6e-6, SupportsCacheBreakdown: true, LongContextInputThreshold: 200000, MaxInputTokens: 1000000}}
		accounts.byGroup[group.ID] = []Account{{ID: group.ID, Platform: group.Platform, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5", "gpt-5.4": "gpt-5.4", "claude-audio": "claude-audio"}}}}
	}
	return &ModelCatalogService{channels: &channelRoutingCatalogFake{}, selector: NewChannelRoutingSelector(nil, access, &config.Config{}), pricing: pricing, accounts: accounts}
}
func publicGroup(id int64, multiplier float64) Group {
	return Group{ID: id, Name: "private-supplier", Status: StatusActive, Platform: PlatformAnthropic, RateMultiplier: multiplier}
}

func TestPublicModelPricesDefaultRetailPrivacyTTLandStableHash(t *testing.T) {
	groups := []Group{publicGroup(1, 1), publicGroup(2, 0.2), publicGroup(3, 0.5)}
	svc := publicCatalogFixture(groups)
	now := time.Now()
	snapshot, err := svc.PublicPrices(context.Background(), now)
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 2)
	require.Equal(t, "discounted", snapshot.Items[0].Channel)
	require.Equal(t, "0.6", snapshot.Items[0].InputPricePerMillionUSD)
	require.Equal(t, "3", snapshot.Items[0].OutputPricePerMillionUSD)
	require.Equal(t, "0.75", *snapshot.Items[0].CacheWrite5mPricePerMillionUSD)
	require.Equal(t, "1.2", *snapshot.Items[0].CacheWrite1hPricePerMillionUSD)
	require.Equal(t, 200000, *snapshot.Items[0].MaxContextTokens)
	require.Nil(t, snapshot.EffectiveAt)
	require.Regexp(t, `^sha256:[a-f0-9]{64}$`, snapshot.SnapshotVersion)
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	for _, private := range []string{"private-supplier", "group_id", "account_id", "official_input", "effective_multiplier", "rate_source"} {
		require.NotContains(t, string(encoded), private)
	}
	access := svc.selector.apiKeys.(*publicCatalogAccessFake)
	access.groups = []Group{groups[2], groups[1], groups[0]}
	later, err := svc.PublicPrices(context.Background(), now.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, snapshot.SnapshotVersion, later.SnapshotVersion)
	require.NotEqual(t, snapshot.GeneratedAt, later.GeneratedAt)
	svc.pricing.(*modelCatalogPricingFake).byGroup[1].BasePricing.OutputPricePerToken = 16e-6
	changed, err := svc.PublicPrices(context.Background(), now)
	require.NoError(t, err)
	require.NotEqual(t, snapshot.SnapshotVersion, changed.SnapshotVersion)
}

func TestPublicModelPricesSelectsOneCoherentVector(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1), publicGroup(2, 1)})
	pricing := svc.pricing.(*modelCatalogPricingFake)
	pricing.byGroup[1].BasePricing = &ModelPricing{InputPricePerToken: 1e-6, OutputPricePerToken: 100e-6}
	pricing.byGroup[2].BasePricing = &ModelPricing{InputPricePerToken: 10e-6, OutputPricePerToken: 20e-6}
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.Equal(t, "10", snapshot.Items[0].InputPricePerMillionUSD)
	require.Equal(t, "20", snapshot.Items[0].OutputPricePerMillionUSD)
	require.Nil(t, snapshot.Items[0].CacheWrite5mPricePerMillionUSD)
}

func TestPublicModelPricesFiltersAccessSchedulingRestrictionsAndPublication(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*ModelCatalogService)
	}{
		{"exclusive", func(s *ModelCatalogService) {
			s.selector.apiKeys.(*publicCatalogAccessFake).groups[0].IsExclusive = true
		}},
		{"subscription", func(s *ModelCatalogService) {
			s.selector.apiKeys.(*publicCatalogAccessFake).groups[0].SubscriptionType = SubscriptionTypeSubscription
		}},
		{"claude code only", func(s *ModelCatalogService) {
			s.selector.apiKeys.(*publicCatalogAccessFake).groups[0].ClaudeCodeOnly = true
		}},
		{"inactive group", func(s *ModelCatalogService) {
			s.selector.apiKeys.(*publicCatalogAccessFake).groups[0].Status = StatusDisabled
		}},
		{"disabled account", func(s *ModelCatalogService) { s.accounts.(*modelCatalogAccountFake).byGroup[1][0].Schedulable = false }},
		{"suspended mapped upstream", func(s *ModelCatalogService) {
			s.accounts.(*modelCatalogAccountFake).byGroup[1][0].Credentials = map[string]any{"model_mapping": map[string]any{"claude-sonnet-4-5": "kling-ai-avatar-pro"}}
		}},
		{"undiscovered", func(s *ModelCatalogService) { s.accounts.(*modelCatalogAccountFake).byGroup[1][0].Credentials = nil }},
		{"restricted", func(s *ModelCatalogService) {
			s.channels.(*channelRoutingCatalogFake).restricted = map[int64]bool{1: true}
		}},
		{"model cooldown", func(s *ModelCatalogService) {
			s.accounts.(*modelCatalogAccountFake).byGroup[1][0].Extra = map[string]any{modelRateLimitsKey: map[string]any{"claude-sonnet-4-5": map[string]any{"rate_limit_reset_at": time.Now().Add(time.Hour).Format(time.RFC3339)}}}
		}},
		{"paused canonical", func(s *ModelCatalogService) {
			s.cfg = &config.Config{}
			s.cfg.ModelGovernance.AuthorizationMode = "enforce"
			s.publicationStore = &fakePublicationStore{channelEligibleFn: func(context.Context, int64, string) (bool, error) { return false, nil }}
		}},
		{"eligibility error", func(s *ModelCatalogService) {
			s.cfg = &config.Config{}
			s.cfg.ModelGovernance.AuthorizationMode = "enforce"
			s.publicationStore = &fakePublicationStore{channelEligibleFn: func(context.Context, int64, string) (bool, error) { return false, errors.New("unavailable") }}
		}},
		{"per request", func(s *ModelCatalogService) {
			s.pricing.(*modelCatalogPricingFake).byGroup[1].Mode = BillingModePerRequest
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
			test.change(svc)
			snapshot, err := svc.PublicPrices(context.Background(), time.Now())
			require.NoError(t, err)
			require.Empty(t, snapshot.Items)
		})
	}
}

func TestPublicStandardPricingUsesResolverIntervalCacheAndBound(t *testing.T) {
	input, output, write, read := 2e-6, 8e-6, 4e-6, 0.2e-6
	bound := 123000
	resolved := &ResolvedPricing{Mode: BillingModeToken, SupportsCacheBreakdown: true, BasePricing: &ModelPricing{MaxInputTokens: 1000000, LongContextInputThreshold: 200000}, Intervals: []PricingInterval{{MinTokens: 0, MaxTokens: &bound, InputPrice: &input, OutputPrice: &output, CacheWritePrice: &write, CacheReadPrice: &read}}}
	pricing, limit := publicStandardPricing(resolved)
	require.Equal(t, 123000, *limit)
	require.Equal(t, write, pricing.CacheCreation5mPrice)
	require.Equal(t, write, pricing.CacheCreation1hPrice)
}

func TestAPIKeyServicePublicCatalogGroupsNeverLoadsIdentity(t *testing.T) {
	groups := []Group{publicGroup(1, 1), publicGroup(2, 1), publicGroup(3, 1), publicGroup(4, 1), publicGroup(5, 1)}
	groups[1].IsExclusive = true
	groups[2].SubscriptionType = SubscriptionTypeSubscription
	groups[3].ClaudeCodeOnly = true
	groups[4].Status = StatusDisabled
	repo := &stubGroupRepoForAvailable{activeGroups: groups}
	svc := &APIKeyService{groupRepo: repo}
	public, err := svc.GetPublicCatalogGroups(context.Background())
	require.NoError(t, err)
	require.Equal(t, []Group{groups[0]}, public)
	require.Equal(t, 1, repo.listActiveCalls)
}

func TestPublicModelPricesUsesUSDDefaultAndAccountChannelEligibility(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
	legacyUSD := 0.9
	svc.selector.apiKeys.(*publicCatalogAccessFake).groups[0].RateMultiplier = 0.25
	svc.selector.apiKeys.(*publicCatalogAccessFake).groups[0].RateMultiplierUSD = &legacyUSD
	svc.channels = &channelRoutingCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}}}}}
	svc.cfg = &config.Config{}
	svc.cfg.ModelGovernance.AuthorizationMode = "enforce"
	store := &fakePublicationStore{decisionFn: func(_ context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
		require.Equal(t, int64(1), key.AccountID)
		require.Equal(t, int64(10), key.ChannelID)
		return ModelAuthorizationDecision{Eligibility: PublicationEligibilityBlocked}, nil
	}}
	svc.publicationStore = store
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, snapshot.Items)
	require.Positive(t, store.decisionCalls)
	store.decisionFn = nil
	snapshot, err = svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.Equal(t, "discounted", snapshot.Items[0].Channel)
	require.Equal(t, "0.75", snapshot.Items[0].InputPricePerMillionUSD)
}

func TestPublicModelPricesPreservesZeroRetailPrices(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
	svc.pricing.(*modelCatalogPricingFake).byGroup[1].BasePricing = &ModelPricing{InputPricePerToken: 1e-6, OutputPricePerToken: 0, CacheCreationPriceExplicit: true}
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.Equal(t, "0", snapshot.Items[0].OutputPricePerMillionUSD)
	require.Equal(t, "0", *snapshot.Items[0].CacheWrite5mPricePerMillionUSD)
	require.Equal(t, "0", *snapshot.Items[0].CacheWrite1hPricePerMillionUSD)
}

func TestPublicModelPricesPreservesExplicitZeroCacheRead(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
	zero := 0.0
	resolved := svc.pricing.(*modelCatalogPricingFake).byGroup[1]
	resolved.BasePricing.CacheReadPricePerToken = 0
	resolved.BasePricing.SupportsCacheBreakdown = false
	resolved.channelPricing = &ChannelModelPricing{CacheReadPrice: &zero}
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.NotNil(t, snapshot.Items[0].CacheReadPricePerMillionUSD)
	require.Equal(t, "0", *snapshot.Items[0].CacheReadPricePerMillionUSD)
}

func TestPublicModelPricesMissingCacheReadWithWriteTTLIsUnknown(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
	resolved := svc.pricing.(*modelCatalogPricingFake).byGroup[1]
	resolved.BasePricing.CacheReadPricePerToken = 0
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.Nil(t, snapshot.Items[0].CacheReadPricePerMillionUSD)
	require.Equal(t, "3.75", *snapshot.Items[0].CacheWrite5mPricePerMillionUSD)
	require.Equal(t, "6", *snapshot.Items[0].CacheWrite1hPricePerMillionUSD)
}

func TestPublicModelPricesIgnoresFlatCacheReadWhenIntervalsAreConfigured(t *testing.T) {
	svc := publicCatalogFixture([]Group{publicGroup(1, 1)})
	resolved := svc.pricing.(*modelCatalogPricingFake).byGroup[1]
	resolved.BasePricing.CacheReadPricePerToken = 0
	zero, input := 0.0, 2e-6
	resolved.channelPricing = &ChannelModelPricing{CacheReadPrice: &zero}
	resolved.Intervals = []PricingInterval{{MinTokens: 100, InputPrice: &input}}
	snapshot, err := svc.PublicPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, snapshot.Items, 1)
	require.Nil(t, snapshot.Items[0].CacheReadPricePerMillionUSD)
}

func TestPublicTextPricesAllFamiliesUsesDefaultRoutesAndExplicitZero(t *testing.T) {
	groups := []Group{publicGroup(1, 1), publicGroup(2, 0)}
	groups[1].Platform = PlatformOpenAI
	svc := publicCatalogFixture(groups)
	now := time.Now()
	legacy, err := svc.PublicPrices(context.Background(), now)
	require.NoError(t, err)
	require.Len(t, legacy.Items, 1)
	all, err := svc.PublicTextPrices(context.Background(), now)
	require.NoError(t, err)
	require.Len(t, all.Items, 2)
	require.Empty(t, all.OfficialSource)
	require.True(t, all.OfficialVerifiedAt.IsZero())
	found := false
	for _, price := range all.Items {
		if price.ID == "gpt-5.4" {
			found = true
			require.Equal(t, "0", price.InputPricePerMillionUSD)
			require.Equal(t, "0", price.OutputPricePerMillionUSD)
			require.Equal(t, "discounted", price.Channel)
		}
	}
	require.True(t, found, "actual OpenAI row must be present")
	bytes, err := json.Marshal(all)
	require.NoError(t, err)
	for _, private := range []string{"user_id", "group_id", "account_id", "private-supplier", "supplier"} {
		require.NotContains(t, string(bytes), private)
	}
}

func TestPublicImageTokenPricesUsesSeparateImageRatesAndExplicitZero(t *testing.T) {
	group := publicGroup(1, 0.5)
	group.Platform = PlatformOpenAI
	svc := publicCatalogFixture([]Group{group})
	accounts, ok := svc.accounts.(*modelCatalogAccountFake)
	require.True(t, ok)
	pricing, ok := svc.pricing.(*modelCatalogPricingFake)
	require.True(t, ok)
	accounts.byGroup[1][0].Credentials = map[string]any{"model_mapping": map[string]any{"gpt-image-2": "gpt-image-2"}}
	pricing.byGroup[1].BasePricing = &ModelPricing{InputPricePerToken: 5e-6, OutputPricePerToken: 30e-6, ImageInputPricePerToken: 8e-6, ImageOutputPricePerToken: 0, ImageOutputPriceExplicit: true}
	image, err := svc.PublicImageTokenPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, image.Items, 1)
	require.Equal(t, "gpt-image-2", image.Items[0].ID)
	require.Equal(t, "default_retail_image_tokens", image.PriceScope)
	require.Equal(t, "2.5", image.Items[0].InputPricePerMillionUSD)
	require.Equal(t, "15", image.Items[0].OutputPricePerMillionUSD)
	require.Equal(t, "4", *image.Items[0].ImageInputPricePerMillionUSD)
	require.Equal(t, "0", *image.Items[0].ImageOutputPricePerMillionUSD)
	text, err := svc.PublicTextPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, text.Items)
	// A request/image-priced route must not be mislabeled as a token rate.
	pricing.byGroup[1].Mode = BillingModeImage
	pricing.byGroup[1].DefaultPerRequestPrice = 0.2
	image, err = svc.PublicImageTokenPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, image.Items)
	// Unsynced accounts cannot synthesize the four private supplier aliases.
	pricing.byGroup[1].Mode = BillingModeToken
	accounts.byGroup[1][0].Credentials = map[string]any{}
	image, err = svc.PublicImageTokenPrices(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, image.Items)
}
