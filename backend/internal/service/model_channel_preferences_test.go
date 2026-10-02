package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelChannelPreferenceSaveIsIsolatedAndNormalized(t *testing.T) {
	repo := &channelPreferenceRepoFake{disabledGroups: []int64{1}}
	svc := NewChannelPreferenceService(repo, nil, nil, nil)
	ctx := context.Background()
	model, err := svc.SetUserModelChannelPreference(ctx, 42, " CLAUDE-A ", ModelChannelOfficial)
	require.NoError(t, err)
	require.Equal(t, "claude-a", model)
	_, err = svc.SetUserModelChannelPreference(ctx, 42, "claude-b", ModelChannelCloudVendor)
	require.NoError(t, err)
	_, err = svc.SetUserModelChannelPreference(ctx, 43, "claude-a", ModelChannelCloudVendor)
	require.NoError(t, err)
	first, err := svc.GetUserModelChannelPreferences(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"claude-a": ModelChannelOfficial, "claude-b": ModelChannelCloudVendor}, first)
	other, err := svc.GetUserModelChannelPreference(ctx, 43, "CLAUDE-A")
	require.NoError(t, err)
	require.Equal(t, ModelChannelCloudVendor, other)
	require.Equal(t, []int64{1}, repo.disabledGroups)
	require.Empty(t, repo.replacedDisabled)
	for _, invalid := range []string{"", "*", "claude-*", "bad model", "/gpt-5", "模型", strings.Repeat("a", 201)} {
		_, err := svc.SetUserModelChannelPreference(ctx, 42, invalid, ModelChannelOfficial)
		require.Error(t, err, invalid)
	}
	_, err = svc.SetUserModelChannelPreference(ctx, 42, "claude-a", "other")
	require.Error(t, err)
	after, err := svc.GetUserModelChannelPreferences(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, first, after)
}

func TestModelChannelPreferenceRoutingIsolatesModelsAndLegacyDefaults(t *testing.T) {
	groups := []Group{
		{ID: 1, Platform: PlatformAnthropic, RateMultiplier: 0.2, Status: StatusActive},
		{ID: 2, Platform: PlatformAnthropic, RateMultiplier: 0.5, Status: StatusActive},
		{ID: 3, Platform: PlatformAnthropic, RateMultiplier: 1, Status: StatusActive},
	}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, &channelRoutingAccessFake{groups: groups}, channelRoutingConfig(true, 1))
	prefs := &groupRoutingPreferencesFake{models: map[string]string{"claude-a": ModelChannelOfficial}}
	selector.groupPreferences = prefs
	key := &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels}
	selectGroup := func(model string) ChannelRoutingCandidate {
		candidates, err := selector.Candidates(context.Background(), key, model, ChannelRoutingFamilyAnthropic, time.Now())
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		return candidates[0]
	}
	selected := selectGroup("claude-a")
	require.Equal(t, int64(3), selected.Group.ID)
	require.Equal(t, int64(1), selectGroup("claude-b").Group.ID)
	bound := selected.Apply(key)
	require.Equal(t, int64(3), *bound.GroupID)
	require.Equal(t, float64(1), bound.Group.RateMultiplier)
	require.Nil(t, key.GroupID)
	// One explicit cloud choice overrides an old group opt-out only for A.
	prefs.disabled = []int64{1, 2}
	prefs.models["claude-a"] = ModelChannelCloudVendor
	require.Equal(t, int64(1), selectGroup("claude-a").Group.ID)
	require.Equal(t, int64(3), selectGroup("claude-b").Group.ID)
	selector.cfg.Gateway.ChannelRoutingMaxCandidates = 3
	cloud, err := selector.Candidates(context.Background(), key, "claude-a", ChannelRoutingFamilyAnthropic, time.Now())
	require.NoError(t, err)
	require.Len(t, cloud, 3)
	require.Equal(t, int64(3), cloud[2].Group.ID) // existing official fallback
	prefs.models["claude-a"] = ModelChannelOfficial
	official, err := selector.Candidates(context.Background(), key, "claude-a", ChannelRoutingFamilyAnthropic, time.Now())
	require.NoError(t, err)
	require.Len(t, official, 1) // no discounted retry candidate survives
}

func TestModelChannelPreferenceFiltersVendorBeforeCandidateLimit(t *testing.T) {
	groups := []Group{
		{ID: 1, Platform: PlatformGrok, RateMultiplier: 0.1, Status: StatusActive},
		{ID: 2, Platform: PlatformDeepseek, RateMultiplier: 0.2, Status: StatusActive},
		{ID: 3, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive},
	}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, &channelRoutingAccessFake{groups: groups}, channelRoutingConfig(true, 1))
	key := &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels}
	for model, expected := range map[string]int64{"gpt-5.6-sol": 3, "grok-4": 1, "deepseek-chat": 2} {
		candidates, err := selector.Candidates(context.Background(), key, model, ChannelRoutingFamilyOpenAI, time.Now())
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		require.Equal(t, expected, candidates[0].Group.ID)
	}
}

func TestModelChannelPreferenceCompositeFamilyPreservesKeyAuthorization(t *testing.T) {
	composite := Group{ID: 3, Platform: PlatformComposite, RateMultiplier: 1, Status: StatusActive}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 3}}}}}, &channelRoutingAccessFake{groups: []Group{composite}}, channelRoutingConfig(true, 3))
	selector.groupPreferences = &groupRoutingPreferencesFake{models: map[string]string{"gpt-5.1": ModelChannelOfficial}}
	for _, mode := range []string{APIKeyRoutingModeAutoChannels, APIKeyRoutingModeChannels} {
		key := &APIKey{UserID: 42, RoutingMode: mode, ChannelIDs: []int64{10}}
		for _, request := range []struct{ model, family string }{
			{"gpt-5.1", ChannelRoutingFamilyOpenAI},
			{"claude-a", ChannelRoutingFamilyAnthropic},
			{"gemini-a", ChannelRoutingFamilyAnthropic},
		} {
			candidates, err := selector.Candidates(context.Background(), key, request.model, request.family, time.Now())
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			require.Equal(t, composite.ID, candidates[0].Group.ID)
		}
		_, err := selector.Candidates(context.Background(), key, "gpt-5.1", ChannelRoutingFamilyAnthropic, time.Now())
		require.Error(t, err, "a known GPT model must not enter the Anthropic handler")
		if mode == APIKeyRoutingModeChannels {
			key.ChannelIDs = []int64{99}
			_, err := selector.Candidates(context.Background(), key, "gpt-5.1", ChannelRoutingFamilyOpenAI, time.Now())
			require.Error(t, err, "Composite must not expand a key's selected channels")
		}
	}
	require.Equal(t, ChannelRoutingFamilyAnthropic, channelRoutingGroupFamily(PlatformComposite, "custom-alias"), "unknown aliases keep legacy family resolution")
}

func TestModelChannelPreferenceUsesUSDOverridesAndPeakMultiplier(t *testing.T) {
	usdDiscount, usdOfficial := 0.4, 1.0
	groups := []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 1, RateMultiplierUSD: &usdDiscount, Status: StatusActive},
		{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 0.2, RateMultiplierUSD: &usdOfficial, Status: StatusActive},
		{ID: 3, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive},
	}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, &channelRoutingAccessFake{groups: groups, rates: map[int64]float64{3: 0.5}}, channelRoutingConfig(true, 3))
	selector.groupPreferences = &groupRoutingPreferencesFake{models: map[string]string{"gpt-5.6-sol": ModelChannelOfficial}}
	candidates, err := selector.Candidates(context.Background(), &APIKey{UserID: 42, User: &User{BillingCurrency: CurrencyUSD}, RoutingMode: APIKeyRoutingModeAutoChannels}, "gpt-5.6-sol", ChannelRoutingFamilyOpenAI, time.Now())
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, int64(2), candidates[0].Group.ID)
	require.False(t, modelChannelAllowsGroup(ModelChannelOfficial, nil, 1, 0.8))
	require.True(t, modelChannelAllowsGroup(ModelChannelOfficial, nil, 1, 0.8*1.5))
}

func TestModelChannelPreferenceNeverBroadensExplicitKeyChannels(t *testing.T) {
	group := Group{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 0.2, Status: StatusActive}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}}}}}, &channelRoutingAccessFake{groups: []Group{group, {ID: 2, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive}}}, channelRoutingConfig(true, 3))
	selector.groupPreferences = &groupRoutingPreferencesFake{models: map[string]string{"gpt-5.6-sol": ModelChannelOfficial}}
	_, err := selector.Candidates(context.Background(), &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeChannels, ChannelIDs: []int64{10}}, "gpt-5.6-sol", ChannelRoutingFamilyOpenAI, time.Now())
	require.ErrorIs(t, err, ErrNoChannelRoutingCandidate)
}
