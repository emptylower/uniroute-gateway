package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type channelRoutingCatalogFake struct {
	channels   []AvailableChannel
	restricted map[int64]bool
}

func (f *channelRoutingCatalogFake) ListAvailable(context.Context) ([]AvailableChannel, error) {
	return f.channels, nil
}

func (f *channelRoutingCatalogFake) IsModelRestricted(_ context.Context, groupID int64, _ string) bool {
	return f.restricted[groupID]
}

type channelRoutingAccessFake struct {
	groups []Group
	rates  map[int64]float64
}

type groupRoutingPreferencesFake struct {
	disabled []int64
}

func (f *groupRoutingPreferencesFake) GetUserDisabledGroupIDs(context.Context, int64) ([]int64, error) {
	return append([]int64(nil), f.disabled...), nil
}

func (f *channelRoutingAccessFake) GetAvailableGroups(context.Context, int64) ([]Group, error) {
	return f.groups, nil
}

func (f *channelRoutingAccessFake) GetUserGroupRates(context.Context, int64) (map[int64]float64, error) {
	return f.rates, nil
}

func channelRoutingConfig(enabled bool, max int) *config.Config {
	cfg := &config.Config{}
	cfg.Gateway.ChannelRoutingEnabled = enabled
	cfg.Gateway.ChannelRoutingMaxCandidates = max
	return cfg
}

func TestChannelRoutingSelector_LegacyModeKeepsOriginalGroup(t *testing.T) {
	group := Group{ID: 7, RateMultiplier: 1.25}
	key := &APIKey{
		RoutingMode: APIKeyRoutingModeChannels,
		GroupID:     &group.ID,
		Group:       &group,
	}
	selector := NewChannelRoutingSelector(nil, nil, channelRoutingConfig(false, 3))

	candidates, err := selector.Candidates(context.Background(), key, "claude-test", ChannelRoutingFamilyAnthropic, time.Now())

	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, int64(7), candidates[0].Group.ID)
}

func TestChannelRoutingSelector_FiltersSortsAndCapsCandidates(t *testing.T) {
	now := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.Local)
	groups := []Group{
		{ID: 1, Platform: PlatformAnthropic, RateMultiplier: 0.6, SortOrder: 2, Status: StatusActive},
		{
			ID: 2, Platform: PlatformAnthropic, RateMultiplier: 0.4, SortOrder: 1, Status: StatusActive,
			SubscriptionType: SubscriptionTypeSubscription, PeakRateEnabled: true,
			PeakStart: "00:00", PeakEnd: "23:59", PeakRateMultiplier: 2,
		},
		{ID: 3, Platform: PlatformAnthropic, RateMultiplier: 0.6, SortOrder: 1, Status: StatusActive},
		{ID: 4, Platform: PlatformOpenAI, RateMultiplier: 0.1, Status: StatusActive},
		{ID: 5, Platform: PlatformAnthropic, RateMultiplier: 0.05, ClaudeCodeOnly: true, Status: StatusActive},
		{ID: 6, Platform: PlatformAnthropic, RateMultiplier: 0.2, Status: StatusActive},
	}
	catalog := &channelRoutingCatalogFake{
		channels: []AvailableChannel{
			{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}, {ID: 2}, {ID: 4}, {ID: 5}}},
			{ID: 20, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 3}, {ID: 6}}},
			{ID: 30, Status: "disabled", Groups: []AvailableGroupRef{{ID: 6}}},
		},
		restricted: map[int64]bool{6: true},
	}
	access := &channelRoutingAccessFake{groups: groups, rates: map[int64]float64{1: 0.6}}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	key := &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeChannels, ChannelIDs: []int64{10, 20}}

	candidates, err := selector.Candidates(context.Background(), key, "claude-test", ChannelRoutingFamilyAnthropic, now)

	require.NoError(t, err)
	require.Len(t, candidates, 3)
	require.Equal(t, []int64{3, 1, 2}, []int64{
		candidates[0].Group.ID,
		candidates[1].Group.ID,
		candidates[2].Group.ID,
	})
	require.InDelta(t, 0.6, candidates[0].EffectiveMultiplier, 0.000001)
	require.InDelta(t, 0.8, candidates[2].EffectiveMultiplier, 0.000001)
}

func TestChannelRoutingSelector_ChannelModeDoesNotFallBackWhenEmpty(t *testing.T) {
	selector := NewChannelRoutingSelector(
		&channelRoutingCatalogFake{},
		&channelRoutingAccessFake{},
		channelRoutingConfig(true, 3),
	)
	key := &APIKey{RoutingMode: APIKeyRoutingModeChannels}

	_, err := selector.Candidates(context.Background(), key, "claude-test", ChannelRoutingFamilyAnthropic, time.Now())

	require.ErrorIs(t, err, ErrNoChannelRoutingCandidate)
}

func TestChannelRoutingSelector_AutomaticModeUsesAllCurrentChannels(t *testing.T) {
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.Local)
	access := &channelRoutingAccessFake{groups: []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 0.8, Status: StatusActive},
		{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 0.2, Status: StatusActive},
	}}
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{
		{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}}},
	}}
	selector := NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3))
	key := &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels}

	first, err := selector.Candidates(context.Background(), key, "gpt-5.4", ChannelRoutingFamilyOpenAI, now)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 1}, []int64{first[0].Group.ID, first[1].Group.ID})
	require.Zero(t, first[0].ChannelID, "groups no longer need a channel-catalog attachment")
	require.Equal(t, int64(10), first[1].ChannelID)

	access.groups = append(access.groups, Group{
		ID: 3, Platform: PlatformOpenAI, RateMultiplier: 0.1, Status: StatusActive,
	})
	updated, err := selector.Candidates(context.Background(), key, "gpt-5.4", ChannelRoutingFamilyOpenAI, now)

	require.NoError(t, err)
	require.Equal(t, []int64{3, 2, 1}, []int64{updated[0].Group.ID, updated[1].Group.ID, updated[2].Group.ID})
}

func TestChannelRoutingSelector_UsesSettlementCurrencyMultiplier(t *testing.T) {
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.Local)
	cnyCheap, cnyExpensive := 0.2, 0.8
	usdExpensive, usdCheap := 0.9, 0.1
	access := &channelRoutingAccessFake{groups: []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 1, RateMultiplierCNY: &cnyCheap, RateMultiplierUSD: &usdExpensive, Status: StatusActive},
		{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 1, RateMultiplierCNY: &cnyExpensive, RateMultiplierUSD: &usdCheap, Status: StatusActive},
	}}
	selector := NewChannelRoutingSelector(&channelRoutingCatalogFake{}, access, channelRoutingConfig(true, 3))
	key := &APIKey{UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels, User: &User{BillingCurrency: CurrencyCNY}}

	cnyCandidates, err := selector.Candidates(context.Background(), key, "gpt-5.6-sol", ChannelRoutingFamilyOpenAI, now)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, []int64{cnyCandidates[0].Group.ID, cnyCandidates[1].Group.ID})

	key.User.BillingCurrency = CurrencyUSD
	usdCandidates, err := selector.Candidates(context.Background(), key, "gpt-5.6-sol", ChannelRoutingFamilyOpenAI, now)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 1}, []int64{usdCandidates[0].Group.ID, usdCandidates[1].Group.ID})
}

func TestChannelRoutingSelector_ExcludesUserDisabledGroups(t *testing.T) {
	now := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.Local)
	access := &channelRoutingAccessFake{groups: []Group{
		{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 0.2, Status: StatusActive},
		{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 0.8, Status: StatusActive},
	}}
	selector := NewChannelRoutingSelector(
		&channelRoutingCatalogFake{channels: []AvailableChannel{{
			ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}, {ID: 2}},
		}}},
		access,
		channelRoutingConfig(true, 3),
	)
	selector.groupPreferences = &groupRoutingPreferencesFake{disabled: []int64{1}}

	candidates, err := selector.Candidates(context.Background(), &APIKey{
		UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels,
	}, "gpt-5.6-sol", ChannelRoutingFamilyOpenAI, now)

	require.NoError(t, err)
	require.Equal(t, []int64{2}, []int64{candidates[0].Group.ID})
}

func TestChannelRoutingSelector_PreferredFamilyUsesRequestModelWithoutAnchor(t *testing.T) {
	selector := NewChannelRoutingSelector(nil, nil, channelRoutingConfig(true, 3))
	key := &APIKey{RoutingMode: APIKeyRoutingModeAutoChannels}

	openAIFamily, ok, err := selector.PreferredFamily(context.Background(), key, "gpt-5.6-sol", time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ChannelRoutingFamilyOpenAI, openAIFamily)

	anthropicFamily, ok, err := selector.PreferredFamily(context.Background(), key, "claude-sonnet-4-5", time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ChannelRoutingFamilyAnthropic, anthropicFamily)
}

func TestChannelRoutingSelector_PreferredFamilyUsesLiveCandidatesForCustomAlias(t *testing.T) {
	now := time.Date(2026, time.August, 11, 8, 0, 0, 0, time.Local)
	anthropic := Group{ID: 1, Platform: PlatformAnthropic, RateMultiplier: 0.8, Status: StatusActive}
	openAI := Group{ID: 2, Platform: PlatformOpenAI, RateMultiplier: 0.2, Status: StatusActive}
	selector := NewChannelRoutingSelector(
		&channelRoutingCatalogFake{channels: []AvailableChannel{{
			ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: anthropic.ID}, {ID: openAI.ID}},
		}}},
		&channelRoutingAccessFake{groups: []Group{anthropic, openAI}},
		channelRoutingConfig(true, 3),
	)

	family, ok, err := selector.PreferredFamily(context.Background(), &APIKey{
		UserID: 42, RoutingMode: APIKeyRoutingModeAutoChannels,
	}, "company-text-model", now)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ChannelRoutingFamilyOpenAI, family)
}

func TestIsChannelRoutingEndpoint(t *testing.T) {
	for _, path := range []string{
		"/responses", "/v1/responses", "/responses/compact", "/backend-api/codex/responses",
		"/chat/completions", "/v1/chat/completions",
	} {
		require.True(t, IsChannelRoutingEndpoint(path), path)
	}
	for _, path := range []string{"/v1/messages", "/v1/models", "/v1/images/generations"} {
		require.False(t, IsChannelRoutingEndpoint(path), path)
	}
}
