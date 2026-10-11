//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"
)

func TestModelDiscoveryOmitsSuspendedIDsAndMappedAliases(t *testing.T) {
	mapping := map[string]any{
		"safe-looking-alias":   "omnihuman-1-5",
		"gpt-5.4":              "gpt-5.4",
		"wan/2-2-animate-move": "wan/2-2-animate-move",
	}
	for _, model := range append(append([]string{}, suspendedTestModels...), "infinitalk", "kling-ai-avatar", "wan-speech-to-video-turbo") {
		mapping[model] = model
	}
	account := Account{ID: 1, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": mapping}}
	repo := &modelsListAccountRepoStub{all: []Account{account}}
	svc := &GatewayService{accountRepo: repo, modelsListCache: gocache.New(time.Minute, time.Minute), modelsListCacheTTL: time.Minute, schedulerSnapshot: &SchedulerSnapshotService{cache: &modelsDiscoveryCacheStub{accounts: map[int64]*Account{1: &account}}}}
	for range 2 {
		require.Equal(t, []string{"gpt-5.4", "wan/2-2-animate-move"}, svc.GetAvailableModels(context.Background(), nil, ""))
	}
	require.Equal(t, int64(1), repo.listAllCalls.Load(), "filtered models remain cached")
	seeds := catalogAccountModels(&account)
	require.Len(t, seeds, 2)
	for _, seed := range seeds {
		require.Contains(t, []string{"gpt-5.4", "wan/2-2-animate-move"}, seed.name)
	}
	for model := range mapping {
		if model != "gpt-5.4" && model != "wan/2-2-animate-move" {
			require.False(t, catalogAccountSupportsModel(&account, model), model)
		}
	}
	require.Equal(t, []string{"gpt-5.4", "wan/2-2-animate-move"}, batchImageModelsFromAccountMapping(&account))

	// Already populated caches are filtered again without mutating their slices.
	cached := []string{"omnihuman-1-5", "infinitalk", "gpt-5.4"}
	svc.modelsListCache.Set(modelsListCacheKey(nil, ""), availableModelsCacheEntry{models: cached, accountIDs: []int64{1}, sources: map[string][]int64{"omnihuman-1-5": {1}, "infinitalk": {1}, "gpt-5.4": {1}}}, time.Minute)
	require.Equal(t, []string{"gpt-5.4"}, svc.GetAvailableModels(context.Background(), nil, ""))
	require.Len(t, cached, 3)
	require.Nil(t, FilterAvailableModelIDs(nil))
}

func TestPublicMediaCatalogPreservesInternalCatalog(t *testing.T) {
	internal := MediaTaskCatalog()
	public := PublicMediaTaskCatalog()
	require.NotEmpty(t, public)
	for _, model := range public {
		require.NoError(t, CheckModelAvailability(model.ModelID, model.Slug, model.upstream))
	}
	after := MediaTaskCatalog()
	require.Len(t, after, len(internal))
	for i := range internal {
		require.Equal(t, internal[i].ModelID, after[i].ModelID)
	}
}

func TestAvailableModelsWarmCacheRechecksMappedTargets(t *testing.T) {
	first := Account{ID: 1, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.4"}}}
	second := Account{ID: 2, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.4"}}}
	repo := &modelsListAccountRepoStub{all: []Account{first}}
	cache := &modelsDiscoveryCacheStub{accounts: map[int64]*Account{1: &first}}
	svc := &GatewayService{accountRepo: repo, modelsListCache: gocache.New(time.Minute, time.Minute), modelsListCacheTTL: time.Minute, schedulerSnapshot: &SchedulerSnapshotService{cache: cache}}
	require.Equal(t, []string{"alias"}, svc.GetAvailableModels(context.Background(), nil, ""))
	first.Credentials["model_mapping"] = map[string]any{"alias": "omnihuman-1-5"}
	require.Empty(t, svc.GetAvailableModels(context.Background(), nil, ""))
	require.Equal(t, int64(1), repo.listAllCalls.Load(), "warm validation uses account snapshots, not a full repository scan")

	// A suspended route must not hide a different safe serving account.
	svc.modelsListCache.Flush()
	first.Credentials["model_mapping"] = map[string]any{"alias": "gpt-5.4"}
	repo.all = []Account{first, second}
	cache.accounts[2] = &second
	require.Equal(t, []string{"alias"}, svc.GetAvailableModels(context.Background(), nil, ""))
	first.Credentials["model_mapping"] = map[string]any{"alias": "omnihuman-1-5"}
	require.Equal(t, []string{"alias"}, svc.GetAvailableModels(context.Background(), nil, ""))

	// An unavailable snapshot recomputes from current repository truth.
	cache.err = errors.New("snapshot unavailable")
	repo.all = []Account{first}
	require.Empty(t, svc.GetAvailableModels(context.Background(), nil, ""))
	require.Equal(t, int64(3), repo.listAllCalls.Load())
}

func TestAvailableModelsAllSuspendedMappingIsNonNilEmpty(t *testing.T) {
	repo := &modelsListAccountRepoStub{all: []Account{{ID: 1, Platform: PlatformGemini, Credentials: map[string]any{"model_mapping": map[string]any{"*": "omnihuman-1-5"}}}}}
	svc := &GatewayService{accountRepo: repo}
	models := svc.GetAvailableModels(context.Background(), nil, PlatformGemini)
	require.NotNil(t, models)
	require.Empty(t, models)
}

func TestListTextDelistsMappedRiskAliasButPreservesOtherUnavailableModels(t *testing.T) {
	now := time.Date(2026, time.October, 11, 12, 0, 0, 0, time.UTC)
	models := []SupportedModel{
		{Name: "safe-looking-alias", Platform: PlatformOpenAI},
		{Name: "shared-alias", Platform: PlatformOpenAI},
		{Name: "gpt-5.4", Platform: PlatformOpenAI},
		{Name: "offline-unknown", Platform: PlatformOpenAI},
	}
	catalog := &channelRoutingCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1, Platform: PlatformOpenAI}}, SupportedModels: models}}}
	access := &channelRoutingAccessFake{groups: []Group{{ID: 1, Platform: PlatformOpenAI, RateMultiplier: 1, Status: StatusActive}}}
	accounts := &modelCatalogAccountFake{byGroup: map[int64][]Account{
		1: {
			{ID: 1, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"safe-looking-alias": "omnihuman-1-5", "shared-alias": "omnihuman-1-5", "gpt-5.4": "gpt-5.4"}}},
			{ID: 2, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"shared-alias": "gpt-5.4"}}},
		},
		2: {{ID: 3, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"safe-looking-alias": "gpt-5.4"}}}},
	}}
	svc := &ModelCatalogService{
		channels: catalog,
		selector: NewChannelRoutingSelector(catalog, access, channelRoutingConfig(true, 3)),
		pricing:  &modelCatalogPricingFake{byGroup: map[int64]*ResolvedPricing{1: {Mode: BillingModeToken, BasePricing: &ModelPricing{InputPricePerToken: 1e-6}}}},
		accounts: accounts,
	}
	items, err := svc.ListText(context.Background(), 42, now)
	require.NoError(t, err)
	byName := make(map[string]TextModelCatalogItem)
	for _, item := range items {
		byName[item.Name] = item
	}
	require.NotContains(t, byName, "safe-looking-alias")
	require.True(t, byName["shared-alias"].Available)
	require.True(t, byName["gpt-5.4"].Available)
	require.Contains(t, byName, "offline-unknown")
	require.False(t, byName["offline-unknown"].Available)

	// Disabled user routes must not reintroduce the removed alias as unavailable.
	svc.selector.groupPreferences = &groupRoutingPreferencesFake{disabled: []int64{1}}
	offline, err := svc.ListText(context.Background(), 42, now)
	require.NoError(t, err)
	for _, item := range offline {
		require.NotEqual(t, "safe-looking-alias", item.Name)
	}

	// Public channel sections use only the user's visible serving groups.
	filtered, err := svc.FilterPublicSupportedModels(context.Background(), models, []int64{1})
	require.NoError(t, err)
	var names []string
	for _, model := range filtered {
		names = append(names, model.Name)
	}
	require.ElementsMatch(t, []string{"shared-alias", "gpt-5.4", "offline-unknown"}, names)
}

func TestPublicFallbackCandidatesHonorWildcardRiskExceptions(t *testing.T) {
	account := Account{ID: 1, Platform: PlatformAntigravity, Credentials: map[string]any{"model_mapping": map[string]any{"*": "gemini-2.5-flash", "gemini-3-pro": "omnihuman-1-5"}}}
	repo := &modelsListAccountRepoStub{all: []Account{account}}
	cache := &modelsDiscoveryCacheStub{accounts: map[int64]*Account{1: &account}}
	svc := &GatewayService{accountRepo: repo, modelsListCache: gocache.New(time.Minute, time.Minute), modelsListCacheTTL: time.Minute, schedulerSnapshot: &SchedulerSnapshotService{cache: cache}}
	for range 2 {
		require.Equal(t, []string{"gemini-2.5-flash"}, svc.FilterPublicModelCandidates(context.Background(), nil, PlatformAntigravity, []string{"gemini-3-pro", "gemini-2.5-flash"}))
	}
	require.Equal(t, int64(1), repo.listAllCalls.Load())
}
