//go:build unit

package handler

import (
	"bytes"
	"context"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type bindingPreferenceRepo struct {
	service.ChannelPreferenceRepository
	choices map[string]string
}

type bindingUsageBilling struct {
	service.UsageBillingRepository
	commands []*service.UsageBillingCommand
}

type bindingUsageLogs struct {
	service.UsageLogRepository
	logs []*service.UsageLog
}

func (r *bindingUsageLogs) Create(_ context.Context, log *service.UsageLog) (bool, error) {
	copyLog := *log
	r.logs = append(r.logs, &copyLog)
	return true, nil
}

type bindingAvailability struct{ supportedGroup int64 }

func (a bindingAvailability) DiagnoseModelAvailabilityForPlatform(_ context.Context, group *int64, _, _ string) service.ModelAvailabilityDiagnosis {
	return service.ModelAvailabilityDiagnosis{HasAccountsInPool: true, HasModelSupport: group != nil && *group == a.supportedGroup}
}

func TestModelChannelBindingUsesFullAuthorizationAndSkipsUnusableGroups(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	h.cfg.Gateway.ChannelRoutingMaxCandidates = 1
	repo := &bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelCloudVendor}}
	attachBindingPreferences(h, repo)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	require.NoError(t, modelChannelGroupAllowed(c.Request.Context(), h.channelRoutingSelector, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, 202), "authorized existing group must survive a ranking change or cap")
	bound, err := bindFirstModelChannel(c, h.channelRoutingSelector, h.apiKeyService, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, bindingAvailability{supportedGroup: 202})
	require.NoError(t, err)
	require.Equal(t, int64(202), *bound.GroupID, "a cheaper group with no matching account must not hide an available route")
	_, err = bindFirstModelChannel(c, h.channelRoutingSelector, h.apiKeyService, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, bindingAvailability{supportedGroup: 999})
	require.Error(t, err)
}

func TestModelChannelBindingSkipsMissingSubscriptionWithoutProviderCall(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	h.cfg.Gateway.ChannelRoutingMaxCandidates = 1
	h.SetChannelRoutingSelector(service.ProvideChannelRoutingSelector(
		channelRoutingHandlerCatalog{channels: []service.AvailableChannel{
			{ID: 10, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 101}}},
			{ID: 20, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 202}}},
		}},
		channelRoutingHandlerAccess{groups: []service.Group{
			{ID: 101, Platform: service.PlatformOpenAI, RateMultiplier: 0.2, Status: service.StatusActive, SubscriptionType: service.SubscriptionTypeSubscription},
			{ID: 202, Platform: service.PlatformOpenAI, RateMultiplier: 1, Status: service.StatusActive},
		}},
		service.NewChannelPreferenceService(&bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelCloudVendor}}, nil, nil, nil), h.cfg, nil,
	))
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	bound, err := bindFirstModelChannel(c, h.channelRoutingSelector, nil, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI)
	require.NoError(t, err)
	require.Equal(t, int64(202), *bound.GroupID)
	_, err = bindFirstModelChannel(c, nil, nil, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI)
	require.Error(t, err, "channel mode must fail closed when the selector is missing")
}

func (r *bindingUsageBilling) Apply(_ context.Context, command *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	copyCommand := *command
	r.commands = append(r.commands, &copyCommand)
	return &service.UsageBillingApplyResult{Applied: false}, nil
}

func (r *bindingPreferenceRepo) GetUserDisabledGroupIDs(context.Context, int64) ([]int64, error) {
	return nil, nil
}
func (r *bindingPreferenceRepo) GetUserModelChannelPreference(_ context.Context, _ int64, model string) (string, error) {
	return r.choices[model], nil
}

func attachBindingPreferences(h *OpenAIGatewayHandler, repo *bindingPreferenceRepo) {
	h.SetChannelRoutingSelector(service.ProvideChannelRoutingSelector(
		channelRoutingHandlerCatalog{channels: []service.AvailableChannel{
			{ID: 10, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 101}}},
			{ID: 20, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 202}}},
		}},
		channelRoutingHandlerAccess{groups: []service.Group{
			{ID: 101, Platform: service.PlatformOpenAI, RateMultiplier: 0.2, Status: service.StatusActive, AllowMessagesDispatch: true},
			{ID: 202, Platform: service.PlatformOpenAI, RateMultiplier: 1, Status: service.StatusActive, AllowMessagesDispatch: true},
		}},
		service.NewChannelPreferenceService(repo, nil, nil, nil), h.cfg, nil,
	))
}

func TestModelChannelBindingPreservesBillingGroupAndChecksEveryTurn(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	repo := &bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelOfficial}}
	attachBindingPreferences(h, repo)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Set(string(middleware.ContextKeySubscription), &service.UserSubscription{GroupID: 101})
	bound, err := bindFirstModelChannel(c, h.channelRoutingSelector, h.apiKeyService, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI)
	require.NoError(t, err)
	require.Equal(t, int64(202), *bound.GroupID)
	require.Equal(t, float64(1), bound.Group.RateMultiplier)
	require.Equal(t, bound.Group, c.Request.Context().Value(ctxkey.Group))
	contextKey, ok := middleware.GetAPIKeyFromContext(c)
	require.True(t, ok)
	require.Same(t, bound, contextKey)
	subscription, _ := middleware.GetSubscriptionFromContext(c)
	require.Nil(t, subscription)
	require.Equal(t, int64(101), *key.GroupID, "auth-cache key must not be mutated")
	require.Equal(t, key.UserID, bound.UserID)
	require.Error(t, modelChannelGroupAllowed(c.Request.Context(), h.channelRoutingSelector, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, 101))
	require.NoError(t, modelChannelGroupAllowed(c.Request.Context(), h.channelRoutingSelector, key, "gpt-5.2", service.ChannelRoutingFamilyOpenAI, 101))
	delete(repo.choices, "gpt-5.1")
	require.NoError(t, modelChannelGroupAllowed(c.Request.Context(), h.channelRoutingSelector, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, 101))
	repo.choices["gpt-5.1"] = service.ModelChannelOfficial
	require.Error(t, modelChannelGroupAllowed(c.Request.Context(), h.channelRoutingSelector, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, 101), "an established WS or native fallback must recheck saved choices")
}

func TestModelChannelPreferenceControlsActualOpenAIUpstream(t *testing.T) {
	for _, endpoint := range []string{"responses", "chat/completions", "messages"} {
		t.Run(endpoint, func(t *testing.T) {
			h, upstream, key := newChannelRoutingOpenAIHandler(t, endpoint != "responses", false, false)
			key.User.BillingCurrency = service.CurrencyUSD
			// Production scheduling respects groups; simple mode intentionally
			// scans every account on a platform and cannot prove isolation.
			standard := *h.cfg
			standard.RunMode = config.RunModeStandard
			usage := &bindingUsageBilling{}
			logs := &bindingUsageLogs{}
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			t.Cleanup(func() { _ = redisClient.Close() })
			cache := repository.NewGatewayCache(redisClient)
			require.NoError(t, cache.(service.CanonicalWalletLeaseStore).InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: "routing-test-lease", PlatformUserID: "routing-test-user", Currency: service.CurrencyUSD, BudgetUnits: 100_000_000_000, ExpiresAt: time.Now().Add(time.Hour)}))
			pricing := service.NewBillingService(&standard, nil)
			snapshots := service.NewBillingSnapshotService(&standard, service.NewModelPricingResolver(nil, pricing), pricing, service.NewUSDPriceService(&standard), nil)
			h.gatewayService = service.NewOpenAIGatewayService(
				channelRoutingAccountRepo{accounts: []service.Account{
					{ID: 1, Name: "cloud", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{101}, Credentials: map[string]any{"api_key": "cloud"}},
					{ID: 2, Name: "official", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{202}, Credentials: map[string]any{"api_key": "official"}},
				}}, logs, usage, nil, nil, nil, cache, &standard, nil, nil, nil, service.NewConcurrencyService(repository.NewConcurrencyCache(redisClient, 5, 30)),
				pricing, nil, nil, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, snapshots,
			)
			repo := &bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelOfficial}}
			attachBindingPreferences(h, repo)
			for _, model := range []string{"gpt-5.1", "gpt-5.2"} {
				body := `{"model":"` + model + `","max_tokens":100,"stream":false,"input":"hello","messages":[{"role":"user","content":"hello"}]}`
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, bytes.NewBufferString(body))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Set(string(middleware.ContextKeyAPIKey), key)
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 100, Concurrency: 10})
				switch endpoint {
				case "responses":
					h.Responses(c)
				case "messages":
					h.Messages(c)
				default:
					h.ChatCompletions(c)
				}
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			}
			require.Equal(t, []int64{2, 1}, upstream.accountHits(), "switching one model must not reroute another model in the same group")
			require.Len(t, usage.commands, 2)
			require.Equal(t, int64(2), usage.commands[0].AccountID)
			require.Equal(t, int64(1), usage.commands[1].AccountID)
			require.Zero(t, usage.commands[0].BalanceCost)
			require.Zero(t, usage.commands[1].BalanceCost)
			require.Len(t, logs.logs, 2)
			require.Greater(t, logs.logs[0].ActualCost, logs.logs[1].ActualCost)
			require.Equal(t, int64(202), *logs.logs[0].GroupID)
			require.Equal(t, int64(101), *logs.logs[1].GroupID)
			require.Equal(t, float64(1), logs.logs[0].RateMultiplier)
			require.Equal(t, float64(0.2), logs.logs[1].RateMultiplier)
		})
	}
}

type bindingSubscriptionLookup struct {
	service.UserSubscriptionRepository
	err  error
	seen []int64
}

func (r *bindingSubscriptionLookup) GetActiveByUserIDAndGroupID(_ context.Context, user, group int64) (*service.UserSubscription, error) {
	r.seen = append(r.seen, group)
	return nil, r.err
}
func TestModelChannelBindingSkipsKnownMissingSubscriptionAndFailsClosedOnStoreError(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		wantFallback bool
	}{
		{"known_missing", service.ErrSubscriptionNotFound, true},
		{"unknown_store_failure", errors.New("isolated synthetic store unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
			key.RoutingMode = service.APIKeyRoutingModeAutoChannels
			key.ChannelIDs = nil
			h.cfg.Gateway.ChannelRoutingMaxCandidates = 1
			h.SetChannelRoutingSelector(service.ProvideChannelRoutingSelector(channelRoutingHandlerCatalog{}, channelRoutingHandlerAccess{groups: []service.Group{
				{ID: 101, Platform: service.PlatformOpenAI, RateMultiplier: 0.2, Status: service.StatusActive, SubscriptionType: service.SubscriptionTypeSubscription},
				{ID: 202, Platform: service.PlatformOpenAI, RateMultiplier: 1, Status: service.StatusActive},
			}}, service.NewChannelPreferenceService(&bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelCloudVendor}}, nil, nil, nil), h.cfg, nil))
			subRepo := &bindingSubscriptionLookup{err: tc.err}
			keys := service.NewAPIKeyService(nil, nil, nil, subRepo, nil, nil, h.cfg)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			bound, err := bindFirstModelChannel(c, h.channelRoutingSelector, keys, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI)
			require.Equal(t, []int64{101}, subRepo.seen)
			if tc.wantFallback {
				require.NoError(t, err, "known subscription absence must skip unusable group101 and reach valid202")
				require.Equal(t, int64(202), *bound.GroupID)
			} else {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, bound)
			}
		})
	}
}

func TestModelChannelBindingDiagnosesCompositeTargetWithinSelectedGroup(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	h.cfg.RunMode = config.RunModeStandard
	key.RoutingMode = service.APIKeyRoutingModeAutoChannels
	key.ChannelIDs = nil
	group := service.Group{ID: 303, Platform: service.PlatformComposite, Status: service.StatusActive, RateMultiplier: 1}
	selector := service.ProvideChannelRoutingSelector(channelRoutingHandlerCatalog{}, channelRoutingHandlerAccess{groups: []service.Group{group}}, service.NewChannelPreferenceService(&bindingPreferenceRepo{choices: map[string]string{"claude-a": service.ModelChannelOfficial}}, nil, nil, nil), h.cfg, nil)
	accountRepo := channelRoutingAccountRepo{accounts: []service.Account{{ID: 33, Platform: service.PlatformAnthropic, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{303}, Credentials: map[string]any{"model_mapping": map[string]any{"claude-a": "claude-a"}}}}}
	// This service only diagnoses catalogue access; it performs no upstream writes.
	diagnosisCfg := *h.cfg
	diagnosisCfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	diagnoser := service.NewGatewayService(accountRepo, nil, nil, nil, nil, nil, nil, nil, &diagnosisCfg, nil, nil, nil, nil, service.NewBillingService(&diagnosisCfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	bound, err := bindFirstModelChannel(c, selector, h.apiKeyService, key, "claude-a", service.ChannelRoutingFamilyAnthropic, diagnoser)
	require.NoError(t, err, "allowed composite group has an actual active Anthropic account serving this exact model")
	require.Equal(t, int64(303), *bound.GroupID)
	platform, ok := service.ResolvedTargetPlatformFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, service.PlatformAnthropic, platform)
}

func TestModelChannelBindingDiagnosesGeminiCompositeTargetWithinSelectedGroup(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	h.cfg.RunMode = config.RunModeStandard
	key.RoutingMode = service.APIKeyRoutingModeAutoChannels
	key.ChannelIDs = nil
	group := service.Group{ID: 303, Platform: service.PlatformComposite, Status: service.StatusActive, RateMultiplier: 1}
	selector := service.ProvideChannelRoutingSelector(channelRoutingHandlerCatalog{}, channelRoutingHandlerAccess{groups: []service.Group{group}}, service.NewChannelPreferenceService(&bindingPreferenceRepo{choices: map[string]string{"gemini-a": service.ModelChannelOfficial}}, nil, nil, nil), h.cfg, nil)
	accountRepo := channelRoutingAccountRepo{accounts: []service.Account{{ID: 33, Platform: service.PlatformGemini, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{303}, Credentials: map[string]any{"model_mapping": map[string]any{"gemini-a": "gemini-a"}}}}}
	// This service only diagnoses catalogue access; it performs no upstream writes.
	diagnosisCfg := *h.cfg
	diagnosisCfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	diagnoser := service.NewGatewayService(accountRepo, nil, nil, nil, nil, nil, nil, nil, &diagnosisCfg, nil, nil, nil, nil, service.NewBillingService(&diagnosisCfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	bound, err := bindFirstModelChannel(c, selector, h.apiKeyService, key, "gemini-a", service.ChannelRoutingFamilyAnthropic, diagnoser)
	require.NoError(t, err, "allowed composite group has an actual active Gemini account serving this exact model")
	require.Equal(t, int64(303), *bound.GroupID)
	platform, ok := service.ResolvedTargetPlatformFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, service.PlatformGemini, platform)
}

func TestModelChannelBindingDiagnosesOpenAICompositeTargetWithinSelectedGroup(t *testing.T) {
	h, _, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	h.cfg.RunMode = config.RunModeStandard
	key.RoutingMode = service.APIKeyRoutingModeAutoChannels
	key.ChannelIDs = nil
	group := service.Group{ID: 303, Platform: service.PlatformComposite, Status: service.StatusActive, RateMultiplier: 1}
	selector := service.ProvideChannelRoutingSelector(channelRoutingHandlerCatalog{}, channelRoutingHandlerAccess{groups: []service.Group{group}}, service.NewChannelPreferenceService(&bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelOfficial}}, nil, nil, nil), h.cfg, nil)
	accountRepo := channelRoutingAccountRepo{accounts: []service.Account{{ID: 33, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{303}, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-5.1": "gpt-5.1"}}}}}
	// This service only diagnoses catalogue access; it performs no upstream writes.
	diagnosisCfg := *h.cfg
	diagnosisCfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	diagnoser := service.NewOpenAIGatewayService(accountRepo, nil, nil, nil, nil, nil, nil, &diagnosisCfg, nil, nil, nil, nil, service.NewBillingService(&diagnosisCfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	bound, err := bindFirstModelChannel(c, selector, h.apiKeyService, key, "gpt-5.1", service.ChannelRoutingFamilyOpenAI, diagnoser)
	require.NoError(t, err, "allowed composite group has an actual active OpenAI account serving this exact model")
	require.Equal(t, int64(303), *bound.GroupID)
	platform, ok := service.ResolvedTargetPlatformFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, service.PlatformOpenAI, platform)
}

func TestModelChannelPreferenceCompositeControlsActualOpenAIUpstream(t *testing.T) {
	for _, endpoint := range []string{"responses", "chat/completions", "messages"} {
		t.Run(endpoint, func(t *testing.T) {
			h, upstream, key := newChannelRoutingOpenAIHandler(t, endpoint != "responses", false, false)
			key.User.BillingCurrency = service.CurrencyUSD
			// Production scheduling respects groups; simple mode intentionally
			// scans every account on a platform and cannot prove isolation.
			standard := *h.cfg
			standard.RunMode = config.RunModeStandard
			usage := &bindingUsageBilling{}
			logs := &bindingUsageLogs{}
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			t.Cleanup(func() { _ = redisClient.Close() })
			cache := repository.NewGatewayCache(redisClient)
			require.NoError(t, cache.(service.CanonicalWalletLeaseStore).InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: "routing-test-lease", PlatformUserID: "routing-test-user", Currency: service.CurrencyUSD, BudgetUnits: 100_000_000_000, ExpiresAt: time.Now().Add(time.Hour)}))
			pricing := service.NewBillingService(&standard, nil)
			snapshots := service.NewBillingSnapshotService(&standard, service.NewModelPricingResolver(nil, pricing), pricing, service.NewUSDPriceService(&standard), nil)
			h.gatewayService = service.NewOpenAIGatewayService(
				channelRoutingAccountRepo{accounts: []service.Account{
					{ID: 1, Name: "cloud", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{101}, Credentials: map[string]any{"api_key": "cloud"}},
					{ID: 2, Name: "official", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{202}, Credentials: map[string]any{"api_key": "official"}},
				}}, logs, usage, nil, nil, nil, cache, &standard, nil, nil, nil, service.NewConcurrencyService(repository.NewConcurrencyCache(redisClient, 5, 30)),
				pricing, nil, nil, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, snapshots,
			)
			repo := &bindingPreferenceRepo{choices: map[string]string{"gpt-5.1": service.ModelChannelOfficial}}
			h.SetChannelRoutingSelector(service.ProvideChannelRoutingSelector(channelRoutingHandlerCatalog{channels: []service.AvailableChannel{{ID: 10, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 101}}}, {ID: 20, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: 202}}}}}, channelRoutingHandlerAccess{groups: []service.Group{{ID: 101, Platform: service.PlatformOpenAI, RateMultiplier: 0.2, Status: service.StatusActive, AllowMessagesDispatch: true}, {ID: 202, Platform: service.PlatformComposite, RateMultiplier: 1, Status: service.StatusActive, AllowMessagesDispatch: true}}}, service.NewChannelPreferenceService(repo, nil, nil, nil), h.cfg, nil))
			for _, model := range []string{"gpt-5.1", "gpt-5.2"} {
				body := `{"model":"` + model + `","max_tokens":100,"stream":false,"input":"hello","messages":[{"role":"user","content":"hello"}]}`
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, bytes.NewBufferString(body))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Set(string(middleware.ContextKeyAPIKey), key)
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 100, Concurrency: 10})
				switch endpoint {
				case "responses":
					h.Responses(c)
				case "messages":
					h.Messages(c)
				default:
					h.ChatCompletions(c)
				}
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			}
			require.Equal(t, []int64{2, 1}, upstream.accountHits(), "switching one model must not reroute another model in the same group")
			require.Len(t, usage.commands, 2)
			require.Equal(t, int64(2), usage.commands[0].AccountID)
			require.Equal(t, int64(1), usage.commands[1].AccountID)
			require.Zero(t, usage.commands[0].BalanceCost)
			require.Zero(t, usage.commands[1].BalanceCost)
			require.Len(t, logs.logs, 2)
			require.Greater(t, logs.logs[0].ActualCost, logs.logs[1].ActualCost)
			require.Equal(t, int64(202), *logs.logs[0].GroupID)
			require.Equal(t, int64(101), *logs.logs[1].GroupID)
			require.Equal(t, float64(1), logs.logs[0].RateMultiplier)
			require.Equal(t, float64(0.2), logs.logs[1].RateMultiplier)
		})
	}
}
