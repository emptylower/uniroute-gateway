//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type channelRoutingHandlerCatalog struct {
	channels []service.AvailableChannel
}

func (f channelRoutingHandlerCatalog) ListAvailable(context.Context) ([]service.AvailableChannel, error) {
	return f.channels, nil
}

func (f channelRoutingHandlerCatalog) IsModelRestricted(context.Context, int64, string) bool {
	return false
}

type channelRoutingHandlerAccess struct {
	groups []service.Group
}

func (f channelRoutingHandlerAccess) GetAvailableGroups(context.Context, int64) ([]service.Group, error) {
	return f.groups, nil
}

func (f channelRoutingHandlerAccess) GetUserGroupRates(context.Context, int64) (map[int64]float64, error) {
	return nil, nil
}

type channelRoutingAccountRepo struct {
	service.AccountRepository
	accounts []service.Account
}

type channelRoutingAPIKeyAuthRepo struct {
	service.APIKeyRepository
	key *service.APIKey
}

func (r channelRoutingAPIKeyAuthRepo) GetByKeyForAuth(_ context.Context, key string) (*service.APIKey, error) {
	if r.key == nil || r.key.Key != key {
		return nil, service.ErrAPIKeyNotFound
	}
	clone := *r.key
	return &clone, nil
}

func (r channelRoutingAPIKeyAuthRepo) UpdateLastUsed(context.Context, int64, time.Time) error {
	return nil
}

func (r channelRoutingAccountRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			account := r.accounts[i]
			return &account, nil
		}
	}
	return nil, service.ErrNoAvailableAccounts
}

func (r channelRoutingAccountRepo) ListSchedulableByGroupIDAndPlatform(_ context.Context, groupID int64, platform string) ([]service.Account, error) {
	return r.forGroup(groupID, platform), nil
}

func (r channelRoutingAccountRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.forGroup(0, platform), nil
}

func (r channelRoutingAccountRepo) ListModelAvailabilityCandidates(_ context.Context, groupID *int64, platforms []string, _ bool) ([]service.Account, error) {
	var id int64
	if groupID != nil {
		id = *groupID
	}
	var accounts []service.Account
	for _, platform := range platforms {
		accounts = append(accounts, r.forGroup(id, platform)...)
	}
	return accounts, nil
}

func (r channelRoutingAccountRepo) ListSchedulableUngroupedByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.forGroup(0, platform), nil
}

func (r channelRoutingAccountRepo) forGroup(groupID int64, platform string) []service.Account {
	out := make([]service.Account, 0, len(r.accounts))
	for _, account := range r.accounts {
		if account.Platform != platform {
			continue
		}
		if groupID != 0 {
			matched := false
			for _, id := range account.GroupIDs {
				matched = matched || id == groupID
			}
			if !matched {
				continue
			}
		}
		out = append(out, account)
	}
	return out
}

type channelRoutingHTTPUpstream struct {
	service.HTTPUpstream
	mu            sync.Mutex
	hits          []int64
	streamSuccess bool
	failCheap     bool
	partialCheap  bool
}

func (u *channelRoutingHTTPUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.hits = append(u.hits, accountID)
	u.mu.Unlock()
	if accountID == 1 && u.partialCheap {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(bytes.NewBufferString(
				"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\",\"model\":\"gpt-5.1\",\"status\":\"in_progress\",\"output\":[]}}\n\n" +
					"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_partial\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
					"data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
					"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}\n\n",
			)),
		}, nil
	}
	if accountID == 1 && u.failCheap {
		return &http.Response{
			StatusCode: 520,
			Header:     http.Header{"Content-Type": []string{"text/html"}},
			Body:       io.NopCloser(bytes.NewBufferString("<html>temporary upstream failure</html>")),
		}, nil
	}
	if u.streamSuccess {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(bytes.NewBufferString(
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_channel_ok\",\"model\":\"gpt-5.1\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
			)),
		}, nil
	}
	if req.URL.Path == "/v1/chat/completions" || req.URL.Path == "/chat/completions" {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(bytes.NewBufferString(
				`{"id":"chatcmpl_channel_ok","object":"chat.completion","model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			)),
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(bytes.NewBufferString(
			`{"id":"resp_channel_ok","object":"response","model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`,
		)),
	}, nil
}

func (u *channelRoutingHTTPUpstream) accountHits() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.hits...)
}

func newChannelRoutingOpenAIHandler(t *testing.T, streamSuccess, failCheap, partialCheap bool) (*OpenAIGatewayHandler, *channelRoutingHTTPUpstream, *service.APIKey) {
	t.Helper()
	cheap := service.Group{ID: 101, Platform: service.PlatformOpenAI, RateMultiplier: 0.2, Status: service.StatusActive}
	stable := service.Group{ID: 202, Platform: service.PlatformOpenAI, RateMultiplier: 1, Status: service.StatusActive}
	accounts := []service.Account{
		{ID: 1, Name: "cheap", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{cheap.ID}, Credentials: map[string]any{"api_key": "cheap"}},
		{ID: 2, Name: "stable", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{stable.ID}, Credentials: map[string]any{"api_key": "stable"}},
	}
	upstream := &channelRoutingHTTPUpstream{streamSuccess: streamSuccess, failCheap: failCheap, partialCheap: partialCheap}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.Holds = "on"
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"usd_wallet_policy_version":"usd-wallet-v1","lease_id":"routing-test-lease","platform_user_id":"routing-test-user","currency":"USD","unit_version":"usd-e8-v1","scale":8,"budget":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"reserved":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"captured":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"released":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"headroom":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"capture_seq":0,"status":"active","expires_at":%q,"outcome":"reused","clamped_by":"none"}}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	t.Cleanup(control.Close)
	cfg.CanonicalWallet.ControlPlaneURL = control.URL
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cache := repository.NewGatewayCache(redisClient)
	store := cache.(service.CanonicalWalletLeaseStore)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: "routing-test-lease", PlatformUserID: "routing-test-user", Currency: service.CurrencyUSD, BudgetUnits: 100_000_000_000, ExpiresAt: time.Now().Add(time.Hour)}))
	cfg.Gateway.ChannelRoutingEnabled = true
	cfg.Gateway.ChannelRoutingMaxCandidates = 3
	pricing := service.NewBillingService(cfg, nil)
	snapshots := service.NewBillingSnapshotService(cfg, service.NewModelPricingResolver(nil, pricing), pricing, service.NewUSDPriceService(cfg), nil)
	gateway := service.NewOpenAIGatewayService(
		channelRoutingAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, cfg, nil, nil,
		nil, service.NewConcurrencyService(repository.NewConcurrencyCache(redisClient, 5, 30)), pricing, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil,
		snapshots)
	bridge := service.NewCanonicalWalletBridge(cfg, store, nil, nil)
	t.Cleanup(bridge.Close)
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil, bridge)
	t.Cleanup(billing.Stop)
	apiKeys := service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(repository.NewConcurrencyCache(redisClient, 5, 30)), billing, apiKeys, nil, nil, nil, nil, cfg)
	h.maxAccountSwitches = 1
	h.SetChannelRoutingSelector(service.NewChannelRoutingSelector(
		channelRoutingHandlerCatalog{channels: []service.AvailableChannel{
			{ID: 10, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: cheap.ID}}},
			{ID: 20, Status: service.StatusActive, Groups: []service.AvailableGroupRef{{ID: stable.ID}}},
		}},
		channelRoutingHandlerAccess{groups: []service.Group{cheap, stable}},
		cfg,
	))
	key := &service.APIKey{
		ID: 99, UserID: 100, GroupID: &cheap.ID, Group: &cheap,
		RoutingMode: service.APIKeyRoutingModeChannels, ChannelIDs: []int64{10, 20},
		User: &service.User{ID: 100, PlatformUserID: "routing-test-user", BillingCurrency: service.CurrencyUSD},
	}
	return h, upstream, key
}

func TestOpenAIChannelRouting_FailsOverAcrossGroupsForSupportedEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name          string
		path          string
		body          string
		streamSuccess bool
		run           func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{name: "responses", path: "/v1/responses", body: `{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":false,"input":"hello"}`, run: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) }},
		{name: "chat completions", path: "/v1/chat/completions", body: `{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":false,"messages":[{"role":"user","content":"hello"}]}`, streamSuccess: true, run: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, upstream, key := newChannelRoutingOpenAIHandler(t, tt.streamSuccess, true, false)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tt.path, bytes.NewBufferString(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Set(string(middleware2.ContextKeyAPIKey), key)
			c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 10})

			tt.run(h, c)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, []int64{1, 2}, upstream.accountHits())
		})
	}
}

func TestOpenAIChannelRouting_CheapSuccessDoesNotCallStableGroup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, upstream, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(`{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":false,"input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware2.ContextKeyAPIKey), key)
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 10})

	h.Responses(c)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []int64{1}, upstream.accountHits(), rec.Body.String())
}

func TestOpenAIAutomaticRouting_DeletedCompatibilityAnchorReachesLiveCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, upstream, key := newChannelRoutingOpenAIHandler(t, false, false, false)
	deletedGroupID := int64(999)
	key.Key = "dynamic-auth-key"
	key.Status = service.StatusActive
	key.RoutingMode = service.APIKeyRoutingModeAutoChannels
	key.ChannelIDs = nil
	key.GroupID = &deletedGroupID
	key.Group = nil
	key.User = &service.User{ID: key.UserID, PlatformUserID: "routing-test-user", BillingCurrency: service.CurrencyUSD, Role: service.RoleAdmin, Status: service.StatusActive}

	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.Holds = "on"
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"usd_wallet_policy_version":"usd-wallet-v1","lease_id":"routing-test-lease","platform_user_id":"routing-test-user","currency":"USD","unit_version":"usd-e8-v1","scale":8,"budget":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"reserved":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"captured":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"released":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"headroom":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"capture_seq":0,"status":"active","expires_at":%q,"outcome":"reused","clamped_by":"none"}}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	t.Cleanup(control.Close)
	cfg.CanonicalWallet.ControlPlaneURL = control.URL
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cache := repository.NewGatewayCache(redisClient)
	store := cache.(service.CanonicalWalletLeaseStore)
	require.NoError(t, store.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: "routing-test-lease", PlatformUserID: "routing-test-user", Currency: service.CurrencyUSD, BudgetUnits: 100_000_000_000, ExpiresAt: time.Now().Add(time.Hour)}))
	cfg.Gateway.ChannelRoutingEnabled = true
	authService := service.NewAPIKeyService(channelRoutingAPIKeyAuthRepo{key: key}, nil, nil, nil, nil, nil, cfg)
	router := gin.New()
	router.Use(gin.HandlerFunc(middleware2.NewAPIKeyAuthMiddleware(authService, nil, cfg)))
	router.POST("/responses", h.Responses)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/responses", bytes.NewBufferString(`{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":false,"input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key.Key)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, []int64{1}, upstream.accountHits())
}

func TestOpenAIChannelRouting_StreamDoesNotReplayAfterFirstEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, upstream, key := newChannelRoutingOpenAIHandler(t, true, false, true)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(`{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":true,"input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware2.ContextKeyAPIKey), key)
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 10})

	h.Responses(c)

	require.Equal(t, []int64{1}, upstream.accountHits(), rec.Body.String())
	require.Contains(t, rec.Body.String(), "partial")
}

func TestOpenAIChannelRouting_StreamCanFailOverBeforeFirstEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, upstream, key := newChannelRoutingOpenAIHandler(t, true, true, false)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(`{"model":"gpt-5.1","max_output_tokens":32,"max_tokens":32,"stream":true,"input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware2.ContextKeyAPIKey), key)
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 10})

	h.Responses(c)

	require.Equal(t, []int64{1, 2}, upstream.accountHits())
	require.Contains(t, rec.Body.String(), "response.completed")
}

func TestRecoverableChannelBillingError_OnlyCandidateScopedFailures(t *testing.T) {
	for _, err := range []error{
		service.ErrSubscriptionInvalid,
		service.ErrDailyLimitExceeded,
		service.ErrWeeklyLimitExceeded,
		service.ErrMonthlyLimitExceeded,
		service.ErrGroupRPMExceeded,
	} {
		require.True(t, isRecoverableChannelBillingError(err), err)
	}
	for _, err := range []error{
		service.ErrInsufficientBalance,
		service.ErrBillingServiceUnavailable,
		service.ErrUserRPMExceeded,
		service.ErrAPIKeyRateLimit1dExceeded,
	} {
		require.False(t, isRecoverableChannelBillingError(err), err)
	}
}
