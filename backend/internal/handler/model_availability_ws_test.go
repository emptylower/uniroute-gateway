package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestModelSuspensionWebSocketRequestedBillingCannotHideChannelOrAccountMapping(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		for _, accountAlias := range []bool{false, true} {
			name := mode + "/channel"
			mapped := "kling/ai-avatar-pro"
			var accountMapping map[string]any
			if accountAlias {
				name = mode + "/account"
				mapped = "avatar-account-alias"
				accountMapping = map[string]any{"gpt-5.6-sol": "gpt-5.6-sol", mapped: "kling/ai-avatar-pro"}
			}
			t.Run(name, func(t *testing.T) {
				got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
					firstPayload:        `{"type":"response.create","model":"gpt-5.6-sol","max_output_tokens":32,"stream":false}`,
					secondPayload:       `{"type":"response.create","model":"gpt-5.6-terra","max_output_tokens":32,"stream":false}`,
					ingressMode:         mode,
					billingModelSource:  service.BillingModelSourceRequested,
					channelMapping:      map[string]string{"gpt-5.6-terra": mapped},
					accountModelMapping: accountMapping,
					suspendedSecondTurn: true,
				})
				require.Len(t, got.upstreamPayloads, 1, "the suspended turn must never reach the provider")
				require.Len(t, got.logs, 1, "only the allowed first turn may be billed")
			})
		}
	}
}

type modelAvailabilitySelectionRepo struct {
	*openAIWSFailoverHandlerAccountRepoStub
	selections int
}

func (r *modelAvailabilitySelectionRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]service.Account, error) {
	r.selections++
	if r.selections > 1 {
		return nil, service.ErrNoChannelRoutingCandidate
	}
	return r.openAIWSFailoverHandlerAccountRepoStub.ListSchedulableByGroupIDAndPlatform(ctx, groupID, platform)
}

func (r *modelAvailabilitySelectionRepo) ListSchedulableByPlatform(ctx context.Context, platform string) ([]service.Account, error) {
	r.selections++
	if r.selections > 1 {
		return nil, service.ErrNoChannelRoutingCandidate
	}
	return r.openAIWSFailoverHandlerAccountRepoStub.ListSchedulableByPlatform(ctx, platform)
}

func (r *modelAvailabilitySelectionRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

func TestModelSuspensionResponsesAccountAliasTerminatesAfterOneSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	accountRepo := &modelAvailabilitySelectionRepo{openAIWSFailoverHandlerAccountRepoStub: &openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{{
		ID: 9910, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Priority: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.example.test", "model_mapping": map[string]any{"gpt-5.2": "kling/ai-avatar-pro"}},
		Extra:       map[string]any{"openai_passthrough": true},
	}}}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	upstream := &openAIHTTPPassthroughFailoverUpstream{}
	billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil, nil)
	t.Cleanup(billingCache.Stop)
	walletCache, snapshots := newUSDHandlerTestWallet(t, cfg, service.NewBillingService(cfg, nil))
	gateway := service.NewOpenAIGatewayService(accountRepo, nil, nil, nil, nil, nil, walletCache, cfg, nil, nil, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, snapshots)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.2","max_output_tokens":32,"input":"hello","stream":false}`))
	c.Request.Header.Set("Content-Type", "application/json")
	groupID := int64(4203)
	c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{
		ID: 1803, GroupID: &groupID,
		User:  &service.User{PlatformUserID: "usd-fixture-user", ID: 1703, Status: service.StatusActive},
		Group: &service.Group{RateMultiplier: 1, ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	})
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1703, Concurrency: 0})
	h.Responses(c)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), service.ModelTemporarilyUnavailableCode)
	require.Contains(t, recorder.Body.String(), "模型暂时下架")
	require.Equal(t, 1, accountRepo.selections)
	require.Empty(t, upstream.calls(), "the suspended model must never reach the provider")
}
