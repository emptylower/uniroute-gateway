//go:build unit

package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type pricingPreflightUpstream struct {
	calls atomic.Int64
}

func (u *pricingPreflightUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls.Add(1)
	return nil, context.Canceled
}

func (u *pricingPreflightUpstream) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls.Add(1)
	return nil, context.Canceled
}

func TestGatewayChatCompletionsRejectsUnknownMappedModelBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(9200)
	group := &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic, Status: service.StatusActive}
	account := &service.Account{
		ID: 9201, Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"api_key":       "not-used",
			"model_mapping": map[string]any{"claude-sonnet-4": "gpt-unknown-model"},
		},
		AccountGroups: []service.AccountGroup{{AccountID: 9201, GroupID: groupID}},
	}
	schedulerSnapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: []*service.Account{account}}, nil, nil, nil, nil)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	upstream := &pricingPreflightUpstream{}
	pricing := service.NewBillingService(cfg, nil)
	gatewayService := service.NewGatewayService(
		nil, &fakeGroupRepo{group: group}, nil, nil, nil, nil, nil, nil, cfg,
		schedulerSnapshot, nil, pricing, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCache.Stop)
	h := &GatewayHandler{
		gatewayService:      gatewayService,
		billingCacheService: billingCache,
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(&fakeConcurrencyCache{}), SSEPingFormatClaude, 0),
		maxAccountSwitches:  1,
		cfg:                 cfg,
	}
	apiKey := &service.APIKey{
		ID: 9202, UserID: 9203, GroupID: &groupID, Group: group, Status: service.StatusActive,
		User: &service.User{ID: 9203, Concurrency: 10, Balance: 100},
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(
		`{"model":"claude-sonnet-4","messages":[{"role":"user","content":"hello"}],"stream":false}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware.ContextKeyAPIKey), apiKey)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.UserID, Concurrency: 10})

	h.ChatCompletions(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.Zero(t, upstream.calls.Load(), "pricing failure must stop before invoking the paid upstream")
}

func TestGatewayResponsesRejectsUnknownMappedModelBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(9300)
	group := &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic, Status: service.StatusActive}
	account := &service.Account{
		ID: 9301, Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"api_key":       "not-used",
			"model_mapping": map[string]any{"claude-sonnet-4": "gpt-unknown-model"},
		},
		AccountGroups: []service.AccountGroup{{AccountID: 9301, GroupID: groupID}},
	}
	schedulerSnapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: []*service.Account{account}}, nil, nil, nil, nil)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	upstream := &pricingPreflightUpstream{}
	pricing := service.NewBillingService(cfg, nil)
	gatewayService := service.NewGatewayService(
		nil, &fakeGroupRepo{group: group}, nil, nil, nil, nil, nil, nil, cfg,
		schedulerSnapshot, nil, pricing, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCache.Stop)
	h := &GatewayHandler{
		gatewayService:      gatewayService,
		billingCacheService: billingCache,
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(&fakeConcurrencyCache{}), SSEPingFormatClaude, 0),
		maxAccountSwitches:  1,
		cfg:                 cfg,
	}
	apiKey := &service.APIKey{
		ID: 9302, UserID: 9303, GroupID: &groupID, Group: group, Status: service.StatusActive,
		User: &service.User{ID: 9303, Concurrency: 10, Balance: 100},
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(
		`{"model":"claude-sonnet-4","input":"hello","stream":false}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware.ContextKeyAPIKey), apiKey)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.UserID, Concurrency: 10})

	h.Responses(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.Zero(t, upstream.calls.Load(), "pricing failure must stop before invoking the paid upstream")
}
