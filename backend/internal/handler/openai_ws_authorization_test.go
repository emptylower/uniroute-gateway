//go:build unit

package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type wsAuthTestHarness struct {
	server      *httptest.Server
	upstream    *httptest.Server
	accountRepo *openAIWSFailoverHandlerAccountRepoStub
}

func newWSAuthTestHarness(t *testing.T, mode string, ingressMode string) *wsAuthTestHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)

	harness := &wsAuthTestHarness{}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{
			CompressionMode: coderws.CompressionContextTakeover,
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		for turn := 1; ; turn++ {
			readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
			_, payload, readErr := conn.Read(readCtx)
			cancelRead()
			if readErr != nil {
				return
			}
			msgType := gjson.GetBytes(payload, "type").String()
			if msgType != "response.create" {
				continue
			}
			respID := fmt.Sprintf("resp_auth_test_%d", turn)
			writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
			_ = conn.Write(writeCtx, coderws.MessageText, []byte(fmt.Sprintf(
				`{"type":"response.completed","response":{"id":%q,"model":%q,"usage":{"input_tokens":10,"output_tokens":5}}}`,
				respID,
				gjson.GetBytes(payload, "model").String(),
			)))
			cancelWrite()
		}
	}))
	harness.upstream = upstream

	account := service.Account{
		ID:          9801,
		Name:        "ws-auth-account",
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":  "sk-auth-test",
			"base_url": upstream.URL,
		},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_enabled": true,
			"openai_apikey_responses_websockets_v2_mode":    ingressMode,
		},
	}
	harness.accountRepo = &openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{account}}

	cfg := &config.Config{}
	cfg.RunMode = config.RunModeSimple
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.CanonicalWallet.Mode = mode

	billingCacheSvc := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil, nil)
	gatewaySvc := service.NewOpenAIGatewayService(
		harness.accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg, nil, nil,
		nil,
		nil,
		service.NewBillingService(cfg, nil),
		nil,
		billingCacheSvc,
		nil,
		&service.DeferredService{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(ctx context.Context, accountID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
	}
	h := &OpenAIGatewayHandler{
		gatewayService:      gatewaySvc,
		billingCacheService: billingCacheSvc,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Second),
	}

	groupID := int64(4301)
	apiKey := &service.APIKey{
		ID:      1901,
		GroupID: &groupID,
		User: &service.User{
			ID:              1801,
			Status:          service.StatusActive,
			BillingCurrency: service.CurrencyUSD,
		},
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformOpenAI,
			Status:   service.StatusActive,
		},
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	harness.server = httptest.NewServer(router)
	return harness
}

func TestWSAuthorization_TwoTurnSessionsOnBothIngressModes(t *testing.T) {
	modes := []struct {
		name        string
		ingressMode string
	}{
		{name: "passthrough", ingressMode: service.OpenAIWSIngressModePassthrough},
		{name: "pooled", ingressMode: service.OpenAIWSIngressModeCtxPool},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			service.ResetAuthorizationMetricsForTest()
			harness := newWSAuthTestHarness(t, config.CanonicalWalletModeEnforce, tc.ingressMode)
			defer harness.server.Close()
			defer harness.upstream.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelDial()
			clientConn, _, err := coderws.Dial(
				dialCtx,
				"ws"+strings.TrimPrefix(harness.server.URL, "http")+"/openai/v1/responses",
				&coderws.DialOptions{CompressionMode: coderws.CompressionContextTakeover},
			)
			require.NoError(t, err)
			defer func() { _ = clientConn.CloseNow() }()

			// Turn 1
			writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
			err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`))
			cancelWrite()
			require.NoError(t, err)

			readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
			_, event1, err := clientConn.Read(readCtx)
			cancelRead()
			require.NoError(t, err)
			require.Equal(t, "response.completed", gjson.GetBytes(event1, "type").String())

			// Turn 2
			writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
			err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","previous_response_id":"resp_auth_test_1"}`))
			cancelWrite()
			require.NoError(t, err)

			readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
			_, event2, err := clientConn.Read(readCtx)
			cancelRead()
			require.NoError(t, err)
			require.Equal(t, "response.completed", gjson.GetBytes(event2, "type").String())

			_ = clientConn.Close(coderws.StatusNormalClosure, "done")

			metrics := service.AuthorizationMetricsSnapshot()
			require.Equal(t, int64(2), metrics.WritesAuthorized, "two turns authorized")
			require.Equal(t, int64(0), metrics.WritesRefused)
		})
	}
}

type stubWalletLeaseStore struct {
	service.GatewayCache
}

func (s *stubWalletLeaseStore) GetCanonicalWalletLease(ctx context.Context, platformUserID, currency string) (*service.CanonicalWalletLease, error) {
	return nil, nil
}
func (s *stubWalletLeaseStore) PutCanonicalWalletLease(ctx context.Context, lease service.CanonicalWalletLease, ttl time.Duration) error {
	return nil
}
func (s *stubWalletLeaseStore) ClearCanonicalWalletLease(ctx context.Context, platformUserID, currency string) error {
	return nil
}
func (s *stubWalletLeaseStore) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, currency string, amountUnits int64, gatewayRequestID string, reservationTTL time.Duration) (*service.CanonicalWalletReservation, error) {
	return nil, nil
}

func TestWSAuthorization_RefusalCloses4402WithoutReportingAccountFailure(t *testing.T) {
	service.ResetAuthorizationMetricsForTest()
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{
			CompressionMode: coderws.CompressionContextTakeover,
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		time.Sleep(2 * time.Second)
	}))
	defer upstream.Close()

	var reportedFailure atomic.Bool
	account := service.Account{
		ID:          9802,
		Name:        "ws-refusal-account",
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":  "sk-auth-test",
			"base_url": upstream.URL,
		},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_enabled": true,
			"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModePassthrough,
		},
	}
	accountRepo := &openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{account}}

	cfg := &config.Config{}
	cfg.RunMode = config.RunModeSimple
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.RequestTimeoutMS = 100

	billingCacheSvc := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil, nil)
	snapshots := service.NewBillingSnapshotService(cfg, nil, service.NewBillingService(cfg, nil), nil, nil)
	gatewaySvc := service.NewOpenAIGatewayService(
		accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		&stubWalletLeaseStore{},
		cfg, nil, nil,
		nil,
		nil,
		service.NewBillingService(cfg, nil),
		nil,
		billingCacheSvc,
		nil,
		&service.DeferredService{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		snapshots,
	)

	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(ctx context.Context, accountID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
	}
	h := &OpenAIGatewayHandler{
		gatewayService:      gatewaySvc,
		billingCacheService: billingCacheSvc,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Second),
	}

	groupID := int64(4302)
	apiKey := &service.APIKey{
		ID:      1902,
		GroupID: &groupID,
		User: &service.User{
			ID:              1802,
			PlatformUserID:  "user_1802",
			Status:          service.StatusActive,
			BillingCurrency: service.CurrencyUSD,
		},
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformOpenAI,
			Status:   service.StatusActive,
		},
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	server := httptest.NewServer(router)
	defer server.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	clientConn, _, err := coderws.Dial(
		dialCtx,
		"ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses",
		&coderws.DialOptions{CompressionMode: coderws.CompressionContextTakeover},
	)
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, _, readErr := clientConn.Read(readCtx)
	cancelRead()
	require.Error(t, readErr)

	var wsCloseErr coderws.CloseError
	if errors.As(readErr, &wsCloseErr) {
		require.Equal(t, coderws.StatusCode(service.AuthorizationRefusedWSCloseStatus), wsCloseErr.Code)
		require.Contains(t, wsCloseErr.Reason, service.AuthorizationRefusedWSCloseReason)
	}

	require.False(t, reportedFailure.Load(), "refusal must not report account failure")
}
