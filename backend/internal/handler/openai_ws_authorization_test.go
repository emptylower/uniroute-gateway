//go:build unit

package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	server       *httptest.Server
	upstream     *httptest.Server
	accountRepo  *openAIWSFailoverHandlerAccountRepoStub
	gatewaySvc   *service.OpenAIGatewayService
	usageInputs  []*service.OpenAIRecordUsageInput
	authAttempts []authAttemptRecord
	mu           sync.Mutex
}

type authAttemptRecord struct {
	callOrdinal int
	snapshot    *service.BillingSnapshot
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
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
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
	pricingSvc := service.NewPricingService(cfg, nil)
	pricingSvc.SetModelPricingForTest("gpt-5.1", &service.LiteLLMModelPricing{
		InputCostPerToken:  0.000003,
		OutputCostPerToken: 0.000015,
		MaxInputTokens:     128000,
		MaxOutputTokens:    4096,
	})
	billingSvc := service.NewBillingService(cfg, pricingSvc)
	resolver := service.NewModelPricingResolver(nil, billingSvc)
	fx := service.NewExchangeRateService(cfg)
	snapshots := service.NewBillingSnapshotService(cfg, resolver, billingSvc, fx, nil)
	leaseStore := &stubWalletLeaseStore{
		lease: &service.CanonicalWalletLease{
			LeaseID:        "lease_ws_auth_test",
			PlatformUserID: "user_1801",
			Currency:       "CNY",
			BudgetUnits:    1_000_000_000,
			ConsumedUnits:  0,
			ExpiresAt:      time.Now().Add(time.Hour),
		},
	}
	gatewaySvc := service.NewOpenAIGatewayService(
		harness.accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		leaseStore,
		cfg, nil, nil,
		nil,
		nil,
		billingSvc,
		nil,
		billingCacheSvc,
		nil,
		&service.DeferredService{},
		nil,
		nil,
		resolver,
		nil,
		nil,
		nil,
		nil,
		snapshots,
	)

	gatewaySvc.SetRecordUsageHookForTest(func(input *service.OpenAIRecordUsageInput) {
		harness.mu.Lock()
		defer harness.mu.Unlock()
		harness.usageInputs = append(harness.usageInputs, input)
	})

	turnCounter := &atomic.Int64{}
	gatewaySvc.SetAuthorizeBillableAttemptHookForTest(func(snap *service.BillingSnapshot, apiKey *service.APIKey, estimate service.EstimateInput) {
		harness.mu.Lock()
		defer harness.mu.Unlock()
		t := int(turnCounter.Add(1))
		harness.authAttempts = append(harness.authAttempts, authAttemptRecord{callOrdinal: t, snapshot: snap})
	})
	harness.gatewaySvc = gatewaySvc

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

	apiKey := &service.APIKey{
		ID: 1901,
		User: &service.User{
			ID:              1801,
			PlatformUserID:  "user_1801",
			Status:          service.StatusActive,
			BillingCurrency: "CNY",
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

			harness.mu.Lock()
			usageInputs := harness.usageInputs
			authAttempts := harness.authAttempts
			harness.mu.Unlock()

			require.Len(t, usageInputs, 2, "two RecordUsage calls")
			require.NotEmpty(t, usageInputs[0].AuthorizationToken, "turn 1 token non-empty")
			require.NotEmpty(t, usageInputs[0].AuthorizationID, "turn 1 authorization ID non-empty")
			require.NotEmpty(t, usageInputs[1].AuthorizationToken, "turn 2 token non-empty")
			require.NotEmpty(t, usageInputs[1].AuthorizationID, "turn 2 authorization ID non-empty")
			require.NotEqual(t, usageInputs[0].AuthorizationToken, usageInputs[1].AuthorizationToken, "two distinct tokens across turns 1 and 2")
			require.NotNil(t, usageInputs[0].BillingSnapshot, "turn 1 snapshot non-nil")
			require.NotNil(t, usageInputs[1].BillingSnapshot, "turn 2 snapshot non-nil")

			require.Len(t, authAttempts, 2, "two AuthorizeTurn calls")
			// The hook carries no turn argument; callOrdinal records the invocation sequence (1, 2).
			// The holder-turn match is proven by require.NotNil(t, authAttempts[i].snapshot)
			// because openai_gateway_handler.go:2009-2011 sets turnSnap only when snapshot.turn == turn.
			require.Equal(t, 1, authAttempts[0].callOrdinal, "turn 1 call ordinal equals 1")
			require.NotNil(t, authAttempts[0].snapshot, "turn 1 snapshot non-nil")
			require.Equal(t, 2, authAttempts[1].callOrdinal, "turn 2 call ordinal equals 2")
			require.NotNil(t, authAttempts[1].snapshot, "turn 2 snapshot non-nil")
		})
	}
}

type stubWalletLeaseStore struct {
	lease *service.CanonicalWalletLease
}

func (s *stubWalletLeaseStore) GetSessionAccountID(ctx context.Context, groupID int64, sessionHash string) (int64, error) {
	return 0, errors.New("not found")
}
func (s *stubWalletLeaseStore) SetSessionAccountID(ctx context.Context, groupID int64, sessionHash string, accountID int64, ttl time.Duration) error {
	return nil
}
func (s *stubWalletLeaseStore) RefreshSessionTTL(ctx context.Context, groupID int64, sessionHash string, ttl time.Duration) error {
	return nil
}
func (s *stubWalletLeaseStore) DeleteSessionAccountID(ctx context.Context, groupID int64, sessionHash string) error {
	return nil
}
func (s *stubWalletLeaseStore) InstallCanonicalWalletLease(ctx context.Context, lease service.CanonicalWalletLease) error {
	s.lease = &lease
	return nil
}
func (s *stubWalletLeaseStore) GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*service.CanonicalWalletLease, error) {
	return s.lease, nil
}
func (s *stubWalletLeaseStore) GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*service.CanonicalWalletLease, error) {
	return s.lease, nil
}
func (s *stubWalletLeaseStore) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*service.CanonicalWalletReservation, error) {
	if s.lease == nil {
		return nil, errors.New("no lease")
	}
	return &service.CanonicalWalletReservation{
		Lease: *s.lease,
	}, nil
}

// Phase 3.4b (Task 2): the hold surface is not driven by any handler test —
// not-implemented stubs keep the (partial) lease-store surface loud. (This
// stub never carried SealCanonicalWalletLease either; ProvideBillingCacheService's
// type assertion therefore does not select it.)
func (s *stubWalletLeaseStore) ArmCanonicalWalletHold(context.Context, string, string, string, string, int64, int64, time.Time) (string, int64, bool, error) {
	return "", 0, false, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ReleaseCanonicalWalletHold(context.Context, string, string, string, string) (int64, error) {
	return 0, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ConvertCanonicalWalletHold(context.Context, string, string, string, int64, time.Time) (service.CanonicalWalletHoldConversion, error) {
	return service.CanonicalWalletHoldConversion{}, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) GetCanonicalWalletHold(context.Context, string, string) (*service.CanonicalWalletHold, error) {
	return nil, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ListCanonicalWalletHolds(context.Context, string, int) ([]string, error) {
	return nil, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ListCanonicalWalletHoldUsers(context.Context, uint64, int64) ([]string, uint64, error) {
	return nil, 0, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) PruneCanonicalWalletHoldUser(context.Context, string) error {
	return errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) TryCanonicalWalletReaperLease(context.Context, time.Duration) (bool, error) {
	return false, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ForgetCanonicalWalletHold(context.Context, string, string) error {
	return errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) MarkCanonicalWalletHoldUserEmpty(context.Context, string, time.Duration) (bool, error) {
	return false, errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) ClearCanonicalWalletHoldUserEmpty(context.Context, string) error {
	return errors.New("not implemented in this stub")
}
func (s *stubWalletLeaseStore) MarkCanonicalWalletHoldClass(context.Context, string, string, string) (*service.CanonicalWalletHold, error) {
	return nil, errors.New("not implemented in this stub")
}

// Phase 3.5 (Task 2): the reservation release is not driven by any handler
// test — same not-implemented convention as the hold surface above.
func (s *stubWalletLeaseStore) ReleaseCanonicalWalletReservation(context.Context, string, string, string, int64, bool) (bool, error) {
	return false, errors.New("not implemented in this stub")
}

type openAIWSRefusalReportingAccountRepoStub struct {
	openAIWSFailoverHandlerAccountRepoStub
	reportedFailure *atomic.Bool
}

func (s *openAIWSRefusalReportingAccountRepoStub) SetRateLimited(ctx context.Context, id int64, resetAt time.Time) error {
	if s.reportedFailure != nil {
		s.reportedFailure.Store(true)
	}
	return s.openAIWSFailoverHandlerAccountRepoStub.SetRateLimited(ctx, id, resetAt)
}

func (s *openAIWSRefusalReportingAccountRepoStub) SetError(ctx context.Context, id int64, errorMsg string) error {
	if s.reportedFailure != nil {
		s.reportedFailure.Store(true)
	}
	return nil
}

func (s *openAIWSRefusalReportingAccountRepoStub) SetTempUnschedulable(ctx context.Context, id int64, until time.Time, reason string) error {
	if s.reportedFailure != nil {
		s.reportedFailure.Store(true)
	}
	return nil
}

func (s *openAIWSRefusalReportingAccountRepoStub) SetModelRateLimit(ctx context.Context, id int64, scope string, resetAt time.Time, reason ...string) error {
	if s.reportedFailure != nil {
		s.reportedFailure.Store(true)
	}
	return nil
}

func (s *openAIWSRefusalReportingAccountRepoStub) SetOverloaded(ctx context.Context, id int64, until time.Time) error {
	if s.reportedFailure != nil {
		s.reportedFailure.Store(true)
	}
	return nil
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
	accountRepo := &openAIWSRefusalReportingAccountRepoStub{
		openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{account}},
		reportedFailure:                        &reportedFailure,
	}

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
	gatewaySvc.SetReportScheduleResultHookForTest(func(accountID int64, model string, success bool, firstTokenMs *int) {
		if !success {
			reportedFailure.Store(true)
		}
	})

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
			BillingCurrency: "CNY",
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
