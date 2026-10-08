//go:build media_integration

package media_integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type walletHandlerAccounts struct {
	service.AccountRepository
	accounts []service.Account
}

func (r walletHandlerAccounts) GetByID(_ context.Context, id int64) (*service.Account, error) {
	for _, a := range r.accounts {
		if a.ID == id {
			return &a, nil
		}
	}
	return nil, service.ErrNoAvailableAccounts
}
func (r walletHandlerAccounts) ListSchedulableByPlatform(context.Context, string) ([]service.Account, error) {
	return r.accounts, nil
}
func (r walletHandlerAccounts) ListSchedulableUngroupedByPlatform(context.Context, string) ([]service.Account, error) {
	return r.accounts, nil
}
func (r walletHandlerAccounts) ListSchedulableByGroupIDAndPlatform(context.Context, int64, string) ([]service.Account, error) {
	return r.accounts, nil
}

func TestExternalResponsesHandlerUnknown503HasOneAuthorizationAndPreservesStatus(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	f.poolFunds(t, 100000000, 0, 100000000)
	f.cfg.RunMode = config.RunModeSimple
	f.cfg.Default.RateMultiplier = 1
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("x-request-id", "fixture-original-503")
		w.WriteHeader(503)
		_, _ = io.WriteString(w, `{"error":{"message":"temporary unavailable"}}`)
	}))
	defer upstream.Close()
	// The transport sends the provider's request through a zero-cost fixture,
	// preserving its decorated wallet context and actual request body.
	port := service.NewAuthorizingHTTPUpstream(&walletHandlerUpstream{target: upstream.URL, client: upstream.Client()}, f.cfg)
	accounts := walletHandlerAccounts{accounts: []service.Account{{ID: 1, Name: "fixture-one", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 0, Credentials: map[string]any{"api_key": "fixture-key", "base_url": "https://api.openai.com"}}, {ID: 2, Name: "fixture-two", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 0, Credentials: map[string]any{"api_key": "fixture-key", "base_url": "https://api.openai.com"}}}}
	pricing := service.NewBillingService(f.cfg, nil)
	snapshots := service.NewBillingSnapshotService(f.cfg, service.NewModelPricingResolver(nil, pricing), pricing, service.NewUSDPriceService(f.cfg), repository.ProvideBillingSnapshotStore(f.db))
	gateway := service.NewOpenAIGatewayService(accounts, nil, nil, f.users, nil, nil, repository.NewGatewayCache(f.rdb), f.cfg, f.db, repository.ProvideWalletOutboxStore(f.db), nil, nil, pricing, nil, nil, port, nil, nil, nil, nil, nil, nil, nil, nil, snapshots)
	defer gateway.CloseOpenAIWSPool()
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, f.cfg, nil, nil)
	defer billing.Stop()
	h := handler.NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing, f.apiKeys, nil, nil, nil, nil, f.cfg)
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-5.1","max_output_tokens":32,"stream":false,"input":"hi"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	groupID := int64(1)
	c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 99, User: user, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, RateMultiplier: 1}, RoutingMode: service.APIKeyRoutingModeLegacyGroup})
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: f.userID})
	h.Responses(c)
	require.Equal(t, int64(1), calls.Load(), "status=%d body=%s", recorder.Code, recorder.Body.String())
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.NotContains(t, recorder.Body.String(), "wallet_authorization_refused")
	var attempts, segments int
	require.NoError(t, f.db.QueryRow(`SELECT count(DISTINCT parent_authorization_id),count(*) FROM wallet_authorization_segment WHERE kind='llm'`).Scan(&attempts, &segments))
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, segments)
	status, ok := c.Get(service.OpsUpstreamStatusCodeKey)
	require.True(t, ok)
	require.EqualValues(t, 503, status)
}

type walletHandlerUpstream struct {
	service.HTTPUpstream
	target string
	client *http.Client
}

func (u *walletHandlerUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	req = req.Clone(req.Context())
	target, _ := http.NewRequest("POST", u.target, nil)
	req.URL = target.URL
	return u.client.Do(req)
}
func (u *walletHandlerUpstream) DoWithTLS(req *http.Request, p string, a int64, c int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, p, a, c)
}
