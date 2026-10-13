//go:build unit

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func publicPricesRoleRouter(t *testing.T, dataPlaneOnly bool, catalog *handler.ModelCatalogHandler) (*gin.Engine, *int) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Server.DataPlaneOnly = dataPlaneOnly
	cfg.PlatformIdentity = config.PlatformIdentityConfig{Enabled: true, Issuer: "router-role-test", Audience: "gateway-role-test", Secret: "0123456789abcdef0123456789abcdef", Version: "v1"}
	cfg.CanonicalWallet.ReconciliationReadToken = "router-role-test-token-0123456789abcdef"
	h := &handler.Handlers{
		Auth: &handler.AuthHandler{}, Setting: &handler.SettingHandler{},
		ModelCatalog: catalog,
		APIKey:       &handler.APIKeyHandler{}, AvailableChannel: &handler.AvailableChannelHandler{}, Usage: &handler.UsageHandler{},
		Gateway: &handler.GatewayHandler{}, OpenAIGateway: &handler.OpenAIGatewayHandler{},
		PlatformIdentity: &handler.PlatformIdentityHandler{}, PlatformInference: handler.NewPlatformInferenceHandler(nil),
		Admin: &handler.AdminHandlers{WalletReconciliation: adminhandler.NewWalletReconciliationHandler(nil, cfg)},
	}
	authCalls := 0
	jwt := middleware.JWTAuthMiddleware(func(c *gin.Context) { authCalls++; c.AbortWithStatus(http.StatusUnauthorized) })
	admin := middleware.AdminAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusUnauthorized) })
	apiKey := middleware.APIKeyAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusUnauthorized) })
	audit := middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() })
	stepUp := middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() })
	r := gin.New()
	// Exercise the real role selection and complete route registration, rather
	// than calling the auth/public subgroup directly and bypassing production.
	registerRoutes(r, h, jwt, admin, apiKey, audit, stepUp, &service.APIKeyService{}, nil, nil, nil, &service.UserService{}, &service.PlatformIdentityService{}, nil, cfg, nil)
	return r, &authCalls
}

func TestRegisterRoutesPublicPricesAcrossServerRoles(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, dataPlaneOnly := range []bool{true, false} {
		name := "normal"
		if dataPlaneOnly {
			name = "data-plane-only"
		}
		t.Run(name, func(t *testing.T) {
			r, authCalls := publicPricesRoleRouter(t, dataPlaneOnly, handler.NewModelCatalogHandler(nil, nil, nil))
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices?category=text", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Zero(t, *authCalls, "anonymous retail prices must not traverse JWT middleware")
			var envelope struct {
				Data struct {
					Items      []json.RawMessage `json:"items"`
					PriceScope string            `json:"price_scope"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
			require.NotNil(t, envelope.Data.Items)
			require.Equal(t, "default_retail_standard_tokens", envelope.Data.PriceScope)
		})
	}
}

func TestRegisterRoutesDataPlaneKeepsHumanAndCommercialSurfaceAbsent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r, authCalls := publicPricesRoleRouter(t, true, handler.NewModelCatalogHandler(nil, nil, nil))
	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/auth/login"}, {http.MethodPost, "/api/v1/auth/register"},
		{http.MethodGet, "/api/v1/auth/me"}, {http.MethodGet, "/api/v1/admin/users"},
		{http.MethodGet, "/api/v1/payment/config"}, {http.MethodPost, "/api/v1/payment/orders"},
		{http.MethodPost, "/api/v1/payment/public/orders/verify"}, {http.MethodPost, "/api/v1/payment/webhook/stripe"},
	} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, nil))
		require.Equal(t, http.StatusNotFound, rec.Code, "%s %s stays unregistered", route.method, route.path)
	}
	require.Zero(t, *authCalls, "absent control-plane routes must not enter human auth")
	for _, path := range []string{"/v1/models", "/api/v1/admin/wallet/reconciliation/summary", "/api/v1/admin/wallet/reconciliation/watermark", "/api/internal/v1/users/owner/models/catalog", "/api/internal/v1/users/owner/inference/chat/catalog"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusUnauthorized, rec.Code, "%s retains its existing machine/API-key gate", path)
	}
}

func TestRegisterRoutesPublicPricesAbsentWhenHandlerUnwired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r, _ := publicPricesRoleRouter(t, true, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}
