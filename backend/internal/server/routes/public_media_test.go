package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaRoutesExposeVerifiedCatalogAndStrictRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	svc, err := service.NewMediaTaskService(cfg, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	platform := "projected-key"
	key := &service.APIKey{ID: 1, UserID: 1, PlatformKeyID: &platform, Status: service.StatusActive, RoutingMode: service.APIKeyRoutingModeAutoChannels, User: &service.User{ID: 1, Status: service.StatusActive, PlatformUserID: "platform-user", Balance: 0}}
	auth := middleware.APIKeyAuthMiddleware(func(c *gin.Context) { c.Set(string(middleware.ContextKeyAPIKey), key); c.Next() })
	router := gin.New()
	registerPublicMediaRoutes(router, &handler.Handlers{PlatformInference: handler.NewPlatformInferenceHandler(svc)}, auth)
	for _, tt := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/v1/media/catalog", "", 200}, {"POST", "/v1/media/quote", `{"model":"nano-banana-2-lite","option":"1:1"}`, 200}, {"POST", "/v1/media/quote", `{"model":"bad"}`, 400}, {"POST", "/v1/media/tasks", `{"model":"nano-banana-2-lite","max_cost_usd":null}`, 400}, {"POST", "/v1/media/tasks", `{"model":"nano-banana-2-lite","user_id":999}`, 400}, {"GET", "/v1/media/tasks?limit=101", "", 400}} {
		req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, tt.status, w.Code, w.Body.String())
		var envelope map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
		require.Contains(t, envelope, "code")
		require.Contains(t, envelope, "message")
		require.Contains(t, envelope, "data")
	}
	// A legal 20k Unicode Omni prompt must reach admission, which is disabled
	// in this zero-dependency fixture; body truncation would instead return 400.
	payload, _ := json.Marshal(map[string]any{"model": "google/gemini-omni-flash-1-1", "prompt": strings.Repeat("中", 20000)})
	longRequest := httptest.NewRequest(http.MethodPost, "/v1/media/tasks", strings.NewReader(string(payload)))
	longRequest.Header.Set("Content-Type", "application/json")
	longRequest.Header.Set("Idempotency-Key", "long-unicode")
	longResponse := httptest.NewRecorder()
	router.ServeHTTP(longResponse, longRequest)
	require.Equal(t, 503, longResponse.Code, longResponse.Body.String())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/media/legacy-read", strings.NewReader(`{}`)))
	require.Equal(t, 404, w.Code)
}
