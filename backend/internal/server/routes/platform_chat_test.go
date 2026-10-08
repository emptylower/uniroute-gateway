//go:build unit

package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPlatformChatRoutesRejectUnsignedWrongScopeAndSubject(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{PlatformIdentity: delegatedTestConfig()}
	cfg.Gateway.TextMaxBodySize = 1 << 20
	store := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: store.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	router := gin.New()
	h := &handler.Handlers{PlatformInference: handler.NewPlatformInferenceHandler(nil), Gateway: &handler.GatewayHandler{}, OpenAIGateway: &handler.OpenAIGatewayHandler{}}
	RegisterPlatformChatRoutes(router, h, service.NewPlatformIdentityService(delegatedIdentityRepoStub{}), &service.UserService{}, &service.APIKeyService{}, nil, nil, nil, nil, cfg, redisClient)
	for _, tc := range []struct {
		name, method, path, assertion string
		status                        int
	}{
		{"unsigned create", http.MethodPost, "/api/internal/v1/users/owner/inference/chat", "", http.StatusUnauthorized},
		{"unsigned catalog", http.MethodGet, "/api/internal/v1/users/owner/inference/chat/catalog", "", http.StatusUnauthorized},
		{"read cannot create", http.MethodPost, "/api/internal/v1/users/owner/inference/chat", signDelegatedAssertion(t, cfg.PlatformIdentity, "owner", service.PlatformInferenceReadScope, "read-create"), http.StatusForbidden},
		{"create cannot read", http.MethodGet, "/api/internal/v1/users/owner/inference/chat/catalog", signDelegatedAssertion(t, cfg.PlatformIdentity, "owner", service.PlatformInferenceCreateScope, "create-read"), http.StatusForbidden},
		{"cross user", http.MethodPost, "/api/internal/v1/users/other/inference/chat", signDelegatedAssertion(t, cfg.PlatformIdentity, "owner", service.PlatformInferenceCreateScope, "cross-user"), http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.assertion != "" {
				request.Header.Set("Authorization", "Bearer "+tc.assertion)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPlatformChatRoutesDisabledWithoutIdentityBridge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterPlatformChatRoutes(router, &handler.Handlers{}, nil, nil, nil, nil, nil, nil, nil, &config.Config{}, nil)
	require.Empty(t, router.Routes())
}
