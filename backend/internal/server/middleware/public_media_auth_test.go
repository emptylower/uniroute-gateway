//go:build unit

package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaAuthKeepsReadonlyAndReplayAfterExhaustion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/v1/media/catalog", "/v1/media/quote", "/v1/media/tasks", "/v1/media/tasks/task-1"} {
		t.Run(path, func(t *testing.T) {
			key := &service.APIKey{ID: 1, UserID: 2, Status: service.StatusAPIKeyQuotaExhausted, Quota: 1, QuotaUsed: 1, RoutingMode: service.APIKeyRoutingModeAutoChannels, User: &service.User{ID: 2, Status: service.StatusActive, Balance: 0}}
			repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) { return key, nil }}
			cfg := &config.Config{}
			svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
			router := gin.New()
			router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(svc, nil, cfg)))
			router.Any(path, func(c *gin.Context) {
				actual, ok := GetAPIKeyFromContext(c)
				require.True(t, ok)
				require.Equal(t, int64(1), actual.ID)
				c.Status(200)
			})
			req := httptest.NewRequest(http.MethodPost, path, nil)
			req.Header.Set("Authorization", "Bearer real-key")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code, w.Body.String())
			key.Status = service.StatusAPIKeyDisabled
			w = httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, 401, w.Code)
			var envelope map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
			require.Contains(t, envelope, "message")
			require.Contains(t, envelope, "data")
		})
	}
}
