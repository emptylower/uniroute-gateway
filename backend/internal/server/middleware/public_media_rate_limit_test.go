package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaRateLimitsDoNotUseFinancialAllowance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		method, path string
		allowed      int
	}{{http.MethodGet, "/v1/media/tasks/owned-task", 120}, {http.MethodPost, "/v1/media/tasks", 10}} {
		router := gin.New()
		router.Use(func(c *gin.Context) {
			c.Set(string(ContextKeyAPIKey), &service.APIKey{ID: 1, Status: service.StatusAPIKeyQuotaExhausted, Quota: 1, QuotaUsed: 1})
		}, PublicMediaRateLimit())
		router.Any(tt.path, func(c *gin.Context) { c.Status(200) })
		for i := 0; i < tt.allowed; i++ {
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
			require.Equal(t, 200, w.Code)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
		require.Equal(t, 429, w.Code)
		require.Equal(t, "60", w.Header().Get("Retry-After"))
		require.Contains(t, w.Body.String(), `"data":null`)
	}
}
