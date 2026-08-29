//go:build unit

package routes

import (
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const walletReconciliationRoutesTestToken = "routes-test-token-0123456789abcdef"

// Phase 4.1-G: the reconciliation group's mounting rule, through the REAL
// RegisterAdminRoutes. An empty canonical_wallet.reconciliation_read_token
// leaves the group ABSENT — an undeployed key is inert (404); a configured
// token mounts it as a sibling of the admin group where the token gate is
// the only replaced link: no token is 401, the right token reaches the
// handler (503 against an unwired service — never a panic).
func TestWalletReconciliationGroupMounting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stubAuth := middleware.AdminAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(401) })
	stubAudit := middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() })
	stubStepUp := middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() })

	build := func(token string) *gin.Engine {
		cfg := &config.Config{}
		cfg.CanonicalWallet.ReconciliationReadToken = token
		h := &handler.Handlers{Admin: &handler.AdminHandlers{
			WalletReconciliation: admin.NewWalletReconciliationHandler(nil, cfg),
		}}
		r := gin.New()
		v1 := r.Group("/api/v1")
		RegisterAdminRoutes(v1, h, stubAuth, stubAudit, stubStepUp, nil, nil)
		return r
	}

	summary := "/api/v1/admin/wallet/reconciliation/summary?platform_user_id=shipany-user-7&since=2026-08-01T00:00:00Z&until=2026-08-02T00:00:00Z"

	t.Run("empty token: the group is absent (404), inert", func(t *testing.T) {
		r := build("")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", summary, nil))
		require.Equal(t, 404, rec.Code)
	})

	t.Run("configured token: 401 without it, 503 with it (unwired service, never a panic)", func(t *testing.T) {
		r := build(walletReconciliationRoutesTestToken)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", summary, nil))
		require.Equal(t, 401, rec.Code)

		req := httptest.NewRequest("GET", summary, nil)
		req.Header.Set("Authorization", "Bearer "+walletReconciliationRoutesTestToken)
		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 503, rec.Code, "past the token gate, reaching the handler")

		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/watermark", nil))
		require.Equal(t, 401, rec.Code)
	})
}
