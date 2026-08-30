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
// RegisterWalletReconciliationRoutes. An empty
// canonical_wallet.reconciliation_read_token leaves the group ABSENT — an
// undeployed key is inert (404); a configured token mounts it where the
// token gate is the only auth link: no token is 401, the right token
// reaches the handler (503 against an unwired service — never a panic).
//
// This function is called directly (never through RegisterAdminRoutes) —
// production discovered why: RegisterAdminRoutes and everything inside it
// is skipped entirely when cfg.Server.DataPlaneOnly is true (ShipAny's own
// deployment shape, SERVER_DATA_PLANE_ONLY=true), which made this
// machine-to-machine endpoint unreachable when it lived inside that
// function — the runbook's §1.7 reconcile cron 404'd end to end despite a
// correctly configured token. The test below (no AdminAuthMiddleware, no
// StepUpAuthMiddleware, no SettingService — none of RegisterAdminRoutes's
// scaffolding) is the regression guard: it proves the group mounts without
// any of that, the exact shape DataPlaneOnly deployments run in.
func TestWalletReconciliationGroupMounting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stubAudit := middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() })

	build := func(token string) *gin.Engine {
		cfg := &config.Config{}
		cfg.CanonicalWallet.ReconciliationReadToken = token
		h := &handler.Handlers{Admin: &handler.AdminHandlers{
			WalletReconciliation: admin.NewWalletReconciliationHandler(nil, cfg),
		}}
		r := gin.New()
		v1 := r.Group("/api/v1")
		RegisterWalletReconciliationRoutes(v1, h, stubAudit, nil)
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
