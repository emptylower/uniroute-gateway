//go:build unit

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// walletReconciliationTestToken is 32 bytes — Validate()'s floor for a
// configured token under enforce/shadow.
const walletReconciliationTestToken = "recon-token-0123456789abcdef0123456789"

// Phase 4.1-G: the machine-to-machine bearer gate. No header and a wrong
// token are both 401 with WWW-Authenticate: Bearer; the right token lets
// the next handler run; the compare is subtle.ConstantTimeCompare (pinned
// by the source assertion below — the plan's own requirement).
func TestWalletReconciliationTokenMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("no header is 401 with WWW-Authenticate", func(t *testing.T) {
		r := gin.New()
		reached := false
		r.GET("/summary", NewWalletReconciliationTokenAuth(walletReconciliationTestToken), func(c *gin.Context) { reached = true })
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/summary", nil))
		require.Equal(t, 401, rec.Code)
		require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
		require.False(t, reached)
	})

	t.Run("wrong token is 401, right token passes", func(t *testing.T) {
		r := gin.New()
		reached := false
		r.GET("/summary", NewWalletReconciliationTokenAuth(walletReconciliationTestToken), func(c *gin.Context) { reached = true; c.Status(200) })
		req := httptest.NewRequest("GET", "/summary", nil)
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", len(walletReconciliationTestToken)))
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 401, rec.Code)
		require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
		require.False(t, reached)

		req = httptest.NewRequest("GET", "/summary", nil)
		req.Header.Set("Authorization", "Bearer "+walletReconciliationTestToken)
		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)
		require.True(t, reached)
	})

	t.Run("malformed Authorization header is 401", func(t *testing.T) {
		r := gin.New()
		r.GET("/summary", NewWalletReconciliationTokenAuth(walletReconciliationTestToken), func(c *gin.Context) { c.Status(200) })
		for _, header := range []string{"", "Basic abc", "Bearer", "Bearer    "} {
			req := httptest.NewRequest("GET", "/summary", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			require.Equal(t, 401, rec.Code, "header %q", header)
		}
	})

	t.Run("the compare is constant-time (source contract)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join("wallet_reconciliation_token.go"))
		require.NoError(t, err)
		require.Contains(t, string(raw), "subtle.ConstantTimeCompare", "the plan pins the compare to crypto/subtle")
	})
}

// The synthetic principal: with the REAL audit middleware behind the token
// gate, a recorded call is attributed to service:shipany-reconciliation
// with auth_method service_token — never to a numeric admin user id.
func TestWalletReconciliationTokenAuditsUnderTheServiceActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &walletReconAuditCaptureRepository{}
	auditService := service.NewAuditLogService(repo, nil)
	auditService.Start()

	r := gin.New()
	r.GET("/api/v1/admin/wallet/reconciliation/summary",
		NewWalletReconciliationTokenAuth(walletReconciliationTestToken),
		gin.HandlerFunc(NewAuditLogMiddleware(auditService)),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"schema": 1}) },
	)

	req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary", nil)
	req.Header.Set("Authorization", "Bearer "+walletReconciliationTestToken)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code)
	auditService.Stop()

	logs := repo.snapshot()
	require.Len(t, logs, 1, "the whitelisted sensitive read is recorded")
	require.Equal(t, "service:shipany-reconciliation", logs[0].ActorEmail)
	require.Equal(t, "service_token", logs[0].AuthMethod)
	require.Nil(t, logs[0].ActorUserID, "a machine principal has no numeric user id")
}

// The limiter link: mounted behind the token gate, a subject-bearing caller
// exhausts the panel limiter's fixed window and the chain short-circuits
// 429. (The shipped machine principal deliberately carries NO numeric
// subject — a synthetic user id would pollute audit ActorUserID — so it
// passes the user-scoped limiter through; this leg pins the link itself,
// which is what round-1 MAJOR-2 kept in the chain.)
func TestWalletReconciliationChainRateLimits(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	panel := &PanelRateLimiter{limiter: middleware.NewRateLimiter(rdb), settingService: &service.SettingService{}}
	require.Equal(t, 240, service.DefaultPanelRateLimitSettings().UserRPM)

	r := gin.New()
	r.GET("/api/v1/admin/wallet/reconciliation/summary",
		func(c *gin.Context) { c.Set(string(ContextKeyUser), AuthSubject{UserID: 424242}) },
		NewWalletReconciliationTokenAuth(walletReconciliationTestToken),
		panel.Global(),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"schema": 1}) },
	)

	hit := func() int {
		req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary", nil)
		req.Header.Set("Authorization", "Bearer "+walletReconciliationTestToken)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec.Code
	}
	for i := 0; i < 240; i++ {
		require.Equal(t, 200, hit(), "request %d within the window", i+1)
	}
	require.Equal(t, 429, hit(), "the first request past the window is 429")
}

type walletReconAuditCaptureRepository struct {
	mu   sync.Mutex
	logs []*service.AuditLog
}

func (r *walletReconAuditCaptureRepository) BatchInsert(_ context.Context, logs []*service.AuditLog) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, logs...)
	return int64(len(logs)), nil
}

func (r *walletReconAuditCaptureRepository) Insert(_ context.Context, log *service.AuditLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, log)
	return nil
}

func (r *walletReconAuditCaptureRepository) List(context.Context, *service.AuditLogFilter) (*service.AuditLogList, error) {
	return &service.AuditLogList{}, nil
}

func (r *walletReconAuditCaptureRepository) GetByID(context.Context, int64) (*service.AuditLog, error) {
	return nil, service.ErrAuditLogNotFound
}

func (r *walletReconAuditCaptureRepository) Count(context.Context) (int64, error) { return 0, nil }

func (r *walletReconAuditCaptureRepository) TruncateAll(context.Context) error { return nil }

func (r *walletReconAuditCaptureRepository) DeleteBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (r *walletReconAuditCaptureRepository) snapshot() []*service.AuditLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*service.AuditLog(nil), r.logs...)
}
