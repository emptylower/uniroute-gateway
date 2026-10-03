//go:build unit

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDelegatedAPIKeyEnforcesNativeAuthenticationAndUSDGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name   string
		modify func(*service.APIKey, *config.Config)
		status int
	}{
		{"active", func(*service.APIKey, *config.Config) {}, http.StatusOK},
		{"wrong owner", func(k *service.APIKey, _ *config.Config) { k.UserID = 99 }, http.StatusServiceUnavailable},
		{"inactive user", func(k *service.APIKey, _ *config.Config) { k.User.Status = service.StatusDisabled }, http.StatusUnauthorized},
		{"disabled key", func(k *service.APIKey, _ *config.Config) { k.Status = service.StatusDisabled }, http.StatusUnauthorized},
		{"expired key", func(k *service.APIKey, _ *config.Config) { past := time.Now().Add(-time.Hour); k.ExpiresAt = &past }, http.StatusForbidden},
		{"zero balance", func(k *service.APIKey, _ *config.Config) { k.User.Balance = 0 }, http.StatusForbidden},
		{"disabled group", func(k *service.APIKey, _ *config.Config) {
			k.Group = &service.Group{ID: 1, Status: service.StatusDisabled}
			k.GroupID = &k.Group.ID
		}, http.StatusForbidden},
		{"wallet disabled", func(_ *service.APIKey, c *config.Config) { c.CanonicalWallet.USDWalletEnabled = false }, http.StatusServiceUnavailable},
		{"simple mode", func(_ *service.APIKey, c *config.Config) { c.RunMode = config.RunModeSimple }, http.StatusServiceUnavailable},
		{"missing canonical identity", func(k *service.APIKey, _ *config.Config) { k.User.PlatformUserID = "" }, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{RunMode: config.RunModeStandard}
			cfg.CanonicalWallet.Mode, cfg.CanonicalWallet.USDWalletEnabled = config.CanonicalWalletModeEnforce, true
			key := &service.APIKey{ID: 88, UserID: 7, Status: service.StatusActive, User: &service.User{ID: 7, Status: service.StatusActive, PlatformUserID: "platform-owner", Balance: 1}}
			tc.modify(key, cfg)
			var bearerLookup, resolveCalls int
			repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
				bearerLookup++
				return nil, service.ErrAPIKeyNotFound
			}, updateLastUsed: func(context.Context, int64, time.Time) error { return nil }}
			apiKeys := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(string(ContextKeyUser), AuthSubject{UserID: 7}); c.Next() })
			router.Use(DelegatedAPIKeyAuth(func(_ context.Context, user int64) (*service.APIKey, error) {
				resolveCalls++
				require.Equal(t, int64(7), user)
				return key, nil
			}, apiKeys, nil, cfg, false))
			router.Use(CanonicalUSDWalletGate(cfg))
			router.POST("/v1/chat/completions", func(c *gin.Context) {
				loaded, ok := GetAPIKeyFromContext(c)
				require.True(t, ok)
				require.Same(t, key, loaded)
				c.Status(http.StatusOK)
			})
			request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude-sonnet-5-5","messages":[]}`))
			request.Header.Set("Authorization", "Bearer attacker-controlled-key")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Equal(t, 1, resolveCalls)
			require.Zero(t, bearerLookup, "browser credentials must never replace the signed user's projection")
		})
	}
}

func TestDelegatedAPIKeyRequiresResolvedIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(DelegatedAPIKeyAuth(func(context.Context, int64) (*service.APIKey, error) {
		t.Fatal("unsigned request reached key resolver")
		return nil, nil
	}, nil, nil, nil, false))
	router.POST("/chat", func(c *gin.Context) { t.Fatal("unsigned request reached handler") })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/chat", nil))
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}
