package middleware

import (
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCanonicalUSDWalletGateFailsClosedBeforeUpstream(t *testing.T) {
	for _, name := range []string{"disabled", "shadow", "simple", "no user link", "subscription", "enforced"} {
		t.Run(name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
			cfg.CanonicalWallet.USDWalletEnabled = true
			key := &service.APIKey{User: &service.User{PlatformUserID: "console-user"}}
			switch name {
			case "disabled":
				cfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
			case "shadow":
				cfg.CanonicalWallet.Mode = config.CanonicalWalletModeShadow
			case "simple":
				cfg.RunMode = config.RunModeSimple
			case "no user link":
				key.User.PlatformUserID = ""
			case "subscription":
				key.Group = &service.Group{SubscriptionType: service.SubscriptionTypeSubscription}
			}
			forwarded := false
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(string(ContextKeyAPIKey), key) })
			router.POST("/v1/responses", CanonicalUSDWalletGate(cfg), func(c *gin.Context) { forwarded = true; c.Status(http.StatusNoContent) })
			res := httptest.NewRecorder()
			router.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
			if name == "enforced" {
				require.True(t, forwarded)
				require.Equal(t, 204, res.Code)
			} else {
				require.False(t, forwarded)
				require.Equal(t, 503, res.Code)
			}
		})
	}
}
func TestUSDMaintenanceKeepsDrainReadsAndRefusesWebsocketAdmission(t *testing.T) {
	t.Setenv("USD_LEDGER_MAINTENANCE", "true")
	router := gin.New()
	router.Use(USDLedgerMaintenance())
	router.Any("/test", func(c *gin.Context) { c.Status(204) })
	for _, tc := range []struct {
		method    string
		websocket bool
		status    int
	}{{"GET", false, 204}, {"POST", false, 503}, {"GET", true, 503}} {
		req := httptest.NewRequest(tc.method, "/test", nil)
		if tc.websocket {
			req.Header.Set("Upgrade", "websocket")
		}
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		require.Equal(t, tc.status, res.Code)
	}
}
