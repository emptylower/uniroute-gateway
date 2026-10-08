package middleware

import (
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strings"
)

func USDLedgerMaintenance() gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("USD_LEDGER_MAINTENANCE")), "true") && (c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead && c.Request.Method != http.MethodOptions || strings.EqualFold(c.GetHeader("Upgrade"), "websocket")) {
			c.Header("Retry-After", "60")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"type": "maintenance", "message": "USD wallet maintenance in progress"}})
		}
	}
}

// All public billable paths require canonical USD enforcement, including simple
// mode and subscription groups. Missing wallet dependencies fail before upstream.
func CanonicalUSDWalletGate(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		USDLedgerMaintenance()(c)
		if c.IsAborted() {
			return
		}
		if c.Request.Method == http.MethodGet && !strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			return
		}
		key, ok := GetAPIKeyFromContext(c)
		if cfg == nil || cfg.RunMode == config.RunModeSimple || cfg.CanonicalWallet.Mode != config.CanonicalWalletModeEnforce || !cfg.CanonicalWallet.USDWalletEnabled || !ok || key == nil || key.User == nil || strings.TrimSpace(key.User.PlatformUserID) == "" || key.Group != nil && key.Group.IsSubscriptionType() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"type": "wallet_unavailable", "message": "USD wallet authorization is unavailable"}})
		}
	}
}
