package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// WalletReconciliationActor (Phase 4.1-G, redesign §15.3) is the synthetic
// principal every reconciliation read is audited under. The caller is
// ShipAny's scheduled job, not a person: there is no numeric user id (a
// synthetic one would pollute audit ActorUserID), no role, and no
// compliance acceptance a machine could give — which is also why the
// recon group omits AdminComplianceGuard (stated in the phase record).
const WalletReconciliationActor = "service:shipany-reconciliation"

// NewWalletReconciliationTokenAuth is the machine-to-machine bearer gate of
// the read-only reconciliation group (Phase 4.1-G): it replaces ONLY the
// admin JWT link of the admin chain — the panel rate limiter and the audit
// log stay, so every call is audited under WalletReconciliationActor. The
// compare is crypto/subtle.ConstantTimeCompare; a bad or missing token is
// 401 with WWW-Authenticate: Bearer; the token itself is never logged.
// The group is only mounted for a non-empty configured token
// (routes/admin.go), so an undeployed key is inert.
func NewWalletReconciliationTokenAuth(token string) gin.HandlerFunc {
	expected := []byte(token)
	return func(c *gin.Context) {
		header := strings.TrimSpace(c.GetHeader("Authorization"))
		parts := strings.SplitN(header, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
			c.Header("WWW-Authenticate", "Bearer")
			AbortWithError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Authorization required")
			return
		}
		presented := []byte(strings.TrimSpace(parts[1]))
		if subtle.ConstantTimeCompare(presented, expected) != 1 {
			c.Header("WWW-Authenticate", "Bearer")
			AbortWithError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid reconciliation token")
			return
		}
		// The synthetic principal, set BEFORE the audit link runs so the
		// recorded actor is the service, never an unattributed panel user.
		c.Set(auditCtxKeyActorEmail, WalletReconciliationActor)
		c.Set("auth_method", "service_token")
		c.Next()
	}
}
