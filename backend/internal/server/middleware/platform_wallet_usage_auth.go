package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"regexp"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var platformWalletUsageKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// RequirePlatformWalletUsageAvailability validates the isolated bindings after
// signature verification and before consuming the shared replay token.
func RequirePlatformWalletUsageAvailability(cfg config.PlatformIdentityConfig, redisClient *redis.Client) gin.HandlerFunc {
	return requirePlatformAssertion(cfg, service.PlatformWalletUsageAvailabilityScope, redisClient, func(c *gin.Context, assertion *PlatformAssertion) bool {
		owner := c.Param("platform_user_id")
		normalized, ownerErr := service.NormalizePlatformUserID(owner)
		scope, scopeOK := assertion.claims["scope"].(string)
		issuedAt, issuedOK := assertion.claims["iat"].(float64)
		expiresAt, expiresOK := assertion.claims["exp"].(float64)
		if !scopeOK || scope != service.PlatformWalletUsageAvailabilityScope || !issuedOK || !expiresOK ||
			math.IsNaN(issuedAt) || math.IsInf(issuedAt, 0) || math.IsNaN(expiresAt) || math.IsInf(expiresAt, 0) ||
			math.Trunc(issuedAt) != issuedAt || math.Trunc(expiresAt) != expiresAt || expiresAt-issuedAt <= 0 || expiresAt-issuedAt > 30 ||
			ownerErr != nil || normalized != owner || assertion.Subject != owner ||
			c.Request.Method != http.MethodPost || c.Request.URL.RawQuery != "" || c.Request.URL.ForceQuery ||
			c.Request.URL.Path != "/api/internal/v1/users/"+owner+"/wallet/usage-availability" {
			response.Forbidden(c, "wallet usage assertion binding denied")
			c.Abort()
			return false
		}
		claim := func(name string) string {
			value, _ := assertion.claims[name].(string)
			return value
		}
		if claim("purpose") != "usage-wallet-availability" || claim("platform_user_id") != owner || claim("http_method") != http.MethodPost || claim("http_path") != c.Request.URL.EscapedPath() {
			response.Forbidden(c, "wallet usage assertion binding denied")
			c.Abort()
			return false
		}
		if key, present := assertion.claims["platform_key_id"]; present {
			keyID, valid := key.(string)
			if !valid || !platformWalletUsageKeyPattern.MatchString(keyID) {
				response.Forbidden(c, "wallet usage assertion key binding denied")
				c.Abort()
				return false
			}
		}
		controller := http.NewResponseController(c.Writer)
		if err := controller.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			response.Error(c, http.StatusServiceUnavailable, "wallet usage body deadline unavailable")
			c.Abort()
			return false
		}
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			// Close while the deadline is active so rejection cannot start an
			// unbounded drain of a slow or oversized request body.
			_ = c.Request.Body.Close()
		}
		if clearErr := controller.SetReadDeadline(time.Time{}); clearErr != nil {
			response.Error(c, http.StatusServiceUnavailable, "wallet usage body deadline unavailable")
			c.Abort()
			return false
		}
		if err != nil || len(body) > 1<<20 {
			response.BadRequest(c, "invalid bounded wallet usage snapshot")
			c.Abort()
			return false
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		digest := sha256.Sum256(body)
		if claim("body_sha256") != hex.EncodeToString(digest[:]) {
			response.Forbidden(c, "wallet usage assertion body binding denied")
			c.Abort()
			return false
		}
		return true
	})
}
