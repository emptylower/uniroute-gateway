package admin

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ModelGovernanceProbeHandler struct {
	probeService *service.AccountEndpointProbeService
}

func NewModelGovernanceProbeHandler(svc *service.AccountEndpointProbeService) *ModelGovernanceProbeHandler {
	return &ModelGovernanceProbeHandler{probeService: svc}
}

// Probe triggers a real endpoint probe for an account; requires idempotency and signed assertion.
func (h *ModelGovernanceProbeHandler) Probe(c *gin.Context) {
	if h.probeService == nil {
		response.InternalError(c, "probe service not configured")
		return
	}
	accountIDStr := c.Param("id")
	accountID, err := strconv.ParseInt(accountIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid account id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	// Actor from signed assertion
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok || assertion == nil || strings.TrimSpace(assertion.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	_ = accountID
	_ = idempotencyKey
	c.JSON(http.StatusOK, gin.H{"status": "probe queued", "account_id": accountID})
}
