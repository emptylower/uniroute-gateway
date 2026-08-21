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

type ModelGovernanceConnectionHandler struct {
	connService *service.UpstreamConnectionService
	designation *service.AggregatorDesignationService
	reuse       *service.AggregatorConnectionReuseService
}

func NewModelGovernanceConnectionHandler(connSvc *service.UpstreamConnectionService, desig *service.AggregatorDesignationService, reuse *service.AggregatorConnectionReuseService) *ModelGovernanceConnectionHandler {
	return &ModelGovernanceConnectionHandler{connService: connSvc, designation: desig, reuse: reuse}
}

func (h *ModelGovernanceConnectionHandler) DesignateAggregator(c *gin.Context) {
	connIDStr := c.Param("id")
	connID, err := strconv.ParseInt(connIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid connection id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	ifMatch := strings.TrimSpace(c.GetHeader("If-Match"))
	if ifMatch == "" {
		response.BadRequest(c, "If-Match header is required")
		return
	}
	expectedVersion, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid If-Match")
		return
	}
	if h.designation == nil {
		response.InternalError(c, "designation service not configured")
		return
	}
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok || assertion == nil || strings.TrimSpace(assertion.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	input := service.DesignateAggregatorInput{
		ConnectionID:   connID,
		ExpectedVersion: expectedVersion,
		ActorID:        assertion.Subject,
		IdempotencyKey: idempotencyKey,
		EvidenceRef:    strings.TrimSpace(c.GetHeader("X-Evidence-Ref")),
	}
	if err := h.designation.DesignateAggregator(c.Request.Context(), input); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "designated"})
}

func (h *ModelGovernanceConnectionHandler) ReuseAggregatorConnection(c *gin.Context) {
	connIDStr := c.Param("id")
	connID, err := strconv.ParseInt(connIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid connection id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	ifMatch := strings.TrimSpace(c.GetHeader("If-Match"))
	if ifMatch == "" {
		response.BadRequest(c, "If-Match header is required")
		return
	}
	credVersion, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid If-Match")
		return
	}
	if h.reuse == nil {
		response.InternalError(c, "reuse service not configured")
		return
	}
	// Parse body for provider/protocol/endpoint
	var req struct {
		Provider string `json:"provider"`
		Protocol string `json:"protocol"`
		Endpoint string `json:"endpoint"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	normalizedEndpoint, err := service.NormalizeEndpointPath(req.Endpoint)
	if err != nil {
		response.BadRequest(c, "invalid endpoint")
		return
	}
	assertion2, ok2 := middleware.PlatformAssertionFromContext(c)
	if !ok2 || assertion2 == nil || strings.TrimSpace(assertion2.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	input := service.ReuseAggregatorConnectionInput{
		ConnectionID:       connID,
		Provider:           service.GovernanceProvider(req.Provider),
		Protocol:           service.AccountProtocol(req.Protocol),
		NormalizedEndpoint: normalizedEndpoint,
		ClientRequestID:    idempotencyKey,
		CredentialVersion:  credVersion,
		ActorID:            assertion2.Subject,
	}
	accountID, err := h.reuse.Reuse(c.Request.Context(), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"account_id": accountID})
}
