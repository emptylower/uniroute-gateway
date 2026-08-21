package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type ModelAuthorizationActivationHandler struct {
	activationService *service.ModelAuthorizationActivationService
}

func NewModelAuthorizationActivationHandler(svc *service.ModelAuthorizationActivationService) *ModelAuthorizationActivationHandler {
	return &ModelAuthorizationActivationHandler{activationService: svc}
}

type activationRequest struct {
	InventoryHash    string           `json:"inventory_hash"`
	RegistryVersion  int64            `json:"registry_version"`
	ChannelVersions  map[int64]int64  `json:"channel_versions"`
	ProjectedBatchID string           `json:"projected_batch_id"`
	AcknowledgedBy   string           `json:"acknowledged_by"`
	IdempotencyKey   string           `json:"idempotency_key"`
}

func (h *ModelAuthorizationActivationHandler) Activate(c *gin.Context) {
	var req activationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	actorID := c.GetString("actor_id")
	if actorID == "" {
		actorID = "unknown"
	}
	input := service.ActivationInput{
		InventoryHash: req.InventoryHash, RegistryVersion: req.RegistryVersion,
		ChannelVersions: req.ChannelVersions, ProjectedBatchID: req.ProjectedBatchID,
		AcknowledgedBy: req.AcknowledgedBy, IdempotencyKey: req.IdempotencyKey, ActorID: actorID,
	}
	if err := h.activationService.Activate(c.Request.Context(), input); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "activated", "mode": "enforce"})
}

func (h *ModelAuthorizationActivationHandler) Deactivate(c *gin.Context) {
	var req struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	actorID := c.GetString("actor_id")
	if actorID == "" {
		actorID = "unknown"
	}
	if err := h.activationService.Deactivate(c.Request.Context(), req.IdempotencyKey, actorID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deactivated", "mode": "shadow"})
}
