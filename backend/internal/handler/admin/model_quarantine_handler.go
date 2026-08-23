package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ModelQuarantineHandler struct {
	quarantineService *service.ModelQuarantineService
}

func NewModelQuarantineHandler(svc *service.ModelQuarantineService) *ModelQuarantineHandler {
	return &ModelQuarantineHandler{quarantineService: svc}
}

type quarantineRequest struct {
	BatchID                 string                   `json:"batch_id"`
	IdempotencyKey          string                   `json:"idempotency_key"`
	ExpectedRegistryVersion int64                    `json:"expected_registry_version"`
	ChannelVersions         map[int64]int64          `json:"channel_versions"`
	Items                   []service.QuarantineItem `json:"items"`
}

type restoreRequest struct {
	IdempotencyKey   string `json:"idempotency_key"`
	AccountID        int64  `json:"account_id"`
	ChannelID        int64  `json:"channel_id"`
	CanonicalModelID string `json:"canonical_model_id"`
}

func (h *ModelQuarantineHandler) Quarantine(c *gin.Context) {
	var req quarantineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	actorID := c.GetString("actor_id")
	if actorID == "" {
		actorID = "unknown"
	}
	input := service.QuarantineInput{
		BatchID: req.BatchID, IdempotencyKey: req.IdempotencyKey,
		ExpectedRegistryVersion: req.ExpectedRegistryVersion, ChannelVersions: req.ChannelVersions,
		ActorID: actorID, Items: req.Items,
	}
	if err := h.quarantineService.Quarantine(c.Request.Context(), input); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	response.Success(c, gin.H{"status": "quarantined"})
}

func (h *ModelQuarantineHandler) Restore(c *gin.Context) {
	var req restoreRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	actorID := c.GetString("actor_id")
	if actorID == "" {
		actorID = "unknown"
	}
	input := service.RestoreInput{
		IdempotencyKey: req.IdempotencyKey, AccountID: req.AccountID, ChannelID: req.ChannelID,
		CanonicalModelID: req.CanonicalModelID, ActorID: actorID,
	}
	if err := h.quarantineService.Restore(c.Request.Context(), input); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	response.Success(c, gin.H{"status": "restored"})
}
