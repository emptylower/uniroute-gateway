package handler

import (
	"context"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type PlatformInferenceHandler struct{ service *service.MediaTaskService }

func NewPlatformInferenceHandler(s *service.MediaTaskService) *PlatformInferenceHandler {
	return &PlatformInferenceHandler{service: s}
}

func (h *PlatformInferenceHandler) PlaygroundKey(ctx context.Context, userID int64) (*service.APIKey, error) {
	return h.service.PlaygroundKey(ctx, userID)
}

func (h *PlatformInferenceHandler) Catalog(c *gin.Context) {
	response.Success(c, gin.H{"models": service.MediaTaskCatalog(), "currency": "USD", "policy_version": "usd-wallet-v1", "enabled": h.service.Enabled()})
}
func (h *PlatformInferenceHandler) Quote(c *gin.Context) {
	var in struct {
		Model  string `json:"model"`
		Option string `json:"option"`
	}
	if decodeStrictJSON(c, &in) != nil {
		response.BadRequest(c, "invalid quote request")
		return
	}
	quote, err := h.service.Quote(in.Model, in.Option)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, quote)
}
func (h *PlatformInferenceHandler) Create(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user required")
		return
	}
	var in service.MediaCreateInput
	if decodeStrictJSON(c, &in) != nil {
		response.BadRequest(c, "invalid media request")
		return
	}
	task, err := h.service.Create(c.Request.Context(), subject.UserID, c.GetHeader("Idempotency-Key"), in)
	if err != nil {
		if writeWalletRiskResponse(c, err) {
			return
		}
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, task)
}
func (h *PlatformInferenceHandler) Get(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user required")
		return
	}
	task, err := h.service.Get(c.Request.Context(), subject.UserID, c.Param("task_id"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, task)
}
func (h *PlatformInferenceHandler) List(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user required")
		return
	}
	limit := 20
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			response.BadRequest(c, "limit must be 1 to 100")
			return
		}
		limit = n
	}
	tasks, err := h.service.List(c.Request.Context(), subject.UserID, limit, c.Query("cursor"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tasks)
}
func (h *PlatformInferenceHandler) LegacyRead(c *gin.Context) {
	var in struct {
		ProviderTaskID string `json:"provider_task_id"`
		Model          string `json:"model"`
		MediaType      string `json:"media_type"`
	}
	if decodeStrictJSON(c, &in) != nil {
		response.BadRequest(c, "invalid legacy task query")
		return
	}
	result, err := h.service.LegacyRead(c.Request.Context(), in.ProviderTaskID, in.Model, in.MediaType)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
func (h *PlatformInferenceHandler) Availability(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user required")
		return
	}
	var in service.WalletAvailabilityInput
	if decodeStrictJSON(c, &in) != nil {
		response.BadRequest(c, "invalid wallet snapshot")
		return
	}
	result, err := h.service.Availability(c.Request.Context(), subject.UserID, in)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
