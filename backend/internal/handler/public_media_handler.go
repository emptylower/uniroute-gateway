package handler

import (
	"encoding/json"
	"strconv"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func publicMediaKey(c *gin.Context) *service.APIKey {
	key, ok := middleware.GetAPIKeyFromContext(c)
	if !ok {
		publicMediaError(c, infraerrors.Unauthorized("API_KEY_REQUIRED", "API key authentication required"))
		return nil
	}
	return key
}
func (h *PlatformInferenceHandler) PublicCatalog(c *gin.Context) {
	key := publicMediaKey(c)
	if key == nil {
		return
	}
	models, err := h.service.PublicCatalog(key)
	if err != nil {
		publicMediaError(c, err)
		return
	}
	response.Success(c, gin.H{"models": models, "currency": "USD", "policy_version": "usd-wallet-v1", "enabled": h.service.PublicEnabled(key.ID) && h.service.Enabled()})
}
func (h *PlatformInferenceHandler) PublicQuote(c *gin.Context) {
	key := publicMediaKey(c)
	if key == nil {
		return
	}
	var in struct {
		Model  string `json:"model"`
		Option string `json:"option"`
	}
	if decodeStrictJSON(c, &in) != nil {
		publicMediaError(c, infraerrors.BadRequest("INVALID_REQUEST", "invalid quote request"))
		return
	}
	quote, err := h.service.PublicQuote(key, in.Model, in.Option)
	if err != nil {
		publicMediaError(c, err)
		return
	}
	response.Success(c, quote)
}
func (h *PlatformInferenceHandler) PublicCreate(c *gin.Context) {
	key := publicMediaKey(c)
	if key == nil {
		return
	}
	var in service.MediaCreateInput
	if decodeStrictJSON(c, &in) != nil {
		publicMediaError(c, infraerrors.BadRequest("INVALID_REQUEST", "invalid media request"))
		return
	}
	task, err := h.service.CreatePublicChecked(c.Request.Context(), key, c.GetHeader("Idempotency-Key"), in, func(normalized service.MediaCreateInput) error {
		subject, ok := middleware.GetAuthSubjectFromContext(c)
		if !ok {
			return infraerrors.Unauthorized("USER_REQUIRED", "API key subject required")
		}
		body, _ := json.Marshal(gin.H{"prompt": normalized.Prompt})
		decision := h.publicAudit.checkSecurityAudit(c, nil, key, subject, service.ContentModerationProtocolOpenAIImages, normalized.Model, body)
		if decision != nil && !decision.AllowNextStage {
			return infraerrors.New(securityAuditStatus(decision), securityAuditErrorCode(decision), securityAuditMessage(decision))
		}
		return nil
	})
	if err != nil {
		if status, retry, ok := service.WalletRiskRefusalDetails(err); ok {
			c.Header("Retry-After", strconv.Itoa(retry))
			publicMediaError(c, infraerrors.New(status, "WALLET_RISK_REFUSED", "USD wallet admission refused; retry later"))
			return
		}
		publicMediaError(c, err)
		return
	}
	response.Accepted(c, task)
}
func (h *PlatformInferenceHandler) PublicGet(c *gin.Context) {
	key := publicMediaKey(c)
	if key == nil {
		return
	}
	task, err := h.service.GetPublic(c.Request.Context(), key, c.Param("task_id"))
	if err != nil {
		publicMediaError(c, err)
		return
	}
	response.Success(c, task)
}
func (h *PlatformInferenceHandler) PublicList(c *gin.Context) {
	key := publicMediaKey(c)
	if key == nil {
		return
	}
	limit := 20
	if raw := c.Query("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			publicMediaError(c, infraerrors.BadRequest("INVALID_LIMIT", "limit must be 1 to 100"))
			return
		}
		limit = value
	}
	tasks, err := h.service.ListPublic(c.Request.Context(), key, limit, c.Query("cursor"))
	if err != nil {
		publicMediaError(c, err)
		return
	}
	response.Success(c, tasks)
}

func (h *PlatformInferenceHandler) SetPublicMediaAudit(audit *OpenAIGatewayHandler) {
	h.publicAudit = audit
}

func publicMediaError(c *gin.Context, err error) {
	status, detail := infraerrors.ToHTTP(err)
	c.JSON(status, gin.H{"code": status, "message": detail.Message, "reason": detail.Reason, "data": nil})
}
