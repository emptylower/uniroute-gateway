package handler

import (
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type PlatformIdentityHandler struct {
	service *service.PlatformIdentityService
}

func NewPlatformIdentityHandler(service *service.PlatformIdentityService) *PlatformIdentityHandler {
	return &PlatformIdentityHandler{service: service}
}

type upsertPlatformIdentityRequest struct {
	PlatformUserID string `json:"platform_user_id" binding:"required"`
	Username       string `json:"username"`
	Status         string `json:"status"`
}

type upsertPlatformIdentityResponse struct {
	Identity *service.PlatformIdentity `json:"identity"`
	Created  bool                      `json:"created"`
}

func (h *PlatformIdentityHandler) Upsert(c *gin.Context) {
	var request upsertPlatformIdentityRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok {
		response.Unauthorized(c, "invalid internal assertion")
		return
	}
	if assertion.Subject != strings.TrimSpace(request.PlatformUserID) {
		response.ErrorFrom(c, infraerrors.Forbidden("PLATFORM_SUBJECT_MISMATCH", "assertion subject does not match platform_user_id"))
		return
	}

	identity, created, err := h.service.Upsert(c.Request.Context(), request.PlatformUserID, request.Username, request.Status)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	payload := upsertPlatformIdentityResponse{Identity: identity, Created: created}
	if created {
		response.Created(c, payload)
		return
	}
	response.Success(c, payload)
}

func (h *PlatformIdentityHandler) Get(c *gin.Context) {
	platformUserID := strings.TrimSpace(c.Param("platform_user_id"))
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok {
		response.Unauthorized(c, "invalid internal assertion")
		return
	}
	if assertion.Subject != platformUserID {
		response.ErrorFrom(c, infraerrors.Forbidden("PLATFORM_SUBJECT_MISMATCH", "assertion subject does not match platform_user_id"))
		return
	}

	identity, err := h.service.Get(c.Request.Context(), platformUserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, identity)
}
