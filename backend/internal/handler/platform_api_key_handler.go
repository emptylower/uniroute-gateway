package handler

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type PlatformAPIKeyHandler struct {
	service *service.PlatformAPIKeyService
}

func NewPlatformAPIKeyHandler(service *service.PlatformAPIKeyService) *PlatformAPIKeyHandler {
	return &PlatformAPIKeyHandler{service: service}
}

type upsertPlatformAPIKeyRequest struct {
	PlatformKeyID string `json:"platform_key_id" binding:"required"`
	KeySHA256     string `json:"key_sha256" binding:"required"`
	KeyPrefix     string `json:"key_prefix" binding:"required"`
	Status        string `json:"status"`
	Version       int64  `json:"version" binding:"required"`
	Name          string `json:"name"`
}

type revokePlatformAPIKeyRequest struct {
	Version int64 `json:"version" binding:"required"`
}

type platformAPIKeyMutationResponse struct {
	Projection *service.PlatformAPIKeyProjection `json:"projection"`
	Created    bool                              `json:"created,omitempty"`
	Revoked    bool                              `json:"revoked,omitempty"`
}

func (h *PlatformAPIKeyHandler) Upsert(c *gin.Context) {
	var request upsertPlatformAPIKeyRequest
	if err := decodeStrictJSON(c, &request); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user is required")
		return
	}
	projection, created, err := h.service.Upsert(c.Request.Context(), subject.UserID, service.PlatformAPIKeyUpsert{
		PlatformKeyID: request.PlatformKeyID,
		KeySHA256:     request.KeySHA256,
		KeyPrefix:     request.KeyPrefix,
		Status:        request.Status,
		Version:       request.Version,
		Name:          request.Name,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	payload := platformAPIKeyMutationResponse{Projection: projection, Created: created}
	if created {
		response.Created(c, payload)
		return
	}
	response.Success(c, payload)
}

func (h *PlatformAPIKeyHandler) Revoke(c *gin.Context) {
	var request revokePlatformAPIKeyRequest
	if err := decodeStrictJSON(c, &request); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "mapped gateway user is required")
		return
	}
	projection, revoked, err := h.service.Revoke(c.Request.Context(), subject.UserID, strings.TrimSpace(c.Param("platform_key_id")), request.Version)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, platformAPIKeyMutationResponse{Projection: projection, Revoked: revoked})
}

func decodeStrictJSON(c *gin.Context, target any) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return err
	}
	return nil
}
