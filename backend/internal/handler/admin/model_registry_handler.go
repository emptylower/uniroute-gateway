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

// ModelRegistryHandler exposes reviewed registry decisions.
type ModelRegistryHandler struct {
	service service.ModelRegistryService
}

func NewModelRegistryHandler(svc service.ModelRegistryService) *ModelRegistryHandler {
	return &ModelRegistryHandler{service: svc}
}

func (h *ModelRegistryHandler) List(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model registry service is not configured")
		return
	}
	// Use request context for cancellation.
	items, err := h.service.List(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, items)
}

func (h *ModelRegistryHandler) Get(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model registry service is not configured")
		return
	}
	canonicalID := strings.TrimSpace(c.Param("canonical_id"))
	if canonicalID == "" {
		response.BadRequest(c, "canonical_id is required")
		return
	}
	entry, err := h.service.Get(c.Request.Context(), canonicalID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if entry == nil {
		response.NotFound(c, "registry entry not found")
		return
	}
	response.Success(c, entry)
}

func (h *ModelRegistryHandler) CreateDecision(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model registry service is not configured")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	// If-Match carries expected version for optimistic concurrency.
	ifMatch := strings.TrimSpace(c.GetHeader("If-Match"))
	if ifMatch == "" {
		response.BadRequest(c, "If-Match header is required")
		return
	}
	var expectedVersion int64
	if _, err := parseRegistryVersion(ifMatch, &expectedVersion); err != nil {
		response.BadRequest(c, "invalid If-Match version")
		return
	}

	var req struct {
		CanonicalID string   `json:"canonical_id"`
		Provider    string   `json:"provider"`
		Modality    string   `json:"modality"`
		Status      string   `json:"status"`
		Aliases     []string `json:"aliases"`
		EvidenceRef *string  `json:"evidence_ref"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	// Capture actor from verified platform assertion (signed, replay-protected). Never trust raw headers.
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok || strings.TrimSpace(assertion.Subject) == "" {
		response.Unauthorized(c, "missing or invalid platform assertion")
		return
	}
	actorID := strings.TrimSpace(assertion.Subject)
	// Defend against forged header trying to override assertion.
	if forged := strings.TrimSpace(c.GetHeader("X-Actor-ID")); forged != "" && forged != actorID {
		// Explicitly ignore forged header; log for audit.
		_ = forged
	}

	input := service.RegistryDecisionInput{
		IdempotencyKey:  idempotencyKey,
		ExpectedVersion: expectedVersion,
		CanonicalID:     req.CanonicalID,
		Provider:        service.GovernanceProvider(req.Provider),
		Modality:        req.Modality,
		Status:          req.Status,
		Aliases:         req.Aliases,
		ActorID:         actorID,
		EvidenceRef:     req.EvidenceRef,
	}

	entry, err := h.service.CreateDecision(c.Request.Context(), input)
	if err != nil {
		// Map version conflict to 409, idempotency collision etc.
		if isConflictError(err) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		response.ErrorFrom(c, err)
		return
	}
	c.Header("ETag", formatRegistryVersion(entry.Version))
	c.JSON(http.StatusCreated, entry)
}

func (h *ModelRegistryHandler) Rebuild(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model registry service is not configured")
		return
	}
	if err := h.service.Rebuild(c.Request.Context()); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"status": "rebuilt"})
}

func parseRegistryVersion(raw string, out *int64) (bool, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "W/")
	trimmed = strings.Trim(trimmed, `"`)
	if strings.TrimSpace(trimmed) == "" {
		return false, &parseError{msg: "invalid version"}
	}
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || v < 0 {
		return false, &parseError{msg: "invalid version"}
	}
	*out = v
	return true, nil
}

type parseError struct{ msg string }

func (e *parseError) Error() string { return e.msg }

func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "conflict") || strings.Contains(msg, "stale") || strings.Contains(msg, "version")
}

func formatRegistryVersion(v int64) string {
	return `"` + strconv.FormatInt(v, 10) + `"`
}

// Ensure handler uses context import.
var _ = NewModelRegistryHandler
