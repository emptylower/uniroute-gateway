package admin

import (
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ModelCatalogCandidateHandler exposes read-only external catalog evidence
// views plus audited source settings. It never exposes gateway assertions or
// raw upstream snapshots.
type ModelCatalogCandidateHandler struct {
	service service.ModelCatalogCandidateViews
}

func NewModelCatalogCandidateHandler(svc service.ModelCatalogCandidateViews) *ModelCatalogCandidateHandler {
	return &ModelCatalogCandidateHandler{service: svc}
}

func (h *ModelCatalogCandidateHandler) ListSources(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model catalog service is not configured")
		return
	}
	statuses, err := h.service.SourceStatus(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, statuses)
}

func (h *ModelCatalogCandidateHandler) ListCandidates(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model catalog service is not configured")
		return
	}
	candidates, err := h.service.CandidateList(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, candidates)
}

func (h *ModelCatalogCandidateHandler) ListRetirements(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model catalog service is not configured")
		return
	}
	suggestions, err := h.service.RetirementSuggestions(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, suggestions)
}

func (h *ModelCatalogCandidateHandler) ListPriceAnomalies(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model catalog service is not configured")
		return
	}
	anomalies, err := h.service.PriceAnomalies(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, anomalies)
}

type updateCatalogSourceSettingRequest struct {
	Enabled          *bool `json:"enabled"`
	ThresholdPercent *int  `json:"count_drop_threshold_percent"`
}

func (h *ModelCatalogCandidateHandler) UpdateSourceSettings(c *gin.Context) {
	if h.service == nil {
		response.InternalError(c, "model catalog service is not configured")
		return
	}
	source := strings.TrimSpace(c.Param("source"))
	if source == "" {
		response.BadRequest(c, "source is required")
		return
	}
	var req updateCatalogSourceSettingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	var enabled *bool
	if req.Enabled != nil {
		value := *req.Enabled
		enabled = &value
	}
	var threshold *int
	if req.ThresholdPercent != nil {
		if *req.ThresholdPercent < 1 || *req.ThresholdPercent > 100 {
			response.BadRequest(c, "count_drop_threshold_percent must be within 1..100")
			return
		}
		value := *req.ThresholdPercent
		threshold = &value
	}
	// The gateway-admin group is scoped by :platform_user_id; use it as the audit actor.
	actorID := strings.TrimSpace(c.Param("platform_user_id"))
	if actorID == "" {
		actorID = "unknown-admin"
	}
	if err := h.service.UpdateSourceSetting(c.Request.Context(), actorID, source, enabled, threshold); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"source": source})
}
