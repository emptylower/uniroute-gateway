package admin

import (
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ModelGovernanceReadHandler exposes read-only governance evidence:
// the quarantine pool and the append-only governance event history
// (Phase 7 Task 0 G2). Responses carry identifiers, reasons, versions, and
// timestamps only — never credentials, raw snapshots, or upstream
// authorization headers.
type ModelGovernanceReadHandler struct {
	repo service.ModelGovernanceReadRepository
}

func NewModelGovernanceReadHandler(repo service.ModelGovernanceReadRepository) *ModelGovernanceReadHandler {
	return &ModelGovernanceReadHandler{repo: repo}
}

func (h *ModelGovernanceReadHandler) Quarantine(c *gin.Context) {
	if h.repo == nil {
		response.InternalError(c, "model governance read repository is not configured")
		return
	}
	page, pageSize := paginationParams(c)
	result, err := h.repo.ListQuarantinePool(c.Request.Context(), page, pageSize)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ModelGovernanceReadHandler) Connections(c *gin.Context) {
	if h.repo == nil {
		response.InternalError(c, "model governance read repository is not configured")
		return
	}
	page, pageSize := paginationParams(c)
	result, err := h.repo.ListConnections(c.Request.Context(), page, pageSize)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ModelGovernanceReadHandler) Events(c *gin.Context) {
	if h.repo == nil {
		response.InternalError(c, "model governance read repository is not configured")
		return
	}
	stream := strings.TrimSpace(c.Query("stream"))
	if stream == "" {
		stream = service.GovernanceEventStreamPublication
	}
	page, pageSize := paginationParams(c)
	result, err := h.repo.ListGovernanceEvents(c.Request.Context(), stream, page, pageSize)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func paginationParams(c *gin.Context) (page, pageSize int) {
	page = 1
	pageSize = 50
	if parsed, err := strconv.Atoi(strings.TrimSpace(c.Query("page"))); err == nil && parsed > 0 {
		page = parsed
	}
	if parsed, err := strconv.Atoi(strings.TrimSpace(c.Query("page_size"))); err == nil && parsed > 0 {
		pageSize = parsed
	}
	return page, pageSize
}
