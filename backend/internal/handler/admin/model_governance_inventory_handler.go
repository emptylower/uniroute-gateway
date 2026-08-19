package admin

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type modelGovernanceInventoryLister interface {
	List(ctx context.Context) ([]service.InventoryItem, error)
}

type ModelGovernanceInventoryHandler struct {
	service modelGovernanceInventoryLister
}

func NewModelGovernanceInventoryHandler(inventoryService *service.ModelGovernanceInventoryService) *ModelGovernanceInventoryHandler {
	return &ModelGovernanceInventoryHandler{service: inventoryService}
}

func (h *ModelGovernanceInventoryHandler) List(c *gin.Context) {
	items, err := h.service.List(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, items)
}
