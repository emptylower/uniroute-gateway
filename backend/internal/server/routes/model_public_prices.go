package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
)

// RegisterPublicModelPriceRoutes exposes the anonymous, read-only retail
// projection in both panel and data-plane-only deployments. Human identity
// and commercial control-plane routes keep their separate registration gate.
func RegisterPublicModelPriceRoutes(v1 *gin.RouterGroup, h *handler.Handlers, panelRateLimiter *middleware.PanelRateLimiter) {
	if h == nil || h.ModelCatalog == nil {
		return
	}
	v1.GET("/models/public-prices", panelRateLimiter.PublicIP(), h.ModelCatalog.ListPublicPrices)
}
