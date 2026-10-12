package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
)

func registerPublicMediaRoutes(r *gin.Engine, h *handler.Handlers, auth middleware.APIKeyAuthMiddleware) {
	if h == nil || h.PlatformInference == nil {
		return
	}
	h.PlatformInference.SetPublicMediaAudit(h.OpenAIGateway)
	media := r.Group("/v1/media")
	media.Use(middleware.RequestBodyLimit(256<<10), gin.HandlerFunc(auth), middleware.PublicMediaRateLimit())
	media.GET("/catalog", h.PlatformInference.PublicCatalog)
	media.POST("/quote", h.PlatformInference.PublicQuote)
	media.POST("/tasks", h.PlatformInference.PublicCreate)
	media.GET("/tasks", h.PlatformInference.PublicList)
	media.GET("/tasks/:task_id", h.PlatformInference.PublicGet)
}
