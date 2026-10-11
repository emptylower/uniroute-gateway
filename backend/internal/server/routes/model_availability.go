package routes

import (
	"errors"
	"net/http"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func rejectUnavailableModel(c *gin.Context, models ...string) bool {
	if service.CheckModelAvailability(models...) == nil {
		return false
	}
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{
		"type": "model_unavailable_error", "code": service.ModelTemporarilyUnavailableCode,
		"message": service.ModelTemporarilyUnavailableMessage,
	}})
	return true
}

// Protect all generation protocols before routing, while preserving the body
// for the existing parsers. Gemini carries its model in the URL wildcard.
func modelAvailabilityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet {
			c.Next()
			return
		}
		if rejectUnavailableModel(c, compositeGeminiModelFromParams(c)) {
			return
		}
		body, err := pkghttputil.ReadRequestBodyWithPrealloc(c.Request)
		if err != nil {
			status := http.StatusBadRequest
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				status = http.StatusRequestEntityTooLarge
			}
			c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"type": "invalid_request_error", "message": "Failed to read request body"}})
			return
		}
		resetRequestBody(c, body)
		if rejectUnavailableModel(c, compositeRequestModelFromBody(c.GetHeader("Content-Type"), body)) {
			return
		}
		c.Next()
	}
}
