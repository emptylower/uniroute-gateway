package routes

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func rejectUnavailableModel(c *gin.Context, models ...string) bool {
	if service.CheckModelAvailability(models...) == nil {
		return false
	}
	abortUnavailableModel(c)
	return true
}

func abortUnavailableModel(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{
		"type": "model_unavailable_error", "code": service.ModelTemporarilyUnavailableCode,
		"message": service.ModelTemporarilyUnavailableMessage,
	}})
}

func unavailableRequestModel(contentType string, body []byte, live bool) bool {
	if service.CheckJSONModelAvailability(body) != nil {
		return true
	}
	if live && service.CheckLiveCallModelAvailability(body) != nil {
		return true
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "multipart/form-data") {
		return false
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, err := reader.NextPart()
		if err != nil {
			return false // Existing multipart parser reports malformed requests.
		}
		modelField := strings.EqualFold(part.FormName(), "model")
		sessionField := live && strings.EqualFold(part.FormName(), "session")
		if (!modelField && !sessionField) || part.FileName() != "" {
			continue
		}
		value, err := io.ReadAll(part)
		if err != nil {
			return false
		}
		if modelField && service.CheckModelAvailability(string(value)) != nil {
			return true
		}
		if sessionField && service.CheckJSONModelAvailability(value) != nil {
			return true
		}
	}
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
		live := c.FullPath() == "/v1/live" || c.FullPath() == "/backend-api/codex/realtime/calls"
		if unavailableRequestModel(c.GetHeader("Content-Type"), body, live) {
			abortUnavailableModel(c)
			return
		}
		if rejectUnavailableModel(c, compositeRequestModelFromBody(c.GetHeader("Content-Type"), body)) {
			return
		}
		c.Next()
	}
}
