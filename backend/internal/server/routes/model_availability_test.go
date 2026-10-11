package routes

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestModelAvailabilityMiddlewareRejectsJSONAndPreservesAllowedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, model := range []string{"kling/ai-avatar-pro", "omnihuman-1-5/subject-detection", "wan/2-2-animate-replace", "gpt-5"} {
		t.Run(model, func(t *testing.T) {
			router := gin.New()
			router.Use(modelAvailabilityMiddleware())
			body := `{"model":"` + model + `","prompt":"hello","other":{"untouched":true}}`
			called := false
			router.POST("/v1/videos/generations", func(c *gin.Context) {
				called = true
				actual, err := io.ReadAll(c.Request.Body)
				require.NoError(t, err)
				require.Equal(t, body, string(actual))
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			if model == "gpt-5" {
				require.True(t, called)
				require.Equal(t, http.StatusNoContent, response.Code)
			} else {
				require.False(t, called)
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
				require.Contains(t, response.Body.String(), service.ModelTemporarilyUnavailableCode)
				require.Contains(t, response.Body.String(), "模型暂时下架")
			}
		})
	}
}

func TestModelAvailabilityMiddlewareMultipartAndGeminiWildcard(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, model := range []string{"infinitalk/from-audio", "gpt-image-1"} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		require.NoError(t, writer.WriteField("model", model))
		file, err := writer.CreateFormFile("image", "portrait.png")
		require.NoError(t, err)
		_, err = file.Write([]byte("keep-image-bytes"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		router := gin.New()
		router.Use(modelAvailabilityMiddleware())
		router.POST("/v1/images/edits", func(c *gin.Context) {
			actual, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			require.Equal(t, body.Bytes(), actual)
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
		req.Header.Set("Content-Type", writer.FormDataContentType())
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		want := http.StatusNoContent
		if model == "infinitalk/from-audio" {
			want = http.StatusServiceUnavailable
		}
		require.Equal(t, want, response.Code)
	}
	router := gin.New()
	router.Use(modelAvailabilityMiddleware())
	router.POST("/v1beta/models/*modelAction", func(c *gin.Context) { t.Fatal("suspended Gemini URL reached handler") })
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1beta/models/kling/ai-avatar-pro:generateContent", strings.NewReader(`{"contents":[]}`)))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}

func TestModelAvailabilityCompositeAliasesRejectBeforeRewriteOrHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resolver := service.NewCompositeRouteResolver(compositeRouteRepoStub{routes: []service.CompositeModelRoute{{
		ID: 1, GroupID: 1, PublicModel: "portrait-alias", MatchType: service.CompositeRouteMatchExact,
		TargetPlatform: service.PlatformOpenAI, UpstreamModel: "kling/ai-avatar-pro",
		Endpoint: service.CompositeRouteEndpointAny, Priority: 100, Enabled: true,
	}}})
	for _, gemini := range []bool{false, true} {
		router := gin.New()
		router.Use(func(c *gin.Context) {
			groupID := int64(1)
			c.Set(string(servermiddleware.ContextKeyAPIKey), &service.APIKey{GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformComposite}})
			c.Next()
		})
		router.Use(modelAvailabilityMiddleware())
		path := "/v1/chat/completions"
		if gemini {
			router.Use(compositeGeminiTargetPlatformMiddleware(resolver))
			path = "/v1beta/models/portrait-alias:generateContent"
			router.POST("/v1beta/models/*modelAction", func(c *gin.Context) { t.Fatal("suspended alias reached handler") })
		} else {
			router.Use(compositeTargetPlatformMiddleware(resolver))
			router.POST(path, func(c *gin.Context) { t.Fatal("suspended alias reached handler") })
		}
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"portrait-alias"}`))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
	}
}
