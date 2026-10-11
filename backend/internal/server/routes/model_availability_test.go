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

func TestModelAvailabilityMiddlewareDuplicateJSONModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"model":"gpt-5","model":"kling/ai-avatar-pro"}`,
		`{"model":"kling/ai-avatar-pro","model":"gpt-5"}`,
		`{"model":"gpt-5","mo\u0064el":"kling\/ai-avatar-\u0070ro"}`,
		`{"model":"gpt-5","\u004dodel":"kling/ai-avatar-pro"}`,
		`{"model":"gpt-5","MODEL":"kling/ai-avatar-pro"}`,
		`{"model":"kling/ai-avatar-pro",`,
		`{"model":"gpt-5","model":"gpt-5.2","input":{"model":"kling/ai-avatar-pro"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			router := gin.New()
			router.Use(modelAvailabilityMiddleware())
			called := false
			router.POST("/v1/responses", func(c *gin.Context) {
				called = true
				actual, err := io.ReadAll(c.Request.Body)
				require.NoError(t, err)
				require.Equal(t, body, string(actual))
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			if strings.Contains(body, `"model":"gpt-5.2"`) {
				require.True(t, called)
				require.Equal(t, http.StatusNoContent, response.Code)
			} else {
				require.False(t, called)
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
			}
		})
	}
}

func TestModelAvailabilityMiddlewareAllMultipartModelParts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, models := range [][]string{{"gpt-image-1", "infinitalk/from-audio"}, {"infinitalk/from-audio", "gpt-image-1"}, {"gpt-image-1", "gpt-image-2"}} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		for _, model := range models {
			require.NoError(t, writer.WriteField("model", model))
		}
		file, err := writer.CreateFormFile("model", "input.txt")
		require.NoError(t, err)
		_, err = file.Write([]byte("kling/ai-avatar-pro"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		router := gin.New()
		router.Use(modelAvailabilityMiddleware())
		called := false
		router.POST("/v1/images/edits", func(c *gin.Context) {
			called = true
			actual, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			require.Equal(t, body.Bytes(), actual)
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
		req.Header.Set("Content-Type", writer.FormDataContentType())
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if models[1] == "gpt-image-2" {
			require.True(t, called)
			require.Equal(t, http.StatusNoContent, response.Code)
		} else {
			require.False(t, called)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
		}
	}
}

func TestModelAvailabilityMiddlewareLenientJSONModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name    string
		body    string
		blocked bool
	}{
		{"newline before late model", "{\"model\":\"gpt-5\",\"input\":\"x\ny\",\"model\":\"kling/ai-avatar-pro\"}", true},
		{"controls before escaped model", "{\"model\":\"gpt-5\",\"input\":\"x\x00\x7fy\",\"\\u004dodel\":\"kling/ai-avatar-pro\"}", true},
		{"BOM before duplicate model", "\xef\xbb\xbf" + `{"model":"gpt-5","model":"kling/ai-avatar-pro"}`, true},
		{"allowed nested controls", "{\"model\":\"gpt-5\",\"input\":{\"text\":\"x\ny\",\"model\":\"kling/ai-avatar-pro\"}}", false},
		{"allowed BOM", "\xef\xbb\xbf" + `{"model":"gpt-5","input":{"model":"kling/ai-avatar-pro"}}`, false},
	}
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		for _, tc := range cases {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				router := gin.New()
				router.Use(modelAvailabilityMiddleware())
				called := false
				router.POST(path, func(c *gin.Context) {
					called = true
					actual, err := io.ReadAll(c.Request.Body)
					require.NoError(t, err)
					require.Equal(t, tc.body, string(actual), "policy normalization must preserve the request body")
					c.Status(http.StatusNoContent)
				})
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				if tc.blocked {
					require.False(t, called, "suspended model must stop before the lenient generation handler")
					require.Equal(t, http.StatusServiceUnavailable, response.Code)
					require.Contains(t, response.Body.String(), service.ModelTemporarilyUnavailableCode)
				} else {
					require.True(t, called)
					require.Equal(t, http.StatusNoContent, response.Code)
				}
			})
		}
	}
}

func TestModelAvailabilityMiddlewareLiveSessionSelectors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		body, contentType string
		blocked           bool
	}{
		{`{"sdp":"v=0","session":{"model":"gpt-5","model":"kling/ai-avatar-pro"}}`, "application/json", true},
		{`{"session":{"model":"gpt-5"},"\u0053ession":{"\u004dodel":"kling/ai-avatar-pro"}}`, "application/json", true},
		{`{"session":{"model":"gpt-5","metadata":{"model":"kling/ai-avatar-pro"}}}`, "application/json", false},
	}
	for _, sessions := range [][]string{
		{`{"model":"gpt-5"}`, `{"model":"gpt-5","model":"kling/ai-avatar-pro"}`},
		{`{"model":"kling/ai-avatar-pro"}`, `{"model":"gpt-5"}`},
		{`{"model":"gpt-5"}`, `{"model":"gpt-5"}`},
	} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		for _, session := range sessions {
			require.NoError(t, writer.WriteField("session", session))
		}
		require.NoError(t, writer.Close())
		cases = append(cases, struct {
			body, contentType string
			blocked           bool
		}{body.String(), writer.FormDataContentType(), strings.Contains(body.String(), "kling/ai-avatar-pro")})
	}
	for _, path := range []string{"/v1/live", "/backend-api/codex/realtime/calls"} {
		for _, tc := range cases {
			t.Run(path+"/"+tc.body, func(t *testing.T) {
				router := gin.New()
				router.Use(modelAvailabilityMiddleware())
				called := false
				router.POST(path, func(c *gin.Context) {
					called = true
					actual, err := io.ReadAll(c.Request.Body)
					require.NoError(t, err)
					require.Equal(t, tc.body, string(actual))
					c.Status(http.StatusNoContent)
				})
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", tc.contentType)
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, req)
				require.Equal(t, !tc.blocked, called)
				if tc.blocked {
					require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
				} else {
					require.Equal(t, http.StatusNoContent, recorder.Code)
				}
			})
		}
	}
}
