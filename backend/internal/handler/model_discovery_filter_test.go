//go:build unit

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryModelFilteringPreservesUpstreamFields(t *testing.T) {
	account := &service.Account{Credentials: map[string]any{"model_mapping": map[string]any{"alias": "omnihuman-1-5"}}}
	for _, body := range []string{
		`{"models":[{"slug":"kling-ai-avatar-pro"},{"slug":"alias"},{"slug":"gpt-5.4","tools":["custom"]}],"next_page_token":"next","extension":{"x":1}}`,
		`{"models":[{"name":"models/omnihuman-1-5"},{"name":"models/alias"},{"id":"infinitalk/from-audio"},{"name":"models/gemini-3-pro","tools":["custom"]}],"next_page_token":"next","extension":{"x":1}}`,
	} {
		filtered, changed := filterDiscoveryModels([]byte(body), account)
		require.True(t, changed)
		var envelope map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(filtered, &envelope))
		require.JSONEq(t, `"next"`, string(envelope["next_page_token"]))
		require.JSONEq(t, `{"x":1}`, string(envelope["extension"]))
		var entries []map[string]any
		require.NoError(t, json.Unmarshal(envelope["models"], &entries))
		require.Len(t, entries, 1)
		require.Equal(t, []any{"custom"}, entries[0]["tools"])
	}
	for _, body := range [][]byte{nil, []byte(`{`), []byte(`null`), []byte(`{"models":null}`), []byte(`{"models":{}}`), []byte(`{"data":[]}`), []byte(`{"models":[{"slug":"wan/2-2-animate-move"}]}`)} {
		filtered, changed := filterDiscoveryModels(body, account)
		require.False(t, changed)
		require.Equal(t, body, filtered)
	}
}

func TestPublicDiscoveryWritersOmitSuspendedModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformAnthropic, service.PlatformGrok} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		writeCustomModelsList(c, platform, []string{"infinitalk", "wan/2-2-animate-replace", "wan/2-2-animate-move"})
		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Data, 1)
		require.Equal(t, "wan/2-2-animate-move", body.Data[0].ID)
	}
	out := toUserSupportedModels([]service.SupportedModel{{Name: "kling-ai-avatar"}, {Name: "omnihuman-1-5"}, {Name: "gpt-5.4"}}, nil)
	require.Len(t, out, 1)
	require.Equal(t, "gpt-5.4", out[0].Name)
}

func TestGeminiModelDetailsRejectSuspendedBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1beta/models/kling-ai-avatar-pro", nil)
	c.Params = gin.Params{{Key: "model", Value: "kling-ai-avatar-pro"}}
	c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{Group: &service.Group{Platform: service.PlatformGemini}})
	(&GatewayHandler{}).GeminiV1BetaGetModel(c)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCodexDiscoveryFiltersActualUpstreamManifest(t *testing.T) {
	handler, upstream, groupID := newCodexModelsFailoverTestHandler(http.StatusOK)
	upstream.firstBody = `{"models":[{"slug":"kling-ai-avatar-pro"},{"slug":"omnihuman-1-5"},{"slug":"gpt-5.6-sol","display_name":"healthy"}],"extension":{"x":1}}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.144.0", nil)
	c.Request.Header.Set("If-None-Match", `"pre-delisting-manifest"`)
	c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}})
	handler.CodexModels(c)
	require.Equal(t, []string{""}, upstream.clientValidators)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"models":[{"slug":"gpt-5.6-sol","display_name":"healthy"}],"extension":{"x":1}}`, w.Body.String())
	require.Equal(t, []int64{1}, upstream.calls())
}

func TestCustomDiscoveryDoesNotFallbackAfterEveryMappedModelIsSuspended(t *testing.T) {
	models := filterModelsByCustomList([]string{}, []string{"gemini-3-pro"}, []string{"gemini-3-pro"})
	require.Empty(t, models)
	// No configured mapping continues to use the original platform defaults.
	require.Equal(t, []string{"gemini-3-pro"}, filterModelsByCustomList(nil, []string{"gemini-3-pro"}, []string{"gemini-3-pro"}))
}

func TestGeminiDiscoveryFallbacksHonorSuspendedAccountMappings(t *testing.T) {
	body := []byte(`{"models":[{"name":"models/gemini-3-pro","displayName":"Pro"},{"name":"models/gemini-2.5-flash","displayName":"Flash"}],"extension":true}`)
	account := &service.Account{Credentials: map[string]any{"model_mapping": map[string]any{"gemini-3-pro": "omnihuman-1-5", "gemini-2.5-flash": "gemini-2.5-flash"}}}
	filtered, changed := filterDiscoveryModels(body, account)
	require.True(t, changed)
	require.JSONEq(t, `{"models":[{"name":"models/gemini-2.5-flash","displayName":"Flash"}],"extension":true}`, string(filtered))
	filtered, changed = filterDiscoveryModelsWithAllowedIDs(body, nil, []string{"gemini-2.5-flash"})
	require.True(t, changed)
	require.JSONEq(t, `{"models":[{"name":"models/gemini-2.5-flash","displayName":"Flash"}],"extension":true}`, string(filtered))
	filtered, changed = filterDiscoveryModelsWithAllowedIDs(body, nil, []string{})
	require.True(t, changed)
	require.JSONEq(t, `{"models":[],"extension":true}`, string(filtered))
	filtered, changed = filterDiscoveryModelsWithAllowedIDs(body, nil, nil)
	require.False(t, changed)
	require.Equal(t, body, filtered)
}
