package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIGatewayService_Forward_CompactOnlyModelMappingOverridesOAuthUpstreamModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","stream":false,"instructions":"compact-test","input":"hello"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid-compact-map"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"resp_123","status":"completed","model":"gpt-5.4-openai-compact","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}}

	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{
		ID:          1,
		Name:        "openai-oauth",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":          "oauth-token",
			"chatgpt_account_id":    "chatgpt-acc",
			"compact_model_mapping": map[string]any{"gpt-5.4": "gpt-5.4-openai-compact"},
		},
		Status:      StatusActive,
		Schedulable: true,
	}

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "gpt-5.4", result.Model)
	require.Equal(t, "gpt-5.4-openai-compact", result.UpstreamModel)
	require.Equal(t, "gpt-5.4-openai-compact", gjson.GetBytes(upstream.lastBody, "model").String())
}

func TestOpenAIGatewayService_Forward_NonCompactRequestIgnoresCompactOnlyModelMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","stream":false,"instructions":"normal-test","input":"hello"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid-normal-map"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"resp_124","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}}

	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{
		ID:          2,
		Name:        "openai-oauth",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":          "oauth-token",
			"chatgpt_account_id":    "chatgpt-acc",
			"compact_model_mapping": map[string]any{"gpt-5.4": "gpt-5.4-openai-compact"},
		},
		Status:      StatusActive,
		Schedulable: true,
	}

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "gpt-5.4", result.Model)
	require.Equal(t, "gpt-5.4", result.UpstreamModel)
	require.Equal(t, "gpt-5.4", gjson.GetBytes(upstream.lastBody, "model").String())
}

func TestOpenAIGatewayService_OAuthPassthrough_CompactOnlyModelMappingOverridesUpstreamModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", bytes.NewReader(nil))
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.1.0")
	c.Request.Header.Set("Content-Type", "application/json")

	originalBody := []byte(`{"model":"gpt-5.4","stream":true,"store":true,"instructions":"compact-pass","input":[{"type":"text","text":"compact me"}]}`)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid-compact-pass-map"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"cmp_124","model":"gpt-5.4-openai-compact","usage":{"input_tokens":2,"output_tokens":3}}`)),
	}}

	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{
		ID:          3,
		Name:        "openai-oauth-pass",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":          "oauth-token",
			"chatgpt_account_id":    "chatgpt-acc",
			"compact_model_mapping": map[string]any{"gpt-5.4": "gpt-5.4-openai-compact"},
		},
		Extra:       map[string]any{"openai_passthrough": true},
		Status:      StatusActive,
		Schedulable: true,
	}

	result, err := svc.Forward(context.Background(), c, account, originalBody)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "gpt-5.4", result.Model)
	require.Equal(t, "gpt-5.4-openai-compact", result.UpstreamModel)
	require.Equal(t, "gpt-5.4-openai-compact", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "gpt-5.4", gjson.GetBytes(rec.Body.Bytes(), "model").String())
}

func TestOpenAIGatewayService_PassthroughConflictingMappingsUseOriginalRequestDomain(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, test := range []struct {
		name         string
		path         string
		wantUpstream string
	}{
		{name: "ordinary", path: "/v1/responses", wantUpstream: "public"},
		{name: "compact", path: "/v1/responses/compact", wantUpstream: "compact-upstream"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			body := []byte(`{"model":"public","stream":false,"instructions":"passthrough-conflict","input":"hello"}`)
			c.Request = httptest.NewRequest(http.MethodPost, test.path, bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")

			contentType := "application/json"
			responseBody := `{"id":"resp_conflict","status":"completed","model":"` + test.wantUpstream + `","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
			if test.path == "/v1/responses" {
				contentType = "text/event-stream"
				responseBody = "data: " + responseBody + "\n\ndata: [DONE]\n\n"
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{contentType}, "x-request-id": []string{"rid-passthrough-conflict"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
			}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			account := &Account{
				ID: 31, Name: "openai-oauth-pass", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
				Credentials: map[string]any{
					"access_token":          "oauth-token",
					"chatgpt_account_id":    "chatgpt-acc",
					"model_mapping":         map[string]any{"public": "normal-upstream"},
					"compact_model_mapping": map[string]any{"public": "compact-upstream"},
				},
				Extra: map[string]any{
					"openai_passthrough": true,
					"responses_api":      true,
				},
				Status: StatusActive, Schedulable: true,
			}

			result, err := svc.Forward(context.Background(), c, account, body)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, "public", result.Model)
			require.Equal(t, test.wantUpstream, gjson.GetBytes(upstream.lastBody, "model").String())
			require.NotEqual(t, "normal-upstream", gjson.GetBytes(upstream.lastBody, "model").String())
		})
	}
}

func TestOpenAIGatewayService_APIKeyPassthroughConflictingMappingsUseOriginalRequestDomain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/v1/responses", want: "public"},
		{path: "/v1/responses/compact", want: "compact-upstream"},
	} {
		t.Run(test.path, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"id":"resp_api_key","model":"` + test.want + `","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
			}}
			svc := newOpenAIImageGenerationControlTestService(upstream)
			c, _ := newOpenAIImageGenerationControlTestContext(true, "test-client")
			body := []byte(`{"model":"public","stream":false,"instructions":"test","input":"hello"}`)
			c.Request = httptest.NewRequest(http.MethodPost, test.path, bytes.NewReader(body))
			account := newOpenAIImageGenerationControlTestAccount()
			account.Credentials["model_mapping"] = map[string]any{"public": "normal-upstream"}
			account.Credentials["compact_model_mapping"] = map[string]any{"public": "compact-upstream"}
			account.Extra = map[string]any{"openai_passthrough": true, "responses_api": true}

			result, err := svc.Forward(context.Background(), c, account, body)

			require.NoError(t, err)
			require.Equal(t, test.want, gjson.GetBytes(upstream.lastBody, "model").String())
			require.Equal(t, "public", result.Model)
			require.Equal(t, test.want, result.UpstreamModel)
		})
	}
}

func TestOpenAIGatewayService_OAuthLegacyPassthroughCompactExactIsNotNormalized(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"public","stream":false,"instructions":"test","input":"hello"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", bytes.NewReader(body))
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"resp_oauth","model":"openai/gpt-5.4-high","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}}
	account := &Account{ID: 33, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"access_token": "token", "chatgpt_account_id": "account", "model_mapping": map[string]any{"public": "normal-upstream"}, "compact_model_mapping": map[string]any{"public": "openai/gpt-5.4-high"}},
		Extra:       map[string]any{"openai_oauth_passthrough": true},
	}

	result, err := (&OpenAIGatewayService{httpUpstream: upstream}).Forward(context.Background(), c, account, body)

	require.NoError(t, err)
	require.Equal(t, "openai/gpt-5.4-high", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "openai/gpt-5.4-high", result.UpstreamModel)
}

func TestOpenAIGatewayService_PassthroughCompactWildcardPrecedenceInForwardedBody(t *testing.T) {
	for _, test := range []struct{ model, want string }{
		{model: "gpt-5.4", want: "exact-target"},
		{model: "gpt-5.4-mini", want: "long-target"},
		{model: "gpt-other", want: "short-target"},
	} {
		t.Run(test.model, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			body := []byte(`{"model":"` + test.model + `","stream":false,"instructions":"test","input":"hello"}`)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", bytes.NewReader(body))
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"resp","output":[],"usage":{}}`))}}
			account := &Account{ID: 34, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Credentials: map[string]any{"access_token": "token", "chatgpt_account_id": "account", "compact_model_mapping": map[string]any{"gpt-*": "short-target", "gpt-5.4*": "long-target", "gpt-5.4": "exact-target"}},
				Extra:       map[string]any{"openai_passthrough": true},
			}

			_, err := (&OpenAIGatewayService{httpUpstream: upstream}).Forward(context.Background(), c, account, body)

			require.NoError(t, err)
			require.Equal(t, test.want, gjson.GetBytes(upstream.lastBody, "model").String())
		})
	}
}
