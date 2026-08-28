//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newNonBillableJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// Every Count Tokens write succeeds in enforce mode (spec §2.4; index 3.3 exit).
// The decorator runs in enforce mode over a recording stub; the four write sites
// — gateway_count_tokens.go:141 (first write), :168 (signature-rectify retry),
// :265 (Anthropic API-key passthrough) and openai_gateway_count_tokens.go:125
// (ForwardCountTokensAsAnthropic) — must each be admitted as non-billable.
func TestCountTokensWritesSucceedInEnforceMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetAuthorizationMetricsForTest()

	// (1)+(2) ForwardCountTokens non-passthrough: first 400 with a thinking-signature
	// error triggers the :168 retry, whose 200 completes the request — two writes.
	retryStub := &httpUpstreamRecorder{responses: []*http.Response{
		newNonBillableJSONResponse(http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"Invalid signature in thinking block"}}`),
		newNonBillableJSONResponse(http.StatusOK, `{"input_tokens":42}`),
	}}
	dec := newAuthorizingHTTPUpstreamWithMode(retryStub, modeFn(config.CanonicalWalletModeEnforce))
	cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
	gw := &GatewayService{
		cfg:              cfg,
		responseHeaderFilter: compileResponseHeaderFilter(cfg),
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
		settingService:   NewSettingService(&adminComplianceRepoStub{}, cfg),
	}
	account := &Account{
		ID:          301,
		Name:        "claude-oauth-count",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{"access_token": "oauth-token"},
		Status:      StatusActive,
		Schedulable: true,
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
	parsed := &ParsedRequest{
		Body:  NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}`)),
		Model: "claude-sonnet-4-5",
	}
	require.NoError(t, gw.ForwardCountTokens(context.Background(), c, account, parsed))
	require.Equal(t, 2, len(retryStub.requests), "first write + signature-rectify retry")

	// (3) the Anthropic API-key passthrough count-tokens write (:265).
	passStub := &anthropicHTTPUpstreamRecorder{resp: newNonBillableJSONResponse(http.StatusOK, `{"input_tokens":42}`)}
	decPass := newAuthorizingHTTPUpstreamWithMode(passStub, modeFn(config.CanonicalWalletModeEnforce))
	gwPass := &GatewayService{
		cfg:                  cfg,
		responseHeaderFilter: compileResponseHeaderFilter(cfg),
		httpUpstream:         decPass,
		rateLimitService:     &RateLimitService{},
	}
	passAccount := &Account{
		ID:          302,
		Name:        "anthropic-apikey-pass-count",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "upstream-anthropic-key", "base_url": "https://api.anthropic.com"},
		Extra:       map[string]any{"anthropic_passthrough": true},
		Status:      StatusActive,
		Schedulable: true,
	}
	rec2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(rec2)
	c2.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
	parsed2 := &ParsedRequest{
		Body:  NewRequestBodyRef([]byte(`{"model":"claude-3-5-sonnet-latest","messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}`)),
		Model: "claude-3-5-sonnet-latest",
	}
	require.NoError(t, gwPass.ForwardCountTokens(context.Background(), c2, passAccount, parsed2))

	// (4) the OpenAI count-tokens-as-Anthropic write (openai_gateway_count_tokens.go:125).
	openaiStub := &httpUpstreamRecorder{resp: newNonBillableJSONResponse(http.StatusOK, `{"object":"response.input_tokens","input_tokens":42}`)}
	decOpenAI := newAuthorizingHTTPUpstreamWithMode(openaiStub, modeFn(config.CanonicalWalletModeEnforce))
	openaiSvc := &OpenAIGatewayService{
		cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}},
		httpUpstream: decOpenAI,
	}
	openaiAccount := &Account{
		ID:          303,
		Name:        "openai-count",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	rec3 := httptest.NewRecorder()
	c3, _ := gin.CreateTestContext(rec3)
	openaiBody := []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}`)
	c3.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(string(openaiBody)))
	require.NoError(t, openaiSvc.ForwardCountTokensAsAnthropic(context.Background(), c3, openaiAccount, openaiBody, "gpt-5.3-codex"))

	require.Equal(t, int64(4), AuthorizationMetricsSnapshot().WritesNonBillable)
	require.Equal(t, int64(0), AuthorizationMetricsSnapshot().WritesRefused)
}

// One representative probe (upstream_billing_probe.go:592) succeeds in enforce mode.
func TestBillingProbeWriteSucceedsInEnforceMode(t *testing.T) {
	resetAuthorizationMetricsForTest()
	probeStub := &httpUpstreamRecorder{resp: newNonBillableJSONResponse(http.StatusNotFound, `{"error":"not found"}`)}
	dec := newAuthorizingHTTPUpstreamWithMode(probeStub, modeFn(config.CanonicalWalletModeEnforce))
	cfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	accountTestService := &AccountTestService{httpUpstream: dec, cfg: cfg}
	probe := &UpstreamBillingProbeService{accountTestService: accountTestService, now: time.Now}
	account := &Account{
		ID:          304,
		Name:        "openai-billing-probe",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-probe", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	// The probe persists its (failure) snapshot through the account repo; the
	// repo is nil here so the persist step errors — irrelevant, the WRITE at
	// upstream_billing_probe.go:592 has already gone through the decorator.
	_, _ = probe.probeLoadedAccount(context.Background(), account, 30)
	require.Equal(t, 1, len(probeStub.requests))
	require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesNonBillable)
	require.Equal(t, int64(0), AuthorizationMetricsSnapshot().WritesRefused)
}
