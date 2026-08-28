//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// refusalCountingStub is the inner port of every injected-refusal test: if the
// decorator does its job the stub never sees the write (spec §2.0 — the refusal
// must short-circuit before the write leaves the authorization boundary).
type refusalCountingStub struct {
	mu    sync.Mutex
	calls int
}

func (s *refusalCountingStub) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

func (s *refusalCountingStub) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return s.Do(req, proxyURL, accountID, accountConcurrency)
}

func (s *refusalCountingStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// assertRefusalIsTerminal is the plan's template contract: the refusal comes
// back as is, is never a failover, exactly one Do entered the decorator, and
// the inner port never saw the write.
func assertRefusalIsTerminal(t *testing.T, err error, dec *AuthorizingHTTPUpstream, stub *refusalCountingStub) {
	t.Helper()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAuthorizationRefused), "error must be the named terminal refusal, got: %v", err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "a refusal is never a failover")
	require.Equal(t, int64(1), dec.Calls(), "exactly one Do — no forwarder retry")
	require.Equal(t, 0, stub.count(), "the inner port never saw the write")
}

func newRefusalDecorator(t *testing.T) (*AuthorizingHTTPUpstream, *refusalCountingStub) {
	t.Helper()
	stub := &refusalCountingStub{}
	return newAuthorizingHTTPUpstreamWithMode(stub, modeFn(config.CanonicalWalletModeEnforce)), stub
}

func refusalTestContext(t *testing.T, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	return c, rec
}

// 1. OpenAI Responses family (openai_gateway_forward.go:807).
func TestOpenAIResponsesForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: dec,
	}
	body := []byte(`{"model":"gpt-5.5","stream":true,"reasoning":{"effort":"low"},"input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID: 1, Name: "oauth-test", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"},
	}
	_, err := svc.Forward(context.Background(), c, account, body)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 1b. The first-output header guard must NOT convert a refusal into a
// first-output timeout error: the refusal short-circuit runs ahead of
// headerGuard.stopHeaderWait(). The guard timeout is 1s (the config's smallest
// unit is seconds), so the test-only refusal delay is set past it.
func TestOpenAIResponsesForwardRefusalBeatsFirstOutputHeaderGuard(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	dec.refusalDelayForTest = 1200 * time.Millisecond
	svc := &OpenAIGatewayService{
		cfg: &config.Config{Gateway: config.GatewayConfig{
			OpenAIFirstOutputTimeoutSeconds: 1,
			MaxLineSize:                     defaultMaxLineSize,
		}},
		httpUpstream: dec,
	}
	body := []byte(`{"model":"gpt-5.5","stream":true,"reasoning":{"effort":"low"},"input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID: 1, Name: "oauth-test", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"},
	}
	_, err := svc.Forward(context.Background(), c, account, body)
	assertRefusalIsTerminal(t, err, dec, stub)
	// The first-output timeout IS a *UpstreamFailoverError — assertRefusalIsTerminal
	// already proved the error is not one, i.e. the guard did not convert the refusal.
}

// 2. Generic Messages family (gateway_forward.go:371/453/494/573 — one
// AsAuthorizationRefused call in Forward satisfies the function-granular pin;
// this test proves the first write is refused before any retry).
func TestGenericMessagesForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
	}
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/messages", body)
	account := &Account{
		ID:          21,
		Name:        "anthropic-apikey",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"},
		Status:      StatusActive,
		Schedulable: true,
	}
	parsed := &ParsedRequest{
		Body:   NewRequestBodyRef(body),
		Model:  "claude-sonnet-4-5",
		Stream: false,
	}
	_, err := svc.Forward(context.Background(), c, account, parsed)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 3. Gemini compat family (gemini_messages_compat_service.go:776/1306/2725).
func TestGeminiCompatForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GeminiMessagesCompatService{
		tokenProvider: &GeminiTokenProvider{},
		httpUpstream:  dec,
		cfg:           &config.Config{},
	}
	body := []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/messages", body)
	account := &Account{
		ID:          31,
		Name:        "gemini-oauth",
		Platform:    PlatformGemini,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{"access_token": "tok", "project_id": "p"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardNative(context.Background(), c, account, "claude-sonnet-4-5", "generateContent", false, body)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 4. Grok media generation write (grok_media.go:403).
func TestGrokMediaGenerationRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"grok-image","prompt":"a red cube"}`)
	c, _ := refusalTestContext(t, "/v1/images/generations", body)
	account := &Account{
		ID:       41,
		Platform: PlatformGrok,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "upstream-key",
			"base_url": "https://relay.example/v1",
		},
	}
	_, err := svc.ForwardGrokMedia(context.Background(), c, account, GrokMediaEndpointImagesGenerations, "", body, "application/json")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 5. Images family (openai_images.go:621).
func TestImagesForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"gpt-image-1","prompt":"a red cube","n":1}`)
	c, _ := refusalTestContext(t, "/v1/images/generations", body)
	account := &Account{
		ID:          51,
		Name:        "openai-image",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-image", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	parsed := &OpenAIImagesRequest{Model: "gpt-image-1", N: 1}
	_, err := svc.ForwardImages(context.Background(), c, account, body, parsed, "gpt-image-1")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 6. Alpha Search family (openai_alpha_search.go:76/:156).
func TestAlphaSearchForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"gpt-5.5","tools":[{"type":"web_search"}]}`)
	c, _ := refusalTestContext(t, "/v1/alpha/search", body)
	account := &Account{
		ID:          61,
		Name:        "alpha-search",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-alpha", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAlphaSearch(context.Background(), c, account, body)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 7. Embeddings family (openai_embeddings.go:92).
func TestEmbeddingsForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"text-embedding-3-small","input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/embeddings", body)
	account := &Account{
		ID:          71,
		Name:        "embeddings",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-embed", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardEmbeddings(context.Background(), c, account, body, "")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 8. OpenAI chat completions family (openai_gateway_chat_completions.go:282).
func TestOpenAIChatCompletionsForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/chat/completions", body)
	account := &Account{
		ID:          81,
		Name:        "openai-cc",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-cc", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 9. Bedrock family (gateway_bedrock.go:199) — the hosting function is driven
// directly (its only caller builds identical inputs).
func TestBedrockUpstreamRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
	}
	body := []byte(`{"anthropic_version":"bedrock-2023-05-31","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/messages", body)
	account := &Account{
		ID:       91,
		Name:     "bedrock",
		Platform: PlatformAnthropic,
		Type:     AccountTypeBedrock,
		Credentials: map[string]any{
			"aws_access_key_id":     "AKIA-test",
			"aws_secret_access_key": "secret-test",
			"aws_region":            "us-east-1",
		},
		Status:      StatusActive,
		Schedulable: true,
	}
	signer, signerErr := NewBedrockSignerFromAccount(account)
	require.NoError(t, signerErr)
	_, err := svc.executeBedrockUpstream(context.Background(), c, account, body, "anthropic.claude", "us-east-1", false, signer, "", "")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 10. OpenAI messages ForwardAsAnthropic family (openai_gateway_messages.go:352).
func TestOpenAIMessagesForwardAsAnthropicRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/messages", body)
	account := &Account{
		ID:          101,
		Name:        "openai-msg",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-msg", "base_url": "http://upstream.example"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAsAnthropic(context.Background(), c, account, body, "", "")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 11. Grok responses family (openai_gateway_grok.go:109).
func TestGrokResponsesForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"grok-4.5","input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID:          111,
		Name:        "grok-oauth",
		Platform:    PlatformGrok,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.forwardGrokResponses(context.Background(), c, account, body, "grok-4.5", false, time.Now())
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 12. Grok chat bridge family (openai_gateway_grok_chat_bridge.go:600).
func TestGrokChatBridgeRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/chat/completions", body)
	account := &Account{
		ID:          121,
		Name:        "grok-bridge",
		Platform:    PlatformGrok,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.forwardGrokChatCompletionsViaResponses(context.Background(), c, account, body, "", "grok-4.5")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 13. CC pipeline family (openai_gateway_cc_pipeline.go:224) — the hosting
// function is driven directly.
func TestCCPipelineRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
	svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
	c, _ := refusalTestContext(t, "/v1/chat/completions", []byte(`{}`))
	account := &Account{
		ID:          131,
		Name:        "cc-pipeline",
		Platform:    PlatformGrok,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.sendCCUpstreamRequest(context.Background(), c, account, "https://relay.example/v1/chat/completions", []byte(`{"model":"grok-4.5"}`), false, "xai-key", "sub2api-test", "")
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 14. forward-as-chat family (gateway_forward_as_chat_completions.go:129).
func TestForwardAsChatCompletionsRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
	}
	body := []byte(`{"model":"gpt-5.5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/chat/completions", body)
	account := &Account{
		ID:          141,
		Name:        "claude-apikey",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 15. forward-as-responses family (gateway_forward_as_responses.go:134).
func TestForwardAsResponsesRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
	}
	body := []byte(`{"model":"claude-sonnet-4-5","max_output_tokens":16,"input":"hi"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID:          151,
		Name:        "claude-apikey",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAsResponses(context.Background(), c, account, body, nil)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 16. Gemini chat compat family (gemini_chat_completions_compat_service.go:126).
func TestGeminiChatCompatForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GeminiMessagesCompatService{
		tokenProvider: &GeminiTokenProvider{},
		httpUpstream:  dec,
		cfg:           &config.Config{},
	}
	body := []byte(`{"model":"gemini-3.1-pro","messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/chat/completions", body)
	account := &Account{
		ID:          161,
		Name:        "gemini-oauth",
		Platform:    PlatformGemini,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{"access_token": "tok", "project_id": "p"},
		Status:      StatusActive,
		Schedulable: true,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 17. Antigravity family (antigravity_gateway_upstream.go:81 and the retry loop).
func TestAntigravityForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := newAntigravityCompatService(config.GatewayConfig{MaxLineSize: defaultMaxLineSize}, dec)
	body := []byte(`{"model":"gemini-3.1-pro-high","messages":[{"role":"user","content":"Reply exactly: ok"}]}`)
	c, _ := refusalTestContext(t, "/v1/chat/completions", body)
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, newAntigravityCompatAccount(AccountTypeOAuth), body, nil)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 18. Anthropic API-key passthrough family (gateway_anthropic_passthrough.go:112).
func TestAnthropicPassthroughForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     dec,
		rateLimitService: &RateLimitService{},
	}
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	c, _ := refusalTestContext(t, "/v1/messages", body)
	account := &Account{
		ID:          181,
		Name:        "anthropic-pass",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "upstream-key", "base_url": "https://api.anthropic.com"},
		Extra:       map[string]any{"anthropic_passthrough": true},
		Status:      StatusActive,
		Schedulable: true,
	}
	parsed := &ParsedRequest{
		Body:   NewRequestBodyRef(body),
		Model:  "claude-sonnet-4-5",
		Stream: false,
	}
	_, err := svc.Forward(context.Background(), c, account, parsed)
	assertRefusalIsTerminal(t, err, dec, stub)
}

// 19. OpenAI OAuth passthrough family (openai_gateway_passthrough.go:197).
func TestOpenAIPassthroughForwardRefusalIsTerminal(t *testing.T) {
	dec, stub := newRefusalDecorator(t)
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: dec,
	}
	body := []byte(`{"model":"gpt-5.5","stream":false,"input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID: 191, Name: "oauth-passthrough", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"},
	}
	resolution := openAIForwardModelResolution{UpstreamModel: "gpt-5.5", BillingModel: "gpt-5.5"}
	_, err := svc.forwardOpenAIPassthrough(context.Background(), c, account, body, nil, "gpt-5.5", resolution, false, nil, false, time.Now())
	assertRefusalIsTerminal(t, err, dec, stub)
}

// Phase 3.4a (redesign §9.5): the per-family injected-refusal table. Every
// family above proves terminality of its UNMARKED refusal; this table injects
// the handle Authorize would have attached — refused at the authorization
// point with one of the ensure-emitted reasons — and proves the SAME terminal
// contract per family per reason: the refusal returns as is (never a
// failover), exactly one Do, and the inner port never sees the write.
// lease_cap_reached is 3.4a's addition, asserted with the same assertions as
// the other reasons.
func TestInjectedRefusedHandleIsTerminalPerFamilyAndReason(t *testing.T) {
	drivers := map[string]func(ctx context.Context, dec *AuthorizingHTTPUpstream) error{
		"openai responses": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec}
			c, _ := refusalTestContext(t, "/v1/responses", []byte(`{"model":"gpt-5.5","stream":true,"reasoning":{"effort":"low"},"input":"hello"}`))
			account := &Account{ID: 1, Name: "oauth-test", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"}}
			_, err := svc.Forward(ctx, c, account, []byte(`{"model":"gpt-5.5","stream":true,"reasoning":{"effort":"low"},"input":"hello"}`))
			return err
		},
		"generic messages": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec, rateLimitService: &RateLimitService{}}
			body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/messages", body)
			account := &Account{ID: 21, Name: "anthropic-apikey", Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"}, Status: StatusActive, Schedulable: true}
			parsed := &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4-5", Stream: false}
			_, err := svc.Forward(ctx, c, account, parsed)
			return err
		},
		"gemini compat": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GeminiMessagesCompatService{tokenProvider: &GeminiTokenProvider{}, httpUpstream: dec, cfg: &config.Config{}}
			body := []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/messages", body)
			account := &Account{ID: 31, Name: "gemini-oauth", Platform: PlatformGemini, Type: AccountTypeOAuth, Concurrency: 1, Credentials: map[string]any{"access_token": "tok", "project_id": "p"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardNative(ctx, c, account, "claude-sonnet-4-5", "generateContent", false, body)
			return err
		},
		"grok media": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"grok-image","prompt":"a red cube"}`)
			c, _ := refusalTestContext(t, "/v1/images/generations", body)
			account := &Account{ID: 41, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "upstream-key", "base_url": "https://relay.example/v1"}}
			_, err := svc.ForwardGrokMedia(ctx, c, account, GrokMediaEndpointImagesGenerations, "", body, "application/json")
			return err
		},
		"images": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"gpt-image-1","prompt":"a red cube","n":1}`)
			c, _ := refusalTestContext(t, "/v1/images/generations", body)
			account := &Account{ID: 51, Name: "openai-image", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-image", "base_url": "http://upstream.example"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardImages(ctx, c, account, body, &OpenAIImagesRequest{Model: "gpt-image-1", N: 1}, "gpt-image-1")
			return err
		},
		"alpha search": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"gpt-5.5","tools":[{"type":"web_search"}]}`)
			c, _ := refusalTestContext(t, "/v1/alpha/search", body)
			account := &Account{ID: 61, Name: "alpha-search", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-alpha", "base_url": "http://upstream.example"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAlphaSearch(ctx, c, account, body)
			return err
		},
		"embeddings": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"text-embedding-3-small","input":"hello"}`)
			c, _ := refusalTestContext(t, "/v1/embeddings", body)
			account := &Account{ID: 71, Name: "embeddings", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-embed", "base_url": "http://upstream.example"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardEmbeddings(ctx, c, account, body, "")
			return err
		},
		"openai chat completions": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/chat/completions", body)
			account := &Account{ID: 81, Name: "openai-cc", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-cc", "base_url": "http://upstream.example"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAsChatCompletions(ctx, c, account, body, "", "")
			return err
		},
		"bedrock": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec, rateLimitService: &RateLimitService{}}
			body := []byte(`{"anthropic_version":"bedrock-2023-05-31","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/messages", body)
			account := &Account{ID: 91, Name: "bedrock", Platform: PlatformAnthropic, Type: AccountTypeBedrock, Credentials: map[string]any{"aws_access_key_id": "AKIA-test", "aws_secret_access_key": "secret-test", "aws_region": "us-east-1"}, Status: StatusActive, Schedulable: true}
			signer, signerErr := NewBedrockSignerFromAccount(account)
			require.NoError(t, signerErr)
			_, err := svc.executeBedrockUpstream(ctx, c, account, body, "anthropic.claude", "us-east-1", false, signer, "", "")
			return err
		},
		"openai messages": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/messages", body)
			account := &Account{ID: 101, Name: "openai-msg", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-msg", "base_url": "http://upstream.example"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAsAnthropic(ctx, c, account, body, "", "")
			return err
		},
		"grok responses": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"grok-4.5","input":"hello"}`)
			c, _ := refusalTestContext(t, "/v1/responses", body)
			account := &Account{ID: 111, Name: "grok-oauth", Platform: PlatformGrok, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"}, Status: StatusActive, Schedulable: true}
			_, err := svc.forwardGrokResponses(ctx, c, account, body, "grok-4.5", false, time.Now())
			return err
		},
		"grok chat bridge": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/chat/completions", body)
			account := &Account{ID: 121, Name: "grok-bridge", Platform: PlatformGrok, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"}, Status: StatusActive, Schedulable: true}
			_, err := svc.forwardGrokChatCompletionsViaResponses(ctx, c, account, body, "", "grok-4.5")
			return err
		},
		"cc pipeline": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svcCfg := &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}
			svc := &OpenAIGatewayService{cfg: svcCfg, httpUpstream: dec}
			c, _ := refusalTestContext(t, "/v1/chat/completions", []byte(`{}`))
			account := &Account{ID: 131, Name: "cc-pipeline", Platform: PlatformGrok, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "xai-key", "base_url": "https://relay.example/v1"}, Status: StatusActive, Schedulable: true}
			_, err := svc.sendCCUpstreamRequest(ctx, c, account, "https://relay.example/v1/chat/completions", []byte(`{"model":"grok-4.5"}`), false, "xai-key", "sub2api-test", "")
			return err
		},
		"forward as chat": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec, rateLimitService: &RateLimitService{}}
			body := []byte(`{"model":"gpt-5.5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/chat/completions", body)
			account := &Account{ID: 141, Name: "claude-apikey", Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAsChatCompletions(ctx, c, account, body, nil)
			return err
		},
		"forward as responses": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec, rateLimitService: &RateLimitService{}}
			body := []byte(`{"model":"claude-sonnet-4-5","max_output_tokens":16,"input":"hi"}`)
			c, _ := refusalTestContext(t, "/v1/responses", body)
			account := &Account{ID: 151, Name: "claude-apikey", Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.anthropic.com"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAsResponses(ctx, c, account, body, nil)
			return err
		},
		"gemini chat compat": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GeminiMessagesCompatService{tokenProvider: &GeminiTokenProvider{}, httpUpstream: dec, cfg: &config.Config{}}
			body := []byte(`{"model":"gemini-3.1-pro","messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/chat/completions", body)
			account := &Account{ID: 161, Name: "gemini-oauth", Platform: PlatformGemini, Type: AccountTypeOAuth, Concurrency: 1, Credentials: map[string]any{"access_token": "tok", "project_id": "p"}, Status: StatusActive, Schedulable: true}
			_, err := svc.ForwardAsChatCompletions(ctx, c, account, body)
			return err
		},
		"antigravity": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := newAntigravityCompatService(config.GatewayConfig{MaxLineSize: defaultMaxLineSize}, dec)
			body := []byte(`{"model":"gemini-3.1-pro-high","messages":[{"role":"user","content":"Reply exactly: ok"}]}`)
			c, _ := refusalTestContext(t, "/v1/chat/completions", body)
			_, err := svc.ForwardAsChatCompletions(ctx, c, newAntigravityCompatAccount(AccountTypeOAuth), body, nil)
			return err
		},
		"anthropic passthrough": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec, rateLimitService: &RateLimitService{}}
			body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			c, _ := refusalTestContext(t, "/v1/messages", body)
			account := &Account{ID: 181, Name: "anthropic-pass", Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "upstream-key", "base_url": "https://api.anthropic.com"}, Extra: map[string]any{"anthropic_passthrough": true}, Status: StatusActive, Schedulable: true}
			parsed := &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4-5", Stream: false}
			_, err := svc.Forward(ctx, c, account, parsed)
			return err
		},
		"openai oauth passthrough": func(ctx context.Context, dec *AuthorizingHTTPUpstream) error {
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: dec}
			body := []byte(`{"model":"gpt-5.5","stream":false,"input":"hello"}`)
			c, _ := refusalTestContext(t, "/v1/responses", body)
			account := &Account{ID: 191, Name: "oauth-passthrough", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"}}
			resolution := openAIForwardModelResolution{UpstreamModel: "gpt-5.5", BillingModel: "gpt-5.5"}
			_, err := svc.forwardOpenAIPassthrough(ctx, c, account, body, nil, "gpt-5.5", resolution, false, nil, false, time.Now())
			return err
		},
	}
	for _, reason := range []AuthorizationRefusalReason{
		AuthorizationRefusalBalanceShortfall,
		AuthorizationRefusalLeaseCapReached, // 3.4a's addition
		AuthorizationRefusalLeaseUnavailable,
	} {
		for name, drive := range drivers {
			t.Run(string(reason)+"/"+name, func(t *testing.T) {
				dec, stub := newRefusalDecorator(t)
				h, err := newAuthorizationHandle(config.CanonicalWalletModeEnforce)
				require.NoError(t, err)
				h.Refusal = &AuthorizationRefusedError{Reason: reason, AuthorizationID: h.ID}
				ctx := WithAuthorizationHandle(context.Background(), h)
				familyErr := drive(ctx, dec)
				assertRefusalIsTerminal(t, familyErr, dec, stub)
				refused, ok := AsAuthorizationRefused(familyErr)
				require.True(t, ok)
				require.Equal(t, reason, refused.Reason, "the injected refusal crosses the family unchanged")
			})
		}
	}
}

// Phase 3.3a Task 10 Step 5: with canonical_wallet.mode=disabled the decorator
// is a pass-through — no classification, no counters, no context change.
func TestDisabledModeDecoratorScopedCountersStayZero(t *testing.T) {
	resetAuthorizationMetricsForTest()
	inner := &refusalCountingStub{}
	disabled := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeDisabled))
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: disabled,
	}
	body := []byte(`{"model":"gpt-5.5","stream":false,"reasoning":{"effort":"low"},"input":"hello"}`)
	c, _ := refusalTestContext(t, "/v1/responses", body)
	account := &Account{
		ID: 1, Name: "oauth-test", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"access_token": "test-token", "chatgpt_account_id": "test-account"},
	}
	// A completed (non-stream) Responses payload the forwarder accepts.
	okPayload := `{"id":"resp_1","object":"response","status":"completed","model":"gpt-5.5","output":[{"type":"message","id":"msg_1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	inner2 := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(okPayload)),
	}}
	svc.httpUpstream = newAuthorizingHTTPUpstreamWithMode(inner2, modeFn(config.CanonicalWalletModeDisabled))
	_, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	m := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(0), m.WritesAuthorized)
	require.Equal(t, int64(0), m.WritesNonBillable)
	require.Equal(t, int64(0), m.WritesUnmarked)
	require.Equal(t, int64(0), m.WritesRefused)
	require.Equal(t, int64(0), m.OutcomeResult)
	require.Equal(t, int64(0), m.OutcomeNotWritten)
	require.Equal(t, int64(0), m.OutcomeIndeterminate)
	require.Equal(t, int64(0), m.Refused)
	require.Equal(t, int64(0), m.Minted, "Authorize increments Minted only after its disabled-mode early return")
	require.Equal(t, 1, len(inner2.requests), "the write went through unchanged")
}
