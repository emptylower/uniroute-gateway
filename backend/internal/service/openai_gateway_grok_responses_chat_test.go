//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func grokForceChatTestAccount() *Account {
	a := rawChatCompletionsTestAccount()
	a.Platform = PlatformGrok
	a.Credentials["base_url"] = "https://relay.example/api/v1"
	a.Extra = map[string]any{openai_compat.ExtraKeyResponsesMode: "force_chat_completions"}
	return a
}

func grokChatTestReply(usage string, stream bool) *http.Response {
	body := `{"id":"chat_test","model":"grok-4.7-build-fast","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":` + usage + `}`
	header := "application/json"
	if stream {
		header = "text/event-stream"
		chunk := `data: {"id":"chat_test","choices":[],"usage":` + usage + `}` + "\n\n"
		body = `data: {"id":"chat_test","choices":[{"index":0,"delta":{"role":"assistant","content":"OK"},"finish_reason":"stop"}]}` + "\n\n" + chunk + chunk + "data: [DONE]\n\n"
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{header}}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestGrokForceChat_ForwardAndConvertedUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"responses", "messages", "chat/completions"} {
		for _, stream := range []bool{false, true} {
			for _, inclusive := range []bool{false, true} {
				name := endpoint + "/json/separate"
				if stream {
					name = endpoint + "/SSE/separate"
				}
				if inclusive {
					name += "/inclusive"
				}
				t.Run(name, func(t *testing.T) {
					usage := grokSeparateReasoningUsage
					rawOutput := 1
					if inclusive {
						usage = strings.Replace(usage, `"completion_tokens":1,`, `"completion_tokens":77,`, 1)
						rawOutput = 77
					}
					request := map[string]any{"model": "grok-4.7-build-fast", "stream": stream}
					if endpoint == "responses" {
						request["input"] = "hello"
					} else {
						request["messages"] = []map[string]string{{"role": "user", "content": "hello"}}
						request["max_tokens"] = 32
					}
					body, err := json.Marshal(request)
					require.NoError(t, err)
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, bytes.NewReader(body))
					upstream := &httpUpstreamRecorder{resp: grokChatTestReply(usage, stream)}
					svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
					a := grokForceChatTestAccount()
					var result *OpenAIForwardResult
					switch endpoint {
					case "responses":
						result, err = svc.Forward(context.Background(), c, a, body)
					case "messages":
						result, err = svc.ForwardAsAnthropic(context.Background(), c, a, body, "", "")
					case "chat/completions":
						result, err = svc.ForwardAsChatCompletions(context.Background(), c, a, body, "", "")
					}
					require.NoError(t, err)
					require.Len(t, upstream.requests, 1, "transport must be chosen before the only billed request")
					require.Equal(t, "https://relay.example/api/v1/chat/completions", upstream.lastReq.URL.String())
					require.Equal(t, 77, result.Usage.OutputTokens)
					require.Equal(t, 1247, result.Usage.InputTokens)
					require.Equal(t, 1152, result.Usage.CacheReadInputTokens)
					wire := rec.Body.Bytes()
					switch endpoint {
					case "responses":
						require.Equal(t, grokChatRawEndpoint, result.UpstreamEndpoint)
						if stream {
							var ok bool
							wire, ok = extractCodexFinalResponse(string(wire))
							require.True(t, ok)
						}
						require.Equal(t, int64(77), gjson.GetBytes(wire, "usage.output_tokens").Int())
						require.Equal(t, int64(76), gjson.GetBytes(wire, "usage.output_tokens_details.reasoning_tokens").Int())
						require.Equal(t, int64(1152), gjson.GetBytes(wire, "usage.input_tokens_details.cached_tokens").Int())
						require.Equal(t, int64(1324), gjson.GetBytes(wire, "usage.total_tokens").Int())
					case "messages":
						if stream {
							forEachOpenAISSEDataPayload(string(wire), func(data []byte) {
								if gjson.GetBytes(data, "type").String() == "message_delta" {
									wire = append([]byte(nil), data...)
								}
							})
						}
						require.Equal(t, int64(77), gjson.GetBytes(wire, "usage.output_tokens").Int())
						if !stream {
							require.Equal(t, int64(95), gjson.GetBytes(wire, "usage.input_tokens").Int())
							require.Equal(t, int64(1152), gjson.GetBytes(wire, "usage.cache_read_input_tokens").Int())
						}
					default:
						require.Contains(t, string(wire), `"usage":`+usage)
						require.Equal(t, int64(rawOutput), gjson.Get(usage, "completion_tokens").Int())
					}
					cost := (&BillingService{}).computeTokenBreakdown(&ModelPricing{InputPricePerToken: 4e-6, OutputPricePerToken: 12e-6, CacheReadPricePerToken: 1e-6}, openAIUsageTokens(result.Usage), .42, "", false)
					require.InDelta(t, .002456, cost.TotalCost, 1e-12)
					require.InDelta(t, .00103152, cost.ActualCost, 1e-12)
				})
			}
		}
	}
}

func TestGrokForceChat_ModeSelectionAndNativeUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{"", "auto", "force_responses", "invalid", "oauth-force-chat"} {
		t.Run(mode, func(t *testing.T) {
			a := grokForceChatTestAccount()
			a.Extra[openai_compat.ExtraKeyResponsesMode] = mode
			a.Extra[openai_compat.ExtraKeyResponsesSupported] = false // Grok probes must not enable this new opt-in.
			body := []byte(`{"model":"grok-4.7-build-fast","input":"hello"}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			native := `{"id":"resp_test","status":"completed","output":[],"usage":{"input_tokens":1247,"output_tokens":1,"total_tokens":1311}}`
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(native))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			if mode == "oauth-force-chat" {
				a = healthyGrokOAuthGatewayTestAccount(901, "test-token")
				a.Extra = map[string]any{openai_compat.ExtraKeyResponsesMode: "force_chat_completions"}
				repo := &grokQuotaAccountRepo{mockAccountRepoForPlatform: &mockAccountRepoForPlatform{accountsByID: map[int64]*Account{a.ID: a}}}
				svc.grokTokenProvider = NewGrokTokenProvider(repo, nil)
				svc.accountRepo = repo
			}
			result, err := svc.Forward(context.Background(), c, a, body)
			require.NoError(t, err)
			require.Len(t, upstream.requests, 1)
			require.True(t, strings.HasSuffix(upstream.lastReq.URL.Path, "/responses"))
			require.Equal(t, 1, result.Usage.OutputTokens, "unknown total delta cannot create reasoning")
			require.Zero(t, result.Usage.CacheReadInputTokens, "cannot guess cache hits from nearby calls")
		})
	}
}

func TestGrokForceChat_RejectsNativeOnlyBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct{ name, fragment, path string }{
		{"compact", `"input":"hello"`, "/v1/responses/compact"},
		{"previous response", `"input":"hello","previous_response_id":"resp_old"`, ""},
		{"store", `"input":"hello","store":true`, ""},
		{"background", `"input":"hello","background":true`, ""},
		{"invalid background", `"input":"hello","background":"false"`, ""},
		{"include", `"input":"hello","include":["reasoning.encrypted_content"]`, ""},
		{"truncation", `"input":"hello","truncation":"auto"`, ""},
		{"native stream option", `"input":"hello","stream_options":{"include_obfuscation":true}`, ""},
		{"native max tool calls", `"input":"hello","max_tool_calls":3`, ""},
		{"server tool", `"input":"hello","tools":[{"type":"web_search"}]`, ""},
		{"server tool search", `"input":"hello","tools":[{"type":"tool_search","execution":"server"}]`, ""},
		{"additional server tools", `"input":[{"type":"additional_tools","tools":[{"type":"x_search"}]}]`, ""},
		{"unknown input", `"input":[{"type":"future_item"}]`, ""},
		{"compaction item", `"input":[{"type":"compaction","encrypted_content":"state"}]`, ""},
		{"encrypted reasoning", `"input":[{"type":"reasoning","encrypted_content":"state"}]`, ""},
		{"image input", `"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/x.png"}]}]`, ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"model":"grok-4.7-build-fast",` + tt.fragment + `}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			path := tt.path
			if path == "" {
				path = "/v1/responses"
			}
			c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			upstream := &httpUpstreamRecorder{}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			result, err := svc.Forward(context.Background(), c, grokForceChatTestAccount(), body)
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Empty(t, upstream.requests, "unsupported semantics must not incur upstream cost")
		})
	}
}

func TestGrokForceChat_ConverterKeepsClientToolsAndDefaultURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"grok-4.7-build-fast","store":false,"background":false,"truncation":"disabled","input":[{"role":"user","content":"hello"},{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}},{"type":"tool_search","execution":"client"},{"type":"namespace","name":"mcp","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	upstream := &httpUpstreamRecorder{resp: grokChatTestReply(grokSeparateReasoningUsage, false)}
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
	a := grokForceChatTestAccount()
	delete(a.Credentials, "base_url")
	result, err := svc.Forward(context.Background(), c, a, body)
	require.NoError(t, err)
	require.Equal(t, 77, result.Usage.OutputTokens)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "https://api.x.ai/v1/chat/completions", upstream.lastReq.URL.String())
	require.Len(t, gjson.GetBytes(upstream.lastBody, "tools").Array(), 4)
	require.NotEmpty(t, upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "hello", gjson.GetBytes(upstream.lastBody, "messages.0.content").String())
}

func TestGrokForceChat_OtherProviderUsageUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, inclusive := range []bool{false, true} {
		usage := grokSeparateReasoningUsage
		want := 1
		if inclusive {
			usage = strings.Replace(usage, `"completion_tokens":1,`, `"completion_tokens":77,`, 1)
			want = 77
		}
		body := []byte(`{"model":"gpt-6-sol","input":"hello"}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		upstream := &httpUpstreamRecorder{resp: grokChatTestReply(usage, false)}
		svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
		a := forceChatResponsesFallbackAccount()
		result, err := svc.Forward(context.Background(), c, a, body)
		require.NoError(t, err)
		require.Equal(t, want, result.Usage.OutputTokens)
		require.Equal(t, int64(want), gjson.Get(rec.Body.String(), "usage.output_tokens").Int())
		require.False(t, gjson.Get(rec.Body.String(), "usage.output_tokens_details.reasoning_tokens").Exists())
	}
}
