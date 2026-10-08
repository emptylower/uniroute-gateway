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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const grokSeparateReasoningUsage = `{"prompt_tokens":1247,"completion_tokens":1,"total_tokens":1324,"prompt_tokens_details":{"cached_tokens":1152},"completion_tokens_details":{"reasoning_tokens":76}}`

func TestNormalizeGrokReasoningUsage_ConservationAndProvider(t *testing.T) {
	tests := []struct {
		name, value, platform string
		want                  int
	}{
		{"separate reasoning", grokSeparateReasoningUsage, PlatformGrok, 77},
		{"already inclusive", strings.Replace(grokSeparateReasoningUsage, `"completion_tokens":1,`, `"completion_tokens":77,`, 1), PlatformGrok, 77},
		{"OpenAI untouched", grokSeparateReasoningUsage, PlatformOpenAI, 1},
		{"other provider untouched", grokSeparateReasoningUsage, PlatformAnthropic, 1},
		{"standard Responses", `{"input_tokens":1247,"output_tokens":77,"prompt_tokens":1247,"completion_tokens":1,"total_tokens":1324,"completion_tokens_details":{"reasoning_tokens":76}}`, PlatformGrok, 77},
		{"explicit zero standard output", `{"input_tokens":1247,"output_tokens":0,"prompt_tokens":1247,"completion_tokens":1,"total_tokens":1324,"completion_tokens_details":{"reasoning_tokens":76}}`, PlatformGrok, 1},
		{"reasoning zero", strings.Replace(grokSeparateReasoningUsage, `"reasoning_tokens":76`, `"reasoning_tokens":0`, 1), PlatformGrok, 1},
		{"missing total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324,`, ``, 1), PlatformGrok, 1},
		{"mismatched total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":1325`, 1), PlatformGrok, 1},
		{"negative prompt", strings.Replace(grokSeparateReasoningUsage, `"prompt_tokens":1247`, `"prompt_tokens":-1`, 1), PlatformGrok, 1},
		{"negative completion", strings.Replace(grokSeparateReasoningUsage, `"completion_tokens":1,`, `"completion_tokens":-1,`, 1), PlatformGrok, -1},
		{"negative reasoning", strings.Replace(grokSeparateReasoningUsage, `"reasoning_tokens":76`, `"reasoning_tokens":-76`, 1), PlatformGrok, 1},
		{"missing reasoning", strings.Replace(grokSeparateReasoningUsage, `"reasoning_tokens":76`, `"other_tokens":76`, 1), PlatformGrok, 1},
		{"fractional reasoning", strings.Replace(grokSeparateReasoningUsage, `"reasoning_tokens":76`, `"reasoning_tokens":76.5`, 1), PlatformGrok, 1},
		{"fractional total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":1324.5`, 1), PlatformGrok, 1},
		{"string total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":"1324"`, 1), PlatformGrok, 1},
		{"overflow total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":9223372036854775808`, 1), PlatformGrok, 1},
		{"total below prompt", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":1246`, 1), PlatformGrok, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := gjson.Parse(tt.value)
			usage, ok := openAIUsageFromGJSON(value)
			require.True(t, ok)
			before := usage
			normalizeGrokChatCompletionUsage(&Account{Platform: tt.platform}, value, &usage)
			require.Equal(t, tt.want, usage.OutputTokens)
			usage.OutputTokens = before.OutputTokens
			require.Equal(t, before, usage, "all input/cache/tool usage must remain unchanged")
		})
	}
	usage := OpenAIUsage{OutputTokens: 1}
	normalizeGrokChatCompletionUsage(nil, gjson.Parse(grokSeparateReasoningUsage), &usage)
	require.Equal(t, 1, usage.OutputTokens)
	normalizeGrokChatCompletionUsage(&Account{Platform: PlatformGrok}, gjson.Parse(grokSeparateReasoningUsage), nil)
}

// Exercise each production response handler, including provider propagation through
// the two compatibility pipelines. Repeated CC usage chunks must replace usage.
func TestGrokReasoningUsage_ResponseRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routes := []string{"raw JSON", "raw SSE", "Responses JSON", "Responses SSE", "Responses buffered SSE", "CC to Responses JSON", "CC to Responses SSE", "CC to Messages JSON", "CC to Messages SSE", "Responses to CC buffered", "Responses to CC SSE", "Responses to Messages buffered", "Responses to Messages SSE"}
	variants := []struct {
		name, usage, platform string
		want                  int
	}{
		{"separate", grokSeparateReasoningUsage, PlatformGrok, 77},
		{"inclusive", strings.Replace(grokSeparateReasoningUsage, `"completion_tokens":1,`, `"completion_tokens":77,`, 1), PlatformGrok, 77},
		{"OpenAI", grokSeparateReasoningUsage, PlatformOpenAI, 1},
		{"mismatched total", strings.Replace(grokSeparateReasoningUsage, `"total_tokens":1324`, `"total_tokens":1325`, 1), PlatformGrok, 1},
		{"standard Responses", `{"input_tokens":1247,"output_tokens":77,"total_tokens":1324,"input_tokens_details":{"cached_tokens":1152},"output_tokens_details":{"reasoning_tokens":76}}`, PlatformGrok, 77},
	}
	for _, route := range routes {
		for _, variant := range variants {
			// CC upstreams use the CC contract; standard Responses is tested on native
			// Responses and all Responses-to-client bridges.
			if variant.name == "standard Responses" && (strings.HasPrefix(route, "raw") || strings.HasPrefix(route, "CC to")) {
				continue
			}
			t.Run(route+"/"+variant.name, func(t *testing.T) {
				svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
				account := &Account{Platform: variant.platform, Type: AccountTypeAPIKey}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				ccJSON := `{"id":"chat_test","model":"grok-4.7-build-fast","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":` + variant.usage + `}`
				ccSSE := `data: {"id":"chat_test","choices":[{"index":0,"delta":{"role":"assistant","content":"OK"},"finish_reason":"stop"}]}` + "\n\n"
				usageChunk := `data: {"id":"chat_test","choices":[],"usage":` + variant.usage + `}` + "\n\n"
				ccSSE += usageChunk + usageChunk + "data: [DONE]\n\n"
				responseJSON := `{"id":"resp_test","model":"grok-4.7-build-fast","status":"completed","output":[{"type":"message","id":"msg_test","role":"assistant","content":[{"type":"output_text","text":"OK"}]}],"usage":` + variant.usage + `}`
				responseSSE := `data: {"type":"response.completed","response":` + responseJSON + `}` + "\n\n"
				body, contentType := ccJSON, "application/json"
				if strings.HasPrefix(route, "Responses") {
					body = responseJSON
				}
				if strings.Contains(route, "SSE") || strings.Contains(route, "buffered") {
					contentType = "text/event-stream"
					body = ccSSE
					if strings.HasPrefix(route, "Responses") {
						body = responseSSE
					}
				}
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body))}
				var usage OpenAIUsage
				var err error
				var result *OpenAIForwardResult
				now := time.Now()
				const model = "grok-4.7-build-fast"
				switch route {
				case "raw JSON":
					result, err = svc.bufferRawChatCompletions(c, resp, account, model, model, model, nil, nil, now)
				case "raw SSE":
					result, err = svc.streamRawChatCompletions(c, resp, account, model, model, model, nil, nil, now, 1)
				case "Responses JSON", "Responses buffered SSE":
					var r *openaiNonStreamingResult
					r, err = svc.handleNonStreamingResponse(context.Background(), resp, c, account, model, model)
					if r != nil {
						usage = *r.usage
					}
				case "Responses SSE":
					var r *openaiStreamingResult
					r, err = svc.handleStreamingResponse(context.Background(), resp, c, account, now, model, model)
					if r != nil {
						usage = *r.usage
					}
				case "CC to Responses JSON":
					result, err = svc.bufferChatCompletionsAsResponses(c, resp, account, model, nil, false, nil, model, model, nil, nil, now)
				case "CC to Responses SSE":
					result, err = svc.streamChatCompletionsAsResponses(c, resp, account, model, nil, false, nil, model, model, nil, nil, now)
				case "CC to Messages JSON":
					result, err = svc.bufferChatCompletionsAsAnthropic(c, resp, account, model, model, model, nil, nil, now)
				case "CC to Messages SSE":
					result, err = svc.streamChatCompletionsAsAnthropic(c, resp, account, model, model, model, nil, nil, now)
				case "Responses to CC buffered":
					result, err = svc.handleChatBufferedStreamingResponse(resp, c, account, model, model, model, now)
				case "Responses to CC SSE":
					result, err = svc.handleChatStreamingResponse(resp, c, account, model, model, model, now, 1)
				case "Responses to Messages buffered":
					result, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, account, model, model, model, now)
				case "Responses to Messages SSE":
					result, err = svc.handleAnthropicStreamingResponse(resp, c, account, model, model, model, now)
				default:
					t.Fatalf("unknown route %s", route)
				}
				require.NoError(t, err)
				if result != nil {
					usage = result.Usage
				}
				require.Equal(t, 1247, usage.InputTokens)
				require.Equal(t, 1152, usage.CacheReadInputTokens)
				require.Equal(t, variant.want, usage.OutputTokens)
				require.Zero(t, usage.CacheCreationInputTokens)
				if strings.HasPrefix(route, "raw") {
					require.Contains(t, rec.Body.String(), `"usage":`+variant.usage, "upstream usage must pass through unchanged")
				}
				// Match production's uncached-input bucket and use the real billing engine.
				billing := &BillingService{}
				cost := billing.computeTokenBreakdown(&ModelPricing{InputPricePerToken: 4e-6, OutputPricePerToken: 12e-6, CacheReadPricePerToken: 1e-6}, openAIUsageTokens(usage), .42, "", false)
				expected := .001544
				if variant.want == 77 {
					expected = .002456
				}
				require.InDelta(t, expected, cost.TotalCost, 1e-12)
				require.InDelta(t, expected*.42, cost.ActualCost, 1e-12)
			})
		}
	}
}

func TestGrokReasoningUsage_ComposerImageDescription(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, text := range []string{"OK", ""} {
		t.Run("text="+text, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			account := &Account{Platform: PlatformGrok, Type: AccountTypeAPIKey}
			body := `{"id":"resp_test","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + text + `"}]}],"usage":` + grokSeparateReasoningUsage + `}`
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			description, usage, err := svc.describeGrokComposerImage(context.Background(), c, account, "test-token", "https://example.com/image.png", 1)
			if text == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, text, description)
			require.Equal(t, 1247, usage.InputTokens)
			require.Equal(t, 1152, usage.CacheReadInputTokens)
			require.Equal(t, 77, usage.OutputTokens)
		})
	}
}

func TestGrokReasoningUsage_BufferedTerminalTopLevelUsage(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	// Omit the final blank line to exercise the reader's Finish path as well as
	// the usual complete-frame path. Top-level usage is copied into Response.
	for _, responseUsage := range []string{"", `,"usage":null`} {
		for _, ending := range []string{"", "\n\n"} {
			payload := `data: {"type":"response.completed","response":{"id":"resp_test","status":"completed","output":[]` + responseUsage + `},"usage":` + grokSeparateReasoningUsage + `}` + ending
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(payload))}
			final, usage, _, err := svc.readOpenAICompatBufferedTerminal(resp, "test", "", &Account{Platform: PlatformGrok})
			require.NoError(t, err)
			require.NotNil(t, final)
			require.Equal(t, 77, usage.OutputTokens)
			require.Equal(t, 1152, usage.CacheReadInputTokens)
			require.Equal(t, 1, final.Usage.OutputTokens, "typed downstream usage must retain its existing behavior")
		}
	}
}
