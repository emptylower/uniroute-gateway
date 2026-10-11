package handler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

var suspendedDuplicateFrames = []string{
	`{"type":"response.create","model":"gpt-5.6-sol","model":"kling/ai-avatar-pro"}`,
	`{"type":"response.create","model":"kling/ai-avatar-pro","model":"gpt-5.6-sol"}`,
	`{"type":"response.create","model":"gpt-5.6-sol","mo\u0064el":"kling\/ai-avatar-\u0070ro"}`,
	`{"type":"response.create","model":"gpt-5.6-sol","\u004dodel":"kling/ai-avatar-pro"}`,
	`{"type":"session.update","type":"response.create","model":"gpt-5.6-sol","MODEL":"kling/ai-avatar-pro"}`,
	`{"type":"session.update","session":{"model":"kling/ai-avatar-pro"}}`,
	`{"type":"session.update","session":{"model":"gpt-5.6-sol","mo\u0064el":"kling/ai-avatar-pro"}}`,
	`{"session":{"model":"gpt-5.6-sol"},"se\u0073sion":{"MODEL":"kling/ai-avatar-pro"},"type":"response.cancel","type":"session.update"}`,
}

func TestModelSuspensionWebSocketDuplicateFirstFrameRejectedBeforeScheduling(t *testing.T) {
	for _, payload := range suspendedDuplicateFrames {
		for _, msgType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
			t.Run(payload+"/"+msgType.String(), func(t *testing.T) {
				userSlots := 0
				h := newOpenAIHandlerForPreviousResponseIDValidation(t, &concurrencyCacheMock{acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) {
					userSlots++
					return true, nil
				}})
				server := newOpenAIWSHandlerTestServer(t, h, middleware.AuthSubject{UserID: 1, Concurrency: 1})
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
				require.NoError(t, err)
				defer func() { _ = conn.CloseNow() }()
				require.NoError(t, conn.Write(ctx, msgType, []byte(payload)))
				_, _, err = conn.Read(ctx)
				var closeErr coderws.CloseError
				require.ErrorAs(t, err, &closeErr)
				require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
				require.Contains(t, closeErr.Reason, "模型暂时下架")
				require.Zero(t, userSlots, "suspension must precede generation concurrency and scheduling")
			})
		}
	}
}

func TestModelSuspensionWebSocketDuplicateSubsequentFramesNeverReachProvider(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		for _, payload := range suspendedDuplicateFrames {
			for _, msgType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
				t.Run(mode+"/"+payload+"/"+msgType.String(), func(t *testing.T) {
					got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
						firstPayload:        `{"type":"response.create","model":"gpt-5.6-sol","max_output_tokens":32,"stream":false}`,
						secondPayload:       payload,
						ingressMode:         mode,
						suspendedSecondTurn: true,
						secondMessageType:   msgType,
					})
					require.Len(t, got.upstreamPayloads, 1)
					require.Len(t, got.logs, 1)
				})
			}
		}
	}
}

func TestModelSuspensionWebSocketNestedModelExamplesRemainAllowed(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
			firstPayload:  `{"type":"response.create","model":"gpt-5.6-sol","model":"gpt-5.6-sol","max_output_tokens":32,"stream":false,"metadata":{"model":"kling/ai-avatar-pro"}}`,
			secondPayload: `{"type":"response.create","model":"gpt-5.6-sol","model":"gpt-5.6-sol","max_output_tokens":32,"stream":false,"input":[{"model":"omnihuman-1-5"}]}`,
			ingressMode:   mode,
		})
		require.Len(t, got.upstreamPayloads, 2)
		require.Len(t, got.logs, 2)
	}
}

func TestModelSuspensionWebSocketSessionAccountAliasNeverReachesProvider(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		for _, msgType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
			got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				firstPayload:  `{"type":"response.create","model":"gpt-5.6-sol","max_output_tokens":32}`,
				secondPayload: `{"type":"session.update","session":{"model":"portrait-alias"}}`,
				ingressMode:   mode, secondMessageType: msgType, suspendedSecondTurn: true,
				accountModelMapping: map[string]any{"gpt-5.6-sol": "gpt-5.6-sol", "portrait-alias": "kling/ai-avatar-pro"},
			})
			require.Len(t, got.upstreamPayloads, 1)
			require.Len(t, got.logs, 1)
		}
	}
}
