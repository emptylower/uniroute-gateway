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
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

var suspendedTestModels = []string{
	"kling/ai-avatar-pro", "kling/ai-avatar-standard", "omnihuman-1-5",
	"omnihuman-1-5/human-identification", "omnihuman-1-5/subject-detection",
	"infinitalk/from-audio", "volcengine/video-to-video-lip-sync",
	"wan/2-2-a14b-speech-to-video-turbo", "wan/2-2-animate-replace",
}

func requireSuspendedModel(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))
	require.Equal(t, ModelTemporarilyUnavailableCode, infraerrors.Reason(err))
}

func TestModelSuspensionNamedIDsSlugsAndAliases(t *testing.T) {
	for _, id := range suspendedTestModels {
		for _, model := range []string{id, strings.ReplaceAll(id, "/", "-"), " " + strings.ToUpper(id) + " ", "https://uniroute.app/zh/models/" + strings.ReplaceAll(id, "/", "-")} {
			t.Run(model, func(t *testing.T) {
				requireSuspendedModel(t, CheckModelAvailability(model))
				_, _, units, err := resolveMediaQuote(model, "")
				requireSuspendedModel(t, err)
				require.Zero(t, units)
				// No user, database, wallet or provider is needed to reject create.
				_, err = (&MediaTaskService{}).Create(context.Background(), 1, "existing-key", MediaCreateInput{Model: model})
				requireSuspendedModel(t, err)
				// A stale queued task cannot reach the provider or create a write.
				handle, _ := newAuthorizationHandle("enforce")
				id, rejected, err := (&kieMediaProvider{}).Create(context.Background(), model, nil, handle)
				requireSuspendedModel(t, err)
				require.True(t, rejected)
				require.Empty(t, id)
				require.Empty(t, handle.Writes())
			})
		}
	}
	for _, model := range []string{"kling-ai-avatar", "infinitalk", "wan-speech-to-video-turbo", "omnihuman-1.5", "wan/2.2-animate-replace", "kling%2Fai-avatar-pro"} {
		requireSuspendedModel(t, CheckModelAvailability(model))
	}
	for _, model := range []string{"kling/2.6/text-to-video", "wan/2-2-animate-move", "wan-2-2-animate", "google/nano-banana", "gpt-5", "unrelated/kling/ai-avatar-pro", "kling/ai-avatar-pro-extra"} {
		require.NoError(t, CheckModelAvailability(model), model)
	}
	quote, err := mediaQuote("google/nano-banana", "1:1")
	require.NoError(t, err)
	require.Equal(t, "0.01875", quote.QuotedUSD)
}

func TestModelSuspensionAccountAliasesBeforeBillingOrSend(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"avatar-alias": "kling/ai-avatar-pro"}}}
	ctx := context.Background()
	_, err := (&GatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "avatar-alias", "avatar-alias", false, 0, 0)
	requireSuspendedModel(t, err)
	_, err = (&OpenAIGatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "safe-public", "avatar-alias", false)
	requireSuspendedModel(t, err)
	_, err = (&OpenAIGatewayService{}).ForwardImages(ctx, nil, account, nil, &OpenAIImagesRequest{Model: "safe-public"}, "avatar-alias")
	requireSuspendedModel(t, err)
	account.Platform = PlatformGrok
	_, err = (&OpenAIGatewayService{}).ForwardGrokMedia(ctx, nil, account, GrokMediaEndpointVideosGenerations, "", []byte(`{"model":"avatar-alias"}`), "application/json")
	requireSuspendedModel(t, err)
	// Billing off remains unchanged for allowed models.
	snap, err := (&GatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "gpt-5", "gpt-5", false, 0, 0)
	require.NoError(t, err)
	require.Nil(t, snap)
}

func TestModelSuspensionJSONChecksEveryDecodedRootModel(t *testing.T) {
	for _, body := range []string{
		`{"model":"gpt-5","model":"kling/ai-avatar-pro"}`,
		`{"model":"kling/ai-avatar-pro","model":"gpt-5"}`,
		`{"model":"gpt-5","mo\u0064el":"kling/ai-avatar-pro"}`,
		`{"model":"gpt-5","model":"kling\/ai-avatar-\u0070ro"}`,
		`{"model":"gpt-5","MODEL":"kling/ai-avatar-pro"}`,
		`{"model":"gpt-5","\u004dodel":"kling/ai-avatar-pro"}`,
		`{"model":"kling/ai-avatar-pro",`,
		`{"model":"gpt-image-1","model":"kling/ai-avatar-pro",`,
		`{"model":"gpt-image-1","\u004dodel":"kling/ai-avatar-pro",`,
		`{"MODEL":"kling/ai-avatar-pro","model":"gpt-5",`,
		"{\"model\":\"gpt-5\",\"input\":\"x\ny\",\"model\":\"kling/ai-avatar-pro\"}",
		"{\"model\":\"gpt-5\",\"input\":\"x\x00\x7fy\",\"\\u004dodel\":\"kling/ai-avatar-pro\"}",
		"\xef\xbb\xbf" + `{"model":"gpt-5","model":"kling/ai-avatar-pro"}`,
	} {
		t.Run(body, func(t *testing.T) { requireSuspendedModel(t, CheckJSONModelAvailability([]byte(body))) })
	}
	for _, body := range []string{
		`{"model":"gpt-5","model":"gpt-5.2"}`,
		`{"model":"gpt-5","prompt":"kling/ai-avatar-pro","input":{"model":"kling/ai-avatar-pro"},"tools":[{"model":"omnihuman-1-5"}]}`,
		`{"model":{"model":"kling/ai-avatar-pro"}}`,
		`["kling/ai-avatar-pro"]`,
		"{\"model\":\"gpt-5\",\"input\":{\"text\":\"x\ny\",\"model\":\"kling/ai-avatar-pro\"}}",
		"\xef\xbb\xbf" + `{"model":"gpt-5","input":{"model":"kling/ai-avatar-pro"}}`,
	} {
		require.NoError(t, CheckJSONModelAvailability([]byte(body)), body)
	}
}

func TestModelSuspensionWebSocketServiceRejectsRawBeforeMappingOrAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	mapped, authorized := 0, 0
	svc := &OpenAIGatewayService{}
	err := svc.ProxyResponsesWebSocketFromClient(context.Background(), c, &coderws.Conn{},
		&Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, "test-token",
		[]byte(`{"model":"gpt-5","mo\u0064el":"kling/ai-avatar-pro"}`), &OpenAIWSIngressHooks{
			MapRequestModel: func(int, string) (string, error) { mapped++; return "gpt-5", nil },
			AuthorizeTurn:   func(int, EstimateInput) (*AuthorizationHandle, error) { authorized++; return nil, nil },
		})
	require.ErrorIs(t, err, ErrModelTemporarilyUnavailable)
	require.Zero(t, mapped)
	require.Zero(t, authorized)
}

func TestModelSuspensionWebSocketSessionSelectors(t *testing.T) {
	account := &Account{Credentials: map[string]any{"model_mapping": map[string]any{"portrait-alias": "kling/ai-avatar-pro"}}}
	for _, body := range []string{
		`{"type":"session.update","session":{"model":"kling/ai-avatar-pro"}}`,
		`{"type":"session.update","session":{"model":"gpt-5","\u004dodel":"kling/ai-avatar-pro"}}`,
		`{"session":{"model":"gpt-5"},"session":{"MODEL":"kling/ai-avatar-pro"},"type":"response.cancel","t\u0079pe":"session.update"}`,
		`{"type":"session.update","session":{"model":"portrait-alias"}}`,
	} {
		requireSuspendedModel(t, CheckWebSocketModelAvailability([]byte(body), account))
	}
	for _, body := range []string{
		`{"type":"session.update","session":{"model":"gpt-5","metadata":{"model":"kling/ai-avatar-pro"}}}`,
		`{"type":"response.create","model":"gpt-5","session":{"model":"kling/ai-avatar-pro"}}`,
		`{"type":"session.update","input":{"model":"kling/ai-avatar-pro"},"session":{"model":"gpt-5"}}`,
	} {
		require.NoError(t, CheckWebSocketModelAvailability([]byte(body), account), body)
	}
}

func TestModelSuspensionLiveRawSessionBeforeStoreAuthorizationOrSend(t *testing.T) {
	for _, session := range []string{
		`{"model":"gpt-5","model":"kling/ai-avatar-pro"}`,
		`{"model":"gpt-5","\u004dodel":"kling/ai-avatar-pro"}`,
	} {
		request := &LiveCallRequest{SDP: "v=0\r\n", Session: []byte(session)}
		// Empty services have no store/cache/authorization or provider dependencies.
		_, err := (&OpenAIGatewayService{}).CreateLiveCall(context.Background(), request, LiveCallIdentity{}, 1)
		requireSuspendedModel(t, err)
		_, err = (&OpenAIGatewayService{}).createUpstreamLiveCall(context.Background(), nil, request, "", nil)
		requireSuspendedModel(t, err)
	}
	account := &Account{Credentials: map[string]any{"model_mapping": map[string]any{"portrait-alias": "kling/ai-avatar-pro"}}}
	request := &LiveCallRequest{SDP: "v=0\r\n", Session: []byte(`{"model":"gpt-5","model":"portrait-alias"}`)}
	_, err := (&OpenAIGatewayService{}).createUpstreamLiveCall(context.Background(), account, request, "", nil)
	requireSuspendedModel(t, err)
}

func TestModelSuspensionWebSocketSecondRawFramePrecedesMappingAndAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_safe\",\"model\":\"grok-4.3\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_safe\",\"model\":\"grok-4.3\",\"usage\":{\"input_tokens\":4,\"output_tokens\":2}}}\n\n")),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
	account := &Account{ID: 71, Platform: PlatformGrok, Type: AccountTypeOAuth, Concurrency: 1, Status: StatusActive}
	mapped, authorized := 0, 0
	errCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, firstMessage, err := conn.Read(r.Context())
		if err != nil {
			errCh <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r.Clone(r.Context())
		c.Set("api_key", &APIKey{ID: 7101})
		errCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "test-token", firstMessage, &OpenAIWSIngressHooks{
			MapRequestModel: func(_ int, model string) (string, error) { mapped++; return model, nil },
			AuthorizeTurn:   func(int, EstimateInput) (*AuthorizationHandle, error) { authorized++; return nil, nil },
		})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"grok-4.3","input":"hi"}`)))
	for range 2 {
		_, _, err = conn.Read(ctx)
		require.NoError(t, err)
	}
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"grok-4.3","\u004dodel":"kling/ai-avatar-pro"}`)))
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrModelTemporarilyUnavailable)
	case <-ctx.Done():
		t.Fatal("suspended second frame did not stop the proxy")
	}
	// Only the allowed first turn may reach mapping, authorization or the provider.
	require.Equal(t, 1, mapped)
	require.Equal(t, 1, authorized)
	require.Len(t, upstream.requests, 1)
}
