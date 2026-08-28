//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// --- harness -------------------------------------------------------------

// singleConnFakeDialer always returns one conn — the minimal dialer contract.
type singleConnFakeDialer struct{ conn openAIWSClientConn }

func (d *singleConnFakeDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	return d.conn, 0, nil, nil
}

// recordedAuthWSClientDialer decorates a single capture conn with the Task 1
// authorizing wrapper (the exact shape the default constructor produces) and
// records the wrapper so tests can assert the conn's armed state.
type recordedAuthWSClientDialer struct {
	inner openAIWSClientConn
	mode  func() string

	mu            sync.Mutex
	dials         int
	lastDecorated *authorizingOpenAIWSClientConn
}

func (d *recordedAuthWSClientDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	d.dials++
	d.mu.Unlock()
	conn, status, hdr, err := newAuthorizingOpenAIWSClientDialerWithMode(&singleConnFakeDialer{conn: d.inner}, d.mode).Dial(ctx, wsURL, headers, proxyURL)
	if ac, ok := conn.(*authorizingOpenAIWSClientConn); ok {
		d.mu.Lock()
		d.lastDecorated = ac
		d.mu.Unlock()
	}
	return conn, status, hdr, err
}

func (d *recordedAuthWSClientDialer) decorated() *authorizingOpenAIWSClientConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastDecorated
}

// ctxRecordingWSConn records the context of every WriteJSON so the disabled-mode
// byte-for-byte test can assert context purity.
type ctxRecordingWSConn struct {
	*openAIWSCaptureConn
	mu        sync.Mutex
	writeCtxs []context.Context
}

func (c *ctxRecordingWSConn) WriteJSON(ctx context.Context, value any) error {
	c.mu.Lock()
	c.writeCtxs = append(c.writeCtxs, ctx)
	c.mu.Unlock()
	return c.openAIWSCaptureConn.WriteJSON(ctx, value)
}

func (c *ctxRecordingWSConn) recordedWriteCtxs() []context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]context.Context, len(c.writeCtxs))
	copy(out, c.writeCtxs)
	return out
}

// handleRecordingHTTPUpstream records the authorization handle seen on each Do.
type handleRecordingHTTPUpstream struct {
	inner   HTTPUpstream
	mu      sync.Mutex
	handles []*AuthorizationHandle
}

func (u *handleRecordingHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	u.mu.Lock()
	if req != nil {
		u.handles = append(u.handles, AuthorizationHandleFromContext(req.Context()))
	} else {
		u.handles = append(u.handles, nil)
	}
	u.mu.Unlock()
	return u.inner.Do(req, proxyURL, accountID, accountConcurrency)
}

func (u *handleRecordingHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

func (u *handleRecordingHTTPUpstream) seenHandles() []*AuthorizationHandle {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]*AuthorizationHandle, len(u.handles))
	copy(out, u.handles)
	return out
}

type ingressAuthCall struct {
	turn     int
	estimate EstimateInput
}

type ingressAuthHarness struct {
	clientConn  *coderws.Conn
	serverErrCh chan error
	calls       chan ingressAuthCall
	handles     sync.Map // turn -> *AuthorizationHandle
}

func (h *ingressAuthHarness) writeMessage(t *testing.T, payload string) {
	t.Helper()
	writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, h.clientConn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func (h *ingressAuthHarness) readEvent(t *testing.T) []byte {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msgType, event, readErr := h.clientConn.Read(readCtx)
	require.NoError(t, readErr)
	require.Equal(t, coderws.MessageText, msgType)
	return event
}

func (h *ingressAuthHarness) waitCall(t *testing.T) ingressAuthCall {
	t.Helper()
	select {
	case call := <-h.calls:
		return call
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for AuthorizeTurn call")
		return ingressAuthCall{}
	}
}

func (h *ingressAuthHarness) closeAndWait(t *testing.T) {
	t.Helper()
	_ = h.clientConn.Close(coderws.StatusNormalClosure, "done")
	select {
	case serverErr := <-h.serverErrCh:
		if serverErr != nil && !errors.Is(serverErr, context.Canceled) {
			t.Logf("ingress session ended with: %v", serverErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ingress session to end")
	}
}

type ingressAuthFixture struct {
	cfg         *config.Config
	svc         *OpenAIGatewayService
	pool        *openAIWSConnPool
	captureConn *openAIWSCaptureConn
	dialer      *recordedAuthWSClientDialer
}

func newIngressAuthFixture(t *testing.T, mode string) *ingressAuthFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = mode
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

	captureConn := &openAIWSCaptureConn{}
	dialer := &recordedAuthWSClientDialer{inner: captureConn, mode: modeFn(mode)}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	t.Cleanup(pool.Close)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	return &ingressAuthFixture{cfg: cfg, svc: svc, pool: pool, captureConn: captureConn, dialer: dialer}
}

// setCaptureEvents queues the upstream events the capture conn hands out; call
// it before the session's first turn is written.
func (f *ingressAuthFixture) setCaptureEvents(_ *testing.T, events [][]byte) {
	f.captureConn.mu.Lock()
	f.captureConn.events = events
	f.captureConn.mu.Unlock()
}

func (f *ingressAuthFixture) decoratedConn(t *testing.T) *authorizingOpenAIWSClientConn {
	t.Helper()
	conn := f.dialer.decorated()
	require.NotNil(t, conn, "the ingress has dialed through the decorated dialer")
	return conn
}

func (f *ingressAuthFixture) captureConnForTest(t *testing.T) *openAIWSCaptureConn {
	t.Helper()
	return f.captureConn
}

func (f *ingressAuthFixture) newOpenAIAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        "openai-ingress-auth",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}
}

// startWSSession runs ProxyResponsesWebSocketFromClient inside an httptest WS
// server, with an AuthorizeTurn hook that mints one fresh handle per turn and
// records every (turn, estimate) it is given.
func (f *ingressAuthFixture) startWSSession(t *testing.T, extraHooks func(*OpenAIWSIngressHooks)) *ingressAuthHarness {
	t.Helper()
	harness := &ingressAuthHarness{serverErrCh: make(chan error, 1), calls: make(chan ingressAuthCall, 8)}
	hooks := &OpenAIWSIngressHooks{
		AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
			h, err := newAuthorizationHandle("enforce")
			if err != nil {
				return nil, err
			}
			harness.handles.Store(turn, h)
			select {
			case harness.calls <- ingressAuthCall{turn: turn, estimate: estimate}:
			default:
			}
			return h, nil
		},
	}
	if extraHooks != nil {
		extraHooks(hooks)
	}
	account := f.newOpenAIAccount(3141)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			harness.serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			harness.serverErrCh <- readErr
			return
		}
		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		req.Header.Set("User-Agent", "unit-test-agent/1.0")
		ginCtx.Request = req
		harness.serverErrCh <- f.svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
	}))
	t.Cleanup(server.Close)

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.CloseNow() })
	harness.clientConn = clientConn
	return harness
}

func sseBridgeResponse(responseID string, inputTokens, outputTokens int) *http.Response {
	sseBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"` + responseID + `","model":"gpt-5.1"}}`,
		"",
		`data: {"type":"response.output_text.delta","response":{"id":"` + responseID + `"},"delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","model":"gpt-5.1","usage":{"input_tokens":` + strconv.Itoa(inputTokens) + `,"output_tokens":` + strconv.Itoa(outputTokens) + `}}}`,
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"x-request-id": []string{responseID},
		},
		Body: io.NopCloser(strings.NewReader(sseBody)),
	}
}

// --- tests ---------------------------------------------------------------

func TestIngressTurnsMintDistinctTokensThroughTheWSPort(t *testing.T) {
	resetAuthorizationMetricsForTest()
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_tok_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_tok_2","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":2}}}`),
	})
	turnDone := make(chan int, 4)
	harness := f.startWSSession(t, func(hooks *OpenAIWSIngressHooks) {
		hooks.AfterTurn = func(turn int, result *OpenAIForwardResult, turnErr error) {
			if turnErr == nil && result != nil {
				turnDone <- turn
			}
		}
	})

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.turn)
	completed1 := harness.readEvent(t)
	require.Equal(t, "response.completed", gjson.GetBytes(completed1, "type").String())
	require.Equal(t, 1, <-turnDone)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	call2 := harness.waitCall(t)
	require.Equal(t, 2, call2.turn)
	completed2 := harness.readEvent(t)
	require.Equal(t, "response.completed", gjson.GetBytes(completed2, "type").String())
	require.Equal(t, 2, <-turnDone)

	harness.closeAndWait(t)

	h1, _ := harness.handles.Load(1)
	h2, _ := harness.handles.Load(2)
	w1 := h1.(*AuthorizationHandle).Writes()
	w2 := h2.(*AuthorizationHandle).Writes()
	require.Len(t, w1, 1, "turn 1's handle has exactly one write")
	require.Len(t, w2, 1, "turn 2's handle has exactly one write")
	require.NotEmpty(t, w1[0].Token)
	require.NotEqual(t, w1[0].Token, w2[0].Token, "each turn mints a distinct token")
	require.Equal(t, AuthorizationOutcomeResult, w1[0].Outcome)

	snap := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(0), snap.WritesRefused)
	require.Equal(t, int64(2), snap.WritesAuthorized)

	// The conn is disarmed after each turn: AfterTurn fired after the disarm.
	require.Nil(t, f.decoratedConn(t).ArmedAuthorization())
}

func TestIngressContinuationTurnIsClassifiedFromTheSessionsUsage(t *testing.T) {
	resetAuthorizationMetricsForTest()
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	// Turn 1 completes with usage {input: 120, output: 30}; turn 2 with any usage.
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_t1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_t2","model":"gpt-5.1","usage":{"input_tokens":200,"output_tokens":40}}}`),
	})
	harness := f.startWSSession(t, nil)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	call1 := harness.waitCall(t)
	require.Equal(t, ContinuationNone, call1.estimate.Continuation)
	completed1 := harness.readEvent(t)
	require.Equal(t, "resp_t1", gjson.GetBytes(completed1, "response.id").String())

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_t1"}`)
	call2 := harness.waitCall(t)
	require.Equal(t, ContinuationWarm, call2.estimate.Continuation)
	require.Equal(t, 120, call2.estimate.PriorTurnInputTokens)
	require.Equal(t, 30, call2.estimate.PriorTurnOutputTokens)
	harness.readEvent(t)
	harness.closeAndWait(t)
}

func TestIngressContinuationStoreDisabledSessionIsNeverWarm(t *testing.T) {
	resetAuthorizationMetricsForTest()
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_s1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_s2","model":"gpt-5.1","usage":{"input_tokens":200,"output_tokens":40}}}`),
	})
	harness := f.startWSSession(t, nil)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false}`)
	call1 := harness.waitCall(t)
	require.Equal(t, ContinuationNone, call1.estimate.Continuation)
	harness.readEvent(t)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"previous_response_id":"resp_s1"}`)
	call2 := harness.waitCall(t)
	require.Equal(t, ContinuationCold, call2.estimate.Continuation)
	require.Equal(t, 0, call2.estimate.PriorTurnInputTokens, "store disabled: the prior turn's usage is not this turn's input")
	require.Equal(t, 0, call2.estimate.PriorTurnOutputTokens)
	harness.readEvent(t)
	harness.closeAndWait(t)
}

func TestIngressContinuationFirstFrameWithPreviousResponseIDIsCold(t *testing.T) {
	resetAuthorizationMetricsForTest()
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_c1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
	})
	harness := f.startWSSession(t, nil)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_elsewhere"}`)
	call1 := harness.waitCall(t)
	require.Equal(t, ContinuationCold, call1.estimate.Continuation)
	harness.readEvent(t)
	harness.closeAndWait(t)
}

func TestIngressImageTurnEstimatesWithImageCount(t *testing.T) {
	resetAuthorizationMetricsForTest()
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_img1","model":"gpt-image-1","usage":{"input_tokens":1,"output_tokens":1}}}`),
	})
	harness := f.startWSSession(t, nil)

	harness.writeMessage(t, `{"type":"response.create","model":"gpt-image-1","stream":false}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.estimate.ImageCount, "a WS image turn must not be estimated with ImageCount 0")
	imageCfg, cfgErr := resolveOpenAIResponsesImageBillingConfigDetailedFromBody([]byte(`{"type":"response.create","model":"gpt-image-1","stream":false}`), "gpt-image-1")
	require.NoError(t, cfgErr)
	require.Equal(t, imageCfg.SizeTier, call1.estimate.ImageSize)
	harness.readEvent(t)
	harness.closeAndWait(t)
}

func TestBridgedTurnsMintDistinctTokensThroughTheHTTPPort(t *testing.T) {
	for _, tc := range []struct {
		name string
		grok bool
	}{
		{name: "grok_force_bridge", grok: true},
		{name: "openai_threshold_bridge", grok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetAuthorizationMetricsForTest()
			gin.SetMode(gin.TestMode)
			recorder := &httpUpstreamRecorder{responses: []*http.Response{
				sseBridgeResponse("resp_b1", 120, 30),
				sseBridgeResponse("resp_b2", 200, 40),
			}}
			handleSeen := &handleRecordingHTTPUpstream{inner: recorder}
			httpUpstream := newAuthorizingHTTPUpstreamWithMode(handleSeen, modeFn(config.CanonicalWalletModeEnforce))

			cfg := &config.Config{}
			cfg.Security.URLAllowlist.Enabled = false
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.MaxLineSize = defaultMaxLineSize
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			if !tc.grok {
				cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
				cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 1
			}
			svc := &OpenAIGatewayService{
				cfg:              cfg,
				httpUpstream:     httpUpstream,
				cache:            &stubGatewayCache{},
				openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:    NewCodexToolCorrector(),
			}

			var account *Account
			if tc.grok {
				account = &Account{
					ID: 71, Name: "grok", Platform: PlatformGrok, Type: AccountTypeOAuth,
					Concurrency: 1, Status: StatusActive,
					Credentials: map[string]any{"base_url": xai.DefaultCLIBaseURL},
				}
			} else {
				account = &Account{
					ID: 72, Name: "openai-bridge", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
					Concurrency: 1, Status: StatusActive,
					Credentials: map[string]any{"api_key": "sk-test"},
					Extra:       map[string]any{"responses_websockets_v2_enabled": true},
				}
			}

			harness := &ingressAuthHarness{serverErrCh: make(chan error, 1), calls: make(chan ingressAuthCall, 8)}
			hooks := &OpenAIWSIngressHooks{
				AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
					h, err := newAuthorizationHandle("enforce")
					if err != nil {
						return nil, err
					}
					harness.handles.Store(turn, h)
					select {
					case harness.calls <- ingressAuthCall{turn: turn, estimate: estimate}:
					default:
					}
					return h, nil
				},
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
				if err != nil {
					harness.serverErrCh <- err
					return
				}
				defer func() { _ = conn.CloseNow() }()
				readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				_, firstMessage, readErr := conn.Read(readCtx)
				cancel()
				if readErr != nil {
					harness.serverErrCh <- readErr
					return
				}
				rec := httptest.NewRecorder()
				ginCtx, _ := gin.CreateTestContext(rec)
				req := r.Clone(r.Context())
				req.Header = req.Header.Clone()
				ginCtx.Request = req
				harness.serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
			}))
			defer server.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			cancelDial()
			require.NoError(t, err)
			defer func() { _ = clientConn.CloseNow() }()

			write := func(payload string) {
				writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				require.NoError(t, clientConn.Write(writeCtx, coderws.MessageText, []byte(payload)))
			}
			readAll := func() {
				for i := 0; i < 3; i++ {
					readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, _, readErr := clientConn.Read(readCtx)
					cancel()
					require.NoError(t, readErr)
				}
			}

			write(`{"type":"response.create","model":"gpt-5.1","stream":true,"input":"hi"}`)
			call1 := harness.waitCall(t)
			readAll()
			write(`{"type":"response.create","model":"gpt-5.1","stream":true,"input":"second"}`)
			call2 := harness.waitCall(t)
			readAll()

			_ = clientConn.Close(coderws.StatusNormalClosure, "done")
			select {
			case proxyErr := <-harness.serverErrCh:
				require.NoError(t, proxyErr)
			case <-time.After(3 * time.Second):
				t.Fatal("bridge session did not finish")
			}

			require.Equal(t, ContinuationNone, call1.estimate.Continuation, "the bridged payload inlines the history: no continuation term")
			require.Equal(t, ContinuationNone, call2.estimate.Continuation)

			seen := handleSeen.seenHandles()
			require.Len(t, seen, 2, "two bridge Do calls")
			h1, _ := harness.handles.Load(1)
			h2, _ := harness.handles.Load(2)
			require.Same(t, h1.(*AuthorizationHandle), seen[0], "Do saw turn 1's per-turn handle")
			require.Same(t, h2.(*AuthorizationHandle), seen[1], "Do saw turn 2's per-turn handle")
			require.NotSame(t, h1, h2)
			w1 := seen[0].Writes()
			w2 := seen[1].Writes()
			require.Len(t, w1, 1, "one token minted through the HTTP port per turn")
			require.Len(t, w2, 1)
			require.NotEqual(t, w1[0].Token, w2[0].Token, "two distinct tokens")
		})
	}
}

func TestIngressRefusedTurnClosesWithTheNamedStatusAndIsNotRetried(t *testing.T) {
	newRefusal := func() error {
		return NewOpenAIWSClientCloseError(
			coderws.StatusCode(AuthorizationRefusedWSCloseStatus),
			AuthorizationRefusedWSCloseReason+": "+string(AuthorizationRefusalBalanceShortfall),
			&AuthorizationRefusedError{Reason: AuthorizationRefusalBalanceShortfall},
		)
	}

	t.Run("ws_path", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
		account := f.newOpenAIAccount(3141)
		hooks := &OpenAIWSIngressHooks{
			AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
				return nil, newRefusal()
			},
		}
		serverErrCh := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
			if err != nil {
				serverErrCh <- err
				return
			}
			defer func() { _ = conn.CloseNow() }()
			readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			_, firstMessage, readErr := conn.Read(readCtx)
			cancel()
			if readErr != nil {
				serverErrCh <- readErr
				return
			}
			rec := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(rec)
			req := r.Clone(r.Context())
			req.Header = req.Header.Clone()
			ginCtx.Request = req
			serverErrCh <- f.svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
		}))
		defer server.Close()

		dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
		clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		cancelDial()
		require.NoError(t, err)
		defer func() { _ = clientConn.CloseNow() }()

		writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		require.NoError(t, clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","stream":false}`)))
		cancel()

		var proxyErr error
		select {
		case proxyErr = <-serverErrCh:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for the refused turn to end the session")
		}
		require.Error(t, proxyErr)
		require.False(t, isOpenAIWSIngressTurnRetryable(proxyErr), "a refused turn is terminal — no relay retries it")
		var closeErr *OpenAIWSClientCloseError
		require.True(t, errors.As(proxyErr, &closeErr))
		require.Equal(t, coderws.StatusCode(AuthorizationRefusedWSCloseStatus), closeErr.StatusCode())
		require.Contains(t, closeErr.Reason(), AuthorizationRefusedWSCloseReason)
		require.Empty(t, f.captureConnForTest(t).writes, "the upstream saw zero writes")
		_ = clientConn.Close(coderws.StatusNormalClosure, "done")
	})

	t.Run("bridged_path", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		gin.SetMode(gin.TestMode)
		recorder := &httpUpstreamRecorder{}
		httpUpstream := newAuthorizingHTTPUpstreamWithMode(recorder, modeFn(config.CanonicalWalletModeEnforce))
		cfg := &config.Config{}
		cfg.Security.URLAllowlist.Enabled = false
		cfg.Gateway.MaxLineSize = defaultMaxLineSize
		svc := &OpenAIGatewayService{
			cfg:              cfg,
			httpUpstream:     httpUpstream,
			cache:            &stubGatewayCache{},
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
			toolCorrector:    NewCodexToolCorrector(),
		}
		account := &Account{
			ID: 81, Name: "grok-refused", Platform: PlatformGrok, Type: AccountTypeOAuth,
			Concurrency: 1, Status: StatusActive,
			Credentials: map[string]any{"base_url": xai.DefaultCLIBaseURL},
		}
		hooks := &OpenAIWSIngressHooks{
			AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
				return nil, newRefusal()
			},
		}
		serverErrCh := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
			if err != nil {
				serverErrCh <- err
				return
			}
			defer func() { _ = conn.CloseNow() }()
			readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			_, firstMessage, readErr := conn.Read(readCtx)
			cancel()
			if readErr != nil {
				serverErrCh <- readErr
				return
			}
			rec := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(rec)
			req := r.Clone(r.Context())
			req.Header = req.Header.Clone()
			ginCtx.Request = req
			serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
		}))
		defer server.Close()

		dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
		clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		cancelDial()
		require.NoError(t, err)
		defer func() { _ = clientConn.CloseNow() }()

		writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		require.NoError(t, clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"grok","stream":true,"input":"hi"}`)))
		cancel()

		var proxyErr error
		select {
		case proxyErr = <-serverErrCh:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for the refused bridged turn")
		}
		require.Error(t, proxyErr)
		require.False(t, isOpenAIWSIngressTurnRetryable(proxyErr))
		var closeErr *OpenAIWSClientCloseError
		require.True(t, errors.As(proxyErr, &closeErr))
		require.Equal(t, coderws.StatusCode(AuthorizationRefusedWSCloseStatus), closeErr.StatusCode())
		require.Empty(t, recorder.requests, "the bridge never reached the upstream")
		_ = clientConn.Close(coderws.StatusNormalClosure, "done")
	})
}

func TestPrewarmWriteIsNonBillableInEnforce(t *testing.T) {
	resetAuthorizationMetricsForTest()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.Gateway.OpenAIWS.PrewarmGenerateEnabled = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3

	captureConn := &openAIWSCaptureConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_prewarm","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
	}}
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	pool.setClientDialerForTest(newAuthorizingOpenAIWSClientDialerWithMode(&openAIWSCaptureDialer{conn: captureConn}, modeFn(config.CanonicalWalletModeEnforce)))

	svc := &OpenAIGatewayService{cfg: cfg}
	account := &Account{ID: 91, Name: "prewarm", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive}

	lease, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{
		Account: account,
		WSURL:   "wss://example.com/v1/responses",
	})
	require.NoError(t, err)
	defer lease.Release()

	h, err := newAuthorizationHandle("enforce")
	require.NoError(t, err)
	ctx := WithAuthorizationHandle(context.Background(), h)
	decision := OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2}
	err = svc.performOpenAIWSGeneratePrewarm(ctx, lease, decision,
		map[string]any{"type": "response.create", "model": "gpt-5.1", "stream": false},
		"", map[string]any{}, account, nil, 0)
	require.NoError(t, err)

	require.Len(t, captureConn.writes, 1)
	require.Equal(t, false, captureConn.writes[0]["generate"], "the prewarm is the generate:false warm-up")
	require.Empty(t, h.Writes(), "the prewarm mints NO token")
	require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesNonBillable)

	// A following request write under the same handle mints the token.
	require.NoError(t, lease.WriteJSONWithContextTimeout(ctx, json.RawMessage(`{"type":"response.create","model":"gpt-5.1","stream":false}`), time.Second))
	require.Len(t, h.Writes(), 1)
}

func TestForwardOpenAIWSV2IsAuthorizedByTheRequestHandle(t *testing.T) {
	resetAuthorizationMetricsForTest()
	gin.SetMode(gin.TestMode)

	newFixture := func(t *testing.T) (*OpenAIGatewayService, *openAIWSCaptureConn, *Account) {
		cfg := &config.Config{}
		cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
		cfg.Security.URLAllowlist.Enabled = false
		cfg.Security.URLAllowlist.AllowInsecureHTTP = true
		cfg.Gateway.OpenAIWS.Enabled = true
		cfg.Gateway.OpenAIWS.OAuthEnabled = true
		cfg.Gateway.OpenAIWS.APIKeyEnabled = true
		cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
		cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
		cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
		cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
		cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
		cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
		cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
		captureConn := &openAIWSCaptureConn{events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_fwd_ws","model":"gpt-5.1","usage":{"input_tokens":5,"output_tokens":6}}}`),
		}}
		pool := newOpenAIWSConnPool(cfg)
		t.Cleanup(pool.Close)
		pool.setClientDialerForTest(newAuthorizingOpenAIWSClientDialerWithMode(&openAIWSCaptureDialer{conn: captureConn}, modeFn(config.CanonicalWalletModeEnforce)))
		svc := &OpenAIGatewayService{
			cfg:              cfg,
			httpUpstream:     &httpUpstreamRecorder{},
			cache:            &stubGatewayCache{},
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
			toolCorrector:    NewCodexToolCorrector(),
			openaiWSPool:     pool,
		}
		account := &Account{
			ID: 101, Name: "fwd-ws", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Concurrency: 1, Status: StatusActive,
			Credentials: map[string]any{"api_key": "sk-test"},
			Extra:       map[string]any{"responses_websockets_v2_enabled": true},
		}
		return svc, captureConn, account
	}

	t.Run("handle_on_ctx_authorizes_the_request_write", func(t *testing.T) {
		svc, captureConn, account := newFixture(t)
		h, err := newAuthorizationHandle("enforce")
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
		c.Request.Header.Set("User-Agent", "custom-client/1.0")

		result, fwdErr := svc.Forward(WithAuthorizationHandle(context.Background(), h), c, account, []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`))
		require.NoError(t, fwdErr)
		require.NotNil(t, result)
		require.Len(t, h.Writes(), 1, "the request write is authorized by the request handle")
		require.Len(t, captureConn.writes, 1)
	})

	t.Run("no_handle_on_ctx_is_refused", func(t *testing.T) {
		svc, captureConn, account := newFixture(t)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
		c.Request.Header.Set("User-Agent", "custom-client/1.0")

		_, fwdErr := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`))
		require.Error(t, fwdErr)
		require.True(t, errors.Is(fwdErr, ErrAuthorizationRefused))
		require.Empty(t, captureConn.writes, "refused before any frame was written")
	})
}

func TestIngressDisabledModeIsByteForByteUnchanged(t *testing.T) {
	resetAuthorizationMetricsForTest()
	runSession := func(t *testing.T, decorated bool) ([]map[string]any, []context.Context) {
		gin.SetMode(gin.TestMode)
		cfg := &config.Config{}
		cfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
		cfg.Security.URLAllowlist.Enabled = false
		cfg.Security.URLAllowlist.AllowInsecureHTTP = true
		cfg.Gateway.OpenAIWS.Enabled = true
		cfg.Gateway.OpenAIWS.OAuthEnabled = true
		cfg.Gateway.OpenAIWS.APIKeyEnabled = true
		cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
		cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
		cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
		cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
		cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
		cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
		cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
		cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

		captureConn := &openAIWSCaptureConn{events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_d1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_d2","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":2}}}`),
		}}
		recording := &ctxRecordingWSConn{openAIWSCaptureConn: captureConn}
		var pool *openAIWSConnPool
		if decorated {
			pool = newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(newAuthorizingOpenAIWSClientDialerWithMode(&singleConnFakeDialer{conn: recording}, modeFn(config.CanonicalWalletModeDisabled)))
		} else {
			pool = newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&singleConnFakeDialer{conn: recording})
		}
		defer pool.Close()
		svc := &OpenAIGatewayService{
			cfg:              cfg,
			httpUpstream:     &httpUpstreamRecorder{},
			cache:            &stubGatewayCache{},
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
			toolCorrector:    NewCodexToolCorrector(),
			openaiWSPool:     pool,
		}
		account := &Account{
			ID: 111, Name: "disabled-mode", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Status: StatusActive, Schedulable: true, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-test"},
			Extra:       map[string]any{"responses_websockets_v2_enabled": true},
		}
		authorizeCalled := make(chan int, 8)
		hooks := &OpenAIWSIngressHooks{
			AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
				authorizeCalled <- turn
				// Authorize does mint a handle in disabled mode (canonical_wallet_authorizer.go:54-59);
				// returning nil here simulates a minimal hook, while frame-equality and zero-counter
				// assertions below are the substantive proof, and the decorator's disabled short-circuit
				// is covered by TestAuthorizingWSClassification.
				return nil, nil
			},
		}
		serverErrCh := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
			if err != nil {
				serverErrCh <- err
				return
			}
			defer func() { _ = conn.CloseNow() }()
			readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			_, firstMessage, readErr := conn.Read(readCtx)
			cancel()
			if readErr != nil {
				serverErrCh <- readErr
				return
			}
			rec := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(rec)
			req := r.Clone(r.Context())
			req.Header = req.Header.Clone()
			ginCtx.Request = req
			serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
		}))
		defer server.Close()

		dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
		clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		cancelDial()
		require.NoError(t, err)
		defer func() { _ = clientConn.CloseNow() }()

		write := func(payload string) {
			writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, clientConn.Write(writeCtx, coderws.MessageText, []byte(payload)))
		}
		readCompleted := func() {
			readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, event, readErr := clientConn.Read(readCtx)
			cancel()
			require.NoError(t, readErr)
			require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
		}

		write(`{"type":"response.create","model":"gpt-5.1","stream":false}`)
		<-authorizeCalled
		readCompleted()
		write(`{"type":"response.create","model":"gpt-5.1","stream":false}`)
		<-authorizeCalled
		readCompleted()

		_ = clientConn.Close(coderws.StatusNormalClosure, "done")
		select {
		case serverErr := <-serverErrCh:
			require.NoError(t, serverErr)
		case <-time.After(5 * time.Second):
			t.Fatal("disabled-mode session did not finish")
		}

		return captureConn.writes, recording.recordedWriteCtxs()
	}

	decoratedWrites, decoratedCtxs := runSession(t, true)
	bareWrites, _ := runSession(t, false)

	require.Equal(t, len(bareWrites), len(decoratedWrites))
	for i := range bareWrites {
		bareJSON, err := json.Marshal(bareWrites[i])
		require.NoError(t, err)
		decoratedJSON, err := json.Marshal(decoratedWrites[i])
		require.NoError(t, err)
		require.JSONEq(t, string(bareJSON), string(decoratedJSON), "frames identical to a run without the decorator")
	}
	for _, ctx := range decoratedCtxs {
		require.Nil(t, AuthorizationHandleFromContext(ctx), "the ingress's own ctx carries no handle")
		_, marked := NonBillableUpstreamFromContext(ctx)
		require.False(t, marked, "the ingress's own ctx carries no non-billable mark")
	}
	snap := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(0), snap.WritesAuthorized)
	require.Equal(t, int64(0), snap.WritesNonBillable)
	require.Equal(t, int64(0), snap.WritesUnmarked)
	require.Equal(t, int64(0), snap.WritesRefused)
}
