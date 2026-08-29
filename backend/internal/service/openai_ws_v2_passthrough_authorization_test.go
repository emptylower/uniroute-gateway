//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type passthroughAuthHarness struct {
	server      *httptest.Server
	serverErrCh chan error
	clientConn  *coderws.Conn
	upstream    *stagedPassthroughConn
	authCalls   chan passthroughAuthCall
	handles     sync.Map // turn -> *AuthorizationHandle
}

type passthroughAuthCall struct {
	turn     int
	estimate EstimateInput
}

func newPassthroughAuthHarness(t *testing.T, mode string, authFn func(turn int, estimate EstimateInput) (*AuthorizationHandle, error), fastPolicySettings *OpenAIFastPolicySettings) *passthroughAuthHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := passthroughLifecycleConfig()
	cfg.CanonicalWallet.Mode = mode

	upstream := newStagedPassthroughConn()
	dialer := newAuthorizingOpenAIWSClientDialerWithMode(&stagedPassthroughDialer{conn: upstream}, modeFn(mode))

	repo := &openAIFastPolicyRepoStub{values: map[string]string{}}
	if fastPolicySettings != nil {
		raw, err := json.Marshal(fastPolicySettings)
		require.NoError(t, err)
		repo.values[SettingKeyOpenAIFastPolicySettings] = string(raw)
	}

	svc := &OpenAIGatewayService{
		cfg:                       cfg,
		httpUpstream:              &httpUpstreamRecorder{},
		cache:                     &stubGatewayCache{},
		openaiWSResolver:          NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:             NewCodexToolCorrector(),
		openaiWSPassthroughDialer: dialer,
		settingService:            NewSettingService(repo, cfg),
	}

	harness := &passthroughAuthHarness{
		upstream:    upstream,
		authCalls:   make(chan passthroughAuthCall, 16),
		serverErrCh: make(chan error, 1),
	}

	hooks := &OpenAIWSIngressHooks{
		AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
			harness.authCalls <- passthroughAuthCall{turn: turn, estimate: estimate}
			if authFn != nil {
				h, err := authFn(turn, estimate)
				if h != nil {
					harness.handles.Store(turn, h)
				}
				return h, err
			}
			h, err := newAuthorizationHandle(mode)
			if err == nil {
				harness.handles.Store(turn, h)
			}
			return h, err
		},
	}

	account := passthroughLifecycleAccount()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			harness.serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		msgType, firstMessage, err := ReadOpenAIWSClientMessage(
			r.Context(),
			conn,
			3*time.Second,
			coderws.StatusPolicyViolation,
			"missing first response.create message",
		)
		if err != nil {
			harness.serverErrCh <- err
			return
		}
		if msgType != coderws.MessageText {
			harness.serverErrCh <- errors.New("first message was not text")
			return
		}

		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		ginCtx.Request = req
		err = svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
		if err != nil {
			var closeErr *OpenAIWSClientCloseError
			if errors.As(err, &closeErr) {
				_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
			}
		}
		harness.serverErrCh <- err
	}))
	harness.server = server
	return harness
}

func (h *passthroughAuthHarness) dial(t *testing.T, firstMessage string) {
	t.Helper()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(h.server.URL, "http"), nil)
	require.NoError(t, err)
	h.clientConn = clientConn

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(firstMessage))
	require.NoError(t, err)
}

func (h *passthroughAuthHarness) waitCall(t *testing.T) passthroughAuthCall {
	t.Helper()
	select {
	case call := <-h.authCalls:
		return call
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for AuthorizeTurn call")
		return passthroughAuthCall{}
	}
}

func (h *passthroughAuthHarness) writeClient(t *testing.T, payload string) {
	t.Helper()
	writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, h.clientConn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func (h *passthroughAuthHarness) readClient(t *testing.T) ([]byte, error) {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, payload, err := h.clientConn.Read(readCtx)
	return payload, err
}

func (h *passthroughAuthHarness) waitUpstreamWrite(t *testing.T) []byte {
	t.Helper()
	select {
	case payload := <-h.upstream.writes:
		return payload
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for upstream write")
		return nil
	}
}

func (h *passthroughAuthHarness) closeClientAndWait(t *testing.T) {
	t.Helper()
	_ = h.clientConn.Close(coderws.StatusNormalClosure, "done")
	select {
	case serverErr := <-h.serverErrCh:
		if serverErr != nil && !errors.Is(serverErr, context.Canceled) {
			t.Logf("passthrough session ended with: %v", serverErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for passthrough session to end")
	}
}

func TestPassthroughSecondTurnWithPreviousResponseIDAuthorizesOnTheContinuationBranch(t *testing.T) {
	resetAuthorizationMetricsForTest()
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, nil)
	defer harness.server.Close()

	// Turn 1: response.create without previous_response_id
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.turn)
	require.Equal(t, ContinuationNone, call1.estimate.Continuation)
	require.Equal(t, 0, call1.estimate.PriorTurnInputTokens)
	require.Equal(t, 0, call1.estimate.PriorTurnOutputTokens)

	// Upstream receives turn 1 write
	upWrite1 := harness.waitUpstreamWrite(t)
	require.Equal(t, "response.create", gjson.GetBytes(upWrite1, "type").String())

	// Upstream responds and completes turn 1 with usage {input: 120, output: 30}
	harness.upstream.Send(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.1"}}`)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30,"total_tokens":150}}}`)

	ev1, err := harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(ev1, "type").String())
	ev2, err := harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(ev2, "type").String())

	// Inter-turn frame sent by client (e.g. session.update)
	harness.writeClient(t, `{"type":"session.update","session":{"instructions":"be brief"}}`)
	interTurnWrite := harness.waitUpstreamWrite(t)
	require.Equal(t, "session.update", gjson.GetBytes(interTurnWrite, "type").String())

	// Turn 2 with previous_response_id
	harness.writeClient(t, `{"type":"response.create","previous_response_id":"resp_1"}`)
	call2 := harness.waitCall(t)
	require.Equal(t, 2, call2.turn)
	require.Equal(t, ContinuationWarm, call2.estimate.Continuation)
	require.Equal(t, 120, call2.estimate.PriorTurnInputTokens)
	require.Equal(t, 30, call2.estimate.PriorTurnOutputTokens)

	upWrite2 := harness.waitUpstreamWrite(t)
	require.Equal(t, "response.create", gjson.GetBytes(upWrite2, "type").String())

	// Upstream completes turn 2
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.1","usage":{"input_tokens":200,"output_tokens":50,"total_tokens":250}}}`)
	ev3, err := harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(ev3, "type").String())

	harness.closeClientAndWait(t)

	// Handles assertions:
	val1, ok := harness.handles.Load(1)
	require.True(t, ok)
	h1 := val1.(*AuthorizationHandle)
	require.Len(t, h1.Writes(), 1)
	require.Equal(t, AuthorizationOutcomeResult, h1.Writes()[0].Outcome)

	val2, ok := harness.handles.Load(2)
	require.True(t, ok)
	h2 := val2.(*AuthorizationHandle)
	require.Len(t, h2.Writes(), 1)
	require.Equal(t, AuthorizationOutcomeResult, h2.Writes()[0].Outcome)
	require.NotEqual(t, h1.Writes()[0].Token, h2.Writes()[0].Token)

	metrics := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(2), metrics.WritesAuthorized)
	require.Equal(t, int64(1), metrics.WritesNonBillable, "inter-turn frame is classified non-billable")
	require.Equal(t, int64(0), metrics.WritesRefused)
}

// Test 47 (Phase 3.7a, redesign §13.1/§13.3) — the WebSocket form of the
// differential invariant across a warm previous_response_id continuation:
// per turn settled ≤ estimated, and turn 2's estimate carries turn 1's
// usage. The base test's harness with ONE change — the AuthorizeTurn hook
// calls a REAL authorizer (shadow mode: it always admits and mints the
// finite estimate) instead of the bare handle mint. The base script's
// bodies carry max_output_tokens:512 because the unit fixture's fallback
// prices set no model maxima — a token-less body would leave the estimate
// unbounded (shadow admits with EstimatedUnits 0) and the pin vacuous.
func TestPhase37WSContinuationInvariant(t *testing.T) {
	resetAuthorizationMetricsForTest()
	auth, snap, apiKey, _, _ := newAuthorizerFixture(t, config.CanonicalWalletModeShadow)

	type turnRecord struct {
		turn     int
		estimate EstimateInput
		h        *AuthorizationHandle
	}
	records := make(chan turnRecord, 8)
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, func(turn int, est EstimateInput) (*AuthorizationHandle, error) {
		h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: est, User: apiKey.User})
		if err == nil && h != nil {
			records <- turnRecord{turn: turn, estimate: est, h: h}
		}
		return h, err
	}, nil)
	defer harness.server.Close()

	// Turn 1: response.create without previous_response_id
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1","max_output_tokens":512}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.turn)
	require.Equal(t, ContinuationNone, call1.estimate.Continuation)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.1"}}`)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30,"total_tokens":150}}}`)
	ev, err := harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(ev, "type").String())
	ev, err = harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(ev, "type").String())

	// Inter-turn session.update
	harness.writeClient(t, `{"type":"session.update","session":{"instructions":"be brief"}}`)
	interTurnWrite := harness.waitUpstreamWrite(t)
	require.Equal(t, "session.update", gjson.GetBytes(interTurnWrite, "type").String())

	// Turn 2 with previous_response_id — its own body no smaller than
	// turn 1's (the monotonicity precondition).
	harness.writeClient(t, `{"type":"response.create","previous_response_id":"resp_1","max_output_tokens":512}`)
	call2 := harness.waitCall(t)
	require.Equal(t, 2, call2.turn)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.1","usage":{"input_tokens":200,"output_tokens":40,"total_tokens":240}}}`)
	_, err = harness.readClient(t)
	require.NoError(t, err)
	harness.closeClientAndWait(t)

	// Review note 4: the channel is drained, never closed — the harness
	// signals authCalls BEFORE authFn runs, so closeClientAndWait does not
	// provably happen-after every records send, and a late AuthorizeTurn
	// would panic on send-to-closed; a late send to this buffered,
	// unread-again channel is harmless. Reads are bounded by a deadline.
	var recs []turnRecord
	drainDeadline := time.Now().Add(5 * time.Second)
	for len(recs) < 2 && time.Now().Before(drainDeadline) {
		select {
		case r := <-records:
			recs = append(recs, r)
		case <-time.After(100 * time.Millisecond):
		}
	}
	require.Len(t, recs, 2)
	r1, r2 := recs[0], recs[1]
	require.Equal(t, 1, r1.turn)
	require.Equal(t, 2, r2.turn)

	// Turn 2's estimate is warm and carries turn 1's usage.
	require.Equal(t, ContinuationWarm, r2.estimate.Continuation)
	require.Equal(t, 120, r2.estimate.PriorTurnInputTokens)
	require.Equal(t, 30, r2.estimate.PriorTurnOutputTokens)

	// Per-turn differential invariant: settled_k ≤ h_k.EstimatedUnits, with
	// settled_k priced from the SAME snapshot the authorizer used. Loose
	// form: 30/40 output tokens against the 512 bound clears it by ~17× —
	// the tight case is test 46 leg 1 (integration tag, phase37_pins_test.go).
	settled := func(in, out int) int64 {
		cost, cerr := auth.snapshots.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: in, OutputTokens: out}})
		require.NoError(t, cerr)
		return settledUnitsForTest(t, auth.snapshots, snap, cost)
	}
	s1 := settled(120, 30)
	s2 := settled(200, 40)
	t.Logf("test 47: E1=%d settled1=%d E2=%d settled2=%d", r1.h.EstimatedUnits, s1, r2.h.EstimatedUnits, s2)
	require.Greater(t, r1.h.EstimatedUnits, int64(0), "the estimate is finite — a token-less body would make this pin vacuous")
	require.LessOrEqual(t, s1, r1.h.EstimatedUnits, "turn 1: settled ≤ estimated")
	require.LessOrEqual(t, s2, r2.h.EstimatedUnits, "turn 2: settled ≤ estimated")

	// Distinct handles; the warm accumulation is monotone.
	require.NotEqual(t, r1.h.ID, r2.h.ID)
	require.GreaterOrEqual(t, r2.h.EstimatedUnits, r1.h.EstimatedUnits, "turn 2's estimate ≥ turn 1's — the accumulation is monotone")
}

func TestPassthroughFirstFrameWithPreviousResponseIDIsCold(t *testing.T) {
	resetAuthorizationMetricsForTest()
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, nil)
	defer harness.server.Close()

	// A fresh connection whose FIRST frame carries previous_response_id -> ContinuationCold
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_prev"}`)
	call := harness.waitCall(t)
	require.Equal(t, 1, call.turn)
	require.Equal(t, ContinuationCold, call.estimate.Continuation)
	require.Equal(t, 0, call.estimate.PriorTurnInputTokens)
	require.Equal(t, 0, call.estimate.PriorTurnOutputTokens)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":50,"output_tokens":10}}}`)
	_, _ = harness.readClient(t)
	harness.closeClientAndWait(t)
}

func TestPassthroughStoreDisabledSessionIsNeverWarm(t *testing.T) {
	resetAuthorizationMetricsForTest()
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, nil)
	defer harness.server.Close()

	// Turn 1 with store: false
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1","store":false}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.turn)
	require.Equal(t, ContinuationNone, call1.estimate.Continuation)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30}}}`)
	_, _ = harness.readClient(t)

	// Turn 2 with store: false and previous_response_id -> Cold with zero prior tokens
	harness.writeClient(t, `{"type":"response.create","previous_response_id":"resp_1","store":false}`)
	call2 := harness.waitCall(t)
	require.Equal(t, 2, call2.turn)
	require.Equal(t, ContinuationCold, call2.estimate.Continuation)
	require.Equal(t, 0, call2.estimate.PriorTurnInputTokens)
	require.Equal(t, 0, call2.estimate.PriorTurnOutputTokens)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.1","usage":{"input_tokens":100,"output_tokens":20}}}`)
	_, _ = harness.readClient(t)
	harness.closeClientAndWait(t)
}

func TestPassthroughRefusedTurnClosesWith4402AndWritesNothing(t *testing.T) {
	resetAuthorizationMetricsForTest()
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
		return nil, NewOpenAIWSClientCloseError(
			coderws.StatusCode(AuthorizationRefusedWSCloseStatus),
			AuthorizationRefusedWSCloseReason+": "+string(AuthorizationRefusalBalanceShortfall),
			&AuthorizationRefusedError{Reason: AuthorizationRefusalBalanceShortfall, Detail: "insufficient balance"},
		)
	}, nil)
	defer harness.server.Close()

	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	call := harness.waitCall(t)
	require.Equal(t, 1, call.turn)

	// Client should read a close error with status 4402
	_, err := harness.readClient(t)
	var wsCloseErr coderws.CloseError
	require.ErrorAs(t, err, &wsCloseErr)
	require.Equal(t, coderws.StatusCode(AuthorizationRefusedWSCloseStatus), wsCloseErr.Code)
	require.Contains(t, wsCloseErr.Reason, AuthorizationRefusedWSCloseReason)

	// Upstream received zero writes
	select {
	case payload := <-harness.upstream.writes:
		t.Fatalf("upstream should have received zero writes, got: %s", string(payload))
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case serverErr := <-harness.serverErrCh:
		require.True(t, errors.Is(serverErr, ErrAuthorizationRefused))
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit after refusal")
	}
}

func TestPassthroughBlockedTurnIsNeverAuthorized(t *testing.T) {
	resetAuthorizationMetricsForTest()
	policySettings := &OpenAIFastPolicySettings{
		Rules: []OpenAIFastPolicyRule{{
			ServiceTier:    OpenAIFastTierPriority,
			Action:         BetaPolicyActionBlock,
			Scope:          BetaPolicyScopeAll,
			ErrorMessage:   "ws fast blocked by policy",
			ModelWhitelist: []string{"gpt-5.1"},
			FallbackAction: BetaPolicyActionPass,
		}},
	}
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, policySettings)
	defer harness.server.Close()

	// Turn 1 passes (no service_tier)
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	call1 := harness.waitCall(t)
	require.Equal(t, 1, call1.turn)

	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":50,"output_tokens":10}}}`)
	_, _ = harness.readClient(t)

	// Turn 2 is blocked by FastPolicy
	harness.writeClient(t, `{"type":"response.create","model":"gpt-5.1","service_tier":"priority"}`)

	// Client reads the blocked event and/or close
	ev, err := harness.readClient(t)
	if err == nil {
		require.Equal(t, "error", gjson.GetBytes(ev, "type").String())
		_, err = harness.readClient(t)
	}
	var wsCloseErr coderws.CloseError
	require.ErrorAs(t, err, &wsCloseErr)
	require.Equal(t, coderws.StatusPolicyViolation, wsCloseErr.Code)

	// AuthorizeTurn should NOT have been called for turn 2!
	select {
	case call2 := <-harness.authCalls:
		t.Fatalf("AuthorizeTurn should not be called on blocked turn, got turn %d", call2.turn)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case <-harness.serverErrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestPassthroughInterTurnFramesAreNeverRefusedInEnforce(t *testing.T) {
	resetAuthorizationMetricsForTest()
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, nil)
	defer harness.server.Close()

	// Turn 1
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	_ = harness.waitCall(t)
	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":50,"output_tokens":10}}}`)
	_, _ = harness.readClient(t)

	// Send multiple inter-turn frames
	harness.writeClient(t, `{"type":"session.update","session":{"modalities":["text"]}}`)
	w1 := harness.waitUpstreamWrite(t)
	require.Equal(t, "session.update", gjson.GetBytes(w1, "type").String())

	harness.writeClient(t, `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`)
	w2 := harness.waitUpstreamWrite(t)
	require.Equal(t, "conversation.item.create", gjson.GetBytes(w2, "type").String())

	// Turn 2
	harness.writeClient(t, `{"type":"response.create","previous_response_id":"resp_1"}`)
	_ = harness.waitCall(t)
	_ = harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.1","usage":{"input_tokens":80,"output_tokens":20}}}`)
	_, _ = harness.readClient(t)

	harness.closeClientAndWait(t)

	metrics := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(2), metrics.WritesAuthorized)
	require.Equal(t, int64(2), metrics.WritesNonBillable, "two inter-turn frames")
	require.Equal(t, int64(0), metrics.WritesRefused)
}
