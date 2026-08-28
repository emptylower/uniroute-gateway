//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
)

// fakeWSConn records writes; it implements every method the real coder conn has
// (openai_ws_client.go:275-347) so the proxy tests can assert forwarding.
type fakeWSConn struct {
	writes     []fakeWSWrite
	writeErr   error
	idlePing   bool
	closed     bool
	pinged     int
	readFrames [][]byte
}

type fakeWSWrite struct {
	ctx     context.Context
	msgType coderws.MessageType
	payload []byte
	json    any
}

func (c *fakeWSConn) WriteJSON(ctx context.Context, v any) error {
	c.writes = append(c.writes, fakeWSWrite{ctx: ctx, json: v})
	return c.writeErr
}
func (c *fakeWSConn) ReadMessage(ctx context.Context) ([]byte, error) {
	if len(c.readFrames) == 0 {
		return nil, errors.New("eof")
	}
	f := c.readFrames[0]
	c.readFrames = c.readFrames[1:]
	return f, nil
}
func (c *fakeWSConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	b, err := c.ReadMessage(ctx)
	return coderws.MessageText, b, err
}
func (c *fakeWSConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	c.writes = append(c.writes, fakeWSWrite{ctx: ctx, msgType: msgType, payload: payload})
	return c.writeErr
}
func (c *fakeWSConn) Ping(ctx context.Context) error      { c.pinged++; return nil }
func (c *fakeWSConn) Close() error                        { c.closed = true; return nil }
func (c *fakeWSConn) SupportsIdlePingWithoutReader() bool { return c.idlePing }

// bareWSConn has neither ReadFrame/WriteFrame nor SupportsIdlePingWithoutReader.
type bareWSConn struct{ inner *fakeWSConn }

func (c *bareWSConn) WriteJSON(ctx context.Context, v any) error { return c.inner.WriteJSON(ctx, v) }
func (c *bareWSConn) ReadMessage(ctx context.Context) ([]byte, error) {
	return c.inner.ReadMessage(ctx)
}
func (c *bareWSConn) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }
func (c *bareWSConn) Close() error                   { return c.inner.Close() }

type fakeWSDialer struct {
	conn    openAIWSClientConn
	metrics OpenAIWSTransportMetricsSnapshot
	dials   int
}

func (d *fakeWSDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	d.dials++
	return d.conn, http.StatusSwitchingProtocols, http.Header{"X-Request-Id": {"r1"}}, nil
}
func (d *fakeWSDialer) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot { return d.metrics }

// A dialer WITHOUT SnapshotTransportMetrics — the wrapper must not invent one.
type bareWSDialer struct{ conn openAIWSClientConn }

func (d *bareWSDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	return d.conn, http.StatusSwitchingProtocols, nil, nil
}

func dialWrapped(t *testing.T, mode string, inner *fakeWSConn) (*authorizingOpenAIWSClientConn, *fakeWSDialer) {
	t.Helper()
	d := &fakeWSDialer{conn: inner, metrics: OpenAIWSTransportMetricsSnapshot{ProxyClientCacheHits: 7}}
	wrapped := newAuthorizingOpenAIWSClientDialerWithMode(d, modeFn(mode))
	conn, status, hdr, err := wrapped.Dial(context.Background(), "wss://x", nil, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, status)
	require.Equal(t, "r1", hdr.Get("X-Request-Id"))
	ac, ok := conn.(*authorizingOpenAIWSClientConn)
	require.True(t, ok)
	return ac, d
}

func TestAuthorizingWSFullFidelityProxies(t *testing.T) {
	inner := &fakeWSConn{idlePing: true}
	ac, d := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)

	// The four optional interfaces the consumers assert (spec §2.0):
	var _ openaiwsv2.FrameConn = ac                                 // adapter.go:871
	var _ liveFrameConn = ac                                        // openai_live.go:556
	capable, ok := openAIWSClientConn(ac).(openAIWSIdlePingCapable) // openai_ws_pool.go:457
	require.True(t, ok)
	require.True(t, capable.SupportsIdlePingWithoutReader(), "forwards the inner value")
	inner.idlePing = false
	require.False(t, capable.SupportsIdlePingWithoutReader())
	dialerWithMetrics, ok := openAIWSClientDialer(newAuthorizingOpenAIWSClientDialerWithMode(d, modeFn("enforce"))).(openAIWSTransportMetricsDialer) // openai_ws_pool.go:648
	require.True(t, ok)
	require.Equal(t, int64(7), dialerWithMetrics.SnapshotTransportMetrics().ProxyClientCacheHits)

	// Forwarding of every method:
	require.NoError(t, ac.Ping(context.Background()))
	require.Equal(t, 1, inner.pinged)
	inner.readFrames = [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}
	mt, b, err := ac.ReadFrame(context.Background())
	require.NoError(t, err)
	require.Equal(t, coderws.MessageText, mt)
	require.JSONEq(t, `{"a":1}`, string(b))
	msgBytes, err := ac.ReadMessage(context.Background())
	require.NoError(t, err)
	require.JSONEq(t, `{"b":2}`, string(msgBytes))
	require.NoError(t, ac.Close())
	require.True(t, inner.closed)

	cfgDialer := newAuthorizingOpenAIWSClientDialer(d, &config.Config{})
	require.NotNil(t, cfgDialer)
	_, _, _, _ = cfgDialer.Dial(context.Background(), "ws://127.0.0.1", nil, "")
	nilCfgDialer := newAuthorizingOpenAIWSClientDialer(d, nil)
	require.NotNil(t, nilCfgDialer)
	_, _, _, _ = nilCfgDialer.Dial(context.Background(), "ws://127.0.0.1", nil, "")
}

func TestAuthorizingWSIdlePingDoesNotFailOpenOnAMiss(t *testing.T) {
	// A conn without SupportsIdlePingWithoutReader: the wrapper still implements the
	// interface (so the pool finds it) and reports FALSE — the pool's `!ok || …` at
	// openai_ws_pool.go:457-460 would otherwise flip to pinging idle sockets.
	wrapped := &authorizingOpenAIWSClientConn{inner: &bareWSConn{inner: &fakeWSConn{}}, mode: modeFn("shadow")}
	require.False(t, wrapped.SupportsIdlePingWithoutReader())
	_, _, err := wrapped.ReadFrame(context.Background())
	require.ErrorIs(t, err, errOpenAIWSConnClosed, "a conn without ReadFrame cannot be a FrameConn")
}

func TestAuthorizingWSDialerWithoutMetricsForwardsZero(t *testing.T) {
	wrapped := newAuthorizingOpenAIWSClientDialerWithMode(&bareWSDialer{conn: &fakeWSConn{}}, modeFn("shadow"))
	require.Equal(t, OpenAIWSTransportMetricsSnapshot{}, wrapped.SnapshotTransportMetrics())
}

func TestAuthorizingWSClassification(t *testing.T) {
	t.Run("disabled is a pass-through and never touches the ctx", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeDisabled, inner)
		ctx := context.Background()
		require.NoError(t, ac.WriteJSON(ctx, map[string]any{"type": "response.create"}))
		require.True(t, ctx == inner.writes[0].ctx)
		require.Equal(t, int64(0), AuthorizationMetricsSnapshot().WritesUnmarked)
	})
	t.Run("enforce refuses an unmarked frame before it is written", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		err := ac.WriteFrame(context.Background(), coderws.MessageText, []byte(`{"type":"response.create"}`))
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
		require.Empty(t, inner.writes)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesRefused)
	})
	t.Run("shadow admits and counts an unmarked frame", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeShadow, inner)
		require.NoError(t, ac.WriteJSON(context.Background(), map[string]any{}))
		require.Len(t, inner.writes, 1)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesUnmarked)
	})
	t.Run("a frame mark wins over an armed handle", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		h, _ := newAuthorizationHandle("enforce")
		ac.ArmAuthorization(h)
		ctx := WithNonBillableUpstream(context.Background(), NonBillableInterTurnFrame)
		require.NoError(t, ac.WriteFrame(ctx, coderws.MessageText, []byte(`{"type":"input_text"}`)))
		require.Empty(t, h.Writes(), "a marked frame mints no token")
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesNonBillable)
	})
	t.Run("an armed handle authorizes and each write mints a distinct token", func(t *testing.T) {
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		h, _ := newAuthorizationHandle("enforce")
		ac.ArmAuthorization(h)
		require.NoError(t, ac.WriteJSON(context.Background(), map[string]any{"type": "response.create"}))
		require.NoError(t, ac.WriteFrame(context.Background(), coderws.MessageText, []byte(`{"type":"response.create"}`)))
		w := h.Writes()
		require.Len(t, w, 2)
		require.NotEqual(t, w[0].Token, w[1].Token)
		require.Equal(t, AuthorizationOutcomeResult, w[0].Outcome)
		require.Equal(t, w[1].Token, h.SettledToken())
	})
	t.Run("a ctx handle wins over the armed handle", func(t *testing.T) {
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		armed, _ := newAuthorizationHandle("enforce")
		ctxH, _ := newAuthorizationHandle("enforce")
		ac.ArmAuthorization(armed)
		require.NoError(t, ac.WriteJSON(WithAuthorizationHandle(context.Background(), ctxH), map[string]any{}))
		require.Len(t, ctxH.Writes(), 1)
		require.Empty(t, armed.Writes())
	})
	t.Run("a write error is classified indeterminate", func(t *testing.T) {
		inner := &fakeWSConn{writeErr: errors.New("broken pipe")}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeShadow, inner)
		h, _ := newAuthorizationHandle("shadow")
		ac.ArmAuthorization(h)
		require.Error(t, ac.WriteFrame(context.Background(), coderws.MessageText, []byte(`{}`)))
		require.Equal(t, AuthorizationOutcomeIndeterminate, h.Writes()[0].Outcome)
	})
	t.Run("disarm clears the armed handle", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		h, _ := newAuthorizationHandle("enforce")
		ac.ArmAuthorization(h)
		ac.DisarmAuthorization()
		require.Nil(t, ac.ArmedAuthorization())
		err := ac.WriteJSON(context.Background(), map[string]any{})
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
	})
	t.Run("Ping and Close are never gated", func(t *testing.T) {
		inner := &fakeWSConn{}
		ac, _ := dialWrapped(t, config.CanonicalWalletModeEnforce, inner)
		require.NoError(t, ac.Ping(context.Background()))
		require.NoError(t, ac.Close())
	})
}
