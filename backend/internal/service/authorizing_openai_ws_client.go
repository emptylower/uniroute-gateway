package service

import (
	"context"
	"net/http"
	"sync/atomic"

	coderws "github.com/coder/websocket"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// authorizationArmable is what the pool lease and the passthrough session use to
// own a turn's handle on the conn (spec §2.0: handle state owned by the lease —
// cleared in Release() — or by the passthrough session).
type authorizationArmable interface {
	ArmAuthorization(h *AuthorizationHandle)
	DisarmAuthorization()
	ArmedAuthorization() *AuthorizationHandle
}

// authorizingOpenAIWSClientDialer decorates the OpenAI WebSocket dialer at its two
// construction sites. It forwards SnapshotTransportMetrics so openai_ws_pool.go:648
// keeps seeing the real dialer's metrics.
type authorizingOpenAIWSClientDialer struct {
	inner openAIWSClientDialer
	mode  func() string
}

func newAuthorizingOpenAIWSClientDialer(inner openAIWSClientDialer, cfg *config.Config) openAIWSClientDialer {
	return newAuthorizingOpenAIWSClientDialerWithMode(inner, func() string {
		if cfg == nil {
			return config.CanonicalWalletModeDisabled
		}
		return cfg.CanonicalWallet.Mode
	})
}

func newAuthorizingOpenAIWSClientDialerWithMode(inner openAIWSClientDialer, mode func() string) *authorizingOpenAIWSClientDialer {
	return &authorizingOpenAIWSClientDialer{inner: inner, mode: mode}
}

func (d *authorizingOpenAIWSClientDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	conn, status, hdr, err := d.inner.Dial(ctx, wsURL, headers, proxyURL)
	if err != nil || conn == nil {
		return conn, status, hdr, err
	}
	return &authorizingOpenAIWSClientConn{inner: conn, mode: d.mode}, status, hdr, nil
}

// SnapshotTransportMetrics forwards; a dialer without it yields the zero value,
// exactly what the pool's own miss produces today (openai_ws_pool.go:644-650).
func (d *authorizingOpenAIWSClientDialer) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot {
	if m, ok := d.inner.(openAIWSTransportMetricsDialer); ok {
		return m.SnapshotTransportMetrics()
	}
	return OpenAIWSTransportMetricsSnapshot{}
}

// authorizingOpenAIWSClientConn is the authorization boundary on the WS port. It is
// a full-fidelity proxy: openAIWSClientConn, ReadFrame/WriteFrame (openaiwsv2.FrameConn
// and liveFrameConn), SupportsIdlePingWithoutReader (forwarded, never fail-open).
// Classification of a write, in order: an explicit non-billable frame mark on ctx;
// a handle on ctx; the handle armed on the conn; otherwise unmarked.
type authorizingOpenAIWSClientConn struct {
	inner openAIWSClientConn
	mode  func() string
	armed atomic.Pointer[AuthorizationHandle]
}

var (
	_ openAIWSClientConn      = (*authorizingOpenAIWSClientConn)(nil)
	_ openAIWSIdlePingCapable = (*authorizingOpenAIWSClientConn)(nil)
	_ authorizationArmable    = (*authorizingOpenAIWSClientConn)(nil)
)

func (c *authorizingOpenAIWSClientConn) ArmAuthorization(h *AuthorizationHandle) { c.armed.Store(h) }
func (c *authorizingOpenAIWSClientConn) DisarmAuthorization()                    { c.armed.Store(nil) }
func (c *authorizingOpenAIWSClientConn) ArmedAuthorization() *AuthorizationHandle {
	return c.armed.Load()
}

func (c *authorizingOpenAIWSClientConn) WriteJSON(ctx context.Context, value any) error {
	return c.write(ctx, func() error { return c.inner.WriteJSON(ctx, value) })
}

func (c *authorizingOpenAIWSClientConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	fc, ok := c.inner.(interface {
		WriteFrame(context.Context, coderws.MessageType, []byte) error
	})
	if !ok {
		return errOpenAIWSConnClosed
	}
	return c.write(ctx, func() error { return fc.WriteFrame(ctx, msgType, payload) })
}

func (c *authorizingOpenAIWSClientConn) write(ctx context.Context, send func() error) error {
	mode := c.mode()
	if mode == "" || mode == config.CanonicalWalletModeDisabled {
		return send()
	}
	if _, marked := NonBillableUpstreamFromContext(ctx); marked {
		authorizationMetrics.writesNonBillable.Add(1)
		return send()
	}
	handle := AuthorizationHandleFromContext(ctx)
	if handle == nil {
		handle = c.armed.Load()
	}
	if handle == nil {
		authorizationMetrics.writesUnmarked.Add(1)
		if mode == config.CanonicalWalletModeEnforce {
			authorizationMetrics.writesRefused.Add(1)
			return &AuthorizationRefusedError{Reason: AuthorizationRefusalUnmarkedWrite, Detail: "websocket frame"}
		}
		logger.LegacyPrintf("service.authorization", "shadow: unmarked websocket write admitted")
		return send()
	}
	if handle.Refusal != nil {
		authorizationMetrics.writesRefused.Add(1)
		return handle.Refusal
	}
	token := handle.MintWriteToken()
	authorizationMetrics.writesAuthorized.Add(1)
	err := send()
	if err == nil {
		authorizationMetrics.outcomeResult.Add(1)
		handle.RecordOutcome(token, AuthorizationOutcomeResult, nil)
		return nil
	}
	// A frame write has no "wrote headers" signal: a failed Write may have left bytes
	// on the wire. Conservative: indeterminate (3.4b retains the hold; the reaper
	// resolves it). Named residual: a write refused by a closed conn before any byte
	// is also classified indeterminate.
	authorizationMetrics.outcomeIndeterminate.Add(1)
	handle.RecordOutcome(token, AuthorizationOutcomeIndeterminate, err)
	return err
}

func (c *authorizingOpenAIWSClientConn) ReadMessage(ctx context.Context) ([]byte, error) {
	return c.inner.ReadMessage(ctx)
}

func (c *authorizingOpenAIWSClientConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	fc, ok := c.inner.(interface {
		ReadFrame(context.Context) (coderws.MessageType, []byte, error)
	})
	if !ok {
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	}
	return fc.ReadFrame(ctx)
}

func (c *authorizingOpenAIWSClientConn) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *authorizingOpenAIWSClientConn) Close() error {
	c.armed.Store(nil)
	return c.inner.Close()
}

// SupportsIdlePingWithoutReader forwards the inner conn's contract. On a miss it
// reports false — the real conn reports false (openai_ws_client.go:343) and the
// pool's assertion fails OPEN on a missing interface (openai_ws_pool.go:457-460).
func (c *authorizingOpenAIWSClientConn) SupportsIdlePingWithoutReader() bool {
	if capable, ok := c.inner.(openAIWSIdlePingCapable); ok {
		return capable.SupportsIdlePingWithoutReader()
	}
	return false
}
