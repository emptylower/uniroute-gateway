package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/tidwall/gjson"

	coderws "github.com/coder/websocket"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
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

	outcomeMu        sync.Mutex
	pendingWrites    int
	outcomesDone     chan struct{}
	evidenceMu       sync.Mutex
	responseOwners   map[string]string
	terminalEvidence map[string]bool
	readerStarted    map[string]bool
	armMu            sync.Mutex
	armChanged       chan struct{}
	journalRequired  atomic.Bool
	closed           atomic.Bool
}

var (
	_ openAIWSClientConn      = (*authorizingOpenAIWSClientConn)(nil)
	_ openAIWSIdlePingCapable = (*authorizingOpenAIWSClientConn)(nil)
	_ authorizationArmable    = (*authorizingOpenAIWSClientConn)(nil)
)

func (c *authorizingOpenAIWSClientConn) ArmAuthorization(h *AuthorizationHandle) {
	c.armMu.Lock()
	c.armed.Store(h)
	if h != nil && h.requiresReaderJournal {
		c.journalRequired.Store(true)
	}
	if c.armChanged != nil {
		close(c.armChanged)
	}
	c.armChanged = make(chan struct{})
	c.armMu.Unlock()
}
func (c *authorizingOpenAIWSClientConn) DisarmAuthorization() {
	c.armMu.Lock()
	old := c.armed.Swap(nil)
	if c.armChanged != nil {
		close(c.armChanged)
	}
	c.armChanged = make(chan struct{})
	c.armMu.Unlock()
	old.completeWrite()
}
func (c *authorizingOpenAIWSClientConn) ArmedAuthorization() *AuthorizationHandle {
	return c.armed.Load()
}

func (c *authorizingOpenAIWSClientConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	return c.writePayload(ctx, payload, err == nil, func() error { return c.inner.WriteJSON(ctx, value) })
}

func (c *authorizingOpenAIWSClientConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	fc, ok := c.inner.(interface {
		WriteFrame(context.Context, coderws.MessageType, []byte) error
	})
	if !ok {
		return errOpenAIWSConnClosed
	}
	return c.writePayload(ctx, payload, true, func() error { return fc.WriteFrame(ctx, msgType, payload) })
}

func (c *authorizingOpenAIWSClientConn) write(ctx context.Context, send func() error) error {
	return c.writePayload(ctx, nil, false, send)
}

func (c *authorizingOpenAIWSClientConn) writePayload(ctx context.Context, payload []byte, payloadKnown bool, send func() error) error {
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
	handle.captureWalletReaderNormalization(ctx, payload, payloadKnown)
	token := handle.MintWriteToken()
	if err := handle.prepareWrite(ctx, token); err != nil {
		return err
	}
	c.armMu.Lock()
	if c.armChanged != nil {
		close(c.armChanged)
	}
	c.armChanged = make(chan struct{})
	c.armMu.Unlock()
	// The peer can answer before send returns. Keep relay reads behind the write's
	// recorded outcome, including its hold callback, without locking network I/O.
	c.outcomeMu.Lock()
	if c.pendingWrites == 0 {
		c.outcomesDone = make(chan struct{})
	}
	c.pendingWrites++
	c.outcomeMu.Unlock()
	defer func() {
		c.outcomeMu.Lock()
		c.pendingWrites--
		if c.pendingWrites == 0 {
			close(c.outcomesDone)
		}
		c.outcomeMu.Unlock()
	}()
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
	h, unlock, guardErr := c.guardReader(ctx)
	if guardErr != nil {
		return nil, guardErr
	}
	defer unlock()
	payload, err := c.inner.ReadMessage(ctx)
	if err != nil {
		c.armed.Load().completeWrite()
		return payload, err
	}
	if err := c.waitWriteOutcomes(ctx); err != nil {
		return nil, err
	}
	if err := c.observeOwnedFrameEvidence(payload, h); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *authorizingOpenAIWSClientConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	fc, ok := c.inner.(interface {
		ReadFrame(context.Context) (coderws.MessageType, []byte, error)
	})
	if !ok {
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	}
	h, unlock, guardErr := c.guardReader(ctx)
	if guardErr != nil {
		return coderws.MessageText, nil, guardErr
	}
	defer unlock()
	msgType, payload, err := fc.ReadFrame(ctx)
	if err != nil {
		c.armed.Load().completeWrite()
		return msgType, payload, err
	}
	if err := c.waitWriteOutcomes(ctx); err != nil {
		return msgType, nil, err
	}
	if err := c.observeOwnedFrameEvidence(payload, h); err != nil {
		return msgType, nil, err
	}
	return msgType, payload, nil
}

func (c *authorizingOpenAIWSClientConn) guardReader(ctx context.Context) (*AuthorizationHandle, func(), error) {
	for {
		if c.closed.Load() {
			return nil, func() {}, errOpenAIWSConnClosed
		}
		c.armMu.Lock()
		h := c.armed.Load()
		if c.armChanged == nil {
			c.armChanged = make(chan struct{})
		}
		changed := c.armChanged
		c.armMu.Unlock()
		var journal *walletReaderJournal
		var required bool
		if h != nil {
			h.mu.Lock()
			journal = h.readerJournal
			required = h.requiresReaderJournal
			h.mu.Unlock()
		} else {
			required = c.journalRequired.Load()
		}
		if journal == nil {
			if !required {
				return h, func() {}, nil
			}
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, func() {}, ctx.Err()
			}
		}
		if err := journal.beginRead(); err != nil {
			if !errors.Is(err, errWalletReaderFenced) {
				return nil, func() {}, err
			}
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, func() {}, ctx.Err()
			}
		}
		return h, journal.release, nil
	}
}

func (c *authorizingOpenAIWSClientConn) observeOwnedFrameEvidence(payload []byte, h *AuthorizationHandle) error {
	c.evidenceMu.Lock()
	defer c.evidenceMu.Unlock()
	if h == nil {
		h = c.armed.Load()
	}
	if h == nil || h.readerObserved == nil {
		return nil
	}
	owner := h.ID + ":" + h.LastWriteToken()
	if h.LastWriteToken() == "" {
		return nil
	}
	if c.responseOwners == nil {
		c.responseOwners = map[string]string{}
		c.terminalEvidence = map[string]bool{}
		c.readerStarted = map[string]bool{}
	}
	root := gjson.ParseBytes(payload)
	responseID := root.Get("response.id").String()
	if responseID == "" {
		responseID = root.Get("response_id").String()
	}
	typeName := root.Get("type").String()
	if responseID == "" && typeName == "response.created" {
		responseID = root.Get("id").String()
	}
	if responseID == "" && (typeName == "response.output_item.done" || typeName == "image_generation.completed") {
		// An item ID cannot identify its response/authorization. A delayed
		// unbound image event from an earlier turn must not be checkpointed
		// under the current owner. Its owning terminal response can provide
		// selected output identities with the authoritative response ID.
		return nil
	}
	if responseID != "" {
		if prior, exists := c.responseOwners[responseID]; exists && prior != owner {
			return nil
		}
		c.responseOwners[responseID] = owner
	}
	terminal := typeName == "response.completed" || typeName == "response.done" || typeName == "response.failed" || typeName == "response.cancelled" || typeName == "response.incomplete" || typeName == "error"
	key := owner + ":" + responseID
	if c.terminalEvidence[key] {
		return nil
	}
	if !c.readerStarted[owner] {
		if h.readerStarted != nil {
			if err := h.readerStarted(); err != nil {
				return err
			}
		}
		c.readerStarted[owner] = true
	}
	h.mu.Lock()
	var evidence WalletReaderEvidence
	if h.readerEvidence != nil {
		evidence = *h.readerEvidence
	}
	h.mu.Unlock()
	evidence.Source = "llm_ws_usage"
	if h.readerJournal != nil {
		if err := h.readerJournal.checkpointLocked(payload, "llm_ws_usage"); err != nil {
			h.recordReaderEvidence(evidence, err)
			return err
		}
	}
	observeWalletWSUsage(payload, &evidence)
	if err := observeWalletReaderCounts(payload, &evidence, h.readerNormalization); err != nil {
		h.recordReaderEvidence(evidence, err)
		return err
	}
	if !openaiwsv2.ParseUsage(payload).Present && terminal {
		evidence.Present = false
		evidence.Valid = false
	}
	if responseID != "" {
		evidence.ResponseID = responseID
	}
	if terminal {
		evidence.Complete = true
		status := 500
		if eventType := gjson.GetBytes(payload, "type").String(); eventType == "response.completed" || eventType == "response.done" {
			status = 200
		}
		finishWalletReaderCounts(h.readerNormalization, &evidence, status, true)
	}
	if h.readerJournal != nil {
		if err := h.readerJournal.observeLocked(evidence, false); err != nil {
			h.recordReaderEvidence(evidence, err)
			return err
		}
	}
	if err := h.readerObserved(evidence); err != nil {
		if h.readerJournal != nil {
			_ = h.readerJournal.observeLocked(evidence, true)
		}
		h.recordReaderEvidence(evidence, err)
		return err
	}
	h.recordReaderEvidence(evidence, nil)
	if terminal {
		c.terminalEvidence[key] = true
	}
	return nil
}

func (c *authorizingOpenAIWSClientConn) waitWriteOutcomes(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.outcomeMu.Lock()
		pending, done := c.pendingWrites, c.outcomesDone
		c.outcomeMu.Unlock()
		if pending == 0 {
			return nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *authorizingOpenAIWSClientConn) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *authorizingOpenAIWSClientConn) Close() error {
	c.closed.Store(true)
	c.DisarmAuthorization()
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
