//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWalletReaderEvidenceRequiresSelectedValidUsage(t *testing.T) {
	for _, tc := range []struct {
		name, raw                           string
		present, valid, malformed, positive bool
		input, output                       int
	}{
		{"missing", `{"response":{"id":"r1"}}`, false, false, false, false, 0, 0},
		{"null-usage-is-not-reported", `{"usage":null}`, false, false, false, false, 0, 0},
		{"responses-event-null-usage", `{"type":"response.created","response":{"id":"r1","usage":null}}`, false, false, false, false, 0, 0},
		{"null-top-level-falls-through-to-response-usage", `{"usage":null,"response":{"usage":{"input_tokens":8,"output_tokens":2}}}`, true, true, false, true, 8, 2},
		{"message-usage-null-is-not-reported", `{"type":"message_start","message":{"usage":null}}`, false, false, false, false, 0, 0},
		{"gemini-usage-metadata-null-is-not-reported", `{"usageMetadata":null}`, false, false, false, false, 0, 0},
		{"field-level-null-stays-malformed", `{"usage":{"input_tokens":null,"output_tokens":3}}`, true, false, true, true, 0, 0},
		{"explicit-zero", `{"usage":{"input_tokens":0,"output_tokens":0}}`, true, true, false, false, 0, 0},
		{"raw-not-billable-zero", `{"raw_tokens":0,"raw_credits":0,"source":"priced_usage"}`, false, false, false, false, 0, 0},
		{"provider-source-does-not-select", `{"usage":{"source":"trusted","credits":0}}`, true, false, true, false, 0, 0},
		{"invalid-string", `{"usage":{"input_tokens":"0","output_tokens":0}}`, true, false, true, false, 0, 0},
		{"invalid-negative", `{"usage":{"input_tokens":-1,"output_tokens":0}}`, true, false, true, false, 0, 0},
		{"invalid-fraction", `{"usage":{"input_tokens":0.1,"output_tokens":0}}`, true, false, true, true, 0, 0},
		{"positive-plus-invalid", `{"usage":{"input_tokens":11,"output_tokens":"bad"}}`, true, false, true, true, 0, 0},
		{"chat-cache-normalized", `{"usage":{"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":3}}}`, true, true, false, true, 7, 4},
		{"response-usage", `{"type":"response.completed","response":{"id":"r1","usage":{"input_tokens":8,"output_tokens":2}}}`, true, true, false, true, 8, 2},
		{"bedrock", `{"usage":{"inputTokens":3,"outputTokens":4}}`, true, true, false, true, 3, 4},
		{"gemini-thoughts", `{"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":2,"thoughtsTokenCount":3,"cachedContentTokenCount":4}}`, true, true, false, true, 4, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got WalletReaderEvidence
			observeWalletUsage([]byte(tc.raw), &got)
			require.Equal(t, tc.present, got.Present)
			require.Equal(t, tc.valid, got.Valid)
			require.Equal(t, tc.malformed, got.Malformed)
			require.Equal(t, tc.positive, got.ObservedPositive)
			require.Equal(t, tc.input, got.Tokens.InputTokens)
			require.Equal(t, tc.output, got.Tokens.OutputTokens)
		})
	}
}

func TestWalletReaderEvidenceCumulativeAndMalformedNeverErasePositive(t *testing.T) {
	var got WalletReaderEvidence
	observeWalletUsage([]byte(`{"usage":{"input_tokens":20,"output_tokens":3}}`), &got)
	observeWalletUsage([]byte(`{"usage":{"input_tokens":20,"output_tokens":3}}`), &got)
	observeWalletUsage([]byte(`{"usage":{"input_tokens":0,"output_tokens":0}}`), &got)
	require.Equal(t, 20, got.Tokens.InputTokens)
	require.Equal(t, 3, got.Tokens.OutputTokens)
	observeWalletUsage([]byte(`{"usage":{"input_tokens":0,"output_tokens":"bad"}}`), &got)
	require.True(t, got.ObservedPositive)
	require.True(t, got.Malformed)
}

// Ends with a chosen error instead of io.EOF, like a connection that dropped.
type walletErrAfterBody struct {
	io.Reader
	err error
}

func (b walletErrAfterBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF {
		return n, b.err
	}
	return n, err
}
func (walletErrAfterBody) Close() error { return nil }

func readWalletStreamEvidence(t *testing.T, body io.ReadCloser) WalletReaderEvidence {
	t.Helper()
	h := &AuthorizationHandle{}
	var got WalletReaderEvidence
	h.consumeHTTP = func(_ int, e WalletReaderEvidence, _ error) { got = e }
	reader := &walletResponseBody{ReadCloser: body, handle: h, status: 200}
	_, _ = io.ReadAll(reader)
	require.NoError(t, reader.Close())
	return got
}

// OpenAI Responses events that carry the whole response object (created,
// in_progress) have response.usage:null, and Chat chunks have usage:null when
// stream_options.include_usage is on. That must not latch Malformed over a
// strictly valid terminal usage.
func TestWalletResponseBodyStreamNullUsageBeforeFinalUsageIsTrusted(t *testing.T) {
	responses := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\",\"usage\":null}}\n\n" +
		"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"r1\",\"usage\":null}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n"
	completed := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":4477,\"output_tokens\":5,\"total_tokens\":4482}}}\n\n"
	chat := "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"OK\"}}],\"usage\":null}\n\n" +
		"data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"total_tokens\":15}}\n\ndata: [DONE]\n\n"

	t.Run("responses-complete", func(t *testing.T) {
		got := readWalletStreamEvidence(t, io.NopCloser(strings.NewReader(responses+completed)))
		require.True(t, got.Complete)
		require.False(t, got.Malformed)
		require.True(t, walletReaderEvidenceTrusted(got))
		require.Equal(t, 4477, got.Tokens.InputTokens)
		require.Equal(t, 5, got.Tokens.OutputTokens)
		require.True(t, walletReaderEvidenceNeedsFeeRecovery(got))
	})
	t.Run("chat-complete", func(t *testing.T) {
		got := readWalletStreamEvidence(t, io.NopCloser(strings.NewReader(chat)))
		require.True(t, got.Complete)
		require.False(t, got.Malformed)
		require.True(t, walletReaderEvidenceTrusted(got))
		require.Equal(t, 12, got.Tokens.InputTokens)
		require.Equal(t, 3, got.Tokens.OutputTokens)
	})
	t.Run("responses-aborted-before-final-usage", func(t *testing.T) {
		got := readWalletStreamEvidence(t, walletErrAfterBody{Reader: strings.NewReader(responses), err: errors.New("client went away")})
		require.False(t, got.Complete)
		require.False(t, got.Present)
		require.False(t, got.Malformed)
		require.False(t, got.ObservedPositive)
		require.False(t, walletReaderEvidenceNeedsFeeRecovery(got), "an unfinished stream with no usage is unknown cost, not a pending fee")
	})
	t.Run("null-then-explicit-zero-is-a-known-zero", func(t *testing.T) {
		zero := responses + "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n"
		got := readWalletStreamEvidence(t, io.NopCloser(strings.NewReader(zero)))
		require.True(t, got.Complete)
		require.False(t, got.Malformed)
		require.False(t, got.ObservedPositive)
		require.True(t, walletReaderEvidenceTrusted(got))
		require.True(t, walletReaderEvidenceNeedsFeeRecovery(got), "a strict terminal zero is persisted as a known zero fee")
	})
	t.Run("null-does-not-launder-a-malformed-final-usage", func(t *testing.T) {
		bad := responses + "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":7,\"output_tokens\":\"bad\"}}}\n\n"
		got := readWalletStreamEvidence(t, io.NopCloser(strings.NewReader(bad)))
		require.True(t, got.Complete)
		require.True(t, got.Malformed)
		require.True(t, got.ObservedPositive)
		require.False(t, walletReaderEvidenceTrusted(got))
		require.True(t, walletReaderEvidenceNeedsFeeRecovery(got), "a positive but unverifiable final usage still keeps the fee barrier")
	})
}

func TestWalletReaderEvidencePositiveThenNullKeepsThePositive(t *testing.T) {
	var got WalletReaderEvidence
	observeWalletUsage([]byte(`{"type":"response.completed","response":{"id":"r1","usage":{"input_tokens":20,"output_tokens":3}}}`), &got)
	observeWalletUsage([]byte(`{"type":"response.in_progress","response":{"id":"r1","usage":null}}`), &got)
	require.True(t, got.Present && got.Valid && !got.Malformed && got.ObservedPositive)
	require.Equal(t, 20, got.Tokens.InputTokens)
	require.Equal(t, 3, got.Tokens.OutputTokens)
}

// The journal checkpoint must keep exactly the usage the live parser selects.
func TestWalletJournalCheckpointSelectsTheSameUsageAsTheLiveParser(t *testing.T) {
	both := []byte(`{"usage":null,"response":{"usage":{"input_tokens":8,"output_tokens":2}}}`)
	frame, err := selectedWalletUsageFrame(both)
	require.NoError(t, err)
	require.Contains(t, string(frame), `"input_tokens":8`, "a null top-level usage must not hide response.usage from the checkpoint")
	var live WalletReaderEvidence
	observeWalletUsage(both, &live)
	require.Equal(t, 8, live.Tokens.InputTokens)
	nullOnly, err := selectedWalletUsageFrame([]byte(`{"type":"response.created","response":{"id":"r1","usage":null}}`))
	require.NoError(t, err)
	require.NotContains(t, string(nullOnly), "usage", "a null-only frame carries no usage to checkpoint")
}

func TestWalletWSNullUsageBeforeTerminalUsageIsTrusted(t *testing.T) {
	conn := &authorizingOpenAIWSClientConn{}
	h := &AuthorizationHandle{ID: "auth-ws-null", writes: []AuthorizationWrite{{Token: "auth-ws-null.1"}}}
	h.readerStarted = func() error { return nil }
	h.readerObserved = func(WalletReaderEvidence) error { return nil }
	conn.ArmAuthorization(h)
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.created","response":{"id":"response-null","status":"in_progress","usage":null}}`)))
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.in_progress","response":{"id":"response-null","usage":null}}`)))
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.completed","response":{"id":"response-null","usage":{"input_tokens":4477,"output_tokens":5}}}`)))
	require.NotNil(t, h.readerEvidence)
	require.True(t, h.readerEvidence.Complete)
	require.False(t, h.readerEvidence.Malformed, "usage:null on early frames must not latch Malformed")
	require.True(t, walletReaderEvidenceTrusted(*h.readerEvidence))
	require.Equal(t, 4477, h.readerEvidence.Tokens.InputTokens)

	// A terminal frame without reported usage (null) is a missing usage, not a malformed one.
	failed := &AuthorizationHandle{ID: "auth-ws-failed", writes: []AuthorizationWrite{{Token: "auth-ws-failed.1"}}}
	failed.readerStarted = func() error { return nil }
	failed.readerObserved = func(WalletReaderEvidence) error { return nil }
	other := &authorizingOpenAIWSClientConn{}
	other.ArmAuthorization(failed)
	require.NoError(t, other.observeFrameEvidence([]byte(`{"type":"response.created","response":{"id":"response-failed","usage":null}}`)))
	require.NoError(t, other.observeFrameEvidence([]byte(`{"type":"response.failed","response":{"id":"response-failed","usage":null}}`)))
	require.NotNil(t, failed.readerEvidence)
	require.True(t, failed.readerEvidence.Complete)
	require.False(t, failed.readerEvidence.Malformed)
	require.False(t, failed.readerEvidence.Present)
	require.False(t, walletReaderEvidenceNeedsFeeRecovery(*failed.readerEvidence))
}

type walletJoinedBody struct {
	started, stop chan struct{}
	once          sync.Once
	reading       atomic.Bool
}

func (b *walletJoinedBody) Read(p []byte) (int, error) {
	b.reading.Store(true)
	b.once.Do(func() { close(b.started) })
	<-b.stop
	n := copy(p, `{"usage":{"input_tokens":2,"output_tokens":1}}`)
	b.reading.Store(false)
	return n, errors.New("read stopped")
}
func (b *walletJoinedBody) Close() error {
	select {
	case <-b.stop:
	default:
		close(b.stop)
	}
	return nil
}

func TestWalletResponseBodyCloseStopsAndJoinsBeforeOneTransfer(t *testing.T) {
	raw := &walletJoinedBody{started: make(chan struct{}), stop: make(chan struct{})}
	h := &AuthorizationHandle{}
	var transfers atomic.Int32
	h.readerStarted = func() error { return nil }
	h.readerObserved = func(e WalletReaderEvidence) error { require.True(t, e.ObservedPositive); return nil }
	h.consumeHTTP = func(status int, e WalletReaderEvidence, err error) {
		require.False(t, raw.reading.Load(), "raw reader must be joined before transfer")
		require.True(t, e.ObservedPositive)
		require.Equal(t, 2, e.Tokens.InputTokens)
		transfers.Add(1)
	}
	body := &walletResponseBody{ReadCloser: raw, handle: h, status: 401}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = body.Read(make([]byte, 512)) }()
	<-raw.started
	require.NoError(t, body.Close())
	<-done
	require.NoError(t, body.Close())
	require.Equal(t, int32(1), transfers.Load())
}

func TestWalletResponseBodyMissingAndMalformedAreDistinct(t *testing.T) {
	for _, raw := range []string{`{"id":"r1"}`, `{"usage":{"input_tokens":"0"}}`, `{"usage":{"input_tokens":0,"output_tokens":0}}`} {
		h := &AuthorizationHandle{}
		var got WalletReaderEvidence
		h.consumeHTTP = func(_ int, e WalletReaderEvidence, _ error) { got = e }
		body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader(raw)), handle: h, status: 200}
		_, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		require.True(t, got.Complete)
		if strings.Contains(raw, `"input_tokens":0`) {
			require.True(t, got.Present && got.Valid && !got.Malformed)
		} else {
			require.False(t, got.Present && got.Valid && !got.Malformed)
		}
	}
}

func TestWalletPositivePersistenceFailurePreventsLegacyZeroSeal(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	h := &AuthorizationHandle{ID: "auth-positive", SnapshotID: "snapshot", AttemptKind: "llm", writes: []AuthorizationWrite{{Token: "auth-positive.1"}}}
	bridge.installImmediateEvidence(h, "user")
	mock.ExpectExec("UPDATE wallet_authorization_segment SET reader_started").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE wallet_authorization_segment SET reader_evidence").WillReturnError(errors.New("primary unavailable"))
	mock.ExpectExec("UPDATE wallet_authorization_segment SET reader_evidence").WillReturnError(errors.New("primary unavailable"))
	body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":5,"output_tokens":1}}`)), handle: h, status: 401}
	_, err = io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.False(t, h.zeroAcknowledged)
	require.Error(t, h.readerEvidenceErr)
	require.True(t, h.readerEvidence.ObservedPositive)
	require.NoError(t, mock.ExpectationsWereMet(), "no zero RPC, terminal seal or Redis release is allowed after failed positive transfer")
}

func TestWalletQueuedTaskOnlyDispatchesDurableIdentity(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db}
	h := &AuthorizationHandle{ID: "auth-queue", SnapshotID: "snapshot", writes: []AuthorizationWrite{{Token: "auth-queue.1"}}}
	mock.ExpectQuery("SELECT COALESCE\\(reader_evidence").WillReturnRows(sqlmock.NewRows([]string{"reader_evidence"}).AddRow(`{"present":true,"valid":true}`))
	mock.ExpectExec("UPDATE wallet_authorization_segment SET terminal_sealed_at").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT terminal_sealed_at,expiry_terminal_proof").WillReturnRows(sqlmock.NewRows([]string{"terminal_sealed_at", "proof", "eligible"}).AddRow(time.Now(), strings.Repeat("a", 64), false))
	mutable := &struct{ cost int }{cost: 10}
	calls := 0
	queued, err := bridge.prepareDurableUsage(context.Background(), h, func(ctx context.Context) {
		calls++
		stage := ctx.Value(walletUsageStageKey{}).(*walletUsageStage)
		stage.seen = true
		require.Equal(t, 10, mutable.cost)
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	mutable.cost = 999
	queued(context.Background())
	require.Equal(t, 1, calls, "queued task must never execute or capture live pricing closure again")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWalletUsageStagePanicDoesNotClaimTerminalOwnership(t *testing.T) {
	bridge := &CanonicalWalletBridge{}
	h := &AuthorizationHandle{}
	task, err := bridge.prepareDurableUsage(context.Background(), h, func(context.Context) { panic("before durable evidence") })
	require.Error(t, err)
	require.Nil(t, task)
	require.Equal(t, "usage_stage_panic", h.Abandoned())
}

func TestWalletWSReaderBindsResponseBeforeNewTurnEvidence(t *testing.T) {
	conn := &authorizingOpenAIWSClientConn{}
	observed := 0
	makeHandle := func(id string) *AuthorizationHandle {
		h := &AuthorizationHandle{ID: id, writes: []AuthorizationWrite{{Token: id + ".1"}}}
		h.readerStarted = func() error { return nil }
		h.readerObserved = func(WalletReaderEvidence) error { observed++; return nil }
		return h
	}
	first := makeHandle("auth-first")
	conn.ArmAuthorization(first)
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.created","response":{"id":"response-first"}}`)))
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.completed","response":{"id":"response-first","usage":{"input_tokens":3,"output_tokens":1}}}`)))
	second := makeHandle("auth-second")
	conn.ArmAuthorization(second)
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.completed","response":{"id":"response-first","usage":{"input_tokens":99,"output_tokens":99}}}`)))
	require.Equal(t, 2, observed)
	require.Nil(t, second.readerEvidence)
	require.NoError(t, conn.observeFrameEvidence([]byte(`{"type":"response.completed","response":{"id":"response-second","usage":{"input_tokens":0,"output_tokens":0}}}`)))
	require.True(t, second.readerEvidence.Valid)
	require.False(t, second.readerEvidence.ObservedPositive)
	require.Equal(t, "llm_ws_usage", second.readerEvidence.Source)
}

// observeFrameEvidence is the test entry to the owned-frame observation: response
// identity is captured before touching the currently armed handle, so a delayed
// terminal from an earlier turn cannot become the next turn's fee.
func (c *authorizingOpenAIWSClientConn) observeFrameEvidence(payload []byte) error {
	return c.observeOwnedFrameEvidence(payload, c.armed.Load())
}
