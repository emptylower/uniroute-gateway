package openai_ws_v2

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestRelayFinancialTerminalDedupBeforeUsageAndCallbacks(t *testing.T) {
	client := newPassthroughTestFrameConn(nil, false)
	upstream := newPassthroughTestFrameConn([]passthroughTestFrame{
		{coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":2,"output_tokens":1}}}`)},
		{coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":3,"output_tokens":4}}}`)},
		// A prior terminal arriving after a new turn must not charge the new auth.
		{coderws.MessageText, []byte(`{"type":"response.done","response":{"id":"resp_1","usage":{"input_tokens":999,"output_tokens":999}}}`)},
	}, true)
	var turns []RelayTurnResult
	result, exit := Relay(context.Background(), client, upstream, []byte(`{"type":"response.create","model":"gpt-5.1"}`), RelayOptions{
		OnTurnComplete: func(turn RelayTurnResult) { turns = append(turns, turn) },
	})
	require.Nil(t, exit)
	require.Len(t, turns, 2)
	require.Equal(t, 5, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Len(t, client.Writes(), 2, "duplicate cannot unlock the newly armed turn")
}

func TestRelayFinancialUsagePresenceAndStrictIntegers(t *testing.T) {
	for _, tc := range []struct {
		name, usage               string
		present, valid, malformed bool
	}{
		{"missing", "", false, false, false},
		{"zero", `{"input_tokens":0,"output_tokens":0}`, true, true, false},
		{"aliases", `{"prompt_tokens":2,"completion_tokens":1}`, true, true, false},
		{"empty", `{}`, true, false, true},
		{"null", `null`, true, false, true},
		{"negative", `{"input_tokens":-1,"output_tokens":0}`, true, false, true},
		{"fraction", `{"input_tokens":1,"output_tokens":1.5}`, true, false, true},
		{"overflow", `{"input_tokens":9223372036854775808,"output_tokens":0}`, true, false, true},
		{"string", `{"input_tokens":"1","output_tokens":0}`, true, false, true},
		{"cache_negative", `{"input_tokens":1,"output_tokens":1,"input_tokens_details":{"cached_tokens":-1}}`, true, false, true},
		{"cache_write_fraction", `{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":1.2}`, true, false, true},
		{"image_overflow", `{"input_tokens":1,"output_tokens":1,"output_tokens_details":{"image_tokens":18446744073709551616}}`, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := `{"response":{}}`
			if tc.usage != "" {
				message = `{"response":{"usage":` + tc.usage + `}}`
			}
			parsed := ParseUsage([]byte(message))
			require.Equal(t, tc.present, parsed.Present)
			require.Equal(t, tc.valid, parsed.Valid)
			require.Equal(t, tc.malformed, parsed.Malformed)
			if tc.malformed {
				require.Zero(t, parsed.InputTokens)
				require.Zero(t, parsed.OutputTokens)
			}
		})
	}
	parsed := ParseUsage([]byte(`{"response":{"usage":{"input_tokens":5,"output_tokens":"bad"}}}`))
	require.True(t, parsed.ObservedPositive, "failed normalization cannot erase already observed positive usage")
}

type financialJoinFrameConn struct {
	stopped chan struct{}
	release chan struct{}
	read    bool
}

func (c *financialJoinFrameConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if c.read {
		return coderws.MessageText, nil, io.EOF
	}
	c.read = true
	<-ctx.Done()
	close(c.stopped)
	<-c.release
	return coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_join","usage":{"input_tokens":7,"output_tokens":2}}}`), nil
}
func (*financialJoinFrameConn) WriteFrame(context.Context, coderws.MessageType, []byte) error {
	return nil
}
func (*financialJoinFrameConn) Close() error { return nil }

func TestRelayFinancialJoinsReaderBeforeSnapshot(t *testing.T) {
	client := newPassthroughTestFrameConn(nil, true)
	upstream := &financialJoinFrameConn{stopped: make(chan struct{}), release: make(chan struct{})}
	done := make(chan RelayResult, 1)
	go func() {
		result, _ := Relay(context.Background(), client, upstream, []byte(`{"type":"response.create"}`), RelayOptions{UpstreamDrainTimeout: time.Millisecond})
		done <- result
	}()
	select {
	case <-upstream.stopped:
	case <-time.After(time.Second):
		t.Fatal("reader was not stopped")
	}
	select {
	case <-done:
		t.Fatal("relay returned before reader handed off evidence")
	case <-time.After(20 * time.Millisecond):
	}
	close(upstream.release)
	select {
	case result := <-done:
		require.Equal(t, 7, result.Usage.InputTokens)
		require.Equal(t, 2, result.Usage.OutputTokens)
	case <-time.After(time.Second):
		t.Fatal("relay did not join reader")
	}
}

func TestRelayFinancialReaderPanicStopsAndJoins(t *testing.T) {
	client := newPassthroughTestFrameConn(nil, false)
	upstream := newPassthroughTestFrameConn([]passthroughTestFrame{{coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_panic","usage":{"input_tokens":4,"output_tokens":1}}}`)}}, false)
	result, exit := Relay(context.Background(), client, upstream, []byte(`{"type":"response.create"}`), RelayOptions{
		OnTurnComplete: func(RelayTurnResult) { panic("durable handoff fault") },
	})
	require.NotNil(t, exit)
	require.Equal(t, "read_upstream_panic", exit.Stage)
	require.Equal(t, 4, result.Usage.InputTokens)
}

type financialClientJoinFrameConn struct {
	closeSpyFrameConn
	stopped chan struct{}
	release chan struct{}
}

func (c *financialClientJoinFrameConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	<-ctx.Done()
	close(c.stopped)
	<-c.release
	return coderws.MessageText, nil, ctx.Err()
}

func TestRelayFinancialErrorJoinsClientWithoutChoosingCloseCode(t *testing.T) {
	client := &financialClientJoinFrameConn{stopped: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(client.release)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := newPassthroughTestFrameConn([]passthroughTestFrame{{coderws.MessageText, []byte(`{"type":"response.created"}`)}}, false)
	wantErr := errors.New("upstream frame rejected")
	done := make(chan *RelayExit, 1)
	go func() {
		_, exit := Relay(ctx, client, upstream, []byte(`{"type":"response.create"}`), RelayOptions{
			BeforeWriteClient: func(coderws.MessageType, []byte, bool) error { return wantErr },
		})
		done <- exit
	}()
	select {
	case <-client.stopped:
	case <-time.After(time.Second):
		t.Fatal("client reader was not canceled after relay error")
	}
	require.Zero(t, client.CloseCalls(), "relay error must preserve the caller's close-code decision")
	select {
	case <-done:
		t.Fatal("relay returned before its client reader joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(client.release)
	released = true
	select {
	case exit := <-done:
		require.NotNil(t, exit)
		require.Equal(t, "upstream_message", exit.Stage)
		require.ErrorIs(t, exit.Err, wantErr)
		require.Zero(t, client.CloseCalls())
	case <-time.After(time.Second):
		t.Fatal("relay did not join its canceled client reader")
	}
}
