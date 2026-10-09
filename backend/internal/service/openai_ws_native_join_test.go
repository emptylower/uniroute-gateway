//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSFinancialNativeOpenClientJoinsAfterUpstreamNormalClose(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err = conn.Read(r.Context()); err != nil {
			return
		}
		if err = conn.Write(r.Context(), coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"native-normal","usage":{"input_tokens":1,"output_tokens":1}}}`)); err != nil {
			return
		}
		_ = conn.Close(coderws.StatusNormalClosure, "finished")
	}))
	defer upstream.Close()
	completed := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client, err := coderws.Accept(w, r, nil)
		if err != nil {
			completed <- err
			return
		}
		defer client.CloseNow()
		_, first, err := client.Read(r.Context())
		if err != nil {
			completed <- err
			return
		}
		upstreamConn, _, err := coderws.Dial(r.Context(), "ws"+strings.TrimPrefix(upstream.URL, "http"), nil)
		if err != nil {
			completed <- err
			return
		}
		frameClient := &openAIWSClientFrameConn{conn: client, controlCtx: r.Context(), interTurnStarted: make(chan struct{}, 1)}
		_, exit := openaiwsv2.Relay(r.Context(), frameClient, &coderOpenAIWSClientConn{conn: upstreamConn}, first, openaiwsv2.RelayOptions{})
		if exit != nil && !exit.Graceful {
			completed <- exit.Err
			return
		}
		// The caller can seal only after both native readers actually joined.
		completed <- nil
	}))
	defer gateway.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`)))
	_, payload, err := client.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(payload), "native-normal")
	// Keep a healthy client connection open and provide no next frame. A
	// relayCtx cancellation ignored by the adapter used to hang forever here.
	select {
	case err := <-completed:
		require.NoError(t, err)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("native client reader did not stop/join after the upstream normal close")
	}
}

func TestOpenAIWSFinancialNativeRelayErrorRetainsSessionReaderAndCallerCloseCode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, _, _ = conn.Read(r.Context())
		_, _, _ = conn.Read(r.Context())
	}))
	defer upstream.Close()
	completed := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client, err := coderws.Accept(w, r, nil)
		if err != nil {
			completed <- err
			return
		}
		defer client.CloseNow()
		ctx, cleanup := WithOpenAIWSClientReadSession(r.Context(), client)
		defer cleanup()
		upstreamConn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(upstream.URL, "http"), nil)
		if err != nil {
			completed <- err
			return
		}
		frame := &openAIWSClientFrameConn{conn: client, controlCtx: ctx, interTurnStarted: make(chan struct{}, 1)}
		started := time.Now()
		_, exit := openaiwsv2.Relay(ctx, frame, &coderOpenAIWSClientConn{conn: upstreamConn}, []byte(`{"type":"response.create"}`), openaiwsv2.RelayOptions{IdleTimeout: 10 * time.Millisecond})
		if exit == nil || exit.Stage != "idle_timeout" || time.Since(started) >= 1500*time.Millisecond {
			completed <- fmt.Errorf("native error relay did not join its waiter within 1500ms: %v", exit)
			return
		}
		session := frame.clientReadSession()
		session.mu.Lock()
		retained := session.pending
		closed := session.closed
		session.mu.Unlock()
		if retained == nil || closed {
			completed <- fmt.Errorf("relay error lost ingress ownership of the pending native read")
			return
		}
		select {
		case <-retained.done:
			completed <- fmt.Errorf("relay error canceled the native read before caller close policy")
			return
		default:
		}
		if err := frame.WriteFrame(ctx, coderws.MessageText, []byte(`{"relay":"returned"}`)); err != nil {
			completed <- err
			return
		}
		// A replacement provider waiter consumes the original ingress future.
		next := &openAIWSClientFrameConn{conn: client, controlCtx: ctx, interTurnStarted: make(chan struct{}, 1)}
		_, payload, err := next.ReadFrame(ctx)
		if err != nil || string(payload) != `{"next":"provider"}` || next.clientReadSession() != session {
			completed <- fmt.Errorf("session handoff lost or duplicated the next client frame: %s %v", payload, err)
			return
		}
		waiting := make(chan struct{})
		go func() { defer close(waiting); _, _, _ = next.ReadFrame(ctx) }()
		deadline := time.Now().Add(time.Second)
		var pending *openAIWSClientReadFuture
		for pending == nil && time.Now().Before(deadline) {
			session.mu.Lock()
			pending = session.pending
			session.mu.Unlock()
			if pending == nil {
				time.Sleep(time.Millisecond)
			}
		}
		if pending == nil {
			completed <- fmt.Errorf("second native session read did not start")
			return
		}
		started = time.Now()
		next.closeWithStatus(coderws.StatusPolicyViolation, "caller selected policy")
		cleanup()
		select {
		case <-waiting:
		case <-time.After(1500 * time.Millisecond):
			completed <- fmt.Errorf("session termination did not join the relay waiter")
			return
		}
		select {
		case <-pending.done:
		default:
			completed <- fmt.Errorf("session termination returned with an unjoined native actor")
			return
		}
		if time.Since(started) >= 1500*time.Millisecond {
			completed <- fmt.Errorf("session close exceeded 1500ms")
			return
		}
		completed <- nil
	}))
	defer gateway.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	_, payload, err := client.Read(ctx)
	require.NoError(t, err, "the native connection must survive a relay error")
	require.JSONEq(t, `{"relay":"returned"}`, string(payload))
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"next":"provider"}`)))
	_, _, err = client.Read(ctx)
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
	require.Equal(t, "caller selected policy", closeErr.Reason)
	select {
	case err := <-completed:
		require.NoError(t, err)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("native session termination did not stop and join its actors")
	}
}
