package service

import (
	"context"
	"time"

	coderws "github.com/coder/websocket"
)

// CloseOpenAIWSClientConnection attempts the caller's chosen close frame before
// interrupting the transport and joining the native close actor.
func CloseOpenAIWSClientConnection(conn *coderws.Conn, status coderws.StatusCode, reason string) {
	closeOpenAIWSNativeConn(conn, status, reason)
}

// A native peer may stay healthy but stop reading after its last frame. Send
// the intended close code, then stop the transport and join the close actor
// without waiting for coder/websocket's five-second handshake timeout.
func closeOpenAIWSNativeConn(conn *coderws.Conn, status coderws.StatusCode, reason string, cancelReads ...context.CancelFunc) {
	if conn == nil {
		return
	}
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_ = conn.Close(status, reason)
	}()
	select {
	case <-joined:
	case <-time.After(150 * time.Millisecond):
	}
	for _, cancel := range cancelReads {
		cancel()
	}
	// CloseNow cannot interrupt an already running Close in coder/websocket:
	// it waits for that handshake. Canceling CloseRead closes the underlying
	// transport, including when a competing reader holds the read mutex.
	forceCtx, cancelForce := context.WithCancel(context.Background())
	conn.CloseRead(forceCtx)
	cancelForce()
	_ = conn.CloseNow()
	<-joined
}
