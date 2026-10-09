package service

import (
	"context"
	"errors"
	"sync"
	"time"

	coderws "github.com/coder/websocket"
)

type openAIWSClientReadResult struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

type openAIWSClientReadSessionKey struct{}

type openAIWSClientReadFuture struct {
	done   chan struct{}
	result openAIWSClientReadResult
}

// A client input read belongs to the ingress session, across provider failover.
// A relay joins its waiter; only session termination cancels the native read.
// The future retains at most one frame and is never financial reader evidence.
type openAIWSClientReadSession struct {
	mu        sync.Mutex
	conn      *coderws.Conn
	pending   *openAIWSClientReadFuture
	closed    bool
	closeDone chan struct{}
}

func openAIWSClientReadSessionFromContext(ctx context.Context) *openAIWSClientReadSession {
	if ctx == nil {
		return nil
	}
	session, _ := ctx.Value(openAIWSClientReadSessionKey{}).(*openAIWSClientReadSession)
	return session
}

// WithOpenAIWSClientReadSession gives ingress explicit ownership of its native
// reader. Call cleanup after the caller has selected and sent its close frame.
func WithOpenAIWSClientReadSession(ctx context.Context, conn *coderws.Conn) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	session := &openAIWSClientReadSession{conn: conn}
	return context.WithValue(ctx, openAIWSClientReadSessionKey{}, session), func() {
		session.close(coderws.StatusNormalClosure, "", false)
	}
}

func (s *openAIWSClientReadSession) readFrame(ctx context.Context, read func() openAIWSClientReadResult) (coderws.MessageType, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, nil, errOpenAIWSConnClosed
	}
	if s.pending == nil {
		future := &openAIWSClientReadFuture{done: make(chan struct{})}
		s.pending = future
		go func() {
			defer func() {
				if recover() != nil {
					future.result.err = errors.New("native client session reader panicked")
				}
				close(future.done)
			}()
			future.result = read()
		}()
	}
	future := s.pending
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		// Keep the native read owned by ingress. Canceling coder Read here
		// would close the client before failover or caller close-code policy.
		return 0, nil, ctx.Err()
	case <-future.done:
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		s.mu.Lock()
		if s.pending == future {
			s.pending = nil
		}
		s.mu.Unlock()
		return future.result.messageType, future.result.payload, future.result.err
	}
}

func (s *openAIWSClientReadSession) close(status coderws.StatusCode, reason string, force bool) {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return
	}
	s.closed = true
	s.closeDone = make(chan struct{})
	future := s.pending
	s.mu.Unlock()
	defer close(s.closeDone)
	if future != nil || force {
		closeOpenAIWSNativeConn(s.conn, status, reason)
	}
	if future != nil {
		<-future.done
	}
}

// ReadOpenAIWSClientMessage keeps one reader alive while control events send
// their close frame, then closes the transport and joins that reader.
func ReadOpenAIWSClientMessage(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
) (coderws.MessageType, []byte, error) {
	return readOpenAIWSClientMessageWithTimeoutStart(
		controlCtx,
		conn,
		timeout,
		timeoutStatus,
		timeoutReason,
		nil,
		nil,
	)
}

// readOpenAIWSClientMessageWithTimeoutStart supports readers whose timeout
// starts after a state transition, such as a completed passthrough turn. When
// timeoutActive is nil, a positive timeout starts immediately.
func readOpenAIWSClientMessageWithTimeoutStart(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
	timeoutStart <-chan struct{},
	timeoutActive func() bool,
) (coderws.MessageType, []byte, error) {
	if conn == nil {
		return 0, nil, errors.New("openai websocket client connection is nil")
	}
	if controlCtx == nil {
		controlCtx = context.Background()
	}

	readDone := make(chan openAIWSClientReadResult, 1)
	// Keep policy cancellation separate so its close frame can be attempted
	// before cancellation interrupts the native transport read.
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	go func() {
		messageType, payload, err := conn.Read(readCtx)
		readDone <- openAIWSClientReadResult{messageType: messageType, payload: payload, err: err}
	}()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	startTimeout := func() {
		if timeout <= 0 || (timeoutActive != nil && !timeoutActive()) {
			return
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		}
		timeoutCh = timer.C
	}
	if timeoutActive == nil || timeoutActive() {
		startTimeout()
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	closeAndJoin := func(status coderws.StatusCode, reason string, cause error) (coderws.MessageType, []byte, error) {
		closeOpenAIWSNativeConn(conn, status, reason, cancelRead)
		<-readDone
		return 0, nil, NewOpenAIWSClientCloseError(status, reason, cause)
	}

	for {
		select {
		case result := <-readDone:
			return result.messageType, result.payload, result.err
		case <-timeoutStart:
			startTimeout()
		case <-timeoutCh:
			return closeAndJoin(timeoutStatus, timeoutReason, context.DeadlineExceeded)
		case <-controlCtx.Done():
			cause := context.Cause(controlCtx)
			if errors.Is(cause, ErrOpenAIWSIngressLeaseLost) {
				return closeAndJoin(
					coderws.StatusTryAgainLater,
					"websocket ingress capacity lease lost; please reconnect",
					cause,
				)
			}
			return closeAndJoin(coderws.StatusGoingAway, "websocket request canceled", cause)
		}
	}
}
