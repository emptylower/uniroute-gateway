//go:build unit

package service

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalletBodyOwnerEndsOnReadTerminalAndCloseOnce(t *testing.T) {
	var calls atomic.Int64
	var once sync.Once
	h := &AuthorizationHandle{writeEnded: func() { once.Do(func() { calls.Add(1) }) }}
	body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader("result")), handle: h}
	_, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, int64(1), calls.Load())
	var closing sync.WaitGroup
	for range 32 {
		closing.Add(1)
		go func() { defer closing.Done(); _ = body.Close() }()
	}
	closing.Wait()
	require.Equal(t, int64(1), calls.Load())
}

func TestWalletWSDisarmEndsTurnWithoutClosingSession(t *testing.T) {
	var calls atomic.Int64
	h := &AuthorizationHandle{writeEnded: func() { calls.Add(1) }}
	conn := &authorizingOpenAIWSClientConn{}
	conn.ArmAuthorization(h)
	var ending sync.WaitGroup
	for range 32 {
		ending.Add(1)
		go func() { defer ending.Done(); conn.DisarmAuthorization() }()
	}
	ending.Wait()
	require.Equal(t, int64(1), calls.Load())
	require.Nil(t, conn.ArmedAuthorization())
	conn.DisarmAuthorization()
	require.Equal(t, int64(1), calls.Load())
}

func TestWalletGatewayShutdownIsNilSafeAndIdempotent(t *testing.T) {
	var absent *OpenAIGatewayService
	absent.CloseOpenAIWSPool()
	bridge := &CanonicalWalletBridge{stop: make(chan struct{})}
	gateway := &OpenAIGatewayService{canonicalWallet: bridge}
	gateway.CloseOpenAIWSPool()
	gateway.CloseOpenAIWSPool()
	select {
	case <-bridge.stop:
	default:
		t.Fatal("owned bridge remains live after shutdown")
	}
}
