//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// newPoolForAuthorizationTest builds a pool whose clientDialer is the DECORATED
// fake dialer (the shape the default constructor produces after Task 2), so the
// lease/conn handle flow is exercised end to end.
func newPoolForAuthorizationTest(t *testing.T, mode string) (*openAIWSConnPool, *fakeWSDialer) {
	t.Helper()
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = mode
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 4
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	inner := &fakeWSConn{}
	d := &fakeWSDialer{conn: inner}
	pool.setClientDialerForTest(newAuthorizingOpenAIWSClientDialerWithMode(d, modeFn(mode)))
	return pool, d
}

func testAcquireRequest(t *testing.T) openAIWSAcquireRequest {
	t.Helper()
	return openAIWSAcquireRequest{
		Account: &Account{ID: 424242, Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		WSURL:   "wss://example.com/v1/responses",
	}
}

func TestPoolLeaseOwnsTheTurnHandleAndReleaseClearsIt(t *testing.T) {
	pool, dialer := newPoolForAuthorizationTest(t, config.CanonicalWalletModeEnforce)
	lease1, err := pool.Acquire(context.Background(), testAcquireRequest(t))
	require.NoError(t, err)
	h, _ := newAuthorizationHandle("enforce")
	lease1.ArmAuthorization(h)
	require.Same(t, h, lease1.conn.ws.(authorizationArmable).ArmedAuthorization())
	require.NoError(t, lease1.WriteJSONWithContextTimeout(context.Background(), json.RawMessage(`{"type":"response.create"}`), time.Second))
	require.Len(t, h.Writes(), 1)
	lease1.Release()

	lease2, err := pool.Acquire(context.Background(), testAcquireRequest(t))
	require.NoError(t, err)
	require.True(t, lease2.Reused(), "the pool hands the same conn back")
	require.Nil(t, lease2.conn.ws.(authorizationArmable).ArmedAuthorization(), "a reused pooled connection carries no handle from its previous lease")

	// A released lease must not be able to arm the conn the next acquirer now holds
	// (round-1 finding: every other lease method routes through activeConn()).
	lease1.ArmAuthorization(h)
	require.Nil(t, lease2.conn.ws.(authorizationArmable).ArmedAuthorization())

	err = lease2.WriteJSONWithContextTimeout(context.Background(), json.RawMessage(`{"type":"response.create"}`), time.Second)
	require.True(t, errors.Is(err, ErrAuthorizationRefused), "unarmed, unmarked → refused in enforce")
	require.Equal(t, 1, dialer.dials)
}

func TestPoolDialerIsDecoratedByDefault(t *testing.T) {
	cfg := &config.Config{}
	p := newOpenAIWSConnPool(cfg)
	t.Cleanup(p.Close)
	_, ok := p.clientDialer.(*authorizingOpenAIWSClientDialer)
	require.True(t, ok)
}

func TestPassthroughDialerIsDecoratedByDefault(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	_, ok := svc.getOpenAIWSPassthroughDialer().(*authorizingOpenAIWSClientDialer)
	require.True(t, ok)
}
