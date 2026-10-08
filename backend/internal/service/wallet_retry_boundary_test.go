//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWalletUnknown503StopsDecoratedInternalReplay(t *testing.T) {
	h, err := newAuthorizationHandle(config.CanonicalWalletModeEnforce)
	require.NoError(t, err)
	h.beforeWrite = func(context.Context, string) error { return nil }
	h.renewAfterZero = func(context.Context) (*AuthorizationHandle, error) {
		t.Fatal("unknown response cannot renew")
		return nil, nil
	}
	sends := 0
	dec := newAuthorizingHTTPUpstreamWithMode(nil, modeFn(config.CanonicalWalletModeEnforce))
	req := newRequest(t, WithAuthorizationHandle(context.Background(), h), "http://fixture.invalid/v1/messages")
	send := func(*http.Request) (*http.Response, error) {
		sends++
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	}
	resp, err := dec.authorizedWrite(req, h, send)
	require.NoError(t, err)
	require.Equal(t, 503, resp.StatusCode)
	require.False(t, WalletAttemptMayRetry(req.Context()))
	_, err = dec.authorizedWrite(req, h, send)
	require.ErrorIs(t, err, ErrWalletUnknownCostRetry)
	require.Equal(t, 1, sends)
	require.Len(t, h.Writes(), 1)
}

func TestWalletKnownZeroRequiresDurablePinAcknowledgement(t *testing.T) {
	for _, outcome := range []AuthorizationOutcome{AuthorizationOutcomeRejected, AuthorizationOutcomeNotWritten} {
		h, _ := newAuthorizationHandle("enforce")
		h.beforeWrite = func(context.Context, string) error { return nil }
		h.retryCheck = func(context.Context) error { return errors.New("pin ACK unavailable") }
		h.RecordOutcome(h.MintWriteToken(), outcome, nil)
		ctx := WithAuthorizationHandle(context.Background(), h)
		require.False(t, WalletAttemptMayRetry(ctx))
		h.retryCheck = func(context.Context) error { return nil }
		require.True(t, WalletAttemptMayRetry(ctx))
	}
}

func TestWalletRetryGateLeavesNonCanonicalPathsUnchanged(t *testing.T) {
	require.True(t, WalletAttemptMayRetry(context.Background()))
	h, _ := newAuthorizationHandle("shadow")
	h.RecordOutcome(h.MintWriteToken(), AuthorizationOutcomeResult, nil)
	require.True(t, WalletAttemptMayRetry(WithAuthorizationHandle(context.Background(), h)))
}
