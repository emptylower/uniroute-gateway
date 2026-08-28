//go:build unit

package service

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizationRefusedErrorIsAndAs(t *testing.T) {
	cause := errors.New("control plane 503")
	err := &AuthorizationRefusedError{Reason: AuthorizationRefusalLeaseUnavailable, AuthorizationID: "auth_x", Detail: "POST api.openai.com/v1/responses", Cause: cause}
	require.True(t, errors.Is(err, ErrAuthorizationRefused))
	require.True(t, errors.Is(err, cause))
	wrapped := fmt.Errorf("forward: %w", err)
	got, ok := AsAuthorizationRefused(wrapped)
	require.True(t, ok)
	require.Same(t, err, got)
	require.Contains(t, err.Error(), "lease_unavailable")
	require.Contains(t, err.Error(), "auth_x")
	_, ok = AsAuthorizationRefused(errors.New("other"))
	require.False(t, ok)
	_, ok = AsAuthorizationRefused(nil)
	require.False(t, ok)
}

func TestAuthorizationRefusedIsNeverAFailover(t *testing.T) {
	err := &AuthorizationRefusedError{Reason: AuthorizationRefusalUnmarkedWrite}
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover))
	require.Equal(t, http.StatusPaymentRequired, AuthorizationRefusedHTTPStatus)
	require.Equal(t, "wallet_authorization_refused", AuthorizationRefusedErrorType)
}
