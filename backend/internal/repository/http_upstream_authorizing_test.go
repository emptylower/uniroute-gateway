//go:build unit

package repository

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPUpstreamIsDecorated(t *testing.T) {
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	up := NewHTTPUpstream(cfg)
	_, ok := up.(*service.AuthorizingHTTPUpstream)
	require.True(t, ok, "the port must be constructed decorated, at its single DI construction point")
}
