//go:build unit

package repository

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestBillingCacheSatisfiesCanonicalWalletLeaseStore(t *testing.T) {
	var cache any = NewBillingCache(nil) // no call is made; a nil client is fine
	_, ok := cache.(service.CanonicalWalletLeaseStore)
	require.True(t, ok, "repository.NewBillingCache's concrete type must satisfy service.CanonicalWalletLeaseStore — otherwise ProvideBillingCacheService's wallet bridge is silently unwired in shadow and the process panics at startup in enforce (canonical_wallet_wiring.go)")
}

func TestGatewayCacheSatisfiesCanonicalWalletLeaseStore(t *testing.T) {
	var cache any = NewGatewayCache(nil)
	_, ok := cache.(service.CanonicalWalletLeaseStore)
	require.True(t, ok, "repository.NewGatewayCache's concrete type must keep satisfying service.CanonicalWalletLeaseStore (the two gateway construction sites)")
}
