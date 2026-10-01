package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCanonicalUSDWalletPolicyRequiresExplicitVersionAndEnforcedSnapshots(t *testing.T) {
	require.NoError(t, validateCanonicalUSDWalletPolicy(CanonicalWalletConfig{}))
	valid := CanonicalWalletConfig{USDWalletEnabled: true, USDPolicyVersion: CanonicalUSDWalletPolicyVersion, Mode: CanonicalWalletModeEnforce, BillingSnapshotMode: "settle"}
	require.NoError(t, validateCanonicalUSDWalletPolicy(valid))
	for _, version := range []string{"", "usd-wallet-v2"} {
		cfg := valid
		cfg.USDPolicyVersion = version
		require.Error(t, validateCanonicalUSDWalletPolicy(cfg))
	}
	for _, mode := range []string{"disabled", "shadow"} {
		cfg := valid
		cfg.Mode = mode
		require.Error(t, validateCanonicalUSDWalletPolicy(cfg))
	}
	for _, mode := range []string{"off", "record"} {
		cfg := valid
		cfg.BillingSnapshotMode = mode
		require.Error(t, validateCanonicalUSDWalletPolicy(cfg))
	}
}
