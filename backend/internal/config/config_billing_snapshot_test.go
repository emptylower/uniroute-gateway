package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCanonicalWalletBillingSnapshotModeDefaultsToRecord(t *testing.T) {
	viper.Reset()
	setDefaults()
	require.Equal(t, "record", viper.GetString("canonical_wallet.billing_snapshot_mode"))
}

func TestValidateBillingSnapshotMode(t *testing.T) {
	for _, mode := range []string{"off", "record", "settle"} {
		require.NoError(t, validateBillingSnapshotMode(mode), mode)
	}
	err := validateBillingSnapshotMode("shadow")
	require.Error(t, err)
	require.Contains(t, err.Error(), "billing_snapshot_mode")
}

// The flag is validated OUTSIDE the canonical_wallet.mode switch: a typo must
// be refused even when canonical_wallet.mode is "disabled" — the default, and
// the configuration 3.2 runs under — or Mode() would map it to "off" and
// silently disable the snapshot: a fail-open on a money-path flag.
// resetViperWithJWTSecret + Load is how config_test.go builds a Config that
// passes Validate() (config_test.go:520-528).
func TestConfigValidateRefusesBillingSnapshotModeWhenWalletDisabled(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.Equal(t, CanonicalWalletModeDisabled, cfg.CanonicalWallet.Mode)
	cfg.CanonicalWallet.BillingSnapshotMode = "shadow"
	err = cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "billing_snapshot_mode")
}
