package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Phase 4.1-G: the reconciliation read token's config-layer rule — a
// configured token shorter than 32 bytes fails Validate() under
// enforce/shadow (the config layer, not the wiring gate's panic), and no
// token check applies under disabled. resetViperWithJWTSecret + Load is how
// config_test.go builds a Config that passes Validate().
func TestCanonicalWalletReconciliationReadTokenValidation(t *testing.T) {
	base := func(t *testing.T) *Config {
		resetViperWithJWTSecret(t)
		cfg, err := Load()
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		return cfg
	}

	short := strings.Repeat("t", 31)
	long := strings.Repeat("t", 32)

	t.Run("disabled: any token shape passes", func(t *testing.T) {
		cfg := base(t)
		cfg.CanonicalWallet.ReconciliationReadToken = short
		require.NoError(t, cfg.Validate())
		cfg.CanonicalWallet.ReconciliationReadToken = ""
		require.NoError(t, cfg.Validate())
	})

	t.Run("shadow: a short configured token is refused, naming the key", func(t *testing.T) {
		cfg := base(t)
		cfg.CanonicalWallet.Mode = CanonicalWalletModeShadow
		cfg.CanonicalWallet.ControlPlaneURL = "https://controlplane.example.invalid"
		cfg.CanonicalWallet.Issuer = "sub2api-gateway"
		cfg.CanonicalWallet.Audience = "shipany-control-plane"
		cfg.CanonicalWallet.Version = "v1"
		cfg.CanonicalWallet.Secret = strings.Repeat("y", 32)
		require.NoError(t, cfg.Validate(), "the fixture itself must be valid")

		cfg.CanonicalWallet.ReconciliationReadToken = short
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "canonical_wallet.reconciliation_read_token must be at least 32 bytes when canonical_wallet.mode is enforce or shadow")

		cfg.CanonicalWallet.ReconciliationReadToken = long
		require.NoError(t, cfg.Validate())
		cfg.CanonicalWallet.ReconciliationReadToken = ""
		require.NoError(t, cfg.Validate(), "empty means unmounted, always legal")
	})

	t.Run("enforce: same rule", func(t *testing.T) {
		cfg := base(t)
		cfg.CanonicalWallet.Mode = CanonicalWalletModeEnforce
		cfg.CanonicalWallet.ControlPlaneURL = "https://controlplane.example.invalid"
		cfg.CanonicalWallet.Issuer = "sub2api-gateway"
		cfg.CanonicalWallet.Audience = "shipany-control-plane"
		cfg.CanonicalWallet.Version = "v1"
		cfg.CanonicalWallet.Secret = strings.Repeat("y", 32)
		cfg.CanonicalWallet.EnforceReady = true
		cfg.BatchImage.Enabled = false
		require.NoError(t, cfg.Validate())

		cfg.CanonicalWallet.ReconciliationReadToken = short
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "canonical_wallet.reconciliation_read_token")
	})

	t.Run("the key is reachable from the environment", func(t *testing.T) {
		resetViperWithJWTSecret(t)
		t.Setenv("CANONICAL_WALLET_RECONCILIATION_READ_TOKEN", long)
		cfg, err := Load()
		require.NoError(t, err)
		require.Equal(t, long, cfg.CanonicalWallet.ReconciliationReadToken)
	})
}
