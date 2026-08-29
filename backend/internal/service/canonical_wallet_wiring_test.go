//go:build unit

package service

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// wiringGateNonStoreCache is a GatewayCache (the sticky-session interface)
// that does NOT implement CanonicalWalletLeaseStore — the gate's miss case
// at the two gateway constructors.
type wiringGateNonStoreCache struct{}

func (c *wiringGateNonStoreCache) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	return 0, nil
}
func (c *wiringGateNonStoreCache) SetSessionAccountID(context.Context, int64, string, int64, time.Duration) error {
	return nil
}
func (c *wiringGateNonStoreCache) RefreshSessionTTL(context.Context, int64, string, time.Duration) error {
	return nil
}
func (c *wiringGateNonStoreCache) DeleteSessionAccountID(context.Context, int64, string) error {
	return nil
}

// Phase 3.8-G Task 1 (redesign §14.3 a): enforce must refuse to start when
// the gateway cache does not provide CanonicalWalletLeaseStore — the three
// bridge-construction sites used to leave the bridge nil silently, which in
// enforce is a fail-OPEN (3.3a's hand-on). These tests prove the helper's
// four arms and the gate AT the constructors (a helper-level test proves the
// helper; only a constructor-level test proves that a site calls it —
// round-1 MAJOR-1). ProvideBillingCacheService's constructor-level proof
// lives in canonical_wallet_admission_integration_test.go (the tag of the
// :1041 construction it mirrors).

// panicsReturningMessage runs fn, requiring a panic, and returns the
// recovered value's string form.
func panicsReturningMessage(t *testing.T, fn func()) string {
	t.Helper()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		fn()
	}()
	require.NotNil(t, recovered, "the gate must panic")
	msg, ok := recovered.(string)
	require.True(t, ok, "the gate must panic with a string message, got %T", recovered)
	return msg
}

func wiringGateConfig(mode string) *config.Config {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet = canonicalWalletTestConfig(mode)
	return cfg
}

func TestCanonicalWalletWiringGateHelper(t *testing.T) {
	// A plain service object that does NOT implement
	// CanonicalWalletLeaseStore — the helper's miss case (the cache
	// parameter is `any` at the helper, so any non-store value exercises
	// it; the constructor tests below use the interface-typed stub).
	nonStore := &BillingCacheService{}

	t.Run("enforce panics naming the site when the cache is not a store", func(t *testing.T) {
		msg := panicsReturningMessage(t, func() {
			_, _ = requireCanonicalWalletStore(wiringGateConfig(config.CanonicalWalletModeEnforce), nonStore, "unit-test-site")
		})
		require.Contains(t, msg, "unit-test-site", "the panic must name the site")
		require.Contains(t, msg, "does not provide the wallet store")
	})

	t.Run("shadow counts and returns nil-false without panicking", func(t *testing.T) {
		var logs strings.Builder
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
		defer slog.SetDefault(previous)

		before := canonicalWalletWiringMissing.Load()
		store, ok := requireCanonicalWalletStore(wiringGateConfig(config.CanonicalWalletModeShadow), nonStore, "unit-test-site")
		require.False(t, ok)
		require.Nil(t, store)
		require.Equal(t, before+1, canonicalWalletWiringMissing.Load(), "shadow must count canonicalWalletWiringMissing")
		require.Contains(t, logs.String(), "Redis lease store", "shadow logs the existing message")
	})

	t.Run("disabled is silent", func(t *testing.T) {
		var logs strings.Builder
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
		defer slog.SetDefault(previous)

		before := canonicalWalletWiringMissing.Load()
		store, ok := requireCanonicalWalletStore(wiringGateConfig(config.CanonicalWalletModeDisabled), nonStore, "unit-test-site")
		require.False(t, ok)
		require.Nil(t, store)
		require.Zero(t, canonicalWalletWiringMissing.Load()-before, "disabled must not count")
		require.Empty(t, logs.String(), "disabled must not log")
	})

	t.Run("a nil cfg is silent (the handler-package constructions pass nil)", func(t *testing.T) {
		store, ok := requireCanonicalWalletStore(nil, nonStore, "unit-test-site")
		require.False(t, ok)
		require.Nil(t, store)
	})

	t.Run("a cache that implements the store passes in every mode", func(t *testing.T) {
		for _, mode := range []string{config.CanonicalWalletModeEnforce, config.CanonicalWalletModeShadow, config.CanonicalWalletModeDisabled} {
			store := &canonicalWalletStoreStub{}
			got, ok := requireCanonicalWalletStore(wiringGateConfig(mode), store, "unit-test-site")
			require.True(t, ok, "mode %s", mode)
			require.Same(t, store, got, "mode %s", mode)
		}
	})
}

// newWiringGateGatewayService is gateway_record_usage_test.go:20's
// construction (the cheapest in-package NewGatewayService build) with the
// cfg and cache swapped for the gate's arms.
func newWiringGateGatewayService(cache GatewayCache, cfg *config.Config) *GatewayService {
	return NewGatewayService(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cache,
		cfg, nil, nil,
		nil,
		nil,
		NewBillingService(cfg, nil),
		nil,
		nil,
		nil,
		nil,
		&DeferredService{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil, // userPlatformQuotaRepo
	)
}

func TestCanonicalWalletWiringGateAtNewGatewayService(t *testing.T) {
	// wiringGateNonStoreCache: a GatewayCache that does NOT implement
	// CanonicalWalletLeaseStore — the construction must refuse in enforce.
	msg := panicsReturningMessage(t, func() {
		_ = newWiringGateGatewayService(&wiringGateNonStoreCache{}, wiringGateConfig(config.CanonicalWalletModeEnforce))
	})
	require.Contains(t, msg, "NewGatewayService")

	before := canonicalWalletWiringMissing.Load()
	svc := newWiringGateGatewayService(&wiringGateNonStoreCache{}, wiringGateConfig(config.CanonicalWalletModeShadow))
	require.NotNil(t, svc)
	require.Nil(t, svc.canonicalWallet, "shadow constructs with the bridge unwired")
	require.Equal(t, before+1, canonicalWalletWiringMissing.Load())
}

func TestCanonicalWalletWiringGateAtNewOpenAIGatewayService(t *testing.T) {
	msg := panicsReturningMessage(t, func() {
		_ = NewOpenAIGatewayService(
			nil, nil, nil, nil, nil, nil,
			&wiringGateNonStoreCache{},
			wiringGateConfig(config.CanonicalWalletModeEnforce),
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		)
	})
	require.Contains(t, msg, "NewOpenAIGatewayService")

	before := canonicalWalletWiringMissing.Load()
	svc := NewOpenAIGatewayService(
		nil, nil, nil, nil, nil, nil,
		&wiringGateNonStoreCache{},
		wiringGateConfig(config.CanonicalWalletModeShadow),
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	require.NotNil(t, svc)
	require.Nil(t, svc.canonicalWallet, "shadow constructs with the bridge unwired")
	require.Nil(t, svc.authorizer, "the authorizer follows the bridge")
	require.Equal(t, before+1, canonicalWalletWiringMissing.Load())
}
