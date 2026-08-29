package service

import (
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// canonicalWalletWiringMissing counts every shadow-mode construction whose
// cache did not provide the lease store (Phase 3.8-G, redesign §14.3 a). It
// is package-level rather than a field of canonicalWalletMetrics because
// 3.8-G's file allowlist touches only this new file and one line at each of
// the three bridge sites — folding it into CanonicalWalletBridgeStats() is
// left to a later phase alongside the stats surface.
var canonicalWalletWiringMissing atomic.Int64

// requireCanonicalWalletStore is the runtime-wiring gate of redesign §14.3
// (3.3a's hand-on): the three bridge-construction sites (NewGatewayService,
// NewOpenAIGatewayService, ProvideBillingCacheService) derive the lease
// store from the shared gateway cache by type assertion, and a cache that
// does not implement CanonicalWalletLeaseStore used to leave the bridge nil
// SILENTLY — in enforce that is a fail-OPEN (handles are token-only and
// nothing is withheld). The gate makes the miss loud:
//
//   - enforce  → panic naming the site. The precedent for a fail-closed
//     startup panic is 3.4a's enforce probe (canonical_wallet_bridge.go's
//     enforce_ready check): a misconfigured deployment must not start.
//   - shadow   → the existing error log (with the site) plus the
//     canonicalWalletWiringMissing counter; shadow observes, it must not
//     take the process down.
//   - disabled (or a nil cfg — the handler-package constructions pass one)
//     → silent, byte-for-byte today's behavior.
//
// A cache that implements the store passes in every mode; the mode never
// gates whether the bridge is built, only what a miss means.
func requireCanonicalWalletStore(cfg *config.Config, cache any, site string) (CanonicalWalletLeaseStore, bool) {
	store, ok := cache.(CanonicalWalletLeaseStore)
	if ok {
		return store, true
	}
	if cfg == nil {
		return nil, false
	}
	switch cfg.CanonicalWallet.Mode {
	case config.CanonicalWalletModeEnforce:
		panic(fmt.Sprintf("canonical_wallet.mode=enforce but the gateway cache does not provide the wallet store (CanonicalWalletLeaseStore) at %s — the gate would fail OPEN (3.3a hand-on; redesign §14.3)", site))
	case config.CanonicalWalletModeShadow:
		slog.Error("canonical wallet bridge configured without a Redis lease store; shadow observations are disabled", "site", site)
		canonicalWalletWiringMissing.Add(1)
	}
	return nil, false
}
