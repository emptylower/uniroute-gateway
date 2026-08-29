//go:build integration

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Phase 3.4a (redesign §9.6 item 6): the rollout guard. ShipAny 3.4a-S deploys
// FIRST; this half refuses to enforce against a control plane that has no
// /ensure route, counts itself in shadow, and classifies a runtime 404/405
// from the route as ErrCanonicalWalletControlPlaneIncompatible (transient).
// The metrics are GLOBAL atomics shared across every test in the process —
// every assertion here is on a DELTA, never an absolute value
// (canonical_wallet_wire_http_test.go states the rule).
func p34ProbeConfig(mode, url string) *config.Config {
	cfg := &config.Config{}
	cfg.CanonicalWallet = canonicalWalletTestConfig(mode)
	cfg.CanonicalWallet.ControlPlaneURL, cfg.CanonicalWallet.Secret = url, strings.Repeat("s", 32)
	return cfg
}

func TestPhase34ProbeEnsureRouteAtStartup(t *testing.T) {
	ctx := context.Background()

	t.Run("route present, shadow: probe once, compatible", func(t *testing.T) {
		fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })
		beforeProbes := fake.probeCallsLocked()
		baseIncompat := CanonicalWalletBridgeStats()["control_plane_incompatible"]
		b := NewCanonicalWalletBridge(p34ProbeConfig(config.CanonicalWalletModeShadow, fake.Server.URL), &canonicalWalletStoreStub{}, nil, nil)
		t.Cleanup(b.Close)
		require.NotNil(t, b)
		require.Equal(t, 1, fake.probeCallsLocked()-beforeProbes, "the probe ran exactly once at construction")
		require.Equal(t, int64(0), CanonicalWalletBridgeStats()["control_plane_incompatible"]-baseIncompat, "a 405 is the route present — compatible")
	})

	t.Run("route missing, shadow: counted, and runtime ensures are incompatible", func(t *testing.T) {
		fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })
		fake.withoutEnsureRoute = true
		baseIncompat := CanonicalWalletBridgeStats()["control_plane_incompatible"]
		b := NewCanonicalWalletBridge(p34ProbeConfig(config.CanonicalWalletModeShadow, fake.Server.URL), &canonicalWalletStoreStub{}, nil, nil)
		t.Cleanup(b.Close)
		require.NotNil(t, b, "shadow observes; it must not take the process down")
		require.Equal(t, int64(1), CanonicalWalletBridgeStats()["control_plane_incompatible"]-baseIncompat)
		_, err := b.ensureLease(ctx, "user-probe", "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
		require.ErrorIs(t, err, ErrCanonicalWalletControlPlaneIncompatible, "a runtime 404 from /ensure is classified control_plane_incompatible")
	})

	t.Run("route missing, enforce: refuses to start", func(t *testing.T) {
		fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })
		fake.withoutEnsureRoute = true
		require.PanicsWithValue(t,
			"canonical_wallet: control plane has no ensure route — deploy ShipAny 3.4a-S first (redesign §9.6 item 6)",
			func() {
				NewCanonicalWalletBridge(p34ProbeConfig(config.CanonicalWalletModeEnforce, fake.Server.URL), &canonicalWalletStoreStub{}, nil, nil)
			},
		)
	})

	t.Run("disabled: no probe at all, no bridge", func(t *testing.T) {
		fake := newFakeEnsureControlPlane(t, func() time.Time { return time.Now().UTC() })
		beforeProbes := fake.probeCallsLocked()
		b := NewCanonicalWalletBridge(p34ProbeConfig(config.CanonicalWalletModeDisabled, fake.Server.URL), &canonicalWalletStoreStub{}, nil, nil)
		require.Nil(t, b, "disabled constructs no bridge — a refactor that probes before the mode check is caught here")
		require.Equal(t, 0, fake.probeCallsLocked()-beforeProbes, "the probe never runs in disabled mode")
	})

	t.Run("probe answers 500, enforce: probe_failed, no panic", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		baseProbeFailed := CanonicalWalletBridgeStats()["control_plane_probe_failed"]
		b := NewCanonicalWalletBridge(p34ProbeConfig(config.CanonicalWalletModeEnforce, srv.URL), &canonicalWalletStoreStub{}, nil, nil)
		t.Cleanup(b.Close)
		require.NotNil(t, b, "a WAF, a misrouted URL or a transient must never crash-loop a healthy deployment")
		require.Equal(t, int64(1), CanonicalWalletBridgeStats()["control_plane_probe_failed"]-baseProbeFailed)
	})
}
