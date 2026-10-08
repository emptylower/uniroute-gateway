package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Routing/credential fixtures use a real USD lease store and snapshot admission.
// Their upstream and persistence ports remain scoped to each behavior under test.
func newUSDHandlerTestWallet(t *testing.T, cfg *config.Config, billing *service.BillingService) (service.GatewayCache, *service.BillingSnapshotService) {
	t.Helper()
	if billing == nil {
		billing = service.NewBillingService(cfg, nil)
	}
	if cfg.Default.RateMultiplier <= 0 {
		cfg.Default.RateMultiplier = 1
	}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			PlatformUserID string `json:"platform_user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"usd_wallet_policy_version":"usd-wallet-v1","lease_id":"fixture-usd-lease","platform_user_id":%q,"currency":"USD","unit_version":"usd-e8-v1","scale":8,"budget":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"reserved":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"captured":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"released":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"headroom":{"amount_units":"100000000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"capture_seq":0,"status":"active","expires_at":%q,"outcome":"issued","clamped_by":"none"}}`, request.PlatformUserID, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	t.Cleanup(server.Close)
	cfg.CanonicalWallet.ControlPlaneURL = server.URL
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return repository.NewGatewayCache(client), service.NewBillingSnapshotService(cfg, service.NewModelPricingResolver(nil, billing), billing, service.NewUSDPriceService(cfg), nil)
}
