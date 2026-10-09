//go:build unit

package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCanonicalWalletSignedPoolRefreshRetainsExpiredReceiptsWithoutExtendingAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name         string
		signedExpiry time.Duration
		sealed       bool
	}{
		{"same expiry", 120 * time.Second, false},
		{"shortened signed expiry", 60 * time.Second, false},
		{"later signed expiry cannot extend cached expiry", 240 * time.Second, false},
		{"sealed receipt remains sealed", 120 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer rdb.Close()
			store := repository.NewGatewayCache(rdb).(service.CanonicalWalletLeaseStore)
			now := time.Now().UTC().Truncate(time.Millisecond)
			const user, leaseID = "pool-retention-user", "pool-retention-lease"
			expires := now.Add(120 * time.Second)
			signedExpires := now.Add(tc.signedExpiry)
			effectiveExpires := expires
			if signedExpires.Before(effectiveExpires) {
				effectiveExpires = signedExpires
			}
			lease := service.CanonicalWalletLease{LeaseID: leaseID, PlatformUserID: user, Currency: "USD", FundingScope: "legacy", BudgetUnits: 1000, ExpiresAt: expires, RetainUntil: expires.Add(1800 * time.Second)}
			require.NoError(t, store.InstallCanonicalWalletLease(ctx, lease))
			_, err := store.ReserveCanonicalWalletLease(ctx, user, leaseID, "USD", "prior-usage", 100, now)
			require.NoError(t, err)
			_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", "released-hold", 200, 1800000, now)
			require.NoError(t, err)
			_, err = store.ReleaseCanonicalWalletHold(ctx, user, "released-hold", "released", "")
			require.NoError(t, err)
			_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", "existing-hold", 100, 1800000, now)
			require.NoError(t, err)
			if tc.sealed {
				_, _, err = store.SealCanonicalWalletLease(ctx, user, leaseID)
				require.NoError(t, err)
			}
			before, err := store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
			require.NoError(t, err)
			require.True(t, before.RetainUntil.IsZero(), "real Redis getter omits the local-only retention field")
			beforeHold, err := store.GetCanonicalWalletHold(ctx, user, "existing-hold")
			require.NoError(t, err)
			amount := func(units int64) map[string]any {
				return map[string]any{"amount_units": strconv.FormatInt(units, 10), "currency": "USD", "scale": 8, "unit_version": "usd-e8-v1"}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				require.Equal(t, "/api/internal/v2/wallet/leases/pool", r.URL.Path)
				view := map[string]any{
					"lease_id": leaseID, "platform_user_id": user, "currency": "USD", "unit_version": "usd-e8-v1", "scale": 8,
					"usd_wallet_policy_version": config.CanonicalUSDWalletPolicyVersion,
					"funding_scope":             "legacy",
					"budget":                    amount(1200), "reserved": amount(0), "captured": amount(100), "released": amount(0),
					"capture_seq": 1, "status": "active", "expires_at": signedExpires,
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"leases": []map[string]any{view}}}))
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.CanonicalWallet = config.CanonicalWalletConfig{Mode: "enforce", USDWalletEnabled: true, USDPolicyVersion: config.CanonicalUSDWalletPolicyVersion, ControlPlaneURL: server.URL, Secret: "test-only-secret", RequestTimeoutMS: 2000}
			cfg.Gateway.ConcurrencySlotTTLMinutes = 30
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT receipt,funded_units,returned_units,return_revision,funding_scope,funding_owner_id,funding_issuance_key,pending_request FROM wallet_funding_freeze").WithArgs(user, leaseID).WillReturnRows(sqlmock.NewRows([]string{"receipt", "funded_units", "returned_units", "return_revision", "funding_scope", "funding_owner_id", "funding_issuance_key", "pending_request"}))
			mock.ExpectQuery("SELECT returned_units,return_revision,funded_units,funding_scope,funding_owner_id,funding_issuance_key FROM wallet_funding_freeze").WithArgs(user, leaseID).WillReturnRows(sqlmock.NewRows([]string{"returned_units", "return_revision", "funded_units", "funding_scope", "funding_owner_id", "funding_issuance_key"}))
			bridge := service.NewCanonicalWalletBridge(cfg, store, db, nil)
			defer bridge.Close()
			available, err := bridge.HasCanonicalWalletHeadroom(ctx, user, "USD")
			require.NoError(t, err)
			require.Equal(t, !tc.sealed, available)
			require.NoError(t, mock.ExpectationsWereMet(), "signed pool reads do not write SQL financial records")
			after, err := store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
			require.NoError(t, err)
			require.Equal(t, int64(1200), after.BudgetUnits)
			require.Equal(t, effectiveExpires, after.ExpiresAt)
			require.Equal(t, before.ConsumedUnits, after.ConsumedUnits)
			require.Equal(t, before.ReleasedUnits, after.ReleasedUnits)
			require.Equal(t, before.Sealed, after.Sealed)
			afterHold, err := store.GetCanonicalWalletHold(ctx, user, "existing-hold")
			require.NoError(t, err)
			require.Equal(t, beforeHold, afterHold)
			sum := sha256.Sum256([]byte(user))
			tag := "{" + hex.EncodeToString(sum[:]) + "}"
			key := "canonical_wallet:lease:" + tag + ":" + leaseID
			pointer := "canonical_wallet:current:" + tag
			for _, retainedKey := range []string{key, pointer} {
				ttl, err := rdb.PTTL(ctx, retainedKey).Result()
				require.NoError(t, err)
				require.InDelta(t, effectiveExpires.Add(1800*time.Second).Sub(now).Milliseconds(), ttl.Milliseconds(), 1000, "refresh preserves the receipt window")
			}
			mr.FastForward(130 * time.Second)
			_, err = store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
			require.NoError(t, err, "lease receipt survives beyond its authorization expiry")
			_, err = store.ReserveCanonicalWalletLease(ctx, user, leaseID, "USD", "expired-reservation", 1, now.Add(130*time.Second))
			require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExpired)
			_, _, _, err = store.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", "expired-hold", 1, 1800000, now.Add(130*time.Second))
			require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseExpired, "retention never makes expired principal authorizable")
			unchanged, err := store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
			require.NoError(t, err)
			require.Equal(t, after, unchanged)
			after.RequireCachedLease = true
			after.RetainUntil = effectiveExpires.Add(1800 * time.Second)
			require.NoError(t, rdb.Del(ctx, key).Err())
			require.ErrorContains(t, store.InstallCanonicalWalletLease(ctx, *after), "cached lease disappeared")
			exists, err := rdb.Exists(ctx, key).Result()
			require.NoError(t, err)
			require.Zero(t, exists, "funding refresh cannot reconstruct missing receipt state")
		})
	}
}
