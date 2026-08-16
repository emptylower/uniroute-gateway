package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

type canonicalWalletStoreStub struct {
	lease      *CanonicalWalletLease
	reserveErr error
}

func (s *canonicalWalletStoreStub) InstallCanonicalWalletLease(_ context.Context, lease CanonicalWalletLease) error {
	s.lease = &lease
	return nil
}
func (s *canonicalWalletStoreStub) GetCanonicalWalletLease(context.Context, string) (*CanonicalWalletLease, error) {
	if s.lease == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	copy := *s.lease
	return &copy, nil
}
func (s *canonicalWalletStoreStub) ReserveCanonicalWalletLease(_ context.Context, _, _, _ string, amount int64, _ time.Time) (*CanonicalWalletReservation, error) {
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	copy := *s.lease
	copy.ConsumedMicros += amount
	s.lease = &copy
	return &CanonicalWalletReservation{Lease: copy}, nil
}

type canonicalWalletControlStub struct {
	leaseErr error
	lease    CanonicalWalletLease
}

func (s *canonicalWalletControlStub) AcquireLease(context.Context, canonicalWalletLeaseRequest) (*CanonicalWalletLease, error) {
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	copy := s.lease
	return &copy, nil
}
func (s *canonicalWalletControlStub) SubmitSettlement(context.Context, CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	return &CanonicalWalletSettlementResult{Accepted: true}, nil
}

func canonicalWalletTestConfig(mode string) config.CanonicalWalletConfig {
	return config.CanonicalWalletConfig{
		Mode: mode, ControlPlaneURL: "https://control.example.test", Issuer: "gateway", Audience: "control",
		Secret: strings.Repeat("w", 32), Version: "v1", LeaseTTLSeconds: 300, LeaseBudgetMicros: 1000,
		RequestTimeoutMS: 300, SettlementQueueSize: 1, SettlementWorkers: 1, EnforceReady: mode == config.CanonicalWalletModeEnforce,
	}
}

func TestCanonicalWalletCheckAndReserveFailsClosedOnlyInEnforceMode(t *testing.T) {
	failure := errors.New("control plane unavailable")
	event := CanonicalWalletSettlementEvent{GatewayRequestID: "req-1", PlatformUserID: "user-1", Currency: "CNY", AmountMicros: 1}

	shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure})
	allowed, err := shadow.CheckAndReserve(context.Background(), event)
	require.NoError(t, err)
	require.True(t, allowed)

	enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure})
	allowed, err = enforce.CheckAndReserve(context.Background(), event)
	require.ErrorIs(t, err, failure)
	require.False(t, allowed)
}

func TestCanonicalWalletHTTPClientUsesShortScopedAssertion(t *testing.T) {
	secret := strings.Repeat("s", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/internal/v1/wallet/leases/acquire", r.URL.Path)
		require.True(t, strings.HasPrefix(r.Header.Get("Idempotency-Key"), "gwlease_"))
		raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		claims := jwt.MapClaims{}
		token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
			require.Equal(t, jwt.SigningMethodHS256.Alg(), token.Method.Alg())
			require.Equal(t, "v7", token.Header["kid"])
			return []byte(secret), nil
		}, jwt.WithAudience("shipany"), jwt.WithIssuer("gateway"))
		require.NoError(t, err)
		require.True(t, token.Valid)
		require.Equal(t, canonicalWalletLeaseScope, claims["scope"])
		iat, _ := claims.GetIssuedAt()
		exp, _ := claims.GetExpirationTime()
		require.LessOrEqual(t, exp.Time.Sub(iat.Time), 60*time.Second)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-1","platform_user_id":"user-1","currency":"CNY","budget_micros":1000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`))
	}))
	defer server.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret, cfg.Issuer, cfg.Audience, cfg.Version = server.URL, secret, "gateway", "shipany", "v7"
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	lease, err := client.AcquireLease(context.Background(), canonicalWalletLeaseRequest{PlatformUserID: "user-1", Currency: "CNY", RequestedMicros: 1000, RequestedTTLSeconds: 60})
	require.NoError(t, err)
	require.Equal(t, "lease-1", lease.LeaseID)
}

func TestCanonicalWalletSettlementEventIDIsStableAndSensitive(t *testing.T) {
	a := CanonicalWalletSettlementEventID("req-1", "user-1", "CNY", 10)
	require.Equal(t, a, CanonicalWalletSettlementEventID("req-1", "user-1", "CNY", 10))
	require.NotEqual(t, a, CanonicalWalletSettlementEventID("req-1", "user-1", "CNY", 11))
}

func TestCanonicalWalletCNYMicrosMatchesShipAnyCredits(t *testing.T) {
	micros, err := canonicalWalletMicros(0.01)
	require.NoError(t, err)
	require.Equal(t, int64(canonicalWalletCNYMicrosPerCredit), micros)
}
