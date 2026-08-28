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
	// Phase 3.3a: how many times a lease was installed / reserved — the
	// ensureLease rejection tests assert a rejected grant is never installed.
	installCalls int
	reserveCalls int
}

func (s *canonicalWalletStoreStub) InstallCanonicalWalletLease(_ context.Context, lease CanonicalWalletLease) error {
	s.installCalls++
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
func (s *canonicalWalletStoreStub) GetCanonicalWalletLeaseByID(_ context.Context, _, leaseID string) (*CanonicalWalletLease, error) {
	if s.lease == nil || s.lease.LeaseID != leaseID {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	copy := *s.lease
	return &copy, nil
}
func (s *canonicalWalletStoreStub) ReserveCanonicalWalletLease(_ context.Context, _, _, _, _ string, amount int64, _ time.Time) (*CanonicalWalletReservation, error) {
	s.reserveCalls++
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	copy := *s.lease
	copy.ConsumedUnits += amount
	s.lease = &copy
	return &CanonicalWalletReservation{Lease: copy}, nil
}

type canonicalWalletControlStub struct {
	leaseErr error
	lease    CanonicalWalletLease
	// Phase 3.3a: capture of the last acquire request so the authorization
	// tests can assert exactly what ensureLease asked the control plane for.
	acquireCalls int
	lastRequest  canonicalWalletLeaseRequest
}

func (s *canonicalWalletControlStub) AcquireLease(_ context.Context, req canonicalWalletLeaseRequest) (*CanonicalWalletLease, error) {
	s.acquireCalls++
	s.lastRequest = req
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
		Secret: strings.Repeat("w", 32), Version: "v1", LeaseTTLSeconds: 300, LeaseBudgetUnits: 100000,
		RequestTimeoutMS: 300, SettlementQueueSize: 1, SettlementWorkers: 1, EnforceReady: mode == config.CanonicalWalletModeEnforce,
	}
}

func TestCanonicalWalletCheckAndReserveFailsClosedOnlyInEnforceMode(t *testing.T) {
	failure := errors.New("control plane unavailable")
	event := CanonicalWalletSettlementEvent{GatewayRequestID: "req-1", PlatformUserID: "user-1", Currency: "CNY", AmountUnits: 1}

	shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure}, nil, nil)
	allowed, err := shadow.CheckAndReserve(context.Background(), event)
	require.NoError(t, err)
	require.True(t, allowed)

	enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure}, nil, nil)
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

func TestCanonicalWalletSettlementEventIDIsStableAcrossRepricing(t *testing.T) {
	// Same request, two different computed amounts (e.g. a pricing
	// correction) — the amount must NOT be part of the identity. This test
	// only proves the ID function's own stability (the corrected signature
	// takes no amount argument at all — that's the fix). The full repricing
	// scenario, INCLUDING the fixture's actual amount_units values, is
	// proven end-to-end by Task 4's
	// TestWalletOutboxRejectsConflictingPayloadUnderSameEventID, which loads
	// them from the shared fixture file rather than hardcoding matching
	// literals.
	const requestID = "req_fixture_repricing_0001"
	idA := CanonicalWalletSettlementEventID(requestID, "user-1", "CNY")
	idB := CanonicalWalletSettlementEventID(requestID, "user-1", "CNY")
	require.Equal(t, idA, idB, "identical request identity must always produce the identical event ID regardless of amount")

	differentRequest := CanonicalWalletSettlementEventID("req-other", "user-1", "CNY")
	require.NotEqual(t, idA, differentRequest, "different request identity must produce a different event ID")

	differentUser := CanonicalWalletSettlementEventID(requestID, "user-2", "CNY")
	require.NotEqual(t, idA, differentUser, "different platform_user_id must produce a different event ID")
}

func TestCanonicalWalletUnitsFromCNYMatchesUnitsPerCNYConstant(t *testing.T) {
	units, err := canonicalWalletUnitsFromCNY(1.0) // 1 CNY
	require.NoError(t, err)
	require.Equal(t, int64(canonicalWalletUnitsPerCNY), units)
	require.Equal(t, int64(100_000_000), units, "cny-e8-v1: 1 CNY must be exactly 100,000,000 units, not 1,000,000")
}

// newBridgeForEnsureLeaseTest builds a bridge over the stubs with the 500M-unit
// budget the Phase 3.3a ensureLease tests assume; the store stub starts with no
// current lease, so ensureLease always goes to the control plane.
func newBridgeForEnsureLeaseTest(t *testing.T) (*CanonicalWalletBridge, *canonicalWalletStoreStub, *canonicalWalletControlStub) {
	t.Helper()
	store := &canonicalWalletStoreStub{}
	control := &canonicalWalletControlStub{}
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.LeaseBudgetUnits = 500_000_000
	return newCanonicalWalletBridge(cfg, store, control, nil, nil), store, control
}

func TestEnsureLeaseRejectsGrantBelowAmount(t *testing.T) {
	// The control plane clamps to available balance and returns a lease whose
	// budget is below the amount being authorized (spec §2.0.1 step (3)).
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 100_000_000, ConsumedUnits: 0, ExpiresAt: time.Now().Add(5 * time.Minute)}
	_, err := b.ensureLease(context.Background(), "user-1", "CNY", 200_000_000)
	require.ErrorIs(t, err, ErrCanonicalWalletBalanceShortfall)
	require.Equal(t, 0, store.installCalls, "a rejected grant is never installed")
}

func TestEnsureLeaseRejectsExpiredGrant(t *testing.T) {
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(-time.Second)}
	_, err := b.ensureLease(context.Background(), "user-1", "CNY", 1_000_000)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExpired)
	require.Equal(t, 0, store.installCalls)
}

func TestEnsureLeaseAcceptsGrantBelowBudgetButAboveAmount(t *testing.T) {
	// A lease below lease_budget_units is normal (both routes clamp to balance);
	// only a lease below the AMOUNT is rejected (index 3.3 exit).
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 3_000_000, ExpiresAt: time.Now().Add(5 * time.Minute)}
	lease, err := b.ensureLease(context.Background(), "user-1", "CNY", 1_000_000)
	require.NoError(t, err)
	require.Equal(t, "lease-1", lease.LeaseID)
	require.Equal(t, 1, store.installCalls)
}
