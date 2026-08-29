package service

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

type canonicalWalletStoreStub struct {
	lease      *CanonicalWalletLease
	reserveErr error
	// getErr/getCalls (Phase 3.6, test 45): the STORE-OUTAGE injection —
	// both getters return it (and count the call) so the fail-closed arms
	// can be driven without Redis; a transport failure is not a cache miss.
	getErr   error
	getCalls int
	// Phase 3.3a: how many times a lease was installed / reserved — the
	// ensureLease rejection tests assert a rejected grant is never installed.
	installCalls int
	reserveCalls int
	// lastReserveNow (Phase 3.4a, §9.5): the `now` the bridge handed the
	// reserve script — the clock-injection test asserts it is b.clock().
	lastReserveNow time.Time
	// neverPersist (review note M3): InstallCanonicalWalletLease becomes a
	// no-op so benchmark iterations keep missing.
	neverPersist bool
	// holds (Phase 3.4b, Task 2): the recording hold map the unit authorizer
	// tests drive — arm/release/convert/mark-class with the scripts' branch
	// semantics over in-memory state, plus the counters test 29's unit half
	// asserts.
	holds         map[string]*CanonicalWalletHold
	armCalls      int
	armErr        error // when non-nil, ArmCanonicalWalletHold refuses with it (the {4} leg)
	releasedUnits int64 // per-lease released figure the release/convert stubs raise
	holdUsers     map[string]bool
	emptyMarkers  map[string]bool
	// releaseReservationCalls (Phase 3.5, Task 2): the recorded
	// ReleaseCanonicalWalletReservation calls.
	releaseReservationCalls []canonicalWalletReservationReleaseCall
}

func (s *canonicalWalletStoreStub) holdMap() map[string]*CanonicalWalletHold {
	if s.holds == nil {
		s.holds = map[string]*CanonicalWalletHold{}
	}
	return s.holds
}

func (s *canonicalWalletStoreStub) InstallCanonicalWalletLease(_ context.Context, lease CanonicalWalletLease) error {
	s.installCalls++
	// Phase 3.3a review note M3: neverPersist keeps the store empty so a
	// benchmark can measure the MISS path on every iteration (otherwise the
	// first install turns iterations 2..N into hits).
	if s.neverPersist {
		return nil
	}
	s.lease = &lease
	return nil
}
func (s *canonicalWalletStoreStub) GetCanonicalWalletLease(context.Context, string) (*CanonicalWalletLease, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.lease == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	copy := *s.lease
	return &copy, nil
}
func (s *canonicalWalletStoreStub) GetCanonicalWalletLeaseByID(_ context.Context, _, leaseID string) (*CanonicalWalletLease, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.lease == nil || s.lease.LeaseID != leaseID {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	copy := *s.lease
	return &copy, nil
}
func (s *canonicalWalletStoreStub) ReserveCanonicalWalletLease(_ context.Context, _, _, _, _ string, amount int64, now time.Time) (*CanonicalWalletReservation, error) {
	s.reserveCalls++
	s.lastReserveNow = now
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	copy := *s.lease
	copy.ConsumedUnits += amount
	s.lease = &copy
	return &CanonicalWalletReservation{Lease: copy}, nil
}

func (s *canonicalWalletStoreStub) SealCanonicalWalletLease(_ context.Context, _, leaseID string) (int64, int64, error) {
	if s.lease == nil || s.lease.LeaseID != leaseID {
		return 0, 0, ErrCanonicalWalletLeaseMissing
	}
	pre := s.lease.ConsumedUnits
	released := s.releasedUnits
	sealed := *s.lease
	sealed.ConsumedUnits = sealed.BudgetUnits
	s.lease = &sealed
	return pre, released, nil
}

func (s *canonicalWalletStoreStub) ArmCanonicalWalletHold(_ context.Context, platformUserID, leaseID, _, authorizationID string, units int64, _ int64, now time.Time) (string, int64, bool, error) {
	s.armCalls++
	if s.armErr != nil {
		return "", 0, false, s.armErr
	}
	if existing, ok := s.holdMap()[authorizationID]; ok {
		return existing.LeaseID, existing.HeldUnits, true, nil
	}
	if s.lease != nil {
		copy := *s.lease
		copy.ConsumedUnits += units
		s.lease = &copy
	}
	s.holdMap()[authorizationID] = &CanonicalWalletHold{
		AuthorizationID: authorizationID, LeaseID: leaseID, HeldUnits: units, ArmedAt: now, State: "armed",
	}
	if s.holdUsers == nil {
		s.holdUsers = map[string]bool{}
	}
	s.holdUsers[platformUserID] = true
	return leaseID, units, false, nil
}

func (s *canonicalWalletStoreStub) ReleaseCanonicalWalletHold(_ context.Context, _, authorizationID, stateAfter, classAfter string) (int64, error) {
	hold, ok := s.holdMap()[authorizationID]
	if !ok {
		return 0, ErrCanonicalWalletHoldMissing
	}
	if hold.State != "armed" {
		return 0, &CanonicalWalletHoldNotArmedError{State: hold.State, EventID: hold.EventID}
	}
	s.releasedUnits += hold.HeldUnits
	hold.State = stateAfter
	if classAfter != "" {
		hold.Class = classAfter
	}
	return hold.HeldUnits, nil
}

func (s *canonicalWalletStoreStub) ConvertCanonicalWalletHold(_ context.Context, _, authorizationID, eventID string, actualUnits int64, _ time.Time) (CanonicalWalletHoldConversion, error) {
	hold, ok := s.holdMap()[authorizationID]
	if !ok {
		return CanonicalWalletHoldConversion{Code: 1}, nil
	}
	if hold.State != "armed" {
		return CanonicalWalletHoldConversion{Code: 7, State: hold.State, EventID: hold.EventID}, nil
	}
	if actualUnits <= hold.HeldUnits {
		s.releasedUnits += hold.HeldUnits - actualUnits
		hold.State = "settled"
		hold.EventID = eventID
		return CanonicalWalletHoldConversion{Code: 0, LeaseID: hold.LeaseID}, nil
	}
	excess := actualUnits - hold.HeldUnits
	if s.lease != nil && s.lease.ConsumedUnits-s.releasedUnits+excess <= s.lease.BudgetUnits {
		copy := *s.lease
		copy.ConsumedUnits += excess
		s.lease = &copy
		hold.State = "settled"
		hold.EventID = eventID
		return CanonicalWalletHoldConversion{Code: 0, LeaseID: hold.LeaseID}, nil
	}
	s.releasedUnits += hold.HeldUnits
	hold.State = "released"
	return CanonicalWalletHoldConversion{Code: 4}, nil
}

func (s *canonicalWalletStoreStub) GetCanonicalWalletHold(_ context.Context, _, authorizationID string) (*CanonicalWalletHold, error) {
	hold, ok := s.holdMap()[authorizationID]
	if !ok {
		return nil, ErrCanonicalWalletHoldMissing
	}
	copy := *hold
	return &copy, nil
}

func (s *canonicalWalletStoreStub) ListCanonicalWalletHolds(_ context.Context, _ string, limit int) ([]string, error) {
	var ids []string
	for id := range s.holdMap() {
		ids = append(ids, id)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}
	return ids, nil
}

func (s *canonicalWalletStoreStub) ListCanonicalWalletHoldUsers(_ context.Context, _ uint64, _ int64) ([]string, uint64, error) {
	var users []string
	for u := range s.holdUsers {
		users = append(users, u)
	}
	return users, 0, nil
}

func (s *canonicalWalletStoreStub) PruneCanonicalWalletHoldUser(_ context.Context, platformUserID string) error {
	delete(s.holdUsers, platformUserID)
	return nil
}

func (s *canonicalWalletStoreStub) TryCanonicalWalletReaperLease(context.Context, time.Duration) (bool, error) {
	return true, nil
}

func (s *canonicalWalletStoreStub) ForgetCanonicalWalletHold(_ context.Context, _, authorizationID string) error {
	// SREM-only on the real store; the recording stub drops the hold so
	// repeated sweeps stay finite in unit tests.
	delete(s.holdMap(), authorizationID)
	return nil
}

func (s *canonicalWalletStoreStub) MarkCanonicalWalletHoldUserEmpty(_ context.Context, platformUserID string, _ time.Duration) (bool, error) {
	if s.emptyMarkers == nil {
		s.emptyMarkers = map[string]bool{}
	}
	seen := s.emptyMarkers[platformUserID]
	s.emptyMarkers[platformUserID] = true
	return seen, nil
}

func (s *canonicalWalletStoreStub) ClearCanonicalWalletHoldUserEmpty(_ context.Context, platformUserID string) error {
	delete(s.emptyMarkers, platformUserID)
	return nil
}

func (s *canonicalWalletStoreStub) MarkCanonicalWalletHoldClass(_ context.Context, _, authorizationID, class string) (*CanonicalWalletHold, error) {
	hold, ok := s.holdMap()[authorizationID]
	if !ok {
		return nil, ErrCanonicalWalletHoldMissing
	}
	if hold.State != "armed" {
		return nil, &CanonicalWalletHoldNotArmedError{State: hold.State, EventID: hold.EventID}
	}
	hold.Class = class
	copy := *hold
	return &copy, nil
}

// releaseReservationCalls records ReleaseCanonicalWalletReservation's calls
// (Phase 3.5, Task 2) — the unit tests assert the dispatcher's dispositions
// through this stub without Redis.
type canonicalWalletReservationReleaseCall struct {
	PlatformUserID string
	LeaseID        string
	EventID        string
	Units          int64
	DropMarker     bool
}

func (s *canonicalWalletStoreStub) ReleaseCanonicalWalletReservation(_ context.Context, platformUserID, leaseID, eventID string, units int64, dropMarker bool) (bool, error) {
	s.releaseReservationCalls = append(s.releaseReservationCalls, canonicalWalletReservationReleaseCall{
		PlatformUserID: platformUserID, LeaseID: leaseID, EventID: eventID, Units: units, DropMarker: dropMarker,
	})
	return true, nil
}

type canonicalWalletControlStub struct {
	leaseErr error
	lease    CanonicalWalletLease
	// Phase 3.3a: capture of the last acquire request so the authorization
	// tests can assert exactly what ensureLease asked the control plane for.
	// Phase 3.4 (Task 3a): the request is the v2 ensure wire; the stub
	// returns the lease inside a canonicalWalletEnsureResult.
	ensureCalls int
	lastEnsure  canonicalWalletEnsureRequest
	// outcome ("reused" | "issued") defaults to "issued" when empty.
	outcome string
	// nilLease makes EnsureLease return the (nil, nil) grant ensureLease names.
	nilLease bool
}

func (s *canonicalWalletControlStub) EnsureLease(_ context.Context, req canonicalWalletEnsureRequest) (*canonicalWalletEnsureResult, error) {
	s.ensureCalls++
	s.lastEnsure = req
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	if s.nilLease {
		return nil, nil
	}
	outcome := s.outcome
	if outcome == "" {
		outcome = "issued"
	}
	return &canonicalWalletEnsureResult{Lease: s.lease, Outcome: outcome, ClampedBy: "none"}, nil
}
func (s *canonicalWalletControlStub) SubmitSettlement(context.Context, CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	return &CanonicalWalletSettlementResult{Accepted: true}, nil
}

func canonicalWalletTestConfig(mode string) config.CanonicalWalletConfig {
	return config.CanonicalWalletConfig{
		Mode: mode, ControlPlaneURL: "https://control.example.test", Issuer: "gateway", Audience: "control",
		Secret: strings.Repeat("w", 32), Version: "v1", LeaseTTLSeconds: 300, LeaseBudgetUnits: 100000,
		RequestTimeoutMS: 300, ExpirySkewMarginMS: 100, SettlementQueueSize: 1, SettlementWorkers: 1, EnforceReady: mode == config.CanonicalWalletModeEnforce,
		BillingSnapshotMode: "record", LiveWindowMinSeconds: 20, LiveControllerTakeoverSeconds: 15,
		ReceivableRedriveIntervalSeconds: 60, ReceivableRedriveMaxAttempts: 2, RetentionDays: 45,
	}
}

func TestCanonicalWalletCheckAndReserveFailsClosedOnlyInEnforceMode(t *testing.T) {
	failure := errors.New("control plane unavailable")
	event := CanonicalWalletSettlementEvent{GatewayRequestID: "req-1", PlatformUserID: "user-1", Currency: "CNY", AmountUnits: 1}

	shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure}, nil, nil, 0, nil)
	t.Cleanup(shadow.Close)
	allowed, err := shadow.CheckAndReserve(context.Background(), event)
	require.NoError(t, err)
	require.True(t, allowed)

	enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{leaseErr: failure}, nil, nil, 0, nil)
	t.Cleanup(enforce.Close)
	allowed, err = enforce.CheckAndReserve(context.Background(), event)
	require.ErrorIs(t, err, failure)
	require.False(t, allowed)
}

func TestCanonicalWalletHTTPClientUsesShortScopedAssertion(t *testing.T) {
	secret := strings.Repeat("s", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/internal/v2/wallet/leases/ensure", r.URL.Path)
		require.Empty(t, r.Header.Get("Idempotency-Key"), "ensure is idempotent by transaction: no gwlease_ window key")
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
		_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-1","platform_user_id":"user-1","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"1000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"1000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
	}))
	defer server.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret, cfg.Issuer, cfg.Audience, cfg.Version = server.URL, secret, "gateway", "shipany", "v7"
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	res, err := client.EnsureLease(context.Background(), canonicalWalletEnsureRequest{PlatformUserID: "user-1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(1000), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800})
	require.NoError(t, err)
	require.Equal(t, "lease-1", res.Lease.LeaseID)
}

// mustUnits parses an amount object with the production-strict parser — the
// test fakes and wire assertions decode §9.2's objects through exactly the
// code the HTTP client uses, never a lenient second implementation.
func mustUnits(a canonicalWalletAmountObject) int64 {
	v, err := parseCanonicalWalletAmountObject("test", a)
	if err != nil {
		panic(err)
	}
	return v
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
	b := newCanonicalWalletBridge(cfg, store, control, nil, nil, 0, nil)
	t.Cleanup(b.Close)
	return b, store, control
}

func TestEnsureLeaseRejectsGrantBelowAmount(t *testing.T) {
	// The control plane clamps to available balance and returns a lease whose
	// budget is below the amount being authorized. §9.3: this local guard is
	// TRANSIENT and carries its own sentinel — unreachable against a
	// §3-conformant server, retried on the outbox backoff; the server's
	// insufficient_balance refusal keeps ErrCanonicalWalletBalanceShortfall.
	// §11.3 (MAJOR-2 of the §11 review): the guard is AUTHORIZE-ONLY — the
	// settle purpose installs the under-granted lease so the dispatcher can
	// split before reserving (its sibling below).
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 100_000_000, ConsumedUnits: 0, ExpiresAt: time.Now().Add(5 * time.Minute)}
	_, err := b.ensureLease(context.Background(), "user-1", "CNY", 200_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseGrantBelowAmount)
	require.NotErrorIs(t, err, ErrCanonicalWalletBalanceShortfall)
	require.Equal(t, 0, store.installCalls, "a rejected grant is never installed")
}

// TestEnsureLeaseSettlePurposeInstallsTheUnderGrant (Phase 3.5, §11.3): on
// the settle purpose the under-granted lease is INSTALLED and returned —
// what makes the dispatcher's proactive split terminate. The authorize
// purpose keeps refusing (its sibling above).
func TestEnsureLeaseSettlePurposeInstallsTheUnderGrant(t *testing.T) {
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 100_000_000, ConsumedUnits: 0, ExpiresAt: time.Now().Add(5 * time.Minute)}
	lease, err := b.ensureLease(context.Background(), "user-1", "CNY", 200_000_000, canonicalWalletLeasePurposeSettle, "")
	require.NoError(t, err, "the settle purpose never refuses an under-grant")
	require.Equal(t, 1, store.installCalls, "§11.3: the under-granted lease is installed")
	require.NotNil(t, store.lease, "its hash exists")
	require.Equal(t, int64(100_000_000), lease.RemainingUnits(), "the returned lease's remaining is the granted budget B")
	require.Equal(t, "lease-1", lease.LeaseID)
}

func TestEnsureLeaseRejectsExpiredGrant(t *testing.T) {
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(-time.Second)}
	_, err := b.ensureLease(context.Background(), "user-1", "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExpired)
	require.Equal(t, 0, store.installCalls)
}

func TestEnsureLeaseAcceptsGrantBelowBudgetButAboveAmount(t *testing.T) {
	// A lease below lease_budget_units is normal (both routes clamp to balance);
	// only a lease below the AMOUNT is rejected (index 3.3 exit).
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 3_000_000, ExpiresAt: time.Now().Add(5 * time.Minute)}
	lease, err := b.ensureLease(context.Background(), "user-1", "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "lease-1", lease.LeaseID)
	require.Equal(t, 1, store.installCalls)
}

func TestObserveSettlementReturnBoolGuards(t *testing.T) {
	// disabled mode
	disabled := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeDisabled), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	t.Cleanup(disabled.Close)
	require.False(t, disabled.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 100}))

	// nil bridge
	var nilBridge *CanonicalWalletBridge
	require.False(t, nilBridge.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 100}))

	// shadow mode with nil outboxDB / outbox
	shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	require.False(t, shadow.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 100}))
	t.Cleanup(shadow.Close)

	// missing platform user id
	require.False(t, shadow.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "", Currency: "CNY", AmountUnits: 100}))

	// invalid currency
	require.False(t, shadow.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "USD", AmountUnits: 100}))

	// non-positive amount
	require.False(t, shadow.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 0}))

	// committed -> true
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	outboxStub := &outboxStoreStub{}
	mock.ExpectBegin()
	mock.ExpectCommit()
	shadowWithOutbox := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, db, outboxStub, 0, nil)
	t.Cleanup(shadowWithOutbox.Close)
	require.True(t, shadowWithOutbox.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 100}))
	require.NoError(t, mock.ExpectationsWereMet())

	// failing store -> false
	dbFail, mockFail, err := sqlmock.New()
	require.NoError(t, err)
	defer dbFail.Close()

	failingOutbox := &outboxStoreStub{insertErr: errors.New("db insert failed")}
	mockFail.ExpectBegin()
	mockFail.ExpectRollback()
	shadowFailing := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, dbFail, failingOutbox, 0, nil)
	t.Cleanup(shadowFailing.Close)
	require.False(t, shadowFailing.ObserveSettlement(CanonicalWalletSettlementEvent{PlatformUserID: "u1", Currency: "CNY", AmountUnits: 100}))
	require.NoError(t, mockFail.ExpectationsWereMet())
}

type outboxStoreStub struct {
	insertErr error
}

func (s *outboxStoreStub) InsertOutboxEventTx(context.Context, *sql.Tx, CanonicalWalletSettlementEvent) error {
	return s.insertErr
}
func (s *outboxStoreStub) ClaimPendingOutboxEvents(context.Context, string, int) ([]CanonicalWalletOutboxEvent, error) {
	return nil, nil
}
func (s *outboxStoreStub) MarkOutboxEventDelivered(context.Context, int64, string) error {
	return nil
}
func (s *outboxStoreStub) MarkOutboxEventFailed(context.Context, int64, string, time.Time) error {
	return nil
}
func (s *outboxStoreStub) MarkOutboxEventDeadLetter(context.Context, int64, string, string) error {
	return nil
}
func (s *outboxStoreStub) BindOutboxEventLease(context.Context, int64, string, string) error {
	return nil
}
func (s *outboxStoreStub) ReclaimStaleInFlightEvents(context.Context, time.Duration) (int64, error) {
	return 0, nil
}
func (s *outboxStoreStub) SplitOutboxEvent(context.Context, int64, string, int64, string, bool) (int64, error) {
	return 0, errors.New("outboxStoreStub does not implement SplitOutboxEvent")
}
func (s *outboxStoreStub) ClearPendingRelease(context.Context, int64) error {
	return errors.New("outboxStoreStub does not implement ClearPendingRelease")
}
func (s *outboxStoreStub) SumDeadLetterUnits(context.Context, string) (int64, error) {
	return 0, errors.New("outboxStoreStub does not implement SumDeadLetterUnits")
}
func (s *outboxStoreStub) ListReceivableRedriveCandidates(context.Context, time.Time, int, int) ([]CanonicalWalletOutboxEvent, error) {
	return nil, errors.New("outboxStoreStub does not implement ListReceivableRedriveCandidates")
}
func (s *outboxStoreStub) RequeueDeadLetter(context.Context, int64, string) error {
	return errors.New("outboxStoreStub does not implement RequeueDeadLetter")
}

func TestObserveCanonicalWalletSettlementReturnBool(t *testing.T) {
	shadow := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeShadow), &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	t.Cleanup(shadow.Close)
	user := &User{ID: 1, PlatformUserID: "u1", BillingCurrency: "CNY", Balance: 10.0}
	cost := &CostBreakdown{ActualCost: 1.0}

	// nil bridge -> false
	require.False(t, observeCanonicalWalletSettlement(nil, "req-1", user, cost, false, true, nil, "tok", "auth", ""))

	// subscriptionBilling -> false
	require.False(t, observeCanonicalWalletSettlement(shadow, "req-1", user, cost, true, true, nil, "tok", "auth", ""))

	// billingApplied == false -> false
	require.False(t, observeCanonicalWalletSettlement(shadow, "req-1", user, cost, false, false, nil, "tok", "auth", ""))

	// zero cost -> false
	require.False(t, observeCanonicalWalletSettlement(shadow, "req-1", user, &CostBreakdown{ActualCost: 0}, false, true, nil, "tok", "auth", ""))

	// nil user -> false
	require.False(t, observeCanonicalWalletSettlement(shadow, "req-1", nil, cost, false, true, nil, "tok", "auth", ""))

	// nil cost -> false
	require.False(t, observeCanonicalWalletSettlement(shadow, "req-1", user, nil, false, true, nil, "tok", "auth", ""))
}

// Phase 3.4a (§9.5): the bridge's clock goes through the CONSTRUCTOR, and
// every lease-expiry decision and every reserve-script `now` reads b.clock().
func TestBridgeClockFlowsThroughTheConstructor(t *testing.T) {
	fixed := time.Date(2031, 5, 4, 3, 2, 1, 0, time.UTC)
	// The cached lease is unexpired and covering by WALL clock but long past
	// by the FIXED clock — only b.clock() can see it as expired.
	store := &canonicalWalletStoreStub{lease: &CanonicalWalletLease{
		LeaseID: "lease-stale-by-fixed-clock", PlatformUserID: "user-clock", Currency: "CNY",
		BudgetUnits: 500_000_000, ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	}}
	control := &canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: "lease-fresh", PlatformUserID: "user-clock", Currency: "CNY",
		BudgetUnits: 500_000_000, ExpiresAt: fixed.Add(5 * time.Minute),
	}}
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ExpirySkewMarginMS = 100
	b := newCanonicalWalletBridge(cfg, store, control, nil, nil, 0, func() time.Time { return fixed })
	t.Cleanup(b.Close)
	require.NotNil(t, b.now, "the constructor installs the injected clock")

	allowed, err := b.CheckAndReserve(context.Background(), CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-clock", PlatformUserID: "user-clock", Currency: "CNY", AmountUnits: 1_000_000,
	})
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, 1, control.ensureCalls, "the cached lease was expired BY THE FIXED CLOCK, so ensure ran")
	require.Equal(t, fixed, store.lastReserveNow, "the reserve script's now is b.clock() — 2031, never wall-clock")
}

// §9.5's exclusion: the HTTP client's own clock stays wall-clock — it mints
// the service assertion's iat/exp, and a fake clock there would produce JWTs
// the verifier rejects. Proven on the client NewCanonicalWalletBridge builds.
func TestNewCanonicalWalletBridgeClientStaysOnWallClock(t *testing.T) {
	cfg := &config.Config{}
	cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	b := NewCanonicalWalletBridge(cfg, &canonicalWalletStoreStub{}, nil, nil)
	t.Cleanup(b.Close)
	require.NotNil(t, b)
	client, ok := b.control.(*canonicalWalletHTTPClient)
	require.True(t, ok, "NewCanonicalWalletBridge wires the HTTP client")
	assertion, err := client.serviceAssertion(canonicalWalletLeaseScope)
	require.NoError(t, err)
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(strings.TrimPrefix(assertion, "Bearer "), claims, func(token *jwt.Token) (any, error) {
		return []byte(strings.Repeat("w", 32)), nil
	})
	require.NoError(t, err)
	require.True(t, token.Valid)
	iat, _ := claims.GetIssuedAt()
	require.NotNil(t, iat)
	require.LessOrEqual(t, time.Since(iat.Time), 5*time.Second, "iat is minted on wall-clock, not the bridge clock")
}

func TestPhase34ProtoCallerSlotTTLIsWiredFromGatewayConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.Gateway.ConcurrencySlotTTLMinutes = 45
	b := NewCanonicalWalletBridge(cfg, &canonicalWalletStoreStub{}, nil, nil) // nil outbox: no dispatcher goroutine
	t.Cleanup(b.Close)
	require.NotNil(t, b)
	require.Equal(t, 2700, b.callerSlotTTLSeconds, "gateway.concurrency_slot_ttl_minutes × 60")
	require.NotNil(t, b.now)

	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	require.Nil(t, NewCanonicalWalletBridge(cfg, &canonicalWalletStoreStub{}, nil, nil), "disabled mode still constructs no bridge")
}
