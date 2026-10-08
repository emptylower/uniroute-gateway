package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func canonicalWalletTopUpWire(budget, consumed int64, expires time.Time) canonicalWalletEnsureWireResponse {
	return canonicalWalletEnsureWireResponse{
		USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion,
		LeaseID:                "lease-topup", PlatformUserID: "user-topup", Currency: "USD", UnitVersion: CanonicalWalletUnitVersion, Scale: 8,
		Budget: newCanonicalWalletAmountObject(budget), Reserved: newCanonicalWalletAmountObject(0),
		Captured: newCanonicalWalletAmountObject(consumed), Released: newCanonicalWalletAmountObject(0),
		Headroom: newCanonicalWalletAmountObject(budget - consumed), Status: "active", ExpiresAt: expires,
		Outcome: "topped_up", ClampedBy: "none",
	}
}

func TestCanonicalWalletEnsureLeaseTopUpHTTPContract(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*canonicalWalletEnsureRequest, *canonicalWalletEnsureWireResponse)
		valid  bool
	}{
		{"successful top-up", nil, true},
		{"idempotent reuse", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.Outcome = "reused" }, true},
		{"ordinary issuance", func(r *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) {
			r.TopUpLeaseID = ""
			r.MinimumBudgetUnits = ""
			w.Outcome = "issued"
		}, true},
		{"unrequested top-up", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) { r.TopUpLeaseID = "" }, false},
		{"blank target", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) { r.TopUpLeaseID = " " }, false},
		{"wrong lease", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.LeaseID = "other-lease" }, false},
		{"wrong owner", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) {
			w.PlatformUserID = "other-user"
		}, false},
		{"missing requested policy", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.USDWalletPolicyVersion = ""
		}, false},
		{"wrong returned policy", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) {
			w.USDWalletPolicyVersion = "unknown"
		}, false},
		{"settlement cannot top up", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) { r.Purpose = "settle" }, false},
		{"missing minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) { r.MinimumBudgetUnits = "" }, false},
		{"zero minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.MinimumBudgetUnits = "0"
		}, false},
		{"negative minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.MinimumBudgetUnits = "-1"
		}, false},
		{"leading-zero minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.MinimumBudgetUnits = "0500"
		}, false},
		{"overflow minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.MinimumBudgetUnits = "9223372036854775808"
		}, false},
		{"below minimum", func(r *canonicalWalletEnsureRequest, _ *canonicalWalletEnsureWireResponse) {
			r.MinimumBudgetUnits = "501"
		}, false},
		{"unknown outcome", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.Outcome = "funded" }, false},
		{"wrong currency", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.Currency = "CNY" }, false},
		{"wrong amount version", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) {
			w.Budget.UnitVersion = "unknown"
		}, false},
		{"wrong amount scale", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.Headroom.Scale = 6 }, false},
		{"headroom exceeds budget", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) {
			w.Headroom = newCanonicalWalletAmountObject(501)
		}, false},
		{"missing expiry", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.ExpiresAt = time.Time{} }, false},
		{"closed lease", func(_ *canonicalWalletEnsureRequest, w *canonicalWalletEnsureWireResponse) { w.Status = "closed" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := canonicalWalletEnsureRequest{
				PlatformUserID: "user-topup", Currency: "USD", Purpose: "authorize", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion,
				TopUpLeaseID: "lease-topup", PreferLeaseID: "lease-topup", MinimumBudgetUnits: "500",
				MinHeadroom: newCanonicalWalletAmountObject(400), RequestedBudget: newCanonicalWalletAmountObject(500),
				RequestedTTLSeconds: 300, CallerSlotTTLSeconds: 1800,
			}
			wire := canonicalWalletTopUpWire(500, 100, time.Now().UTC().Add(time.Minute))
			if tc.mutate != nil {
				tc.mutate(&req, &wire)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, canonicalWalletEnsurePath, r.URL.Path)
				require.Equal(t, http.MethodPost, r.Method)
				require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "Bearer "))
				var received canonicalWalletEnsureRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
				require.Equal(t, req, received)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": wire}))
			}))
			defer server.Close()
			cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
			cfg.ControlPlaneURL = server.URL
			result, err := newCanonicalWalletHTTPClient(cfg, server.Client()).EnsureLease(context.Background(), req)
			if !tc.valid {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, wire.Outcome, result.Outcome)
			require.Equal(t, int64(500), result.Lease.BudgetUnits)
			require.Equal(t, int64(100), result.Lease.ConsumedUnits)
			require.Equal(t, int64(400), result.Lease.RemainingUnits())
		})
	}
}

type canonicalWalletTopUpPoolStore struct {
	canonicalWalletStoreStub
	beforeArm func()
	segments  []AuthorizationSegment
	installed []CanonicalWalletLease
}

func (s *canonicalWalletTopUpPoolStore) InstallCanonicalWalletLease(ctx context.Context, lease CanonicalWalletLease) error {
	s.installed = append(s.installed, lease)
	return s.canonicalWalletStoreStub.InstallCanonicalWalletLease(ctx, lease)
}

func (s *canonicalWalletTopUpPoolStore) GetCanonicalWalletLeaseByID(ctx context.Context, user, leaseID string) (*CanonicalWalletLease, error) {
	lease, err := s.canonicalWalletStoreStub.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
	if lease != nil {
		lease.RetainUntil = time.Time{} // The production Redis parser does not restore this local-only field.
	}
	return lease, err
}

func (s *canonicalWalletTopUpPoolStore) ArmCanonicalWalletPool(ctx context.Context, user string, segments []AuthorizationSegment, grace int64, now time.Time) error {
	s.beforeArm()
	s.segments = append([]AuthorizationSegment(nil), segments...)
	for _, segment := range segments {
		if _, _, _, err := s.ArmCanonicalWalletHold(ctx, user, segment.LeaseID, "USD", segment.AuthorizationID, segment.HeldUnits, grace, now); err != nil {
			return err
		}
	}
	return nil
}

func TestCanonicalWalletColdBootstrapTopUpArmsPoolOnlyAfterValidFunding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*canonicalWalletEnsureWireResponse)
		valid  bool
	}{
		{"issued then topped up", nil, true},
		{"cannot extend expiry", func(w *canonicalWalletEnsureWireResponse) { w.ExpiresAt = w.ExpiresAt.Add(time.Minute) }, false},
		{"expired funding", func(w *canonicalWalletEnsureWireResponse) { w.ExpiresAt = time.Now().Add(-time.Minute) }, false},
		{"underfunded top-up", func(w *canonicalWalletEnsureWireResponse) {
			w.Budget = newCanonicalWalletAmountObject(331)
			w.Headroom = newCanonicalWalletAmountObject(299)
		}, false},
		{"wrong owner", func(w *canonicalWalletEnsureWireResponse) { w.PlatformUserID = "other-user" }, false},
		{"wrong lease", func(w *canonicalWalletEnsureWireResponse) { w.LeaseID = "other-lease" }, false},
		{"wrong policy", func(w *canonicalWalletEnsureWireResponse) { w.USDWalletPolicyVersion = "unknown" }, false},
		{"unknown outcome", func(w *canonicalWalletEnsureWireResponse) { w.Outcome = "unknown" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			budget := int64(100)
			expires := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Millisecond)
			var outcomes []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodPost, r.Method)
				wire := canonicalWalletTopUpWire(budget, 32, expires)
				if r.URL.Path == "/api/internal/v2/wallet/leases/pool" {
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"leases": []canonicalWalletEnsureWireResponse{wire}}}))
					return
				}
				require.Equal(t, canonicalWalletEnsurePath, r.URL.Path)
				var request canonicalWalletEnsureRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Equal(t, config.CanonicalUSDWalletPolicyVersion, request.USDWalletPolicyVersion)
				if len(outcomes) == 0 {
					require.Empty(t, request.TopUpLeaseID)
					require.Equal(t, "1", request.MinHeadroom.AmountUnits)
					wire.Outcome = "issued"
				} else {
					require.Equal(t, "lease-topup", request.TopUpLeaseID)
					require.Equal(t, "lease-topup", request.PreferLeaseID)
					require.Equal(t, "332", request.MinimumBudgetUnits, "100 existing budget + 232 missing headroom")
					require.Equal(t, "300", request.MinHeadroom.AmountUnits)
					var err error
					budget, err = strconv.ParseInt(request.MinimumBudgetUnits, 10, 64)
					require.NoError(t, err)
					wire = canonicalWalletTopUpWire(budget, 32, expires)
					if tc.mutate != nil {
						tc.mutate(&wire)
					}
				}
				outcomes = append(outcomes, wire.Outcome)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": wire}))
			}))
			defer server.Close()
			cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
			cfg.ControlPlaneURL, cfg.USDWalletEnabled, cfg.USDPolicyVersion = server.URL, true, config.CanonicalUSDWalletPolicyVersion
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			store := &canonicalWalletTopUpPoolStore{beforeArm: func() {
				require.NoError(t, mock.ExpectationsWereMet(), "durable funding plan must commit before any hold")
			}}
			bridge := &CanonicalWalletBridge{cfg: cfg, store: store, control: newCanonicalWalletHTTPClient(cfg, server.Client()), outboxDB: db, callerSlotTTLSeconds: 1800}
			require.NoError(t, bridge.EnsureCanonicalWalletLeaseForAdmission(ctx, "user-topup", "USD"))
			require.Equal(t, int64(100), store.lease.BudgetUnits)
			require.Zero(t, store.armCalls, "bootstrap is not authorization")
			h := &AuthorizationHandle{ID: "auth-topup", SnapshotID: "snapshot-topup", AttemptKind: "ordinary"}
			mock.ExpectQuery("SELECT authorization_id,lease_id,held_units").WithArgs(h.ID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "lease_id", "held_units", "lease_basis", "event_id", "actual_units", "pin_state", "kind", "state", "settlement_payload", "remainder_payload"}))
			if tc.valid {
				mock.ExpectBegin()
				mock.ExpectExec("INSERT INTO wallet_authorization_segment").WithArgs(h.ID, 0, h.ID, "user-topup", h.SnapshotID, "lease-topup", int64(300), sqlmock.AnyArg(), h.AttemptKind, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			}
			err = bridge.authorizePool(ctx, h, "user-topup", 300)
			require.NoError(t, mock.ExpectationsWereMet())
			require.Len(t, outcomes, 2)
			if !tc.valid {
				require.Error(t, err)
				require.False(t, h.HoldArmed)
				require.Zero(t, store.armCalls)
				require.Empty(t, store.segments)
				require.Equal(t, int64(32), store.lease.ConsumedUnits)
				if tc.name == "expired funding" {
					// Funding may shorten expiry; the subsequent pool read must
					// exclude that expired cache entry before planning any hold.
					require.Equal(t, int64(332), store.lease.BudgetUnits)
					require.True(t, store.lease.ExpiresAt.Before(time.Now()))
				} else {
					require.Equal(t, int64(100), store.lease.BudgetUnits)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{"issued", "topped_up"}, outcomes)
			require.True(t, h.HoldArmed)
			require.Equal(t, int64(300), h.HeldUnits)
			require.Equal(t, "lease-topup", h.LeaseID)
			require.Len(t, h.Segments, 1)
			require.Equal(t, int64(332), h.Segments[0].Basis.BudgetUnits)
			require.Equal(t, int64(32), h.Segments[0].Basis.ConsumedUnits, "top-up preserves prior consumption")
			require.Equal(t, expires, h.Segments[0].Basis.ExpiresAt)
			require.Equal(t, int64(332), store.lease.ConsumedUnits, "only the armed hold adds its 300 units")
			require.Equal(t, int64(300), store.holds[h.ID].HeldUnits)
			require.Zero(t, store.releasedUnits)
			require.Len(t, store.installed, 4, "bootstrap, pool refresh, top-up and final pool refresh")
			for _, lease := range store.installed {
				require.Equal(t, expires.Add(1800*time.Second), lease.RetainUntil, "every install retains receipts beyond authorization expiry")
			}
		})
	}
}
