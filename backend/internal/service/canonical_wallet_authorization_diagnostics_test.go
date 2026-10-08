//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestWalletAuthorizationDiagnosticsPreserveEnforceAndShadow(t *testing.T) {
	for _, mode := range []string{config.CanonicalWalletModeEnforce, config.CanonicalWalletModeShadow} {
		t.Run(mode, func(t *testing.T) {
			auth, snap, key, control, _ := newAuthorizerFixture(t, mode)
			core, logs := observer.New(zap.WarnLevel)
			ctx := logger.IntoContext(context.Background(), zap.New(core).With(zap.String("request_id", "request-correlation")))
			const secret = "sk_private_prompt_and_url_secret"
			control.leaseErr = fmt.Errorf("%w: https://secret.invalid/%s", ErrCanonicalWalletBalanceShortfall, secret)
			body := `{"max_tokens":64,"messages":[{"content":"` + secret + `"}]}`
			in := AuthorizeInput{Snapshot: snap, User: key.User, Estimate: estimateFor(body)}
			h, err := auth.Authorize(ctx, in)
			if mode == config.CanonicalWalletModeEnforce {
				require.ErrorIs(t, err, ErrAuthorizationRefused)
				require.ErrorIs(t, err, ErrCanonicalWalletBalanceShortfall)
				require.Same(t, control.leaseErr, h.Refusal.Cause)
			} else {
				require.NoError(t, err)
				require.Nil(t, h.Refusal)
			}
			require.Equal(t, 1, control.ensureCalls)
			require.Equal(t, 1, logs.Len())
			fields := logs.All()[0].ContextMap()
			require.Equal(t, "request-correlation", fields["request_id"])
			require.Equal(t, "lease_ensure", fields["stage"])
			require.Equal(t, "balance_shortfall", fields["reason"])
			require.Equal(t, h.ID, fields["authorization_id"])
			require.Equal(t, snap.ID, fields["snapshot_id"])
			require.EqualValues(t, len(body), fields["input_tokens_upper_bound"])
			require.EqualValues(t, 64, fields["request_max_output_tokens"])
			require.EqualValues(t, h.EstimatedUnits, fields["estimated_units"])
			raw, marshalErr := json.Marshal(fields)
			require.NoError(t, marshalErr)
			require.NotContains(t, string(raw), secret)
			require.NotContains(t, string(raw), "https://")
			logs.TakeAll()
			control.leaseErr = nil
			_, err = auth.Authorize(ctx, in)
			require.NoError(t, err)
			require.Zero(t, logs.Len(), "successful authorization adds no logs")
		})
	}
}

type diagnosticPoolStore struct {
	*canonicalWalletStoreStub
	armError error
	poolArms int
}

func (s *diagnosticPoolStore) ArmCanonicalWalletPool(context.Context, string, []AuthorizationSegment, int64, time.Time) error {
	s.poolArms++
	return s.armError
}

func TestWalletAuthorizationDiagnosticsPoolFailureStages(t *testing.T) {
	for _, stage := range []string{"plan_read", "pool_read", "pool_ensure", "pool_topup", "plan_persist", "pool_arm"} {
		t.Run(stage, func(t *testing.T) {
			ctx, diagnostic := newWalletAuthorizationDiagnostic(context.Background())
			core, logs := observer.New(zap.WarnLevel)
			ctx = logger.IntoContext(ctx, zap.New(core))
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			const secret = "sk_response_body_must_not_be_logged"
			lease := CanonicalWalletLease{LeaseID: "lease-1", PlatformUserID: "user-1", Currency: "USD", BudgetUnits: 200, ConsumedUnits: 150, ExpiresAt: time.Now().Add(time.Hour)}
			store := &diagnosticPoolStore{canonicalWalletStoreStub: &canonicalWalletStoreStub{lease: &lease}, armError: ErrCanonicalWalletLeaseExhausted}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if stage == "pool_read" {
					w.WriteHeader(503)
					_, _ = w.Write([]byte(secret))
					return
				}
				if r.URL.Path == canonicalWalletEnsurePath {
					w.WriteHeader(403)
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"reason": "insufficient_balance", "headroom": newCanonicalWalletAmountObject(7), "message": secret}})
					return
				}
				view := map[string]any{"lease_id": lease.LeaseID, "platform_user_id": lease.PlatformUserID, "currency": "USD", "unit_version": CanonicalWalletUnitVersion, "scale": 8, "usd_wallet_policy_version": config.CanonicalUSDWalletPolicyVersion, "status": "active", "expires_at": lease.ExpiresAt, "budget": newCanonicalWalletAmountObject(200), "captured": newCanonicalWalletAmountObject(150)}
				views := []any{view}
				if stage == "pool_ensure" {
					views = []any{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"leases": views}})
			}))
			defer server.Close()
			cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
			cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
			bridge := &CanonicalWalletBridge{cfg: cfg, store: store, control: newCanonicalWalletHTTPClient(cfg, server.Client()), outboxDB: db, now: time.Now, callerSlotTTLSeconds: 1800}
			query := mock.ExpectQuery("SELECT authorization_id,lease_id,held_units")
			if stage == "plan_read" {
				query.WillReturnError(sqlmock.ErrCancelled)
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "lease_id", "held_units", "lease_basis", "event_id", "actual_units", "pin_state", "kind", "state", "settlement_payload", "remainder_payload"}))
			}
			if stage == "plan_persist" {
				mock.ExpectBegin().WillReturnError(errors.New(secret))
			}
			if stage == "pool_arm" {
				mock.ExpectBegin()
				mock.ExpectExec("INSERT INTO wallet_authorization_segment").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			}
			h := &AuthorizationHandle{ID: "auth_" + strings.Repeat("a", 32), SnapshotID: "bsnap_" + strings.Repeat("b", 32), AttemptKind: "llm", EstimatedUnits: 40}
			if stage == "pool_topup" {
				h.EstimatedUnits = 100
			}
			err = bridge.authorizePool(ctx, h, "user-1", h.EstimatedUnits)
			require.Error(t, err)
			diagnostic.log(ctx, h, AuthorizeInput{}, AuthorizationRefusalLeaseUnavailable, err, true)
			fields := logs.All()[0].ContextMap()
			require.Equal(t, stage, fields["stage"])
			if stage != "plan_read" && stage != "pool_read" {
				require.Equal(t, true, fields["pool_known"])
				if stage == "pool_ensure" {
					require.EqualValues(t, 0, fields["pool_total_units"])
					require.EqualValues(t, 0, fields["pool_headroom_units"])
					require.EqualValues(t, 0, fields["lease_count"])
				} else {
					require.EqualValues(t, 200, fields["pool_total_units"])
					require.EqualValues(t, 50, fields["pool_headroom_units"])
					require.EqualValues(t, 1, fields["lease_count"])
				}
			}
			if stage == "pool_topup" || stage == "pool_ensure" {
				require.ErrorIs(t, err, ErrCanonicalWalletBalanceShortfall)
				require.EqualValues(t, 403, fields["control_http_status"])
				require.EqualValues(t, 7, fields["control_headroom_units"])
				require.Equal(t, "insufficient_balance", fields["control_reason"])
				require.Equal(t, 2, calls, "one pool read and one top-up; diagnostics add no calls")
			}
			raw, marshalErr := json.Marshal(fields)
			require.NoError(t, marshalErr)
			require.NotContains(t, string(raw), secret)
			require.NotContains(t, string(raw), server.URL)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletAuthorizationDiagnosticsUnknownValuesAreRedacted(t *testing.T) {
	for _, reason := range []string{"reconciliation_required", "internal_error", "lease_owner_mismatch"} {
		require.Equal(t, reason, safeWalletControlReason(reason))
	}
	core, logs := observer.New(zap.WarnLevel)
	ctx, diagnostic := newWalletAuthorizationDiagnostic(logger.IntoContext(context.Background(), zap.New(core)))
	const secret = "sk_private_response_prompt_header"
	err := &canonicalWalletStatusError{Status: 502, Reason: secret, Path: "https://secret.invalid/" + secret}
	diagnostic.log(ctx, &AuthorizationHandle{ID: secret, SnapshotID: secret}, AuthorizeInput{Snapshot: &BillingSnapshot{BillingModel: secret}}, AuthorizationRefusalLeaseUnavailable, err, true)
	fields := logs.All()[0].ContextMap()
	require.Equal(t, "other", fields["cause_control_reason"])
	require.Equal(t, "control_http", fields["error_class"])
	require.Equal(t, "redacted", fields["billing_model"])
	require.Equal(t, "redacted", fields["authorization_id"])
	raw, marshalErr := json.Marshal(fields)
	require.NoError(t, marshalErr)
	require.NotContains(t, string(raw), secret)
	require.NotContains(t, string(raw), "https://")
	for _, tc := range []struct {
		err   error
		class string
	}{{context.DeadlineExceeded, "deadline"}, {context.Canceled, "canceled"}, {ErrEstimateUnbounded, "estimate_unbounded"}, {ErrCanonicalWalletUnitsOverflow, "units_overflow"}, {errors.New(secret), "other"}} {
		require.Equal(t, tc.class, walletAuthorizationErrorClass(tc.err))
	}
}
