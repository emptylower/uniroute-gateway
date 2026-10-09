//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWalletFundingTerminalCountsRequireExactPresentSafeNumericIntegers(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"zero", `{"active_pin_count":0,"active_live_pin_count":0}`, true},
		{"live", `{"active_pin_count":2,"active_live_pin_count":1}`, true},
		{"missing", `{}`, false},
		{"missing-live", `{"active_pin_count":0}`, false},
		{"null", `{"active_pin_count":null,"active_live_pin_count":0}`, false},
		{"string", `{"active_pin_count":"0","active_live_pin_count":0}`, false},
		{"fraction", `{"active_pin_count":0.5,"active_live_pin_count":0}`, false},
		{"negative", `{"active_pin_count":-1,"active_live_pin_count":0}`, false},
		{"live-exceeds-total", `{"active_pin_count":0,"active_live_pin_count":1}`, false},
		{"unsafe-integer", `{"active_pin_count":9007199254740992,"active_live_pin_count":0}`, false},
		{"overflow", `{"active_pin_count":9223372036854775808,"active_live_pin_count":0}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire canonicalWalletLeaseWireView
			err := json.Unmarshal([]byte(tc.body), &wire)
			if err == nil {
				_, _, err = fundingPoolPinCounts(wire)
			}
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "missing or malformed counts cannot imply zero active pins")
			}
		})
	}
}

type fundingTerminalGuardStore struct {
	CanonicalWalletLeaseStore
	MediaWalletStateStore
	lease  *CanonicalWalletLease
	raw    MediaWalletRawState
	getErr error
}

func (s *fundingTerminalGuardStore) GetCanonicalWalletLeaseByID(context.Context, string, string) (*CanonicalWalletLease, error) {
	return s.lease, s.getErr
}

func (s *fundingTerminalGuardStore) ReadMediaWalletState(context.Context, string, []string, []string) (MediaWalletRawState, error) {
	return s.raw, nil
}

func TestWalletFundingTerminalCloseRequiresOriginalFinalFreshCanonicalAndAtomicCacheProof(t *testing.T) {
	for _, scenario := range []string{"valid", "pg-pending", "missing-cache", "raw-missing", "raw-changed", "raw-unfrozen", "not-frozen", "held", "active-pin", "active-live", "missing-count", "reserved", "capture-not-acked", "revision-changed", "principal-changed", "source-not-fenced", "owner-changed"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			lease := &CanonicalWalletLease{PlatformUserID: "user", LeaseID: "source", Currency: "USD", FundingScope: "legacy", FundedUnits: 1000,
				BudgetUnits: 100, ReturnedUnits: 900, ReturnRevision: 1, BudgetRevision: 2, ConsumedUnits: 100, ReleasedUnits: 20, FundingFrozen: true}
			store := &fundingTerminalGuardStore{lease: lease, raw: MediaWalletRawState{Leases: []MediaWalletRawLease{{LeaseID: "source", Present: true,
				FundedUnits: 1000, ReturnedUnits: 900, ReturnRevision: 1, BudgetRevision: 2, ConsumedUnits: 100, ReleasedUnits: 20, FundingFrozen: true}}}}
			wire := map[string]any{"lease_id": "source", "platform_user_id": "user", "funding_scope": "legacy", "funding_owner_id": "", "funding_issuance_key": "",
				"currency": "USD", "scale": 8, "unit_version": CanonicalWalletUnitVersion, "status": "active", "expires_at": now.Add(time.Hour),
				"funding_frozen_at": now, "budget": newCanonicalWalletAmountObject(1000), "released": newCanonicalWalletAmountObject(900),
				"captured": newCanonicalWalletAmountObject(80), "reserved": newCanonicalWalletAmountObject(0), "return_revision": 1, "budget_revision": 2,
				"active_pin_count": 0, "active_live_pin_count": 0}
			switch scenario {
			case "missing-cache":
				store.getErr = ErrCanonicalWalletLeaseMissing
			case "raw-missing":
				store.raw.Leases[0].Present = false
			case "raw-changed":
				store.raw.Leases[0].ReturnRevision++
			case "raw-unfrozen":
				store.raw.Leases[0].FundingFrozen = false
			case "not-frozen":
				lease.FundingFrozen = false
			case "held":
				store.raw.Holds = []CanonicalWalletHold{{LeaseID: "source", HeldUnits: 10, State: "armed"}}
			case "active-pin":
				wire["active_pin_count"] = 1
			case "active-live":
				wire["active_pin_count"], wire["active_live_pin_count"] = 1, 1
			case "missing-count":
				delete(wire, "active_live_pin_count")
			case "reserved":
				wire["reserved"] = newCanonicalWalletAmountObject(1)
			case "capture-not-acked":
				wire["captured"] = newCanonicalWalletAmountObject(0)
			case "revision-changed":
				wire["return_revision"] = 2
			case "principal-changed":
				wire["budget"] = newCanonicalWalletAmountObject(1001)
			case "source-not-fenced":
				delete(wire, "funding_frozen_at")
			case "owner-changed":
				wire["funding_owner_id"] = "other-task"
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "/api/internal/v2/wallet/leases/pool", r.URL.Path)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"leases": []any{wire}}}))
			}))
			defer server.Close()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT NOT EXISTS").WithArgs("user", "source").WillReturnRows(sqlmock.NewRows([]string{"ready"}).AddRow(scenario != "pg-pending"))
			cfg := config.CanonicalWalletConfig{ControlPlaneURL: server.URL, Secret: strings.Repeat("s", 32), RequestTimeoutMS: 1000}
			b := &CanonicalWalletBridge{cfg: cfg, store: store, outboxDB: db, control: newCanonicalWalletHTTPClient(cfg, server.Client())}
			err = b.fundingTerminalCloseReady(context.Background(), "user", "source")
			if scenario == "valid" {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			} else {
				require.Error(t, err)
			}
			if scenario == "pg-pending" || scenario == "missing-cache" || scenario == "raw-missing" || scenario == "raw-changed" || scenario == "raw-unfrozen" || scenario == "not-frozen" || scenario == "held" {
				require.Zero(t, calls, "unresolved original obligation cannot progress to canonical close")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletFundingTerminalFinishedBeforeUnprotectRetriesExactIdentity(t *testing.T) {
	for _, finished := range []bool{true, false} {
		t.Run(map[bool]string{true: "original-finished", false: "foreign-or-unfinished"}[finished], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &fundingTerminalCleanupStore{}
			b := &CanonicalWalletBridge{store: store, outboxDB: db}
			mock.ExpectExec("UPDATE wallet_authorization_segment SET state='finished'").WithArgs("auth").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT EXISTS.*state='finished'").WithArgs("auth", "user", "source").WillReturnRows(sqlmock.NewRows([]string{"finished"}).AddRow(finished))
			if finished {
				mock.ExpectQuery("SELECT EXISTS").WithArgs("user", "source").WillReturnRows(sqlmock.NewRows([]string{"pending"}).AddRow(false))
				mock.ExpectExec("UPDATE wallet_authorization_segment SET funding_terminal_cleanup_at").WithArgs("auth", "user", "source").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			require.NoError(t, b.finishPoolSegment(context.Background(), "user", AuthorizationSegment{AuthorizationID: "auth", LeaseID: "source", EventID: "event"}))
			if finished {
				require.Equal(t, 1, store.calls)
			} else {
				require.Zero(t, store.calls)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

type fundingTerminalCleanupStore struct {
	CanonicalWalletLeaseStore
	MediaWalletStateStore
	calls int
}

func (s *fundingTerminalCleanupStore) UnprotectMediaLease(_ context.Context, user, lease, auth, event string, pending bool) error {
	if user != "user" || lease != "source" || auth != "auth" || event != "event" || pending {
		return errors.New("cleanup identity changed")
	}
	s.calls++
	return nil
}
