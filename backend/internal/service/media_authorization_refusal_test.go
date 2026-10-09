//go:build unit

package service

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type mediaRefusalUsers struct {
	UserRepository
	user *User
}

func (s *mediaRefusalUsers) GetByID(context.Context, int64) (*User, error) {
	return s.user, nil
}

type mediaRefusalSnapshots struct {
	BillingSnapshotStore
	snapshot *BillingSnapshot
}

func (s *mediaRefusalSnapshots) GetBillingSnapshot(context.Context, string) (*BillingSnapshot, error) {
	return s.snapshot, nil
}

type mediaRefusalPool struct {
	canonicalWalletStoreStub
	poolArmCalls int
}

func (s *mediaRefusalPool) ArmCanonicalWalletPool(context.Context, string, []AuthorizationSegment, int64, time.Time) error {
	s.poolArmCalls++
	return errors.New("unexpected authorization arm")
}

type mediaRefusalProvider struct {
	mediaProvider
	createCalls int
}

type mediaRefusalJournalOwnerArgument struct{}

func (mediaRefusalJournalOwnerArgument) Match(value driver.Value) bool {
	owner, ok := value.(string)
	if !ok || !strings.HasPrefix(owner, "media-journal-owner:") {
		return false
	}
	_, err := uuid.Parse(strings.TrimPrefix(owner, "media-journal-owner:"))
	return err == nil
}

func (s *mediaRefusalProvider) Create(context.Context, string, map[string]any, *AuthorizationHandle) (string, bool, error) {
	s.createCalls++
	return "", false, errors.New("unexpected provider dispatch")
}

func TestMediaAuthorizationRefusalPreservesZeroFundsAndClassifiesOnlyBalance(t *testing.T) {
	for _, tc := range []struct {
		name, reason, code, logReason string
		status                        int
	}{
		{"zero funds", "insufficient_balance", "INSUFFICIENT_BALANCE", "balance_shortfall", 409},
		{"zero funds 402", "insufficient_balance", "INSUFFICIENT_BALANCE", "balance_shortfall", 402},
		{"policy mismatch", "usd_wallet_policy_mismatch", "AUTHORIZATION_REFUSED", "lease_unavailable", 409},
		{"lease slots full", "lease_cap_reached", "AUTHORIZATION_REFUSED", "lease_unavailable", 409},
		{"contention", "lease_contention", "AUTHORIZATION_REFUSED", "lease_unavailable", 409},
		{"transport failure", "unavailable", "AUTHORIZATION_REFUSED", "lease_unavailable", 503},
		{"service authentication", "unauthorized", "AUTHORIZATION_REFUSED", "lease_unavailable", 401},
		{"untyped balance text", "mentions_insufficient_balance", "AUTHORIZATION_REFUSED", "lease_unavailable", 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var walletPaths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				walletPaths = append(walletPaths, req.URL.Path)
				require.Equal(t, http.MethodPost, req.Method)
				if req.URL.Path == "/api/internal/v2/wallet/leases/pool" {
					_, _ = w.Write([]byte(`{"data":{"leases":[],"captured_event_ids":[]}}`))
					return
				}
				require.Equal(t, canonicalWalletEnsurePath, req.URL.Path, "no pin, settlement or provider call")
				var request canonicalWalletEnsureRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&request))
				require.Equal(t, int64(100800000), mustUnits(request.RequestedBudget), "Veo 4s authoritative $0.14 quote")
				require.Equal(t, int64(1), mustUnits(request.MinHeadroom))
				require.Equal(t, "media", request.FundingScope)
				require.Equal(t, "media_"+strings.Repeat("b", 32), request.FundingOwnerID)
				require.Equal(t, request.FundingOwnerID+".funding.0", request.FundingIssuanceKey)
				require.Equal(t, config.CanonicalUSDWalletPolicyVersion, request.USDWalletPolicyVersion)
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"message": "PRIVATE_WALLET_RESPONSE", "data": map[string]any{"reason": tc.reason}})
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.CanonicalWallet = config.CanonicalWalletConfig{Mode: "enforce", Holds: "on", USDWalletEnabled: true, USDPolicyVersion: config.CanonicalUSDWalletPolicyVersion, ControlPlaneURL: server.URL, Secret: "test-only-secret", RequestTimeoutMS: 2000}
			cfg.CanonicalWallet.ReaderJournalDirectory = t.TempDir()
			journalVolume, err := walletReaderJournalDirectory(cfg.CanonicalWallet.ReaderJournalDirectory)
			require.NoError(t, err)
			user := &User{ID: 42, PlatformUserID: "PRIVATE_OWNER", BillingCurrency: "USD", Status: StatusActive}
			fx, _, err := canonicalUSDWalletSnapshot(user, cfg)
			require.NoError(t, err)
			snapshot := &BillingSnapshot{ID: "snapshot", Family: BillingFamily("media"), FX: fx, Flags: BillingSnapshotFlags{USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion}}
			snapshots := &BillingSnapshotService{store: &mediaRefusalSnapshots{snapshot: snapshot}}
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			walletStore := &mediaRefusalPool{}
			bridge := &CanonicalWalletBridge{cfg: cfg.CanonicalWallet, store: walletStore, control: newCanonicalWalletHTTPClient(cfg.CanonicalWallet, server.Client()), outboxDB: db}
			provider := &mediaRefusalProvider{}
			service := &MediaTaskService{cfg: cfg, db: db, store: &mediaTaskStore{db: db}, bridge: bridge, snapshots: snapshots, authorizer: NewCanonicalWalletAuthorizer(cfg, bridge, snapshots), users: &mediaRefusalUsers{user: user}, provider: provider}
			defer service.closeMediaJournalOwner()
			r := &mediaTaskRecord{ID: "media_" + strings.Repeat("b", 32), UserID: user.ID, PlatformUserID: user.PlatformUserID, AuthorizationID: "auth_" + strings.Repeat("a", 32), SnapshotID: snapshot.ID, Model: "veo-3-1", Option: "4", Prompt: "PRIVATE_PROMPT", QuotedUnits: 100800000, Status: "queued", ClaimedBy: "claim"}
			r.FinancialPolicyVersion = WalletImmediateReleasePolicyVersion
			expectNoSegments := func() {
				mock.ExpectQuery("SELECT authorization_id,lease_id,held_units").WithArgs(r.AuthorizationID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "lease_id", "held_units", "lease_basis", "event_id", "actual_units", "pin_state", "kind", "state", "settlement_payload", "remainder_payload"}))
			}
			expectNoSegments() // Worker reads the durable attempt before authorization.
			// The real process initialization validates the fsynced temporary
			// volume against PG and establishes its dedicated media owner session.
			mock.ExpectBegin()
			mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT EXISTS").WithArgs(journalVolume).WillReturnRows(sqlmock.NewRows([]string{"mismatch"}).AddRow(false))
			mock.ExpectExec("INSERT INTO wallet_reader_journal_volume").WithArgs(journalVolume).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT volume_id FROM wallet_reader_journal_volume").WillReturnRows(sqlmock.NewRows([]string{"volume_id"}).AddRow(journalVolume))
			mock.ExpectCommit()
			mock.ExpectExec("SELECT pg_advisory_lock").WithArgs(mediaRefusalJournalOwnerArgument{}).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE gateway_media_task SET lease_id").WillReturnResult(sqlmock.NewResult(0, 1))
			expectNoSegments() // Actual Authorizer checks the pool plan.
			mock.ExpectQuery("SELECT authorization_id,platform_user_id,billing_snapshot_id,quoted_units FROM gateway_media_task").WithArgs(r.ID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "platform_user_id", "billing_snapshot_id", "quoted_units"}).AddRow(r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits))
			expectNoSegments() // Zero completion verifies there are no debit shares.
			mock.ExpectBegin()
			mock.ExpectCommit()
			mock.ExpectExec("UPDATE gateway_media_task SET financial_state='released_zero'").WithArgs(r.ID).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE gateway_media_task SET lease_id").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectClose()
			mock.ExpectClose() // Dedicated owner plus the normal query connection.
			var logs bytes.Buffer
			priorLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(priorLogger)
			require.NoError(t, service.process(context.Background(), r))
			require.NotNil(t, service.journalOwner)
			require.Equal(t, journalVolume, service.journalOwner.host)
			service.closeMediaJournalOwner()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet(), "no usage, outbox or financial writes beyond zero task completion")
			require.Equal(t, tc.code, r.ErrorCode)
			require.Equal(t, "failed", r.Status)
			require.Equal(t, "finished", r.PinState)
			require.Zero(t, r.HeldUnits)
			require.NotNil(t, r.ActualUnits)
			require.Zero(t, *r.ActualUnits)
			require.Empty(t, r.ProviderTaskID)
			require.Empty(t, r.Segments)
			require.Zero(t, provider.createCalls)
			require.Zero(t, walletStore.poolArmCalls)
			require.Zero(t, walletStore.armCalls)
			require.Zero(t, walletStore.reserveCalls)
			require.Zero(t, walletStore.installCalls)
			expectedPaths := []string{canonicalWalletEnsurePath}
			if tc.reason == "insufficient_balance" {
				expectedPaths = append(expectedPaths, "/api/internal/v2/wallet/leases/pool")
			}
			require.Equal(t, expectedPaths, walletPaths, "typed shortfall may only discover shared free-tail sources; media never arms an existing LLM pool")
			view := mediaTaskView(r)
			require.Equal(t, tc.code, view.ErrorCode)
			require.Equal(t, "released", view.Billing.State)
			require.Equal(t, "0", *view.Billing.ReleasedUSD)
			require.Nil(t, view.Billing.ChargedUSD)
			if tc.code == "INSUFFICIENT_BALANCE" {
				require.Contains(t, r.ErrorMessage, "Insufficient USD balance")
			} else {
				require.NotContains(t, r.ErrorMessage, "Insufficient")
			}
			var logged map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &logged))
			require.Equal(t, tc.logReason, logged["reason"])
			require.Len(t, logged["task_ref"], 12)
			for _, private := range []string{r.ID, r.AuthorizationID, user.PlatformUserID, r.Prompt, "PRIVATE_WALLET_RESPONSE", "test-only-secret"} {
				require.NotContains(t, logs.String(), private)
				require.NotContains(t, r.ErrorMessage, private)
			}
		})
	}
}
