package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func fundingReceiptFixture(t *testing.T) (string, WalletFundingReturnReceipt, CanonicalWalletFundingBasis) {
	t.Helper()
	secret := strings.Repeat("f", 32)
	r := WalletFundingReturnReceipt{Protocol: "wallet-funding-return-v1", ReceiptID: "receipt-1", PlatformUserID: "funding-user", LeaseID: "funding-lease", FundingScope: "legacy", ReturnRevision: 1, BudgetRevision: 2, CaptureSeq: 0,
		FundedUnits: "1000", CapturedUnits: "0", ReturnedBeforeUnits: "0", ReturnedUnits: "900", ReturnedAfterUnits: "900", CreditedUnits: "900", WriteOffUnits: "0", HeldUnits: "100", GatewayConsumedUnits: "100", GatewayReleasedUnits: "0", Mode: "partial", CommittedAt: time.Now().UTC().Format(time.RFC3339Nano), Sources: []WalletFundingReturnSource{{AllocationID: "allocation-original", CreditID: "grant-original", ReturnedUnits: "900", CreditedUnits: "900"}}}
	basis := CanonicalWalletFundingBasis{Lease: CanonicalWalletLease{PlatformUserID: r.PlatformUserID, LeaseID: r.LeaseID, FundingScope: "legacy", BudgetUnits: 1000, FundedUnits: 1000, BudgetRevision: 2, ConsumedUnits: 100}, Holds: []CanonicalWalletHold{{AuthorizationID: "held-1", HeldUnits: 100}}}
	signFundingReceiptFixture(t, secret, &r)
	return secret, r, basis
}

func signFundingReceiptFixture(t *testing.T, secret string, r *WalletFundingReturnReceipt) {
	t.Helper()
	payload, err := fundingReturnSigningPayload(*r)
	require.NoError(t, err)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	r.Signature = hex.EncodeToString(mac.Sum(nil))
}

func TestWalletFundingReceiptRequiresSignatureIdentityAndConservation(t *testing.T) {
	secret, receipt, basis := fundingReceiptFixture(t)
	require.NoError(t, verifyFundingReturnReceipt(secret, basis, "partial", receipt))
	for name, mutate := range map[string]func(*WalletFundingReturnReceipt){
		"signature": func(r *WalletFundingReturnReceipt) { r.Signature = strings.Repeat("0", 64) },
		"identity": func(r *WalletFundingReturnReceipt) {
			r.FundingOwnerID = "another-task"
			signFundingReceiptFixture(t, secret, r)
		},
		"revision":   func(r *WalletFundingReturnReceipt) { r.ReturnRevision = 2; signFundingReceiptFixture(t, secret, r) },
		"obligation": func(r *WalletFundingReturnReceipt) { r.HeldUnits = "99"; signFundingReceiptFixture(t, secret, r) },
		"mint":       func(r *WalletFundingReturnReceipt) { r.CreditedUnits = "901"; signFundingReceiptFixture(t, secret, r) },
		"source": func(r *WalletFundingReturnReceipt) {
			r.Sources[0].CreditedUnits = "899"
			signFundingReceiptFixture(t, secret, r)
		},
		"close-with-held": func(r *WalletFundingReturnReceipt) { r.Mode = "close"; signFundingReceiptFixture(t, secret, r) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			changed.Sources = append([]WalletFundingReturnSource(nil), receipt.Sources...)
			mutate(&changed)
			require.Error(t, verifyFundingReturnReceipt(secret, basis, changed.Mode, changed))
		})
	}
}

type fundingRecoveryStore struct {
	CanonicalWalletLeaseStore
	applied int
}

func (s *fundingRecoveryStore) FreezeCanonicalWalletFunding(context.Context, string, string) (*CanonicalWalletFundingBasis, error) {
	return nil, nil
}
func (s *fundingRecoveryStore) ApplyCanonicalWalletFundingReturn(context.Context, WalletFundingReturnReceipt) error {
	s.applied++
	return nil
}

func TestWalletFundingRecoveryValidatesPrimaryACKBeforeRedis(t *testing.T) {
	for _, scenario := range []string{"valid", "invalid-signature", "wrong-user", "wrong-primary-returned", "wrong-primary-owner"} {
		t.Run(scenario, func(t *testing.T) {
			secret, receipt, _ := fundingReceiptFixture(t)
			returned, owner := int64(900), ""
			switch scenario {
			case "invalid-signature":
				receipt.Signature = strings.Repeat("0", 64)
			case "wrong-user":
				receipt.PlatformUserID = "another-user"
				signFundingReceiptFixture(t, secret, &receipt)
			case "wrong-primary-returned":
				returned = 901
			case "wrong-primary-owner":
				owner = "another-task"
			}
			raw, err := json.Marshal(receipt)
			require.NoError(t, err)
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("SELECT receipt,funded_units,returned_units,return_revision,funding_scope,funding_owner_id,funding_issuance_key").WithArgs("funding-user", "funding-lease").WillReturnRows(sqlmock.NewRows([]string{"receipt", "funded", "returned", "revision", "scope", "owner", "key", "pending"}).AddRow(raw, int64(1000), returned, int64(1), "legacy", owner, "", nil))
			store := &fundingRecoveryStore{}
			bridge := &CanonicalWalletBridge{store: store, outboxDB: db, control: newCanonicalWalletHTTPClient(config.CanonicalWalletConfig{Secret: secret}, nil)}
			if scenario == "valid" {
				mock.ExpectExec("UPDATE wallet_funding_freeze SET redis_applied_revision").WithArgs("funding-user", "funding-lease", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err = bridge.recoverFundingReturn(context.Background(), "funding-user", "funding-lease")
			if scenario == "valid" {
				require.NoError(t, err)
				require.Equal(t, 1, store.applied)
			} else {
				require.Error(t, err)
				require.Zero(t, store.applied)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletFundingPendingReplayRejectsChangedBasisBeforeRPC(t *testing.T) {
	for _, scenario := range []string{"owner", "unfrozen", "principal_currency", "consumed", "hold"} {
		t.Run(scenario, func(t *testing.T) {
			secret, _, basis := fundingReceiptFixture(t)
			basis.Lease.FundingFrozen = true
			request := walletFundingReturnRequest{PlatformUserID: "funding-user", LeaseID: "funding-lease", Mode: "partial", SourceFreezeID: "funding-lease.source-freeze.v1", ExpectedFunded: newCanonicalWalletAmountObject(1000), ExpectedBudgetRevision: 2, GatewayConsumed: newCanonicalWalletAmountObject(100), GatewayReleased: newCanonicalWalletAmountObject(0), Holds: []walletFundingReturnHold{{AuthorizationID: "held-1", Held: newCanonicalWalletAmountObject(100)}}}
			pending := walletFundingPendingRequest{Protocol: "wallet-funding-request-v1", Basis: basis, Request: request}
			switch scenario {
			case "owner":
				pending.Basis.Lease.FundingOwnerID = "another-task"
			case "unfrozen":
				pending.Basis.Lease.FundingFrozen = false
			case "principal_currency":
				pending.Request.ExpectedFunded.Currency = "CNY"
			case "consumed":
				pending.Request.GatewayConsumed = newCanonicalWalletAmountObject(99)
			case "hold":
				pending.Request.Holds[0].AuthorizationID = "another-hold"
			}
			raw, err := json.Marshal(pending)
			require.NoError(t, err)
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT pending_request,funded_units,returned_units,return_revision,funding_scope,funding_owner_id,funding_issuance_key").WithArgs("funding-user", "funding-lease").WillReturnRows(sqlmock.NewRows([]string{"pending", "funded", "returned", "revision", "scope", "owner", "key"}).AddRow(raw, int64(1000), int64(0), int64(0), "legacy", "", ""))
			mock.ExpectRollback()
			store := &fundingRecoveryStore{}
			bridge := &CanonicalWalletBridge{store: store, outboxDB: db, control: newCanonicalWalletHTTPClient(config.CanonicalWalletConfig{Secret: secret, ControlPlaneURL: "http://127.0.0.1:9", RequestTimeoutMS: 10}, nil)}
			err = bridge.replayPendingFundingReturn(context.Background(), "funding-user", "funding-lease")
			require.ErrorContains(t, err, "durable funding request", "validation must fail before any D1 RPC or ACK mutation")
			require.Zero(t, store.applied)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletFundingLifecycleFencesFrozenSourceBeforeControlRPC(t *testing.T) {
	for _, scenario := range []string{"topup", "drain", "prefer"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "prefer", scenario, "frozen source mutation must stop before any control RPC")
				var request canonicalWalletEnsureRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Empty(t, request.PreferLeaseID)
				wire := canonicalWalletTopUpWire(1000, 0, time.Now().Add(time.Minute))
				wire.LeaseID, wire.PlatformUserID, wire.FundingScope, wire.Outcome = "new-unrelated", "funding-user", "llm", "issued"
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": wire}))
			}))
			defer server.Close()
			cfg := config.CanonicalWalletConfig{ControlPlaneURL: server.URL, Secret: strings.Repeat("f", 32), RequestTimeoutMS: 1000}
			b := &CanonicalWalletBridge{outboxDB: db, store: &fundingRecoveryStore{}, control: newCanonicalWalletHTTPClient(cfg, server.Client())}
			request := canonicalWalletEnsureRequest{PlatformUserID: "funding-user", Currency: "USD", Purpose: "authorize", FundingScope: "llm", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(1000)}
			switch scenario {
			case "topup":
				request.TopUpLeaseID = "frozen-source"
				request.MinimumBudgetUnits = "1000"
			case "drain":
				request.Drained = []canonicalWalletDrainEntry{{LeaseID: "frozen-source", GatewayConsumed: newCanonicalWalletAmountObject(0)}}
			case "prefer":
				request.PreferLeaseID = "frozen-source"
			}
			mock.ExpectBegin()
			mock.ExpectExec("SELECT pg_advisory_xact_lock").WithArgs("wallet-funding-lifecycle-v1:funding-user").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT EXISTS").WithArgs("funding-user", "frozen-source").WillReturnRows(sqlmock.NewRows([]string{"frozen"}).AddRow(true))
			if scenario == "prefer" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			result, err := b.ensureFundingLifecycle(context.Background(), request)
			if scenario == "prefer" {
				require.NoError(t, err)
				require.Equal(t, "new-unrelated", result.Lease.LeaseID)
				require.Equal(t, 1, calls)
			} else {
				require.ErrorIs(t, err, ErrCanonicalWalletLeaseContention)
				require.Nil(t, result)
				require.Zero(t, calls)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletFundingSourceFenceRequiresImmutableSignedPrincipalAndBoundedAllocations(t *testing.T) {
	secret := strings.Repeat("s", 32)
	request := walletFundingSourceRequest{PlatformUserID: "user", LeaseID: "lease", FreezeID: "lease.source-freeze.v1", ExpectedFunded: newCanonicalWalletAmountObject(1300), ExpectedBudgetRevision: 1, FundingScope: "legacy"}
	receipt := WalletFundingSourceReceipt{Protocol: "wallet-funding-source-v1", FreezeID: request.FreezeID, PlatformUserID: request.PlatformUserID, LeaseID: request.LeaseID, FundingScope: "legacy", FundedUnits: "1300", BudgetRevision: 1, CommittedAt: time.Now().UTC().Format(time.RFC3339Nano), Sources: []WalletFundingSource{{AllocationID: "original", CreditID: "grant", AllocatedUnits: "1000"}, {AllocationID: "topup", CreditID: "grant2", AllocatedUnits: "300"}}}
	sign := func(r *WalletFundingSourceReceipt) {
		payload, e := fundingSourceSigningPayload(*r)
		require.NoError(t, e)
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(payload)
		r.Signature = hex.EncodeToString(mac.Sum(nil))
	}
	sign(&receipt)
	require.NoError(t, verifyFundingSourceReceipt(secret, request, receipt))
	for name, mutate := range map[string]func(*WalletFundingSourceReceipt){
		"signature":  func(r *WalletFundingSourceReceipt) { r.Signature = strings.Repeat("0", 64) },
		"principal":  func(r *WalletFundingSourceReceipt) { r.FundedUnits = "1000"; sign(r) },
		"revision":   func(r *WalletFundingSourceReceipt) { r.BudgetRevision = 0; sign(r) },
		"owner":      func(r *WalletFundingSourceReceipt) { r.FundingOwnerID = "other"; sign(r) },
		"allocation": func(r *WalletFundingSourceReceipt) { r.Sources[0].AllocatedUnits = "999"; sign(r) },
		"duplicate":  func(r *WalletFundingSourceReceipt) { r.Sources[1].AllocationID = r.Sources[0].AllocationID; sign(r) },
		"long-id":    func(r *WalletFundingSourceReceipt) { r.Sources[1].CreditID = strings.Repeat("x", 129); sign(r) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			changed.Sources = append([]WalletFundingSource(nil), receipt.Sources...)
			mutate(&changed)
			require.Error(t, verifyFundingSourceReceipt(secret, request, changed))
		})
	}
	stale := request
	stale.ExpectedFunded = newCanonicalWalletAmountObject(1000)
	stale.ExpectedBudgetRevision = 0
	require.Error(t, verifyFundingSourceReceipt(secret, stale, receipt), "no principal10 intent may accept signed source13")
}

func TestWalletFundingThousandAndOneSourcesShareProducerConsumerBounds(t *testing.T) {
	secret, receipt, basis := fundingReceiptFixture(t)
	receipt.FundedUnits, receipt.ReturnedUnits, receipt.ReturnedAfterUnits, receipt.CreditedUnits = "1001", "1001", "1001", "1001"
	receipt.HeldUnits, receipt.GatewayConsumedUnits = "0", "0"
	receipt.Sources = nil
	for i := 0; i < 1001; i++ {
		receipt.Sources = append(receipt.Sources, WalletFundingReturnSource{AllocationID: fmt.Sprintf("allocation-%d", i), CreditID: fmt.Sprintf("grant-%d", i), ReturnedUnits: "1", CreditedUnits: "1"})
	}
	basis.Lease.BudgetUnits, basis.Lease.FundedUnits, basis.Lease.ConsumedUnits = 1001, 1001, 0
	basis.Holds = nil
	signFundingReceiptFixture(t, secret, &receipt)
	require.NoError(t, verifyFundingReturnReceipt(secret, basis, "partial", receipt))
	receipt.Sources = nil
	receipt.FundedUnits, receipt.ReturnedUnits, receipt.ReturnedAfterUnits, receipt.CreditedUnits = "1000000000", "1000000000", "1000000000", "1000000000"
	basis.Lease.BudgetUnits, basis.Lease.FundedUnits = 1000000000, 1000000000
	for i := 0; i < walletFundingMaxSources; i++ {
		receipt.Sources = append(receipt.Sources, WalletFundingReturnSource{AllocationID: strings.Repeat("a", 123) + fmt.Sprintf("%05d", i), CreditID: strings.Repeat("c", 128), ReturnedUnits: "100000", CreditedUnits: "100000"})
	}
	signFundingReceiptFixture(t, secret, &receipt)
	require.NoError(t, verifyFundingReturnReceipt(secret, basis, "partial", receipt))
	raw, e := json.Marshal(receipt)
	require.NoError(t, e)
	require.Greater(t, len(raw), 2000000, "complete wire receipt is larger than a D1 cell, while immutable storage parts remain bounded")
	receipt.Sources = append(receipt.Sources, WalletFundingReturnSource{})
	signFundingReceiptFixture(t, secret, &receipt)
	require.Error(t, verifyFundingReturnReceipt(secret, basis, "partial", receipt))
}

func TestWalletFundingHTTPControlHasExplicitEightMiBReceiptBoundary(t *testing.T) {
	for _, size := range []int{(1 << 20) + 100, walletFundingResponseLimit + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"data":{"padding":"` + strings.Repeat("x", size) + `"}}`))
			}))
			defer server.Close()
			client := newCanonicalWalletHTTPClient(config.CanonicalWalletConfig{ControlPlaneURL: server.URL, Secret: strings.Repeat("s", 32), RequestTimeoutMS: 1000}, server.Client())
			var response struct {
				Padding string `json:"padding"`
			}
			err := client.doJSON(context.Background(), http.MethodPost, "/api/internal/v2/wallet/leases/freeze-source", "wallet:lease", "", nil, &response)
			if size < walletFundingResponseLimit {
				require.NoError(t, err)
				require.Len(t, response.Padding, size)
			} else {
				require.ErrorContains(t, err, "bounded protocol body")
			}
		})
	}
}
