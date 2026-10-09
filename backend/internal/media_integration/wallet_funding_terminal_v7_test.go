//go:build media_integration

package media_integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fundingV7Fixture struct {
	f        *mediaFixture
	wallet   service.CanonicalWalletLeaseStore
	base     string
	secret   string
	source   string
	owner    *service.User
	snapshot *service.BillingSnapshot
}

func newFundingV7Fixture(t *testing.T) fundingV7Fixture {
	t.Helper()
	base, secret := immediateV5WireConfig(t)
	user := "funding-v7-" + uuid.NewString()
	f := newMediaFixtureForUser(t, user)
	f.svc.Stop()
	f.bridge.Close()
	source := "funding-v7-source-" + uuid.NewString()
	code, raw := immediateV5WirePost(t, base, secret, "/__fixture/seed", map[string]string{"platform_user_id": user, "grant_units": "1000000000", "lease_id": source, "budget_units": "1000000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: source, PlatformUserID: user, Currency: "USD", FundedUnits: 1000000000, BudgetUnits: 1000000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(time.Hour)}))
	f.cfg.CanonicalWallet.ControlPlaneURL, f.cfg.CanonicalWallet.Secret = base, secret
	f.cfg.CanonicalWallet.Issuer, f.cfg.CanonicalWallet.Audience = "sub2api", "shipany-wallet"
	f.cfg.CanonicalWallet.RequestTimeoutMS = 3000
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode, f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "enabled", "enabled"
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	f.svc.Stop()
	template := f.create(t, "funding-v7-snapshot-template", "google/nano-banana", "1:1")
	var snapshotID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, template.TaskID).Scan(&snapshotID))
	snapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	snapshot.ID = "funding-v7-llm-snapshot-" + uuid.NewString()
	snapshot.Family = service.BillingFamilyOpenAI
	snapshot.RequestedModel, snapshot.BillingModel, snapshot.Candidates = "gpt-5.1", "gpt-5.1", []string{"gpt-5.1"}
	snapshot.Pricing = service.BillingSnapshotPricing{Mode: service.BillingModeToken, Source: service.PricingSourceLiteLLM, Base: &service.ModelPricing{InputPricePerToken: .8}}
	snapshot.Multipliers = service.BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}
	snapshot.Flags.WalletImmediateReleasePolicyVersion = service.WalletImmediateReleasePolicyVersion
	require.NoError(t, f.snapshots.Persist(context.Background(), snapshot))
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, template.TaskID)
	require.NoError(t, err)
	owner, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	return fundingV7Fixture{f: f, wallet: wallet, base: base, secret: secret, source: source, owner: owner, snapshot: snapshot}
}

func (x fundingV7Fixture) authorize(t *testing.T, units int64) *service.AuthorizationHandle {
	t.Helper()
	h, err := service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: x.snapshot, User: x.owner, FixedEstimateUnits: units})
	require.NoError(t, err)
	return h
}

func (x fundingV7Fixture) mediaFunding(t *testing.T, units int64) (*service.AuthorizationHandle, error) {
	t.Helper()
	template := x.f.create(t, "funding-v7-media-template-"+uuid.NewString(), "google/nano-banana", "1:1")
	var snapshotID string
	require.NoError(t, x.f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, template.TaskID).Scan(&snapshotID))
	snapshot, err := repository.ProvideBillingSnapshotStore(x.f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	snapshot.ID = "funding-v7-media-snapshot-" + uuid.NewString()
	snapshot.FrozenAt = time.Now().UTC()
	snapshot.Pricing.DefaultPerRequestPrice = float64(units) / 100000000
	require.NoError(t, repository.ProvideBillingSnapshotStore(x.f.db).InsertBillingSnapshot(context.Background(), snapshot))
	task, auth, event := "media_"+strings.ReplaceAll(uuid.NewString(), "-", ""), "auth_"+strings.ReplaceAll(uuid.NewString(), "-", ""), "funding-v7-event-"+uuid.NewString()
	_, err = x.f.db.Exec(`INSERT INTO gateway_media_task(id,user_id,platform_user_id,api_key_id,idempotency_key,request_hash,model,media_type,option,prompt,request_payload,billing_snapshot_id,quoted_units,authorization_id,settlement_event_id,deadline_at,financial_policy_version,financial_runtime_deadline)
	 SELECT $2,user_id,platform_user_id,api_key_id,$2,request_hash,model,media_type,option,prompt,request_payload,$5,$6,$3,$4,deadline_at,financial_policy_version,financial_runtime_deadline FROM gateway_media_task WHERE id=$1`, template.TaskID, task, auth, event, snapshot.ID, units)
	require.NoError(t, err)
	_, err = x.f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, template.TaskID)
	require.NoError(t, err)
	return service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(service.WithMediaFundingOwner(context.Background(), task), service.AuthorizeInput{Snapshot: snapshot, User: x.owner, FixedEstimateUnits: units, DurableAuthorizationID: auth})
}

func (x fundingV7Fixture) signedZero(t *testing.T, h *service.AuthorizationHandle) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"safe validation rejection"}}`)
	}))
	defer provider.Close()
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
	require.NoError(t, err)
	response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, x.f.cfg).Do(request, "", 0, 1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	var receiptRaw []byte
	require.NoError(t, x.f.db.QueryRow(`SELECT zero_receipt FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0 AND zero_ack_at IS NOT NULL`, h.ID).Scan(&receiptRaw))
	var receipt service.WalletTaskPinReceipt
	require.NoError(t, json.Unmarshal(receiptRaw, &receipt))
	segment := h.Segments[0]
	_, err = service.VerifyWalletTaskPinReceipt(x.secret, service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: x.f.platformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: x.snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "released"}, receipt)
	require.NoError(t, err)
}

// Run this with the actual Worker GATEWAY_WALLET_MAX_ACTIVE_LEASES=1. Its
// production source and database enforce the slot, rather than a mock limit.
func TestExternalFundingV7CapOneEmptyOriginalSourcePartialACKClosesBeforeFreshAuthorization(t *testing.T) {
	x := newFundingV7Fixture(t)
	var originalSegments int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&originalSegments))
	require.Zero(t, originalSegments)
	// This genuine media quote exceeds all available principal. Its failed
	// authorization reclaims the original free source, without an LLM terminal
	// row capable of waking recovery. Any unarmed issued media chunk must close.
	_, err := x.mediaFunding(t, 1100000000)
	require.Error(t, err)
	// Evidence for the refusal class. Observed against the real cap-1 Worker: the
	// quote exceeds all principal, so the refusal is balance_shortfall (not
	// lease_cap_reached), which is exactly the class that triggers the
	// shared-free reclaim. Only class names are logged.
	var refusal *service.AuthorizationRefusedError
	t.Logf("media funding refusal: typed=%v reason=%v cap=%v shortfall=%v", errors.As(err, &refusal), func() any {
		if refusal != nil {
			return refusal.Reason
		}
		return nil
	}(), errors.Is(err, service.ErrCanonicalWalletLeaseCapReached), errors.Is(err, service.ErrCanonicalWalletBalanceShortfall))
	var intents, freezes, works int
	_ = x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&intents)
	_ = x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&freezes)
	_ = x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&works)
	t.Logf("source reclaim state after refusal: source_intent=%d freeze=%d terminal_work=%d", intents, freezes, works)
	started := time.Now()
	var revision, returned, applied int64
	var closed bool
	var receiptRaw []byte
	// Revision model: rev1 is the signed partial return of all 10 that the failed
	// media authorization triggers, rev2 is the terminal-work close that frees the
	// canonical caller slot and returns 0 additional units (asserted below).
	// The second reclaim of the same authorization attempt finds nothing free on
	// the already returned source and must not mint an extra empty revision; an
	// earlier build did, which turned this sequence into rev1, empty rev2, close rev3.
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT return_revision,returned_units,redis_applied_revision,closed,receipt FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&revision, &returned, &applied, &closed, &receiptRaw) == nil && closed && revision == 2 && returned == 1000000000 && applied == revision
	}, 5*time.Second, 20*time.Millisecond)
	var receipt service.WalletFundingReturnReceipt
	require.NoError(t, json.Unmarshal(receiptRaw, &receipt))
	require.Equal(t, "close", receipt.Mode)
	require.Equal(t, "0", receipt.ReturnedUnits, "only capacity changes after the first genuine return of all10")
	require.Equal(t, "0", receipt.HeldUnits)
	require.Equal(t, "0", receipt.CapturedUnits)
	fresh := x.authorize(t, 1000000000)
	require.NotEqual(t, x.source, fresh.LeaseID)
	require.Less(t, time.Since(started), 2500*time.Millisecond, "the original free source must not strand the sole canonical caller slot")
	var generation, appliedGeneration int64
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT generation,applied_generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&generation, &appliedGeneration) == nil && generation == appliedGeneration
	}, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&originalSegments))
	require.Zero(t, originalSegments, "capacity discovery comes from original signed partial ACK, not invented terminal evidence")
}

func TestExternalFundingV7OriginalSourceTerminalGenerationRejectsStaleApply(t *testing.T) {
	x := newFundingV7Fixture(t)
	h := x.authorize(t, 100000000)
	_, err := x.mediaFunding(t, 200000000)
	require.NoError(t, err)
	x.signedZero(t, h)
	var generation int64
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&generation) == nil && generation >= 2
	}, 3*time.Second, 20*time.Millisecond)
	// Replaying a stale completion cannot swallow the real ACK + cleanup's
	// later generation. This is the production CAS executed against actual PG.
	result, err := x.f.db.Exec(`UPDATE wallet_funding_terminal_work SET applied_generation=$3 WHERE platform_user_id=$1 AND lease_id=$2 AND generation=$3`, x.f.platformUserID, x.source, generation-1)
	require.NoError(t, err)
	changed, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, changed)
	_, err = x.f.db.Exec(`DELETE FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source)
	require.ErrorContains(t, err, "wallet funding terminal work is permanent")
	_, err = x.f.db.Exec(`UPDATE wallet_funding_terminal_work SET generation=0 WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source)
	require.Error(t, err)
}

func fundingV7PrimaryReceipt(t *testing.T, db *sql.DB, user, source string) service.WalletFundingReturnReceipt {
	t.Helper()
	var raw []byte
	require.NoError(t, db.QueryRow(`SELECT receipt FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, source).Scan(&raw))
	var receipt service.WalletFundingReturnReceipt
	require.NoError(t, json.Unmarshal(raw, &receipt))
	return receipt
}

func fundingV7Available(t *testing.T, x fundingV7Fixture) int64 {
	t.Helper()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/snapshot", map[string]string{"platform_user_id": x.f.platformUserID}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	var snapshot struct {
		Credits []struct {
			Remaining string `json:"remainingUnits"`
		} `json:"credits"`
	}
	require.NoError(t, json.Unmarshal(raw, &snapshot))
	total := int64(0)
	for _, credit := range snapshot.Credits {
		units, err := strconv.ParseInt(credit.Remaining, 10, 64)
		require.NoError(t, err)
		total += units
	}
	return total
}

// This replay fixture checkpoints only actual signed canonical receipts. It
// represents an already-ACKed source during upgrade/restart; it never invents
// a financial ACK or manufactures a provider fee to exercise scheduling.
func fundingV7CheckpointActualFreeSource(t *testing.T, x fundingV7Fixture, user, source string) {
	t.Helper()
	amount := func(v int64) map[string]any {
		return map[string]any{"amount_units": strconv.FormatInt(v, 10), "currency": "USD", "scale": 8, "unit_version": "usd-e8-v1"}
	}
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/seed", map[string]string{"platform_user_id": user, "grant_units": "1000000000", "lease_id": source, "budget_units": "1000000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	lease := service.CanonicalWalletLease{PlatformUserID: user, LeaseID: source, Currency: "USD", FundingScope: "legacy", FundedUnits: 1000000000, BudgetUnits: 1000000000, ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, x.wallet.InstallCanonicalWalletLease(context.Background(), lease))
	request := map[string]any{"platform_user_id": user, "lease_id": source, "freeze_id": source + ".source-freeze.v1", "expected_funded": amount(1000000000), "expected_budget_revision": 0, "funding_scope": "legacy", "funding_owner_id": "", "funding_issuance_key": ""}
	code, raw = fundingV5WirePost(t, x.base, x.secret, "/api/internal/v2/wallet/leases/freeze-source", "wallet:lease", request)
	require.Equal(t, http.StatusOK, code, string(raw))
	var frozen struct {
		Code int `json:"code"`
		Data struct {
			Receipt service.WalletFundingSourceReceipt `json:"receipt"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &frozen))
	require.Zero(t, frozen.Code)
	sourceReceipt := frozen.Data.Receipt
	require.Equal(t, user, sourceReceipt.PlatformUserID)
	require.Equal(t, source, sourceReceipt.LeaseID)
	require.Equal(t, "1000000000", sourceReceipt.FundedUnits)
	sources, err := json.Marshal(sourceReceipt.Sources)
	require.NoError(t, err)
	payload, err := json.Marshal([]string{sourceReceipt.Protocol, sourceReceipt.FreezeID, sourceReceipt.PlatformUserID, sourceReceipt.LeaseID, sourceReceipt.FundingScope, sourceReceipt.FundingOwnerID, sourceReceipt.FundingIssuanceKey, sourceReceipt.FundedUnits, strconv.FormatInt(sourceReceipt.BudgetRevision, 10), sourceReceipt.CommittedAt, string(sources)})
	require.NoError(t, err)
	mac := hmac.New(sha256.New, []byte(x.secret))
	_, err = mac.Write(payload)
	require.NoError(t, err)
	signature, err := hex.DecodeString(sourceReceipt.Signature)
	require.NoError(t, err)
	require.True(t, hmac.Equal(mac.Sum(nil), signature), "checkpoint only the actual original source fence")
	requestRaw, err := json.Marshal(request)
	require.NoError(t, err)
	frozenRaw, err := json.Marshal(sourceReceipt)
	require.NoError(t, err)
	basis, err := x.wallet.(service.CanonicalWalletFundingStore).FreezeCanonicalWalletFunding(context.Background(), user, source)
	require.NoError(t, err)
	require.NotNil(t, basis)
	require.Empty(t, basis.Holds)
	returnRequest := map[string]any{"platform_user_id": user, "lease_id": source, "mode": "partial", "base_return_revision": 0, "source_freeze_id": source + ".source-freeze.v1", "expected_funded": amount(1000000000), "expected_budget_revision": 0, "gateway_consumed": amount(0), "gateway_released": amount(0), "holds": []any{}}
	code, raw = fundingV5WirePost(t, x.base, x.secret, "/api/internal/v2/wallet/leases/return", "wallet:lease", returnRequest)
	require.Equal(t, http.StatusOK, code, string(raw))
	var returned struct {
		Code int `json:"code"`
		Data struct {
			Receipt service.WalletFundingReturnReceipt `json:"receipt"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &returned))
	require.Zero(t, returned.Code)
	r := returned.Data.Receipt
	require.Equal(t, "partial", r.Mode)
	require.Equal(t, user, r.PlatformUserID)
	require.Equal(t, source, r.LeaseID)
	require.Equal(t, int64(1), r.ReturnRevision)
	require.Equal(t, "1000000000", r.ReturnedAfterUnits)
	require.Equal(t, "0", r.HeldUnits)
	require.Equal(t, "0", r.CapturedUnits)
	sources, err = json.Marshal(r.Sources)
	require.NoError(t, err)
	payload, err = json.Marshal([]string{r.Protocol, r.ReceiptID, r.PlatformUserID, r.LeaseID, r.FundingScope, r.FundingOwnerID, r.FundingIssuanceKey, strconv.FormatInt(r.ReturnRevision, 10), strconv.FormatInt(r.BudgetRevision, 10), strconv.FormatInt(r.CaptureSeq, 10), r.FundedUnits, r.CapturedUnits, r.ReturnedBeforeUnits, r.ReturnedUnits, r.ReturnedAfterUnits, r.CreditedUnits, r.WriteOffUnits, r.HeldUnits, r.GatewayConsumedUnits, r.GatewayReleasedUnits, r.Mode, r.CommittedAt, string(sources)})
	require.NoError(t, err)
	mac = hmac.New(sha256.New, []byte(x.secret))
	_, err = mac.Write(payload)
	require.NoError(t, err)
	signature, err = hex.DecodeString(r.Signature)
	require.NoError(t, err)
	require.True(t, hmac.Equal(mac.Sum(nil), signature), "a genuine canonical partial ACK, not fabricated success, schedules capacity recovery")
	receiptRaw, err := json.Marshal(r)
	require.NoError(t, err)
	pendingRaw, err := json.Marshal(map[string]any{"protocol": "wallet-funding-request-v1", "basis": basis, "request": returnRequest})
	require.NoError(t, err)
	// Install the exact source/return checkpoints together, so independent
	// background readers cannot observe a half-restored upgrade fixture.
	tx, err := x.f.db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO wallet_funding_source_intent(platform_user_id,lease_id,freeze_id,requested_mode,expected_request,source_receipt) VALUES($1,$2,$3,'partial',$4::jsonb,$5::jsonb)`, user, source, source+".source-freeze.v1", string(requestRaw), string(frozenRaw))
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO wallet_funding_freeze(platform_user_id,lease_id,funding_scope,funded_units,pending_request) VALUES($1,$2,'legacy',1000000000,$3::jsonb)`, user, source, string(pendingRaw))
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE wallet_funding_freeze SET return_revision=1,returned_units=1000000000,receipt=$3::jsonb,pending_request=NULL WHERE platform_user_id=$1 AND lease_id=$2`, user, source, string(receiptRaw))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestExternalFundingV7MoreThanSixteenFailedSourcesDoNotStarveReadyClose(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.f.bridge.Close()
	target, err := url.Parse(x.base)
	require.NoError(t, err)
	var mu sync.RWMutex
	blocked := map[string]bool{}
	denied := &atomic.Int64{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if r.URL.Path == "/api/internal/v2/wallet/leases/return" {
			var request struct {
				Lease string `json:"lease_id"`
				Mode  string `json:"mode"`
			}
			_ = json.Unmarshal(raw, &request)
			mu.RLock()
			deny := blocked[request.Lease]
			mu.RUnlock()
			if request.Mode == "close" && deny {
				denied.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"code":-1,"data":{"reason":"isolated_close_transport_failure"}}`)
				return
			}
		}
		forwarded := r.Clone(r.Context())
		forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
		forwarded.Body = io.NopCloser(bytes.NewReader(raw))
		response, err := http.DefaultClient.Do(forwarded)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(proxy.Close)
	x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	t.Cleanup(x.f.bridge.Close)
	for i := 0; i < 17; i++ {
		source, user := "funding-v7-fair-source-"+uuid.NewString(), "funding-v7-fair-user-"+uuid.NewString()
		mu.Lock()
		blocked[source] = true
		mu.Unlock()
		fundingV7CheckpointActualFreeSource(t, x, user, source)
	}
	require.Eventually(t, func() bool { return denied.Load() >= 17 }, 8*time.Second, 20*time.Millisecond)
	readyUser, readySource := "funding-v7-ready-user-"+uuid.NewString(), "funding-v7-ready-source-"+uuid.NewString()
	started := time.Now()
	fundingV7CheckpointActualFreeSource(t, x, readyUser, readySource)
	var closed bool
	var applied, revision int64
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT closed,return_revision,redis_applied_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, readyUser, readySource).Scan(&closed, &revision, &applied) == nil && closed && revision == 2 && applied == revision
	}, 5*time.Second, 20*time.Millisecond)
	require.Less(t, time.Since(started), 2500*time.Millisecond, "17 real earlier failed source RPCs cannot monopolize recovery of another ready source")
	var pending, unchanged int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_terminal_work w JOIN wallet_funding_freeze f USING(platform_user_id,lease_id) WHERE w.applied_generation<w.generation AND NOT f.closed AND f.pending_request IS NOT NULL AND f.return_revision=1 AND w.lease_id LIKE 'funding-v7-fair-source-%'`).Scan(&pending))
	require.Equal(t, 17, pending, "failed close requests remain durable with their original signed partial ACK")
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_freeze WHERE lease_id LIKE 'funding-v7-fair-source-%' AND NOT closed AND return_revision=1 AND returned_units=1000000000`).Scan(&unchanged))
	require.Equal(t, 17, unchanged)
}

func TestExternalFundingV7MoreThan128OriginalSignedTerminalsResumeCleanupWithoutLosingSource(t *testing.T) {
	x := newFundingV7Fixture(t)
	// Suppress only the newly introduced scheduling receipt, to represent old
	// fully signed terminals that predate PG233. Original PG/D1 financial ACKs
	// and actual Redis release/Unprotect are untouched and verified per call.
	_, err := x.f.db.Exec(`CREATE FUNCTION funding_v7_old_cleanup_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.funding_terminal_cleanup_at=NULL; RETURN NEW; END $$; CREATE TRIGGER z_funding_v7_old_cleanup_marker BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION funding_v7_old_cleanup_marker()`)
	require.NoError(t, err)
	for i := 0; i < 129; i++ {
		h := x.authorize(t, 1000000)
		require.Equal(t, x.source, h.LeaseID)
		x.signedZero(t, h)
	}
	_, err = x.f.db.Exec(`DROP TRIGGER z_funding_v7_old_cleanup_marker ON wallet_authorization_segment; DROP FUNCTION funding_v7_old_cleanup_marker()`)
	require.NoError(t, err)
	var originals, withoutCleanup int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*),count(*) FILTER(WHERE funding_terminal_cleanup_at IS NULL) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND state='finished' AND zero_ack_at IS NOT NULL`, x.f.platformUserID, x.source).Scan(&originals, &withoutCleanup))
	require.Equal(t, 129, originals)
	require.Equal(t, 129, withoutCleanup)
	// A failed genuinely unfundable media quote freezes/returns this old free
	// source. Its insertion discovers all129 signed original acknowledgements.
	_, err = x.mediaFunding(t, 1100000000)
	require.Error(t, err)
	var closed bool
	var generation, applied int64
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT f.closed,w.generation,w.applied_generation FROM wallet_funding_freeze f JOIN wallet_funding_terminal_work w USING(platform_user_id,lease_id) WHERE f.platform_user_id=$1 AND f.lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &generation, &applied) == nil && closed && generation == applied
	}, 12*time.Second, 20*time.Millisecond, "a per-lease lifetime history is not a permanent 128-row cleanup barrier")
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FILTER(WHERE funding_terminal_cleanup_at IS NOT NULL) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND zero_ack_at IS NOT NULL`, x.f.platformUserID, x.source).Scan(&withoutCleanup))
	require.Equal(t, 129, withoutCleanup)
	receipt := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
	require.Equal(t, "close", receipt.Mode)
	require.Equal(t, "0", receipt.CapturedUnits)
	require.Equal(t, "1000000000", receipt.ReturnedAfterUnits)
	var zeroCharges, unknown int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE platform_user_id=$1`, x.f.platformUserID).Scan(&zeroCharges))
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE platform_user_id=$1`, x.f.platformUserID).Scan(&unknown))
	require.Zero(t, zeroCharges)
	require.Zero(t, unknown)
	_ = x.authorize(t, 1000000000)
}

func TestExternalFundingV7MissingCacheAndWrongCanonicalIdentityNeverPersistClose(t *testing.T) {
	for _, scenario := range []string{"missing-cache", "wrong-canonical-owner"} {
		t.Run(scenario, func(t *testing.T) {
			x := newFundingV7Fixture(t)
			x.f.bridge.Close()
			user, source := "funding-v7-guard-user-"+uuid.NewString(), "funding-v7-guard-source-"+uuid.NewString()
			target, err := url.Parse(x.base)
			require.NoError(t, err)
			deny, wrongOwner := &atomic.Bool{}, &atomic.Bool{}
			deny.Store(true)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				if r.URL.Path == "/api/internal/v2/wallet/leases/return" && deny.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				forwarded := r.Clone(r.Context())
				forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
				forwarded.Body = io.NopCloser(bytes.NewReader(raw))
				response, err := http.DefaultClient.Do(forwarded)
				if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				if r.URL.Path == "/api/internal/v2/wallet/leases/pool" && wrongOwner.Load() {
					var envelope struct {
						Code int                        `json:"code"`
						Data map[string]json.RawMessage `json:"data"`
					}
					if json.Unmarshal(body, &envelope) == nil {
						var leases []map[string]any
						if json.Unmarshal(envelope.Data["leases"], &leases) == nil {
							for _, lease := range leases {
								if lease["lease_id"] == source {
									lease["funding_owner_id"] = "wrong-owner-identity"
								}
							}
							envelope.Data["leases"], _ = json.Marshal(leases)
							body, _ = json.Marshal(envelope)
						}
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(body)
			}))
			t.Cleanup(proxy.Close)
			x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
			// Checkpoint the real original ACK while the recovery process is
			// stopped; this is a restart observation, not an ACK fabrication.
			fundingV7CheckpointActualFreeSource(t, x, user, source)
			receipt := fundingV7PrimaryReceipt(t, x.f.db, user, source)
			require.NoError(t, x.wallet.(service.CanonicalWalletFundingStore).ApplyCanonicalWalletFundingReturn(context.Background(), receipt))
			basis, err := x.wallet.GetCanonicalWalletLeaseByID(context.Background(), user, source)
			require.NoError(t, err)
			var leaseKey string
			if scenario == "missing-cache" {
				keys, err := x.f.rdb.Keys(context.Background(), "canonical_wallet:lease:*"+source).Result()
				require.NoError(t, err)
				require.Len(t, keys, 1)
				leaseKey = keys[0]
				require.NoError(t, x.f.rdb.Del(context.Background(), leaseKey).Err())
			} else {
				wrongOwner.Store(true)
			}
			deny.Store(false)
			x.f.cfg.CanonicalWallet.LLMImmediateReleaseMode, x.f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "off", "off"
			x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
			t.Cleanup(x.f.bridge.Close)
			require.Never(t, func() bool {
				var closed bool
				var revision int64
				var mode string
				_ = x.f.db.QueryRow(`SELECT closed,return_revision,requested_mode FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, source).Scan(&closed, &revision, &mode)
				return closed || revision != 1 || mode == "close"
			}, 1100*time.Millisecond, 20*time.Millisecond, "an ambiguous cache/canonical identity cannot poison permanent close intent")
			var pending int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2 AND generation>applied_generation`, user, source).Scan(&pending))
			require.Equal(t, 1, pending)
			if scenario == "missing-cache" {
				require.Zero(t, x.f.rdb.Exists(context.Background(), leaseKey).Val(), "background recovery never resurrects C/R from canonical captured0")
				require.NoError(t, x.wallet.InstallCanonicalWalletLease(context.Background(), *basis), "restore only the exact original signed reduced basis")
			} else {
				wrongOwner.Store(false)
			}
			require.Eventually(t, func() bool {
				var closed bool
				return x.f.db.QueryRow(`SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, source).Scan(&closed) == nil && closed
			}, 4*time.Second, 20*time.Millisecond)
		})
	}
}
