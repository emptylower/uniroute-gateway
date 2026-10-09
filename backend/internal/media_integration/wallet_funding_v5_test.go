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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// This fixture seeds a canonical, immutable $2 quote. Provider and money traffic
// still use the genuine Go service and actual isolated Worker/D1 routes.
func TestExternalFundingV5SharedFreeTailFundsIsolatedMediaAndACKClosesOnly(t *testing.T) {
	fundingV5SharedSource(t, "normal")
}

func TestExternalFundingV5ResponseLossRestartReplaysOriginalFrozenRequest(t *testing.T) {
	fundingV5SharedSource(t, "response-loss")
}

func TestExternalFundingV5InflightTopupAndSourceFreezeConserveLatestPrincipal(t *testing.T) {
	fundingV5SharedSource(t, "topup-race")
}

func TestExternalFundingV5DelayedTopupCommitsBeforeCanonicalSourceFreeze(t *testing.T) {
	fundingV5SharedSource(t, "delayed-topup-first")
}
func TestExternalFundingV5CanonicalSourceFreezeRejectsDelayedTopupCommit(t *testing.T) {
	fundingV5SharedSource(t, "delayed-freeze-first")
}
func TestExternalFundingV5SourceFreezeResponseLossRestartReplaysSameFence(t *testing.T) {
	fundingV5SharedSource(t, "freeze-response-loss")
}
func TestExternalFundingV5ThousandAndOneSourceReturnACKReplaysOriginalReceipt(t *testing.T) {
	fundingV5SharedSource(t, "many-sources-response-loss")
}

func TestExternalFundingV5SourceFenceArmedPinHandoffNeverStrandsBacking(t *testing.T) {
	fundingV5SharedSource(t, "pin-handoff")
}

func TestExternalFundingV5PartialReturnKnownZeroRestoresAllWithoutNewMediaOrExpiry(t *testing.T) {
	fundingV5SharedSource(t, "zero-residual")
}

func TestExternalFundingV7KnownPositiveOriginalPGBillingReturnsResidualAndCloses(t *testing.T) {
	fundingV5SharedSource(t, "positive-residual")
}

func TestExternalFundingV7PositiveCaptureAwaitingOriginalDeliveredACKPreservesCloseIntent(t *testing.T) {
	fundingV5SharedSource(t, "positive-delivery-fault")
}

func TestExternalFundingV7OriginalZeroReturnsNewFreeShareWhileOtherHoldRemains(t *testing.T) {
	fundingV5SharedSource(t, "zero-surviving-hold")
}

func TestExternalFundingV7SignedZeroFinishedBeforeRedisCleanupRestartsOff(t *testing.T) {
	fundingV5SharedSource(t, "zero-unprotect-restart")
}

func TestExternalFundingV5IntentAndRedisFreezeRestartBeforePendingRequest(t *testing.T) {
	fundingV5SharedSource(t, "intent-partial")
}

func TestExternalFundingV5CloseIntentRestartBeforePendingRequest(t *testing.T) {
	fundingV5SharedSource(t, "intent-close")
}

type fundingV5FreezeFaultStore struct {
	service.CanonicalWalletLeaseStore
	service.CanonicalWalletPoolStore
	service.CanonicalWalletFundingStore
	service.MediaWalletStateStore
	source    string
	db        *sql.DB
	closeOnly bool
	observed  chan string
}

type fundingV7UnprotectFaultStore struct {
	service.CanonicalWalletLeaseStore
	service.CanonicalWalletPoolStore
	service.CanonicalWalletFundingStore
	service.MediaWalletStateStore
	source string
	deny   atomic.Bool
}

func (s *fundingV7UnprotectFaultStore) UnprotectMediaLease(ctx context.Context, user, lease, auth, event string, pending bool) error {
	if lease == s.source && s.deny.Load() {
		return errors.New("isolated crash after original PG finished before Redis Unprotect")
	}
	return s.MediaWalletStateStore.UnprotectMediaLease(ctx, user, lease, auth, event, pending)
}

func (s *fundingV5FreezeFaultStore) FreezeCanonicalWalletFunding(ctx context.Context, user, leaseID string) (*service.CanonicalWalletFundingBasis, error) {
	basis, err := s.CanonicalWalletFundingStore.FreezeCanonicalWalletFunding(ctx, user, leaseID)
	if err != nil {
		return nil, err
	}
	var principalReady bool
	_ = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2)`, user, leaseID).Scan(&principalReady)
	if basis != nil && principalReady && ((s.closeOnly && basis.Lease.FundingScope == "media") || (!s.closeOnly && leaseID == s.source)) {
		select {
		case s.observed <- leaseID:
		default:
		}
		// The real Redis freeze has happened; inject process interruption before
		// PG can checkpoint its exact pending request. Never fabricate an ACK.
		return nil, errors.New("isolated crash after real Redis freeze before pending request commit")
	}
	return basis, nil
}

func fundingV5SharedSource(t *testing.T, scenario string) {
	loseResponse := scenario == "response-loss" || scenario == "many-sources-response-loss"
	lostFreeze := scenario == "freeze-response-loss"
	manySources := scenario == "many-sources-response-loss"
	pinHandoff := scenario == "pin-handoff"
	zeroResidual := scenario == "zero-residual" || scenario == "zero-surviving-hold" || scenario == "zero-unprotect-restart"
	positiveResidual := scenario == "positive-residual" || scenario == "positive-delivery-fault"
	terminalResidual := zeroResidual || positiveResidual
	survivingHold := scenario == "zero-surviving-hold"
	cleanupRestart := scenario == "zero-unprotect-restart"
	deliveryFault := scenario == "positive-delivery-fault"
	sourceFenced := make(chan struct{}, 1)
	allowPin := make(chan struct{})
	delayedTopup := scenario == "delayed-topup-first" || scenario == "delayed-freeze-first"
	raceTopup := scenario == "topup-race" || delayedTopup
	intentPartial, intentClose := scenario == "intent-partial", scenario == "intent-close"
	grant, sourceFunded, sourceReturn, mediaQuote, finalAvailable := int64(1000000000), int64(1000000000), int64(900000000), int64(200000000), int64(900000000)
	if raceTopup {
		grant, sourceFunded, sourceReturn, mediaQuote, finalAvailable = 1300000000, 1300000000, 1200000000, 600000000, 1200000000
	}
	if scenario == "delayed-freeze-first" {
		sourceFunded, sourceReturn = 1000000000, 900000000
	}
	expectedConsumed := int64(100000000)
	retainedOtherHeld := int64(0)
	if pinHandoff || survivingHold {
		sourceReturn, finalAvailable, expectedConsumed, retainedOtherHeld = 800000000, 800000000, 200000000, 100000000
	}

	base, secret := immediateV5WireConfig(t)
	user := "funding-" + uuid.NewString()
	f := newMediaFixtureForUser(t, user)
	f.svc.Stop()
	f.bridge.Close()
	var err error
	leaseID := "funding-source-" + uuid.NewString()
	seed := map[string]any{"platform_user_id": user, "grant_units": strconv.FormatInt(grant, 10), "lease_id": leaseID, "budget_units": "1000000000"}
	if manySources {
		seed["source_count"] = 1002
		seed["source_prefix_units"] = "100000000"
	}
	code, body := immediateV5WirePost(t, base, secret, "/__fixture/seed", seed, true)
	require.Less(t, code, 300, string(body))
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	original := service.CanonicalWalletLease{LeaseID: leaseID, PlatformUserID: user, Currency: "USD", BudgetUnits: 1000000000, FundedUnits: 1000000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), original))
	walletURL := base
	lostRequest := make(chan []byte, 1)
	lostReceipt := make(chan []byte, 1)
	allowRecovery := &atomic.Bool{}
	topupCommitted := make(chan struct{}, 1)
	mediaEnsureStarted := &atomic.Int64{}
	if loseResponse || raceTopup || lostFreeze || pinHandoff {
		target, e := url.Parse(base)
		require.NoError(t, e)
		drop := &atomic.Bool{}
		drop.Store(true)
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, e := io.ReadAll(r.Body)
			if e != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			forwarded := r.Clone(r.Context())
			forwarded.URL.Scheme, forwarded.URL.Host = target.Scheme, target.Host
			forwarded.RequestURI = ""
			forwarded.Body = io.NopCloser(bytes.NewReader(raw))
			resp, e := http.DefaultClient.Do(forwarded)
			if e != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			response, e := io.ReadAll(resp.Body)
			if e != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if raceTopup && r.URL.Path == "/api/internal/v2/wallet/leases/ensure" {
				var request map[string]any
				_ = json.Unmarshal(raw, &request)
				if request["funding_scope"] == "media" {
					mediaEnsureStarted.Add(1)
				}
				if !delayedTopup && request["top_up_lease_id"] == leaseID && resp.StatusCode >= 200 && resp.StatusCode < 300 {
					select {
					case topupCommitted <- struct{}{}:
					default:
					}
					// The actual D1 top-up is committed, but its response/install is lost.
					// Its PG lifecycle lock stays held until the request is cancelled.
					<-r.Context().Done()
					return
				}
			}
			if pinHandoff && r.URL.Path == "/api/internal/v2/wallet/leases/freeze-source" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var request map[string]any
				_ = json.Unmarshal(raw, &request)
				if request["lease_id"] == leaseID {
					select {
					case sourceFenced <- struct{}{}:
					default:
					}
					select {
					case <-allowPin:
					case <-r.Context().Done():
						return
					}
				}
			}
			if ((r.URL.Path == "/api/internal/v2/wallet/leases/return" && loseResponse) || (r.URL.Path == "/api/internal/v2/wallet/leases/freeze-source" && lostFreeze)) && resp.StatusCode >= 200 && resp.StatusCode < 300 && !allowRecovery.Load() {
				// The genuine D1 transaction has committed. Inject only response
				// loss; never substitute a successful financial receipt.
				if drop.CompareAndSwap(true, false) {
					lostRequest <- raw
					lostReceipt <- response
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"code":-1,"message":"isolated response loss after actual D1 commit","data":{"reason":"fixture_response_lost"}}`))
				return
			}
			for name, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(name, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(response)
		}))
		t.Cleanup(proxy.Close)
		walletURL = proxy.URL
	}
	f.cfg.CanonicalWallet.ControlPlaneURL = walletURL
	f.cfg.CanonicalWallet.Secret = secret
	f.cfg.CanonicalWallet.Issuer = "sub2api"
	f.cfg.CanonicalWallet.Audience = "shipany-wallet"
	f.cfg.CanonicalWallet.RequestTimeoutMS = 3000
	f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "enabled"
	if terminalResidual {
		f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled"
	}
	var bridgeWallet service.CanonicalWalletLeaseStore = wallet
	var unprotectFault *fundingV7UnprotectFaultStore
	if cleanupRestart {
		unprotectFault = &fundingV7UnprotectFaultStore{CanonicalWalletLeaseStore: wallet, CanonicalWalletPoolStore: wallet.(service.CanonicalWalletPoolStore), CanonicalWalletFundingStore: wallet.(service.CanonicalWalletFundingStore), MediaWalletStateStore: wallet.(service.MediaWalletStateStore), source: leaseID}
		bridgeWallet = unprotectFault
	}
	var freezeFault *fundingV5FreezeFaultStore
	if intentPartial || intentClose {
		freezeFault = &fundingV5FreezeFaultStore{CanonicalWalletLeaseStore: wallet, CanonicalWalletPoolStore: wallet.(service.CanonicalWalletPoolStore), CanonicalWalletFundingStore: wallet.(service.CanonicalWalletFundingStore), MediaWalletStateStore: wallet.(service.MediaWalletStateStore), source: leaseID, db: f.db, closeOnly: intentClose, observed: make(chan string, 1)}
		bridgeWallet = freezeFault
	}
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, bridgeWallet, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	f.svc.Stop()
	var llm *service.AuthorizationHandle
	var snapshot *service.BillingSnapshot
	if terminalResidual {
		// Persist the LLM snapshot and policy before its original real hold.
		template := f.create(t, "funding-original-llm-zero", "google/nano-banana", "1:1")
		var snapshotID string
		require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, template.TaskID).Scan(&snapshotID))
		snapshot, err = repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
		require.NoError(t, err)
		snapshot.ID = "snapshot-funding-llm-zero-" + uuid.NewString()
		snapshot.Family = service.BillingFamilyOpenAI
		snapshot.RequestedModel, snapshot.BillingModel = "gpt-5.1", "gpt-5.1"
		snapshot.Candidates = []string{"gpt-5.1"}
		snapshot.Pricing = service.BillingSnapshotPricing{Mode: service.BillingModeToken, Source: service.PricingSourceLiteLLM, Base: &service.ModelPricing{InputPricePerToken: .8}}
		snapshot.Multipliers = service.BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}
		snapshot.Flags.WalletImmediateReleasePolicyVersion = service.WalletImmediateReleasePolicyVersion
		require.NoError(t, f.snapshots.Persist(context.Background(), snapshot))
		_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, template.TaskID)
		require.NoError(t, err)
		owner, e := f.users.GetByID(context.Background(), f.userID)
		require.NoError(t, e)
		llm, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 100000000})
		require.NoError(t, err)
	} else {
		llm, snapshot = f.authorize(t, 100000000, service.BillingFamilyOpenAI)
	}
	require.Len(t, llm.Segments, 1)
	var extra *service.AuthorizationHandle
	var extraSnapshot *service.BillingSnapshot
	if survivingHold {
		owner, e := f.users.GetByID(context.Background(), f.userID)
		require.NoError(t, e)
		extra, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 100000000})
		require.NoError(t, err)
		require.Len(t, extra.Segments, 1)
		require.Equal(t, leaseID, extra.LeaseID)
	}
	if pinHandoff {
		// Both real Redis arms win before the source fence; only their D1 pins
		// lag. A fresh post-fence pool must not plan another LLM hold.
		extra, extraSnapshot = f.authorize(t, 100000000, service.BillingFamilyOpenAI)
		require.Equal(t, leaseID, extra.LeaseID)
	}
	createLLMPin := func() {
		code, body = immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/create", map[string]any{"authorization_id": llm.ID, "authorization_kind": "llm", "gateway_job_id": llm.ID, "platform_user_id": user, "lease_id": leaseID, "held": amount(100000000), "billing_snapshot_id": snapshot.ID, "settlement_event_id": llm.Segments[0].EventID, "usd_wallet_policy_version": "usd-wallet-v1"}, false)
		require.Less(t, code, 300, string(body))
	}
	if !pinHandoff {
		createLLMPin()
	}
	if survivingHold {
		status, response := immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/create", map[string]any{"authorization_id": extra.ID, "authorization_kind": "llm", "gateway_job_id": extra.ID, "platform_user_id": user, "lease_id": leaseID, "held": amount(100000000), "billing_snapshot_id": snapshot.ID, "settlement_event_id": extra.Segments[0].EventID, "usd_wallet_policy_version": "usd-wallet-v1"}, false)
		require.Less(t, status, 300, string(response))
	}

	captureOldHeld := func() {
		converted, e := wallet.ConvertCanonicalWalletHold(context.Background(), user, llm.ID, llm.Segments[0].EventID, 80000000, time.Now())
		require.NoError(t, e)
		require.Equal(t, 0, converted.Code)
		status, response := fundingV5WirePost(t, base, secret, "/api/internal/v2/wallet/settlements", "wallet:settlement", map[string]any{"platform_user_id": user, "lease_id": leaseID, "event_id": llm.Segments[0].EventID, "gateway_request_id": llm.ID, "amount": amount(80000000), "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)})
		require.Less(t, status, 300, string(response))
		status, response = immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/finish", map[string]any{"authorization_id": llm.ID, "authorization_kind": "llm", "gateway_job_id": llm.ID, "platform_user_id": user, "lease_id": leaseID, "held": amount(100000000), "billing_snapshot_id": snapshot.ID, "settlement_event_id": llm.Segments[0].EventID, "usd_wallet_policy_version": "usd-wallet-v1", "resolution": "settled", "actual": amount(80000000), "settlement_event_ids": []string{llm.Segments[0].EventID}}, false)
		require.Less(t, status, 300, string(response))
	}

	template := f.create(t, "funding-template", "google/nano-banana", "1:1")
	taskID := "media_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	authID := "auth_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	eventID := "funding-event-" + uuid.NewString()
	var templateSnapshotID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, template.TaskID).Scan(&templateSnapshotID))
	quotedSnapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), templateSnapshotID)
	require.NoError(t, err)
	quotedSnapshot.ID = "snapshot-funding-" + uuid.NewString()
	quotedSnapshot.FrozenAt = time.Now().UTC()
	quotedSnapshot.Pricing.DefaultPerRequestPrice = float64(mediaQuote) / 100000000
	require.NoError(t, repository.ProvideBillingSnapshotStore(f.db).InsertBillingSnapshot(context.Background(), quotedSnapshot))
	var snapshotID string
	err = f.db.QueryRow(`INSERT INTO gateway_media_task(id,user_id,platform_user_id,api_key_id,idempotency_key,request_hash,model,media_type,option,prompt,request_payload,billing_snapshot_id,quoted_units,authorization_id,settlement_event_id,deadline_at,financial_policy_version,financial_runtime_deadline)
 SELECT $2,user_id,platform_user_id,api_key_id,$2,request_hash,model,media_type,option,prompt,request_payload,$5,$6,$3,$4,deadline_at,financial_policy_version,financial_runtime_deadline FROM gateway_media_task WHERE id=$1 RETURNING billing_snapshot_id`, template.TaskID, taskID, authID, eventID, quotedSnapshot.ID, mediaQuote).Scan(&snapshotID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, template.TaskID)
	require.NoError(t, err)
	mediaSnapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	owner, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	authorizeMedia := func() (*service.AuthorizationHandle, error) {
		return service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(service.WithMediaFundingOwner(context.Background(), taskID), service.AuthorizeInput{Snapshot: mediaSnapshot, User: owner, FixedEstimateUnits: mediaQuote, DurableAuthorizationID: authID})
	}
	var media *service.AuthorizationHandle
	if pinHandoff {
		result := make(chan struct {
			handle *service.AuthorizationHandle
			err    error
		}, 1)
		go func() {
			h, e := authorizeMedia()
			result <- struct {
				handle *service.AuthorizationHandle
				err    error
			}{h, e}
		}()
		select {
		case <-sourceFenced:
		case <-time.After(5 * time.Second):
			t.Fatal("actual source fence did not commit")
		}
		cached, e := wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
		require.NoError(t, e)
		require.False(t, cached.FundingFrozen, "source-side fence freezes principal while the signed ACK is in flight")
		extraHold, e := wallet.GetCanonicalWalletHold(context.Background(), user, extra.ID)
		require.NoError(t, e)
		require.Equal(t, "armed", extraHold.State, "the pre-fence real hold remains backed while its pin is delayed")
		require.Equal(t, int64(100000000), extraHold.HeldUnits)
		status, response := immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/create", map[string]any{"authorization_id": extra.ID, "authorization_kind": "llm", "gateway_job_id": extra.ID, "platform_user_id": user, "lease_id": leaseID, "held": amount(100000000), "billing_snapshot_id": extraSnapshot.ID, "settlement_event_id": extra.Segments[0].EventID, "usd_wallet_policy_version": "usd-wallet-v1"}, false)
		require.Less(t, status, 300, string(response))
		createLLMPin()
		close(allowPin)
		select {
		case completed := <-result:
			media, err = completed.handle, completed.err
		case <-time.After(10 * time.Second):
			t.Fatal("pre-fence held pin handoff stranded return")
		}
	} else if delayedTopup {
		barrierBody := map[string]string{"platform_user_id": user, "lease_id": leaseID}
		status, response := immediateV5WirePost(t, base, secret, "/__fixture/pause-topup", barrierBody, true)
		require.Equal(t, http.StatusOK, status, string(response))
		t.Cleanup(func() { immediateV5WirePost(t, base, secret, "/__fixture/resume-topup", barrierBody, true) })
		topupCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, e := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(topupCtx, service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 1200000000})
			result <- e
		}()
		readBarrier := func() struct {
			Arrived   bool   `json:"arrived"`
			Committed *bool  `json:"committed"`
			Error     string `json:"error"`
		} {
			_, raw := immediateV5WirePost(t, base, secret, "/__fixture/topup-state", barrierBody, true)
			var state struct {
				Arrived   bool   `json:"arrived"`
				Committed *bool  `json:"committed"`
				Error     string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(raw, &state))
			return state
		}
		require.Eventually(t, func() bool { return readBarrier().Arrived }, 5*time.Second, 20*time.Millisecond, "genuine stale topup plan must pause immediately before D1 batch")
		cancel()
		select {
		case e := <-result:
			require.Error(t, e)
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled topup did not release PG lifecycle owner")
		}
		if scenario == "delayed-topup-first" {
			immediateV5WirePost(t, base, secret, "/__fixture/resume-topup", barrierBody, true)
			require.Eventually(t, func() bool { state := readBarrier(); return state.Committed != nil && *state.Committed }, 5*time.Second, 20*time.Millisecond)
			media, err = authorizeMedia()
		} else {
			media, err = authorizeMedia()
			require.NoError(t, err)
			var principal int64
			require.NoError(t, f.db.QueryRow(`SELECT funded_units FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&principal))
			require.Equal(t, int64(1000000000), principal)
			immediateV5WirePost(t, base, secret, "/__fixture/resume-topup", barrierBody, true)
			require.Eventually(t, func() bool { state := readBarrier(); return state.Committed != nil && !*state.Committed }, 5*time.Second, 20*time.Millisecond, "stale topup must atomically abort after canonical source freeze")
			require.Contains(t, readBarrier().Error, "wallet_lease_funding")
		}
	} else if raceTopup {
		topupCtx, cancelTopup := context.WithCancel(context.Background())
		defer cancelTopup()
		topupResult := make(chan error, 1)
		go func() {
			_, e := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(topupCtx, service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 1200000000})
			topupResult <- e
		}()
		select {
		case <-topupCommitted:
		case <-time.After(5 * time.Second):
			t.Fatal("actual D1 top-up did not commit")
		}
		// The cached principal is intentionally old while the genuine source is $13.
		stale, e := wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
		require.NoError(t, e)
		require.Equal(t, int64(1000000000), stale.FundedUnits)
		mediaResult := make(chan struct {
			handle *service.AuthorizationHandle
			err    error
		}, 1)
		go func() {
			h, e := authorizeMedia()
			mediaResult <- struct {
				handle *service.AuthorizationHandle
				err    error
			}{h, e}
		}()
		time.Sleep(100 * time.Millisecond)
		require.Zero(t, mediaEnsureStarted.Load(), "funding ensure cannot cross another in-flight lifecycle gate")
		cancelTopup()
		select {
		case e := <-topupResult:
			require.Error(t, e, "cancelled top-up cannot arm a new obligation")
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled top-up did not release its lifecycle gate")
		}
		select {
		case result := <-mediaResult:
			media, err = result.handle, result.err
		case <-time.After(10 * time.Second):
			t.Fatal("source freeze failed to recover the actual committed top-up principal")
		}
	} else {
		media, err = authorizeMedia()
	}
	if lostFreeze {
		require.Error(t, err, "genuine source fence committed but no immutable PG principal may be inferred from lost response")
		var sent, dropped []byte
		select {
		case sent = <-lostRequest:
		case <-time.After(3 * time.Second):
			t.Fatal("no source freeze request")
		}
		select {
		case dropped = <-lostReceipt:
		case <-time.After(3 * time.Second):
			t.Fatal("no committed source freeze receipt")
		}
		var expected, ack []byte
		require.NoError(t, f.db.QueryRow(`SELECT expected_request,source_receipt FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&expected, &ack))
		require.JSONEq(t, string(sent), string(expected))
		require.Empty(t, ack)
		var absent bool
		require.NoError(t, f.db.QueryRow(`SELECT NOT EXISTS(SELECT 1 FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2)`, user, leaseID).Scan(&absent))
		require.True(t, absent, "unacknowledged source fence cannot lock a guessed principal into PG228")
		f.bridge.Close()
		allowRecovery.Store(true)
		f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
		t.Cleanup(f.bridge.Close)
		require.Eventually(t, func() bool {
			return f.db.QueryRow(`SELECT source_receipt FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&ack) == nil && len(ack) > 0
		}, 10*time.Second, 20*time.Millisecond)
		var genuine struct {
			Data struct {
				Receipt json.RawMessage `json:"receipt"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(dropped, &genuine))
		require.JSONEq(t, string(genuine.Data.Receipt), string(ack), "restart replays the same named signed source fence")
		// The restarted bridge's independent background recovery (runFundingRecovery)
		// completes the durable partial intent with the genuine signed return. Wait
		// for that single return (as the intent-only branch does) so the foreground
		// media authorization does not race it into a redundant zero-money
		// revision that would overwrite the stored receipt of the real return.
		require.Eventually(t, func() bool {
			var revision int64
			return f.db.QueryRow(`SELECT return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&revision) == nil && revision == 1
		}, 10*time.Second, 20*time.Millisecond, "background recovery must obtain the genuine signed return of the replayed fence")
		media, err = authorizeMedia()
	}
	if intentPartial {
		require.Error(t, err)
		select {
		case <-freezeFault.observed:
		case <-time.After(3 * time.Second):
			t.Fatal("real Redis freeze was not interrupted")
		}
		var mode string
		var pending, receipt []byte
		require.NoError(t, f.db.QueryRow(`SELECT requested_mode,pending_request,receipt FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&mode, &pending, &receipt))
		require.Equal(t, "partial", mode)
		require.Empty(t, pending)
		require.Empty(t, receipt)
		frozen, e := wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
		require.NoError(t, e)
		require.True(t, frozen.FundingFrozen)
		f.bridge.Close()
		f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
		t.Cleanup(f.bridge.Close)
		require.Eventually(t, func() bool {
			var revision int64
			return f.db.QueryRow(`SELECT return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&revision) == nil && revision == 1
		}, 10*time.Second, 50*time.Millisecond, "intent-only background recovery must obtain a genuine signed return")
		media, err = authorizeMedia()
	}
	if loseResponse {
		require.Error(t, err, "real D1 committed but the first bridge could not record its ACK")
		var sent, dropped []byte
		select {
		case sent = <-lostRequest:
		case <-time.After(3 * time.Second):
			t.Fatal("no genuine committed return request reached the fault proxy")
		}
		select {
		case dropped = <-lostReceipt:
		case <-time.After(3 * time.Second):
			t.Fatal("no genuine committed return response reached the fault proxy")
		}
		var saved struct {
			Request json.RawMessage `json:"request"`
		}
		var pending, priorReceipt []byte
		var priorRevision int64
		require.NoError(t, f.db.QueryRow(`SELECT pending_request,receipt,return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&pending, &priorReceipt, &priorRevision))
		require.NoError(t, json.Unmarshal(pending, &saved))
		require.JSONEq(t, string(sent), string(saved.Request), "durable request predates the exact real D1 RPC")
		require.Empty(t, priorReceipt)
		require.Zero(t, priorRevision)
		changed := append([]byte(nil), pending...)
		var altered map[string]any
		require.NoError(t, json.Unmarshal(changed, &altered))
		altered["request"].(map[string]any)["mode"] = "close"
		changed, err = json.Marshal(altered)
		require.NoError(t, err)
		_, err = f.db.Exec(`UPDATE wallet_funding_freeze SET pending_request=$3::jsonb WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID, string(changed))
		require.Error(t, err, "the exact pending request cannot be replaced")
		f.bridge.Close()
		// Existing legitimate conversion continues while the original return
		// is committed in D1 but primary ACK and local budget reduction lag.
		captureOldHeld()
		allowRecovery.Store(true)
		f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
		t.Cleanup(f.bridge.Close)
		_, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 1100000000})
		require.Error(t, err, "the recovery probe asks more than all available funds and cannot issue or arm")
		var recovered []byte
		var revision int64
		require.NoError(t, f.db.QueryRow(`SELECT receipt,pending_request,return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&recovered, &pending, &revision))
		require.Empty(t, pending)
		require.Equal(t, int64(1), revision)
		var genuine struct {
			Data struct {
				Receipt json.RawMessage `json:"receipt"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(dropped, &genuine))
		require.JSONEq(t, string(genuine.Data.Receipt), string(recovered), "restart saves the exact original signed receipt despite changed current C/R and pins")
		media, err = authorizeMedia()
	}
	require.NoError(t, err, "unleased zero still funds the full media quote from the signed original source return")
	expectedSegments := 1
	if scenario == "delayed-freeze-first" {
		expectedSegments = 2
	}
	require.Len(t, media.Segments, expectedSegments)
	require.NotEqual(t, leaseID, media.LeaseID)
	require.Equal(t, mediaQuote, media.HeldUnits)
	require.Equal(t, "media", media.Segments[0].Basis.FundingScope)
	require.Equal(t, taskID, media.Segments[0].Basis.FundingOwnerID)
	require.Equal(t, taskID+".funding.0", media.Segments[0].Basis.FundingIssuanceKey)
	source, err := wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
	require.NoError(t, err)
	require.True(t, source.FundingFrozen)
	require.Equal(t, sourceFunded, source.FundedUnits)
	require.Equal(t, sourceReturn, source.ReturnedUnits)
	require.Equal(t, expectedConsumed, source.BudgetUnits)
	require.Equal(t, expectedConsumed, source.ConsumedUnits, "freeze never fabricates consumption")
	expectedReleased := int64(0)
	expectedHoldState := "armed"
	if loseResponse {
		expectedReleased, expectedHoldState = 20000000, "settled"
	}
	require.Equal(t, expectedReleased, source.ReleasedUnits)
	require.Zero(t, source.RemainingUnits(), "no new arm may spend the frozen source")
	held, err := wallet.GetCanonicalWalletHold(context.Background(), user, llm.ID)
	require.NoError(t, err)
	require.Equal(t, int64(100000000), held.HeldUnits)
	require.Equal(t, expectedHoldState, held.State)
	require.Error(t, wallet.InstallCanonicalWalletLease(context.Background(), original), "old snapshot cannot restore the already returned9")
	var receipt service.WalletFundingReturnReceipt
	var raw []byte
	require.NoError(t, f.db.QueryRow(`SELECT receipt FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&raw))
	require.NoError(t, json.Unmarshal(raw, &receipt))
	require.Equal(t, strconv.FormatInt(sourceReturn, 10), receipt.CreditedUnits)
	require.NotEmpty(t, receipt.Signature)
	if pinHandoff {
		require.Equal(t, "200000000", receipt.HeldUnits, "source return retains both genuine pre-freeze winning arms")
		err = wallet.(service.CanonicalWalletPoolStore).ArmCanonicalWalletPool(context.Background(), user, []service.AuthorizationSegment{{AuthorizationID: "post-return-" + uuid.NewString(), LeaseID: leaseID, HeldUnits: 1, Basis: original, Kind: "llm"}}, 0, time.Now())
		require.Error(t, err, "once Redis frozen and return committed no new arm may spend the original principal")
	}
	if manySources {
		require.Len(t, receipt.Sources, 1001, "actual signed return must cross the former consumer1000 limit and ACK successfully")
	}
	if raceTopup {
		require.Equal(t, strconv.FormatInt(sourceFunded, 10), receipt.FundedUnits)
		expectedRevision := int64(1)
		if scenario == "delayed-freeze-first" {
			expectedRevision = 0
		}
		require.Equal(t, expectedRevision, receipt.BudgetRevision)
		var principal int64
		require.NoError(t, f.db.QueryRow(`SELECT funded_units FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&principal))
		require.Equal(t, sourceFunded, principal, "primary intent uses fresh actual D1 principal, never stale Redis10")
	}

	_, err = f.db.Exec(`DELETE FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID)
	require.Error(t, err, "PG funding tombstone survives Redis loss and cannot be deleted")

	if !loseResponse && !terminalResidual {
		// Losing Redis cannot reinstall the pre-return snapshot. The public LLM
		// authorizer replays the genuine signed primary ACK, then refuses missing
		// obligation state. Restoring the confirmed reduced basis preserves held1.
		leaseKeys, err := f.rdb.Keys(context.Background(), "canonical_wallet:lease:*"+leaseID).Result()
		require.NoError(t, err)
		require.Len(t, leaseKeys, 1)
		require.NoError(t, f.rdb.Del(context.Background(), leaseKeys[0], leaseKeys[0]+":funding").Err())
		_, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 100000000})
		require.Error(t, err, "missing source cache cannot fabricate zero consumed")
		require.Equal(t, "1", f.rdb.HGet(context.Background(), leaseKeys[0]+":funding", "return_revision").Val(), "genuine primary ACK rebuilt the permanent Redis fence")
		require.Error(t, wallet.InstallCanonicalWalletLease(context.Background(), original))
		require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), *source))
		held, err = wallet.GetCanonicalWalletHold(context.Background(), user, llm.ID)
		require.NoError(t, err)
		require.Equal(t, expectedHoldState, held.State)
		require.Equal(t, int64(100000000), held.HeldUnits)

	}

	// An accepted provider task reports terminal failure through a real query.
	// Only the actual signed zero ACK and complete cleanup may close its lease.
	f.mode.Store(2)
	f.svc = f.newService(t)
	result := f.wait(t, taskID, "released")
	require.Equal(t, "failed", result.Status)
	require.Equal(t, strconv.FormatInt(mediaQuote/100000000, 10), *result.Billing.ReleasedUSD)
	if intentClose {
		var frozenLease string
		select {
		case frozenLease = <-freezeFault.observed:
		case <-time.After(5 * time.Second):
			t.Fatal("real media Redis freeze was not interrupted")
		}
		require.Equal(t, media.LeaseID, frozenLease)
		var mode string
		var pending, receipt []byte
		var closed bool
		require.NoError(t, f.db.QueryRow(`SELECT requested_mode,pending_request,receipt,closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, media.LeaseID).Scan(&mode, &pending, &receipt, &closed))
		require.Equal(t, "close", mode)
		require.Empty(t, pending)
		require.Empty(t, receipt)
		require.False(t, closed)
		_, e := f.db.Exec(`UPDATE wallet_funding_freeze SET requested_mode='partial' WHERE platform_user_id=$1 AND lease_id=$2`, user, media.LeaseID)
		require.Error(t, e, "restart cannot downgrade the permanent close-only intent")
		f.svc.Stop()
		f.bridge.Close()
		f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
		t.Cleanup(f.bridge.Close)
	}
	require.Eventually(t, func() bool {
		var closed int
		return f.db.QueryRow(`SELECT count(*) FROM wallet_funding_freeze WHERE platform_user_id=$1 AND funding_scope='media' AND funding_owner_id=$2 AND closed`, user, taskID).Scan(&closed) == nil && closed == len(media.Segments)
	}, 10*time.Second, 50*time.Millisecond, "every source-backed chunk must close exactly once before counting available grants")
	code, body = immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": user}, true)
	require.Less(t, code, 300, string(body))
	var inspected struct {
		Credits []struct {
			RemainingUnits string `json:"remainingUnits"`
		} `json:"credits"`
		Leases []struct {
			ID, Status    string
			ReleasedUnits string `json:"releasedUnits"`
		} `json:"leases"`
	}
	require.NoError(t, json.Unmarshal(body, &inspected))
	remaining := int64(0)
	for _, credit := range inspected.Credits {
		n, e := strconv.ParseInt(credit.RemainingUnits, 10, 64)
		require.NoError(t, e)
		remaining += n
	}
	require.Equal(t, finalAvailable, remaining, "original free principal returns exactly once; the unrelated LLM held1 remains backed")
	source, err = wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
	require.NoError(t, err)
	require.Equal(t, expectedConsumed, source.ConsumedUnits)
	held, err = wallet.GetCanonicalWalletHold(context.Background(), user, llm.ID)
	require.NoError(t, err)
	require.Equal(t, expectedHoldState, held.State)
	if positiveResidual {
		f.svc.Stop()
		billingRepository := repository.NewUsageBillingRepository(nil, f.db)
		f.bridge.SetBillingEvidenceRepository(billingRepository)
		usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, f.users, nil, nil, repository.NewGatewayCache(f.rdb), f.cfg, f.db, repository.ProvideWalletOutboxStore(f.db), nil, nil, service.NewBillingService(f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, f.snapshots)
		t.Cleanup(usageService.CloseOpenAIWSPool)
		if deliveryFault {
			_, err = f.db.Exec(`CREATE FUNCTION funding_v7_delivered_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.authorization_id='` + llm.ID + `' AND NEW.status='delivered' THEN RAISE EXCEPTION 'isolated original capture delivered ACK fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER funding_v7_delivered_fault BEFORE UPDATE ON wallet_settlement_outbox FOR EACH ROW EXECUTE FUNCTION funding_v7_delivered_fault()`)
			require.NoError(t, err)
		}
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"funding-v7-original","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":0}}`)
		}))
		defer provider.Close()
		request, e := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), llm), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
		require.NoError(t, e)
		started := time.Now()
		response, e := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, f.cfg).Do(request, "", 0, 1)
		require.NoError(t, e)
		_, e = io.Copy(io.Discard, response.Body)
		require.NoError(t, e)
		require.NoError(t, response.Body.Close())
		var usageErr error
		dispatch, e := llm.PrepareUsageTask(context.Background(), func(ctx context.Context) {
			usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "funding-v7-original-" + llm.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: owner, APIKey: &service.APIKey{ID: snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: snapshot, AuthorizationID: llm.ID, AuthorizationToken: llm.LastWriteToken()})
		})
		require.NoError(t, usageErr, "use original PG billing receipt and canonical outbox, never direct Redis conversion or D1 capture")
		require.NoError(t, e, "the original stopped reader seals its durable normalized billing handoff before dispatch")
		require.NotNil(t, dispatch)
		dispatch(context.Background())
		var fee int64
		var apply, canonical bool
		require.Eventually(t, func() bool {
			return f.db.QueryRow(`SELECT fee_units,apply_ack_at IS NOT NULL,canonical_ack_at IS NOT NULL FROM wallet_billing_pending WHERE parent_authorization_id=$1`, llm.ID).Scan(&fee, &apply, &canonical) == nil && fee == 80000000 && apply && canonical
		}, 5*time.Second, 20*time.Millisecond)
		var receipts int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, llm.ID).Scan(&receipts))
		require.Equal(t, 1, receipts)
		if deliveryFault {
			require.Eventually(t, func() bool {
				code, body := immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": user}, true)
				return code == 200 && strings.Contains(string(body), `"amountUnits":"80000000"`)
			}, 5*time.Second, 20*time.Millisecond, "the genuine D1 capture must commit before the injected original PG delivered-ACK failure")
			var closed bool
			var revision int64
			var mode string
			require.Never(t, func() bool {
				_ = f.db.QueryRow(`SELECT closed,return_revision,requested_mode FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&closed, &revision, &mode)
				return closed || revision != 1 || mode == "close"
			}, 700*time.Millisecond, 20*time.Millisecond, "pending original positive delivery cannot persist close or create zero-money revisions")
			var originalStatus string
			require.NoError(t, f.db.QueryRow(`SELECT status FROM wallet_settlement_outbox WHERE event_id=$1`, llm.Segments[0].EventID).Scan(&originalStatus))
			require.Equal(t, "in_flight", originalStatus, "real D1 capture committed but the injected PG ACK failure retains the dispatch claim")
			_, err = f.db.Exec(`DROP TRIGGER funding_v7_delivered_fault ON wallet_settlement_outbox; DROP FUNCTION funding_v7_delivered_fault()`)
			require.NoError(t, err)
			faultReleased := time.Now()
			require.Eventually(t, func() bool {
				return f.db.QueryRow(`SELECT status FROM wallet_settlement_outbox WHERE event_id=$1 AND amount_units=80000000 AND billing_snapshot_id=$2`, llm.Segments[0].EventID, snapshot.ID).Scan(&originalStatus) == nil && originalStatus == "delivered"
			}, outboxStaleReclaimWindow(f.cfg.CanonicalWallet.RequestTimeoutMS), 20*time.Millisecond, "the existing stale in-flight reclaim (2*batch*RequestTimeout) must obtain the original delivered ACK after the actual PG fault heals")
			t.Logf("actual original PG delivered-ACK fault recovery elapsed=%s; source-tail timing starts after this distinct dependency recovery", time.Since(faultReleased))
			// Healthy positive coverage above retains the original terminal
			// clock. This injected ACK failure separately measures existing
			// stale-claim recovery and availability after the original ACK.
			started = time.Now()
		}
		var closed bool
		var revision, returned int64
		require.Eventually(t, func() bool {
			return f.db.QueryRow(`SELECT closed,return_revision,returned_units,receipt FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&closed, &revision, &returned, &raw) == nil && closed && revision == 2 && returned == 920000000
		}, 5*time.Second, 20*time.Millisecond)
		require.NoError(t, json.Unmarshal(raw, &receipt))
		require.Equal(t, "close", receipt.Mode)
		require.Equal(t, "80000000", receipt.CapturedUnits)
		require.Equal(t, "20000000", receipt.ReturnedUnits)
		require.Equal(t, "0", receipt.HeldUnits)
		require.Equal(t, "100000000", receipt.GatewayConsumedUnits)
		require.Equal(t, "20000000", receipt.GatewayReleasedUnits)
		var delivered bool
		require.NoError(t, f.db.QueryRow(`SELECT status='delivered' AND amount_units=80000000 AND billing_snapshot_id=$2 FROM wallet_settlement_outbox WHERE event_id=$1`, llm.Segments[0].EventID, snapshot.ID).Scan(&delivered))
		require.True(t, delivered)
		fresh, e := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: 900000000})
		require.NoError(t, e, "fresh public LLM authorization spends the genuinely returned9.2 after original positive.8")
		require.NotEqual(t, leaseID, fresh.LeaseID)
		require.Less(t, time.Since(started), 2500*time.Millisecond, "original terminal handoff plus fresh authorization retains <=2s with 500ms isolated runtime jitter")
		return
	}
	if zeroResidual {
		// Complete only the original LLM reader. No new authorization, media
		// request, explicit source-return RPC or expired-lease sweep may trigger
		// the retained $1 return after its genuine signed zero release.
		f.svc.Stop()
		require.True(t, source.ExpiresAt.After(time.Now().Add(50*time.Minute)))
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"safe validation rejection"}}`)
		}))
		defer provider.Close()
		decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, f.cfg)
		request, e := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), llm), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
		require.NoError(t, e)
		zeroStarted := time.Now()
		if cleanupRestart {
			unprotectFault.deny.Store(true)
		}
		response, e := decorated.Do(request, "", 0, 1)
		require.NoError(t, e)
		require.Equal(t, http.StatusUnauthorized, response.StatusCode)
		_, e = io.Copy(io.Discard, response.Body)
		require.NoError(t, e)
		require.NoError(t, response.Body.Close())
		var zeroRaw []byte
		var zeroReleased time.Time
		require.NoError(t, f.db.QueryRow(`SELECT zero_receipt,zero_released_at FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0 AND state='finished' AND zero_ack_at IS NOT NULL`, llm.ID).Scan(&zeroRaw, &zeroReleased))
		var zeroReceipt service.WalletTaskPinReceipt
		require.NoError(t, json.Unmarshal(zeroRaw, &zeroReceipt))
		segment := llm.Segments[0]
		signedAt, e := service.VerifyWalletTaskPinReceipt(secret, service.WalletTaskPinReceiptExpected{GatewayJobID: llm.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: user, LeaseID: leaseID, BillingSnapshotID: snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: llm.LastWriteToken(), Status: "released"}, zeroReceipt)
		require.NoError(t, e)
		require.True(t, zeroReleased.Equal(signedAt))
		require.Less(t, time.Since(zeroStarted), 2500*time.Millisecond, "signed zero release retains the <=2s target with 500ms isolated runtime jitter")
		held, e = wallet.GetCanonicalWalletHold(context.Background(), user, llm.ID)
		require.NoError(t, e)
		require.Equal(t, "released", held.State)
		source, e = wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
		require.NoError(t, e)
		require.Equal(t, expectedConsumed, source.ConsumedUnits)
		require.Equal(t, int64(100000000), source.ReleasedUnits)
		if cleanupRestart {
			var generation, appliedGeneration int64
			require.Eventually(t, func() bool {
				return f.db.QueryRow(`SELECT generation,applied_generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&generation, &appliedGeneration) == nil && generation > appliedGeneration
			}, 2*time.Second, 20*time.Millisecond, "original ACK must durably enqueue cleanup before Unprotect failure")
			var closed bool
			var revision int64
			require.NoError(t, f.db.QueryRow(`SELECT closed,return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&closed, &revision))
			require.False(t, closed)
			require.Equal(t, int64(1), revision)
			f.bridge.Close()
			f.cfg.CanonicalWallet.LLMImmediateReleaseMode, f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "off", "off"
			f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
			t.Cleanup(f.bridge.Close)
			zeroStarted = time.Now()
		}
		var revision, returned int64
		var applied int64
		var pending []byte
		defer func() {
			t.Logf("original signed-zero source recovery: revision=%d returned=%d applied=%d pending_bytes=%d zero_elapsed=%s", revision, returned, applied, len(pending), time.Since(zeroStarted))
		}()
		expectedReturned := grant
		if survivingHold {
			expectedReturned -= 100000000
		}
		require.Eventually(t, func() bool {
			e := f.db.QueryRow(`SELECT return_revision,returned_units,redis_applied_revision,receipt,pending_request FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&revision, &returned, &applied, &raw, &pending)
			return e == nil && revision == 2 && returned == expectedReturned && applied == revision && len(pending) == 0
		}, 5*time.Second, 20*time.Millisecond, "signed revision2 returns the original released $1 without a new request or TTL")
		require.NoError(t, json.Unmarshal(raw, &receipt))
		expectedMode, expectedHeld := "close", "0"
		if survivingHold {
			expectedMode, expectedHeld = "partial", "100000000"
		}
		require.Equal(t, expectedMode, receipt.Mode)
		require.Equal(t, "1000000000", receipt.FundedUnits)
		require.Equal(t, strconv.FormatInt(sourceReturn, 10), receipt.ReturnedBeforeUnits)
		require.Equal(t, "100000000", receipt.ReturnedUnits)
		require.Equal(t, "100000000", receipt.CreditedUnits)
		require.Equal(t, strconv.FormatInt(expectedReturned, 10), receipt.ReturnedAfterUnits)
		require.Equal(t, "0", receipt.CapturedUnits)
		require.Equal(t, expectedHeld, receipt.HeldUnits)
		require.Equal(t, strconv.FormatInt(expectedConsumed, 10), receipt.GatewayConsumedUnits)
		require.Equal(t, "100000000", receipt.GatewayReleasedUnits)
		sourcesJSON, e := json.Marshal(receipt.Sources)
		require.NoError(t, e)
		payload, e := json.Marshal([]string{receipt.Protocol, receipt.ReceiptID, receipt.PlatformUserID, receipt.LeaseID, receipt.FundingScope, receipt.FundingOwnerID, receipt.FundingIssuanceKey, strconv.FormatInt(receipt.ReturnRevision, 10), strconv.FormatInt(receipt.BudgetRevision, 10), strconv.FormatInt(receipt.CaptureSeq, 10), receipt.FundedUnits, receipt.CapturedUnits, receipt.ReturnedBeforeUnits, receipt.ReturnedUnits, receipt.ReturnedAfterUnits, receipt.CreditedUnits, receipt.WriteOffUnits, receipt.HeldUnits, receipt.GatewayConsumedUnits, receipt.GatewayReleasedUnits, receipt.Mode, receipt.CommittedAt, string(sourcesJSON)})
		require.NoError(t, e)
		mac := hmac.New(sha256.New, []byte(secret))
		_, e = mac.Write(payload)
		require.NoError(t, e)
		signature, e := hex.DecodeString(receipt.Signature)
		require.NoError(t, e)
		require.True(t, hmac.Equal(mac.Sum(nil), signature), "the second return is an original-identity signed canonical receipt")
		code, body = immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": user}, true)
		require.Less(t, code, 300, string(body))
		require.NoError(t, json.Unmarshal(body, &inspected))
		remaining = 0
		for _, credit := range inspected.Credits {
			n, e := strconv.ParseInt(credit.RemainingUnits, 10, 64)
			require.NoError(t, e)
			remaining += n
		}
		require.Equal(t, expectedReturned, remaining, "signed zero plus media zero return all free principal while preserving every surviving hold")
		source, e = wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
		require.NoError(t, e)
		require.Equal(t, expectedReturned, source.ReturnedUnits)
		require.Equal(t, grant-expectedReturned, source.BudgetUnits)
		require.True(t, source.ExpiresAt.After(time.Now().Add(50*time.Minute)))
		var debits, unknown int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE authorization_id=$1`, llm.ID).Scan(&debits))
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, llm.ID).Scan(&unknown))
		require.Zero(t, debits)
		require.Zero(t, unknown)
		if survivingHold {
			other, e := wallet.GetCanonicalWalletHold(context.Background(), user, extra.ID)
			require.NoError(t, e)
			require.Equal(t, "armed", other.State)
			require.Equal(t, int64(100000000), other.HeldUnits)
			require.Never(t, func() bool {
				var later int64
				var closed bool
				_ = f.db.QueryRow(`SELECT return_revision,closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&later, &closed)
				return later > 2 || closed
			}, 600*time.Millisecond, 20*time.Millisecond, "unchanged surviving backing cannot create zero revisions or close its original slot")
		} else {
			var closed bool
			require.NoError(t, f.db.QueryRow(`SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&closed))
			require.True(t, closed, "zero residual must release the canonical caller slot too")
			fresh, e := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: owner, FixedEstimateUnits: grant})
			require.NoError(t, e, "the public LLM authorizer may immediately spend the returned original full balance")
			require.NotEqual(t, leaseID, fresh.LeaseID)
			require.Less(t, time.Since(zeroStarted), 2500*time.Millisecond, "original signed zero plus source close and fresh public authorization meet <=2s with 500ms isolated jitter")
		}
		return
	}

	// Freeze observations can become stale when an existing obligation converts
	// and captures. The old observation must not return a second free tail.
	staleReturn := map[string]any{"platform_user_id": user, "lease_id": leaseID, "mode": "partial", "base_return_revision": 1, "source_freeze_id": leaseID + ".source-freeze.v1", "expected_funded": amount(sourceFunded), "expected_budget_revision": receipt.BudgetRevision,
		"gateway_consumed": amount(100000000), "gateway_released": amount(0), "holds": []map[string]any{{"authorization_id": llm.ID, "held": amount(100000000)}}}
	if !loseResponse {
		captureOldHeld()
	}
	code, body = fundingV5WirePost(t, base, secret, "/api/internal/v2/wallet/leases/return", "wallet:lease", staleReturn)
	require.Equal(t, http.StatusConflict, code, string(body))
	require.Contains(t, string(body), "funding_conflict")
	code, body = immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": user}, true)
	require.Less(t, code, 300, string(body))
	require.NoError(t, json.Unmarshal(body, &inspected))
	remaining = 0
	for _, credit := range inspected.Credits {
		n, e := strconv.ParseInt(credit.RemainingUnits, 10, 64)
		require.NoError(t, e)
		remaining += n
	}
	require.Equal(t, finalAvailable, remaining, "stale conversion observation cannot mint or duplicate the original source return")
	source, err = wallet.GetCanonicalWalletLeaseByID(context.Background(), user, leaseID)
	require.NoError(t, err)
	require.Equal(t, expectedConsumed, source.ConsumedUnits)
	require.Equal(t, int64(20000000), source.ReleasedUnits)
	require.Equal(t, grant, remaining+80000000+retainedOtherHeld+(source.BudgetUnits-source.ConsumedUnits+source.ReleasedUnits), "original principal equals available9 + captured.8 + still-backed residual.2")
}

func fundingV5WirePost(t *testing.T, base, secret, path, scope string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(raw))
	require.NoError(t, err)
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": "sub2api", "aud": "shipany-wallet", "sub": "sub2api-gateway", "iat": now.Unix(), "exp": now.Add(30 * time.Second).Unix(), "jti": uuid.NewString(), "scope": scope})
	token.Header["kid"] = "v1"
	signed, err := token.SignedString([]byte(secret))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+signed)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return resp.StatusCode, response
}

// outboxStaleReclaimWindow derives the real recovery bound of the production
// outbox dispatcher instead of guessing it. runOutboxDispatcher reclaims an
// in_flight row after staleAfter = 2 * walletOutboxDispatchBatch(10) *
// RequestTimeout and only on a RequestTimeout tick, so recovery after the PG
// fault heals can take up to staleAfter + one tick. A 50% margin absorbs the
// claim, D1 round trip and scheduler jitter. Production timeouts are never
// changed to fit a test; this is a dependency-fault recovery bound, separate
// from the healthy-path <=2s goal measured after the original ACK.
func outboxStaleReclaimWindow(requestTimeoutMS int) time.Duration {
	perAttempt := time.Duration(requestTimeoutMS) * time.Millisecond
	return (2*10*perAttempt + perAttempt) * 3 / 2
}
