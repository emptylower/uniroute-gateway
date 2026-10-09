//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Provider traffic is the local KIE fixture. Financial RPCs execute the real
// Worker routes against D1, including canonical pool and source-fence discovery.
func mediaV5WireFixture(t *testing.T, mode string) (*mediaFixture, *atomic.Bool) {
	t.Helper()
	base, secret := immediateV5WireConfig(t)
	f := newMediaFixtureForUser(t, "media-user-v5-"+uuid.NewString())
	f.svc.Stop()
	f.bridge.Close()
	leaseID := "media-v5-" + uuid.NewString()
	budget := int64(1000000000)
	f.control.pool = []*testFundingLease{{id: leaseID, budget: budget, expires: time.Now().Add(time.Hour)}}
	f.control.unleased = budget
	f.control.secret = secret
	code, body := immediateV5WirePost(t, base, secret, "/__fixture/seed", map[string]any{"platform_user_id": f.platformUserID, "grant_units": strconv.FormatInt(budget*2, 10), "lease_id": leaseID, "budget_units": strconv.FormatInt(budget, 10)}, true)
	require.Less(t, code, 300, string(body))
	// This lease was just issued by the actual isolated Worker. Install its
	// initial zero-consumption cache before the signed pool can discover it.
	// Production pool discovery never reconstructs an absent historical cache.
	require.NoError(t, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: leaseID, PlatformUserID: f.platformUserID, Currency: "USD", FundingScope: "legacy", BudgetUnits: budget, ExpiresAt: f.control.pool[0].expires}))
	missingZero := &atomic.Bool{}
	target, err := url.Parse(base)
	require.NoError(t, err)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/internal/v2/wallet/task-pins/") || r.URL.Path == "/api/internal/v2/wallet/settlements" || r.URL.Path == "/api/internal/v2/wallet/leases/ensure" || r.URL.Path == "/api/internal/v2/wallet/leases/return" || r.URL.Path == "/api/internal/v2/wallet/leases/pool" || r.URL.Path == "/api/internal/v2/wallet/leases/freeze-source" {
			if missingZero.Load() && (r.URL.Path == "/api/internal/v2/wallet/task-pins/finish" || r.URL.Path == "/api/internal/v2/wallet/task-pins/expire") {
				w.WriteHeader(404)
				return
			}
			forwarded := r.Clone(r.Context())
			forwarded.URL.Scheme, forwarded.URL.Host = target.Scheme, target.Host
			forwarded.RequestURI = ""
			resp, e := http.DefaultClient.Do(forwarded)
			if e != nil {
				w.WriteHeader(502)
				return
			}
			defer resp.Body.Close()
			for name, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(name, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		f.control.handler(w, r)
	}))
	t.Cleanup(proxy.Close)
	f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	f.cfg.CanonicalWallet.Secret = secret
	f.cfg.CanonicalWallet.Issuer = "sub2api"
	f.cfg.CanonicalWallet.Audience = "shipany-wallet"
	f.cfg.CanonicalWallet.MediaImmediateReleaseMode = mode
	f.cfg.CanonicalWallet.RequestTimeoutMS = 3000
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	f.svc.Stop()
	return f, missingZero
}

type mediaV5TaskOptions struct {
	HeldUnits      int64
	LegacyNullKind bool
}

func mediaV5PreparedTask(t *testing.T, f *mediaFixture, providerID string, anchor time.Time, ended bool, options ...mediaV5TaskOptions) service.MediaTaskView {
	t.Helper()
	task := f.create(t, "v5-"+uuid.NewString(), "google/nano-banana", "1:1")
	var parent, snapshotID, event string
	var quote int64
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id,billing_snapshot_id,settlement_event_id,quoted_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&parent, &snapshotID, &event, &quote))
	snapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	holdUnits := quote
	if len(options) > 0 && options[0].HeldUnits > 0 {
		holdUnits = options[0].HeldUnits
	}
	if holdUnits != quote {
		// Reproduce a persisted pre-scope shared plan, whose original hold was
		// smaller than the immutable provider quote. A fresh media issuance must
		// still enforce the exact quote; recovery reuses this historical binding.
		legacy, e := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletLeaseByID(context.Background(), f.platformUserID, f.control.pool[0].id)
		require.NoError(t, e)
		raw, e := json.Marshal(legacy)
		require.NoError(t, e)
		_, e = f.db.Exec(`INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,platform_user_id,billing_snapshot_id,lease_id,held_units,lease_basis,kind,event_id) VALUES($1,0,$1,$7,$2,$3,$4,$5::jsonb,'media',$6)`, parent, snapshotID, legacy.LeaseID, holdUnits, string(raw), event, f.platformUserID)
		require.NoError(t, e)
	}
	handle, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(service.WithMediaFundingOwner(context.Background(), task.TaskID), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: holdUnits, DurableAuthorizationID: parent})
	require.NoError(t, err)
	require.Len(t, handle.Segments, 1)
	require.NoError(t, f.db.QueryRow(`UPDATE wallet_authorization_segment SET event_id=$2 WHERE parent_authorization_id=$1 RETURNING authorization_id`, parent, event).Scan(&parent))
	// Exercise the genuine Go write decorator/token rather than inventing a token.
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: f.upstream.Client()}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), handle), "POST", f.upstream.URL+"/api/v1/jobs/createTask", strings.NewReader(`{}`))
	require.NoError(t, err)
	resp, err := decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())
	token := handle.LastWriteToken()
	require.NotEmpty(t, token)
	basis, err := json.Marshal(handle.Segments[0].Basis)
	require.NoError(t, err)
	var endedAt any
	if ended {
		endedAt = anchor.Add(time.Minute)
	}
	_, err = f.db.Exec(`UPDATE gateway_media_task SET lease_id=$2,lease_basis=$3::jsonb,held_units=$8,authorization_token=$4,provider_task_id=NULLIF($5,''),status=CASE WHEN $5='' THEN 'indeterminate' ELSE 'processing' END,pin_state='active',write_owner='historical-trusted-owner',write_started_at=$6::timestamptz,write_ended_at=$7,accepted_at=$6::timestamptz,financial_runtime_deadline=$6::timestamptz+interval '30 minutes' WHERE id=$1`, task.TaskID, handle.LeaseID, string(basis), token, providerID, anchor.UTC(), endedAt, holdUnits)
	require.NoError(t, err)
	pin := map[string]any{"authorization_id": parent, "gateway_job_id": task.TaskID, "platform_user_id": f.platformUserID, "lease_id": handle.LeaseID, "held": amount(holdUnits), "billing_snapshot_id": snapshotID, "settlement_event_id": event, "authorization_kind": "media", "usd_wallet_policy_version": "usd-wallet-v1"}
	if len(options) > 0 && options[0].LegacyNullKind {
		delete(pin, "authorization_kind")
	}
	code, body := immediateV5WirePost(t, strings.TrimRight(os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_URL"), "/"), f.cfg.CanonicalWallet.Secret, "/api/internal/v2/wallet/task-pins/create", pin, false)
	require.Less(t, code, 300, string(body))
	// This fixture represents a trusted historical handoff after the genuine
	// decorated POST above. Persist that returned write token with its original
	// segment identity rather than leaving a merely prepared funding plan.
	segments, err := f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$4,pin_state='active',first_write_at=$2,write_ended_at=$3 WHERE parent_authorization_id=$1 AND (authorization_token IS NULL OR authorization_token=$4) AND kind='media' AND platform_user_id=$5 AND billing_snapshot_id=$6`, parent, anchor.UTC(), endedAt, token, f.platformUserID, snapshotID)
	require.NoError(t, err)
	bound, err := segments.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(len(handle.Segments)), bound)
	return task
}

func TestExternalMediaV5NoIDCompletedOwnerReleasesImmediatelyAndNeverReposts(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "", time.Now().Add(-2*time.Minute), true)
	f.svc = f.newService(t)
	result := f.wait(t, task.TaskID, "released")
	require.Equal(t, "indeterminate", result.Status)
	require.Nil(t, result.Billing.ChargedUSD)
	var count int
	var released, expired time.Time
	require.NoError(t, f.db.QueryRow(`SELECT count(*),min(c.released_at),min(s.expiry_released_at) FROM wallet_unknown_release_counter c JOIN wallet_authorization_segment s USING(authorization_id)`).Scan(&count, &released, &expired))
	require.Equal(t, 1, count)
	require.True(t, released.Equal(expired))
	require.Equal(t, int64(1), f.creates.Load())
	f.svc.Stop()
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
	f.svc = f.newService(t)
	time.Sleep(1100 * time.Millisecond)
	require.Equal(t, int64(1), f.creates.Load())
	var holds int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE expiry_ack_at IS NULL`).Scan(&holds))
	require.Zero(t, holds)
}
func TestExternalMediaV5ExpiredClaimDoesNotProveWriteEnded(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "", time.Now().Add(-time.Hour), false)
	_, err := f.db.Exec(`UPDATE gateway_media_task SET claimed_by='old-owner',claim_until=now()-interval '90 seconds' WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	f.svc = f.newService(t)
	time.Sleep(2200 * time.Millisecond)
	var state string
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state))
	require.Equal(t, "held", state)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&count))
	require.Zero(t, count)
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalMediaV5ReleasedTaskContinuesQueryAndLateOriginalFeeOnce(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	f.mode.Store(3)
	task := mediaV5PreparedTask(t, f, "vendor-late", time.Now().Add(-31*time.Minute), true)
	var queryCount atomic.Int64
	var success atomic.Bool
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		queryCount.Add(1)
		state := "generating"
		if success.Load() {
			state = "success"
		}
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":%q,"resultJson":"{}"}}`, r.URL.Query().Get("taskId"), state)
	}
	f.svc = f.newService(t)
	released := f.wait(t, task.TaskID, "released")
	require.Equal(t, "processing", released.Status)
	success.Store(true)
	_, err := f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now() WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	charged := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.0273", *charged.Billing.ChargedUSD)
	require.Empty(t, charged.URLs, "empty delivery URL remains billable")
	var amount int64
	var payloadLease, state string
	require.NoError(t, f.db.QueryRow(`SELECT actual_units,COALESCE(settlement_payload->>'lease_id',''),state FROM wallet_authorization_segment`).Scan(&amount, &payloadLease, &state))
	require.Equal(t, int64(2730000), amount)
	var originalLease, capturedLease, auth string
	require.NoError(t, f.db.QueryRow(`SELECT lease_id,authorization_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&originalLease, &auth))
	require.NoError(t, f.db.QueryRow(`SELECT lease_id FROM wallet_settlement_outbox WHERE gateway_request_id=$1 AND authorization_id=$2`, task.TaskID, auth).Scan(&capturedLease))
	require.NotEqual(t, originalLease, capturedLease, "late first positive must use new settlement funding")
	originalHold, holdErr := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
	require.NoError(t, holdErr)
	require.Equal(t, "released", originalHold.State)
	require.Empty(t, originalHold.EventID)
	require.Empty(t, payloadLease)
	require.Equal(t, "finished", state)
	var usage, counter int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs`).Scan(&usage))
	require.Equal(t, 1, usage)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Equal(t, 1, counter)
	require.GreaterOrEqual(t, queryCount.Load(), int64(1))
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalMediaV5Bare404ZeroIsNotACKAndProtectsHeld(t *testing.T) {
	f, missingZero := mediaV5WireFixture(t, "enabled")
	f.mode.Store(2)
	missingZero.Store(true)
	task := mediaV5PreparedTask(t, f, "vendor-zero", time.Now().Add(-2*time.Minute), true)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return state == "zero_pending"
	}, 10*time.Second, 50*time.Millisecond)
	var ack int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE zero_ack_at IS NOT NULL`).Scan(&ack))
	require.Zero(t, ack)
	var auth string
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&auth))
	hold, err := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	missingZero.Store(false)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET next_financial_recovery_at=now() WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	f.wait(t, task.TaskID, "released")
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE zero_ack_at IS NOT NULL`).Scan(&ack))
	require.Equal(t, 1, ack)
	var counter int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter)
}

func TestExternalMediaV5CheckpointClaimLossFencesActualPOST(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	// A database checkpoint pause expires only this job's polling claim. The
	// real decorated beforeWrite callback must reject before reaching KIE.
	_, err := f.db.Exec(`CREATE FUNCTION media_v5_checkpoint_expire() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='submitting' AND OLD.status<>'submitting' THEN NEW.claim_until=now()-interval '1 second'; END IF; RETURN NEW; END $$; CREATE TRIGGER media_v5_checkpoint_expire BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION media_v5_checkpoint_expire()`)
	require.NoError(t, err)
	task := f.create(t, "fenced-"+uuid.NewString(), "google/nano-banana", "1:1")
	f.svc = f.newService(t)
	f.wait(t, task.TaskID, "released")
	require.Zero(t, f.creates.Load(), "an old checkpoint owner must never POST")
	var ack, counter int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE zero_ack_at IS NOT NULL`).Scan(&ack))
	require.Equal(t, 1, ack)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter)
}
func TestExternalMediaV5QueryEpisodeNeverSlidesAndUnknownStateNeverResets(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "vendor-episode", time.Now().Add(-2*time.Minute), true)
	first := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	_, err := f.db.Exec(`UPDATE gateway_media_task SET query_episode_first_failed_at=$2::timestamptz,query_uncertainty_deadline=$2::timestamptz+interval '15 minutes' WHERE id=$1`, task.TaskID, first)
	require.NoError(t, err)
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":"unknown_future_state"}}`, r.URL.Query().Get("taskId"))
	}
	f.svc = f.newService(t)
	time.Sleep(2200 * time.Millisecond)
	f.svc.Stop()
	var actual, deadline time.Time
	require.NoError(t, f.db.QueryRow(`SELECT query_episode_first_failed_at,query_uncertainty_deadline FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&actual, &deadline))
	require.True(t, first.Equal(actual))
	require.True(t, first.Add(15*time.Minute).Equal(deadline))
	_, err = f.db.Exec(`UPDATE gateway_media_task SET query_uncertainty_deadline=query_uncertainty_deadline+interval '1 second' WHERE id=$1`, task.TaskID)
	require.Error(t, err, "episode clock is immutable while active")
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":"generating"}}`, r.URL.Query().Get("taskId"))
	}
	_, err = f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now(),claim_until=NULL,claimed_by=NULL WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var cleared bool
		_ = f.db.QueryRow(`SELECT query_episode_first_failed_at IS NULL AND query_uncertainty_deadline IS NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&cleared)
		return cleared
	}, 5*time.Second, 50*time.Millisecond)
}

func TestExternalMediaV5SuccessEvidenceCommitsBeforeRedisAndSurvivesDeadlineRestart(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "vendor-durable-fee", time.Now().Add(-29*time.Minute-55*time.Second), true)
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":"success","resultJson":"{}"}}`, r.URL.Query().Get("taskId"))
	}
	// Fault the first PG fee-plan step, after the authoritative result transaction
	// committed. Redis and the outbox must still have no positive side effect.
	_, err := f.db.Exec(`CREATE FUNCTION media_v5_fee_plan_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.actual_units>0 AND OLD.actual_units=0 THEN RAISE EXCEPTION 'isolated durable fee-plan fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER media_v5_fee_plan_fault BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION media_v5_fee_plan_fault()`)
	require.NoError(t, err)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return state == "fee_pending"
	}, 4*time.Second, 25*time.Millisecond)
	var evidence bool
	var actual int64
	var auth string
	require.NoError(t, f.db.QueryRow(`SELECT fee_evidence IS NOT NULL AND fee_pending_at IS NOT NULL,actual_units,authorization_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&evidence, &actual, &auth))
	require.True(t, evidence)
	require.Equal(t, int64(2730000), actual)
	hold, err := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	var outbox, counter int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, outbox)
	f.svc.Stop()
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err(), "restart also loses Redis; durable success must restore safely")
	time.Sleep(5 * time.Second)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter, "pending trustworthy success always protects against financial timeout")
	_, err = f.db.Exec(`DROP TRIGGER media_v5_fee_plan_fault ON wallet_authorization_segment;DROP FUNCTION media_v5_fee_plan_fault()`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET next_fee_recovery_at=now(),claimed_by=NULL,claim_until=NULL WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	f.svc = f.newService(t)
	charged := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.0273", *charged.Billing.ChargedUSD)
	var usages int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&usages))
	require.Equal(t, 1, usages)
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaV5RiskHTTP429503BeforeHoldAndExistingAdmissionReplays(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	admitted := f.create(t, "already-admitted", "google/nano-banana", "1:1")
	f.bridge.Close()
	f.bridge = immediateV5Bridge(t, f, f.cfg.CanonicalWallet.ControlPlaneURL, f.cfg.CanonicalWallet.Secret, "enabled")
	handle, _ := f.authorize(t, service.WalletUnknownReleaseSoftLimitUnits, service.BillingFamilyOpenAI)
	deadline := time.Now().Add(-time.Second).UTC().Truncate(time.Millisecond)
	proof := strings.Repeat("d", 64)
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,terminal_sealed_at=now(),terminal_evidence=jsonb_build_object('source','isolated-risk-owner'),write_ended_at=now(),write_active_until=NULL,fee_pending=false,evidence_pending=false WHERE parent_authorization_id=$1`, handle.ID, handle.ID+".1")
	require.NoError(t, err)
	claimed, err := f.bridge.ClaimImmediateWalletExpiry(context.Background(), handle.ID, deadline, proof)
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = f.bridge.RecoverImmediateWalletExpiry(context.Background(), handle.ID)
	require.NoError(t, err)
	var beforeTasks, beforeSnapshots, beforeSegments int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM gateway_media_task`).Scan(&beforeTasks))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_snapshot`).Scan(&beforeSnapshots))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment`).Scan(&beforeSegments))
	request := func(key string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest("POST", "/platform/inference/media/tasks", strings.NewReader(`{"model":"google/nano-banana","option":"1:1","prompt":"A colorful garden"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request.Header.Set("Idempotency-Key", key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: f.userID})
		handler.NewPlatformInferenceHandler(f.svc).Create(c)
		return recorder
	}
	denied := request("risk-new")
	require.Equal(t, 429, denied.Code, denied.Body.String())
	require.NotEmpty(t, denied.Header().Get("Retry-After"))
	require.NotContains(t, denied.Body.String(), "402")
	replay := request("already-admitted")
	require.Equal(t, 200, replay.Code, replay.Body.String())
	require.Contains(t, replay.Body.String(), admitted.TaskID)
	_, err = f.db.Exec(`ALTER TABLE wallet_unknown_release_counter RENAME TO isolated_hidden_risk_counter`)
	require.NoError(t, err)
	unavailable := request("risk-read-unavailable")
	_, restoreErr := f.db.Exec(`ALTER TABLE isolated_hidden_risk_counter RENAME TO wallet_unknown_release_counter`)
	require.NoError(t, restoreErr)
	require.Equal(t, 503, unavailable.Code, unavailable.Body.String())
	require.Equal(t, "1", unavailable.Header().Get("Retry-After"))
	var tasks, snapshots, segments int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM gateway_media_task`).Scan(&tasks))
	require.Equal(t, beforeTasks, tasks)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_snapshot`).Scan(&snapshots))
	require.Equal(t, beforeSnapshots, snapshots)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment`).Scan(&segments))
	require.Equal(t, beforeSegments, segments)
	require.Zero(t, f.creates.Load(), "deny must create no provider write")
}

func TestExternalMediaV5OriginalFeeOverrunIsNotTruncated(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "vendor-overrun", time.Now().Add(-2*time.Minute), true, mediaV5TaskOptions{HeldUnits: 1000000})
	f.svc = f.newService(t)
	charged := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.0273", *charged.Billing.ChargedUSD)
	var sum int64
	var events int
	require.NoError(t, f.db.QueryRow(`SELECT count(*),sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&events, &sum))
	require.Equal(t, 2, events)
	require.Equal(t, int64(2730000), sum)
	var actual, held int64
	require.NoError(t, f.db.QueryRow(`SELECT actual_units,held_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&actual, &held))
	require.Greater(t, actual, held)
}

func TestExternalMediaV5GenuineCreateUnknownEndsOwnerAndReleases(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	f.mode.Store(1)
	task := f.create(t, "genuine-create-unknown", "google/nano-banana", "1:1")
	f.svc = f.newService(t)
	released := f.wait(t, task.TaskID, "released")
	require.Equal(t, "indeterminate", released.Status)
	require.Equal(t, "0", released.Billing.HeldUSD)
	var ended, proof bool
	var token, provider string
	require.NoError(t, f.db.QueryRow(`SELECT write_ended_at IS NOT NULL,financial_terminal_proof IS NOT NULL,authorization_token,COALESCE(provider_task_id,'') FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&ended, &proof, &token, &provider))
	require.True(t, ended)
	require.True(t, proof)
	require.NotEmpty(t, token)
	require.Empty(t, provider)
	require.Equal(t, int64(1), f.creates.Load())
	var counters int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counters))
	require.Equal(t, 1, counters)
}

func TestExternalMediaV5ShadowFinanceLaneDoesNotStarveProviderQueries(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "shadow")
	task := mediaV5PreparedTask(t, f, "vendor-shadow", time.Now().Add(-31*time.Minute), true)
	var reads atomic.Int64
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":"generating"}}`, r.URL.Query().Get("taskId"))
	}
	f.svc = f.newService(t)
	require.Eventually(t, func() bool { return reads.Load() >= 2 }, 8*time.Second, 50*time.Millisecond)
	var state string
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state))
	require.Equal(t, "held", state)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&count))
	require.Zero(t, count)
}

func TestExternalMediaV5CleanupFailureDoesNotStarveQueryOrPositiveEvidence(t *testing.T) {
	f, missingFinance := mediaV5WireFixture(t, "enabled")
	missingFinance.Store(true)
	task := mediaV5PreparedTask(t, f, "vendor-cleanup-query", time.Now().Add(-31*time.Minute), true)
	var reads atomic.Int64
	var success atomic.Bool
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		state := "generating"
		if success.Load() {
			state = "success"
		}
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":%q,"resultJson":"{}"}}`, r.URL.Query().Get("taskId"), state)
	}
	f.svc = f.newService(t)
	require.Eventually(t, func() bool { return reads.Load() >= 2 }, 8*time.Second, 50*time.Millisecond)
	success.Store(true)
	require.Eventually(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return state == "fee_pending"
	}, 5*time.Second, 50*time.Millisecond)
	var fee int64
	var evidence bool
	require.NoError(t, f.db.QueryRow(`SELECT actual_units,fee_evidence IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&fee, &evidence))
	require.Equal(t, int64(2730000), fee)
	require.True(t, evidence)
	missingFinance.Store(false)
	_, err := f.db.Exec(`UPDATE gateway_media_task SET next_fee_recovery_at=now(),claimed_by=NULL,claim_until=NULL WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	f.wait(t, task.TaskID, "charged")
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaV5LegacyNullKindUsesTrustedMappingAndRejectsWrongIdentity(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "", time.Now().Add(-2*time.Minute), true, mediaV5TaskOptions{LegacyNullKind: true})
	f.svc = f.newService(t)
	f.wait(t, task.TaskID, "released")
	var raw []byte
	var legacy string
	require.NoError(t, f.db.QueryRow(`SELECT expiry_receipt,expiry_legacy_mapping_proof FROM wallet_authorization_segment`).Scan(&raw, &legacy))
	require.Len(t, legacy, 64)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(raw, &receipt))
	require.Equal(t, "media", receipt["authorization_kind"])
	input := map[string]any{}
	for _, field := range []string{"authorization_id", "gateway_job_id", "platform_user_id", "lease_id", "held", "billing_snapshot_id", "settlement_event_id", "authorization_kind", "authorization_token", "expiry_version", "expiry_deadline", "expiry_policy_version", "expiry_terminal_proof", "usd_wallet_policy_version"} {
		input[field] = receipt[field]
	}
	input["legacy_completion_proof"] = legacy
	base, secret := immediateV5WireConfig(t)
	for field, wrong := range map[string]string{"authorization_token": "wrong-token", "gateway_job_id": "wrong-job", "platform_user_id": "wrong-user", "billing_snapshot_id": "wrong-snapshot", "legacy_completion_proof": strings.Repeat("f", 64)} {
		t.Run(field, func(t *testing.T) {
			changed := map[string]any{}
			for name, value := range input {
				changed[name] = value
			}
			changed[field] = wrong
			code, body := immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/expire", changed, false)
			require.GreaterOrEqual(t, code, 400, string(body))
		})
	}
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&count))
	require.Equal(t, 1, count)
}
