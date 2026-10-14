//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func newPublicMediaFixture(t *testing.T) *mediaFixture {
	f := newMediaFixture(t)
	f.svc.Stop()
	f.bridge.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/v2/media/moderation" {
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"allowed": true}})
			return
		}
		f.control.handler(w, r)
	}))
	t.Cleanup(control.Close)
	f.cfg.CanonicalWallet.ControlPlaneURL = control.URL
	f.cfg.MediaTasks.PublicAPIEnabled = true
	f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "enabled"
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	return f
}
func publicMediaKey(t *testing.T, f *mediaFixture, name string, quota float64) *service.APIKey {
	platform := "public-" + name
	key := &service.APIKey{UserID: f.userID, PlatformKeyID: &platform, RoutingMode: service.APIKeyRoutingModeAutoChannels, Status: service.StatusActive, Quota: quota, User: &service.User{ID: f.userID, Status: service.StatusActive, PlatformUserID: f.platformUserID, BillingCurrency: "USD"}}
	require.NoError(t, f.db.QueryRow(`INSERT INTO api_keys(user_id,key,name,status,routing_mode,platform_key_id,key_sha256,key_prefix,platform_key_version,quota) VALUES($1,$2::text,$2::text,'active','auto_channels',$3,repeat(md5($2::text),2),'test',1,$4) RETURNING id`, f.userID, name, platform, quota).Scan(&key.ID))
	return key
}
func TestExternalPublicMediaRealKeyIsolationCapReplayAndAccounting(t *testing.T) {
	f := newPublicMediaFixture(t)
	ctx := context.Background()
	a := publicMediaKey(t, f, "a", 0.025)
	b := publicMediaKey(t, f, "b", 0)
	cap := "0.02500000"
	input := service.MediaCreateInput{Model: "nano-banana-2-lite", Option: "1:1", Prompt: "A colorful garden", MaxCostUSD: &cap}
	task, err := f.svc.CreatePublic(ctx, a, "same-id", input)
	require.NoError(t, err)
	require.Equal(t, "queued", task.Billing.State)
	_, err = f.svc.GetPublic(ctx, b, task.TaskID)
	require.Error(t, err)
	list, err := f.svc.ListPublic(ctx, b, 20, "")
	require.NoError(t, err)
	require.Empty(t, list.Tasks)
	f.wait(t, task.TaskID, "charged")
	var actualKey, snapshotKey int64
	var spent string
	require.NoError(t, f.db.QueryRow(`SELECT api_key_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&actualKey))
	require.Equal(t, a.ID, actualKey)
	require.NoError(t, f.db.QueryRow(`SELECT s.api_key_id FROM wallet_billing_snapshot s JOIN gateway_media_task t ON t.billing_snapshot_id=s.id WHERE t.id=$1`, task.TaskID).Scan(&snapshotKey))
	require.Equal(t, a.ID, snapshotKey)
	require.NoError(t, f.db.QueryRow(`SELECT quota_used::text FROM api_keys WHERE id=$1`, a.ID).Scan(&spent))
	require.Equal(t, "0.02500000", spent)
	a.Status = service.StatusAPIKeyQuotaExhausted
	a.QuotaUsed = 0.025
	f.cfg.MediaTasks.PublicAPIEnabled = false
	cap = "0.025"
	replay, err := f.svc.CreatePublic(ctx, a, "same-id", input)
	require.NoError(t, err)
	require.Equal(t, task.TaskID, replay.TaskID)
	require.Equal(t, int64(1), f.creates.Load())
	input.Prompt = "different garden"
	_, err = f.svc.CreatePublic(ctx, a, "same-id", input)
	require.ErrorIs(t, err, service.ErrMediaIdempotencyConflict)
	_, err = f.svc.GetPublic(ctx, a, task.TaskID)
	require.NoError(t, err)
	_, err = f.svc.PublicCatalog(a)
	require.NoError(t, err)
	_, err = f.svc.PublicQuote(a, "nano-banana-2-lite", "1:1")
	require.NoError(t, err)
	f.cfg.MediaTasks.PublicAPIEnabled = true
	input.Prompt = "A colorful garden"
	cap = "0"
	_, err = f.svc.CreatePublic(ctx, b, "same-id", input)
	require.Error(t, err)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM gateway_media_task WHERE api_key_id=$1`, b.ID).Scan(&count))
	require.Zero(t, count)
	cap = "0.025"
	bTask, err := f.svc.CreatePublic(ctx, b, "same-id", input)
	require.NoError(t, err)
	require.NotEqual(t, task.TaskID, bTask.TaskID)
	f.wait(t, bTask.TaskID, "charged")
	_, err = f.svc.GetPublic(ctx, a, bTask.TaskID)
	require.Error(t, err)
}
func TestExternalPublicMediaAdmissionReservesConcurrentAllowance(t *testing.T) {
	f := newPublicMediaFixture(t)
	f.mode.Store(3)
	ctx := context.Background()
	key := publicMediaKey(t, f, "limited", 0.025)
	var wg sync.WaitGroup
	accepted := make(chan string, 2)
	errors := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			task, err := f.svc.CreatePublic(ctx, key, id, service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
			if err == nil {
				accepted <- task.TaskID
			} else {
				errors <- err
			}
		}(id)
	}
	wg.Wait()
	require.Len(t, accepted, 1)
	require.Len(t, errors, 1)
	var pending int64
	require.NoError(t, f.db.QueryRow(`SELECT sum(reserved_units)::bigint FROM api_key_budget_authorization WHERE api_key_id=$1`, key.ID).Scan(&pending))
	require.Equal(t, int64(2500000), pending)
}
func TestExternalPublicMediaRevocationBeforeWriteReleasesAllowance(t *testing.T) {
	f := newPublicMediaFixture(t)
	f.svc.Stop()
	ctx := context.Background()
	key := publicMediaKey(t, f, "revoke", 0)
	task, err := f.svc.CreatePublic(ctx, key, "revoke", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE api_keys SET status='disabled' WHERE id=$1`, key.ID)
	require.NoError(t, err)
	f.svc = f.newService(t)
	f.wait(t, task.TaskID, "released")
	require.Zero(t, f.creates.Load())
	var state string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM api_key_budget_authorization WHERE api_key_id=$1`, key.ID).Scan(&state))
	require.Equal(t, "released", state)
}
func TestExternalPublicMediaModerationFailureNeverAccepts(t *testing.T) {
	f := newMediaFixture(t)
	key := publicMediaKey(t, f, "moderation", 0)
	f.cfg.MediaTasks.PublicAPIEnabled = true
	_, err := f.svc.CreatePublic(context.Background(), key, "moderation", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.Error(t, err)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM gateway_media_task WHERE api_key_id=$1`, key.ID).Scan(&count))
	require.Zero(t, count)
	require.Zero(t, f.creates.Load())
}

func TestExternalPublicMediaWrittenRevokedSoftDeletedKeyStillCaptures(t *testing.T) {
	f := newPublicMediaFixture(t)
	f.mode.Store(3)
	ctx := context.Background()
	key := publicMediaKey(t, f, "written", 0)
	task, err := f.svc.CreatePublic(ctx, key, "written", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.creates.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	_, err = f.db.Exec(`UPDATE api_keys SET status='disabled',deleted_at=now() WHERE id=$1`, key.ID)
	require.NoError(t, err)
	f.mode.Store(0)
	f.wait(t, task.TaskID, "charged")
	var captured int64
	var status string
	var deleted bool
	require.NoError(t, f.db.QueryRow(`SELECT b.captured_units,k.status,k.deleted_at IS NOT NULL FROM api_key_budget_authorization b JOIN api_keys k ON k.id=b.api_key_id WHERE b.api_key_id=$1`, key.ID).Scan(&captured, &status, &deleted))
	require.Equal(t, int64(2500000), captured)
	require.Equal(t, "disabled", status)
	require.True(t, deleted)
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalPublicMediaUnknownWalletReleaseRetainsAllowance(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	f.svc.Stop()
	f.bridge.Close()
	target, err := url.Parse(f.cfg.CanonicalWallet.ControlPlaneURL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/v2/media/moderation" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"allowed": true}})
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(control.Close)
	f.cfg.CanonicalWallet.ControlPlaneURL = control.URL
	f.cfg.MediaTasks.PublicAPIEnabled = true
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	f.svc.Stop()
	f.mode.Store(3)
	ctx := context.Background()
	key := publicMediaKey(t, f, "unknown", 0.025)
	task, err := f.svc.CreatePublic(ctx, key, "unknown", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.NoError(t, err)
	preparePublicMediaHistoricalWrite(t, f, task, key, time.Now().Add(-31*time.Minute))
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		v, e := f.svc.GetPublic(ctx, key, task.TaskID)
		return e == nil && v.Billing.State == "released_unknown"
	}, 30*time.Second, 50*time.Millisecond)
	var state string
	var units int64
	require.NoError(t, f.db.QueryRow(`SELECT state,reserved_units FROM api_key_budget_authorization WHERE api_key_id=$1`, key.ID).Scan(&state, &units))
	require.Contains(t, []string{"written", "unknown"}, state)
	require.Equal(t, int64(2500000), units)
	_, err = f.svc.CreatePublic(ctx, key, "new-after-unknown", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.Error(t, err)
	require.Equal(t, int64(1), f.creates.Load())
	f.mode.Store(0)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now() WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	charged := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.025", *charged.Billing.ChargedUSD)
	require.Equal(t, "0", charged.Billing.HeldUSD)
	var captured int64
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT state,captured_units FROM api_key_budget_authorization WHERE api_key_id=$1`, key.ID).Scan(&state, &captured))
	require.Equal(t, "captured", state)
	require.Equal(t, int64(2500000), captured)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE api_key_id=$1`, key.ID).Scan(&count))
	require.Equal(t, 1, count)
	replay, err := f.svc.CreatePublic(ctx, key, "unknown", service.MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
	require.NoError(t, err)
	require.Equal(t, task.TaskID, replay.TaskID)
	require.Equal(t, int64(1), f.creates.Load())
}

// CreatePublic already freezes the real caller identity and reserves its quote.
// Install a trusted historical first write at an elapsed runtime anchor using
// the same genuine Go write/token and D1 signed pin fixture as the v5 tests.
func preparePublicMediaHistoricalWrite(t *testing.T, f *mediaFixture, task service.MediaTaskView, key *service.APIKey, anchor time.Time) {
	t.Helper()
	ctx := context.Background()
	var parent, snapshotID, event string
	var quote int64
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id,billing_snapshot_id,settlement_event_id,quoted_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&parent, &snapshotID, &event, &quote))
	snapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(ctx, snapshotID)
	require.NoError(t, err)
	user, err := f.users.GetByID(ctx, key.UserID)
	require.NoError(t, err)
	handle, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(service.WithMediaFundingOwner(ctx, task.TaskID), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: quote, DurableAuthorizationID: parent})
	require.NoError(t, err)
	require.Len(t, handle.Segments, 1)
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET event_id=$2 WHERE parent_authorization_id=$1`, parent, event)
	require.NoError(t, err)
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: f.upstream.Client()}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(ctx, handle), http.MethodPost, f.upstream.URL+"/api/v1/jobs/createTask", strings.NewReader(`{}`))
	require.NoError(t, err)
	resp, err := decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	var created struct {
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NotEmpty(t, created.Data.TaskID)
	token := handle.LastWriteToken()
	require.NotEmpty(t, token)
	basis, err := json.Marshal(handle.Segments[0].Basis)
	require.NoError(t, err)
	ended := anchor.Add(time.Minute)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET lease_id=$2,lease_basis=$3::jsonb,held_units=$8,authorization_token=$4,provider_task_id=$5,status='processing',pin_state='active',write_owner='historical-trusted-owner',write_started_at=$6::timestamptz,write_ended_at=$7,accepted_at=$6::timestamptz,financial_runtime_deadline=$6::timestamptz+interval '30 minutes' WHERE id=$1`, task.TaskID, handle.LeaseID, string(basis), token, created.Data.TaskID, anchor.UTC(), ended, quote)
	require.NoError(t, err)
	pin := map[string]any{"authorization_id": parent, "gateway_job_id": task.TaskID, "platform_user_id": f.platformUserID, "lease_id": handle.LeaseID, "held": amount(quote), "billing_snapshot_id": snapshotID, "settlement_event_id": event, "authorization_kind": "media", "usd_wallet_policy_version": "usd-wallet-v1"}
	code, body := immediateV5WirePost(t, strings.TrimRight(os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_URL"), "/"), f.cfg.CanonicalWallet.Secret, "/api/internal/v2/wallet/task-pins/create", pin, false)
	require.Less(t, code, 300, string(body))
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$4,pin_state='active',first_write_at=$2,write_ended_at=$3 WHERE parent_authorization_id=$1 AND (authorization_token IS NULL OR authorization_token=$4) AND kind='media' AND platform_user_id=$5 AND billing_snapshot_id=$6`, parent, anchor.UTC(), ended, token, f.platformUserID, snapshotID)
	require.NoError(t, err)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, service.MarkAPIKeyBudgetWrittenTx(ctx, tx, parent))
	require.NoError(t, tx.Commit())
}
