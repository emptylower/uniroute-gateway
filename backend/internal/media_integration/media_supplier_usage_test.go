//go:build media_integration

package media_integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const supplierTestModel = "google/gemini-3-8-flash-lite-tts"

func supplierTestInput() service.MediaCreateInput {
	cap := "0.1"
	for _, m := range service.MediaTaskCatalog() {
		if m.ModelID == supplierTestModel {
			return service.MediaCreateInput{Model: supplierTestModel, Option: m.Param.Options[0].Value, Prompt: "A colorful garden", MaxCostUSD: &cap}
		}
	}
	panic("supplier fixture model missing")
}

func TestExternalMediaSupplierCompatibleReaderReload(t *testing.T) {
	type readerInput struct {
		Config                 *config.Config
		DSN, RedisAddr, TaskID string
		RedisDB                int
		UserID                 int64
	}
	if inputPath := os.Getenv("UNIROUTE_MEDIA_SUPPLIER_READER_STATE"); inputPath != "" {
		raw, err := os.ReadFile(inputPath)
		require.NoError(t, err)
		var input readerInput
		require.NoError(t, json.Unmarshal(raw, &input))
		dsn, err := url.Parse(input.DSN)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:55432", dsn.Host)
		require.True(t, strings.HasPrefix(dsn.Path, "/uniroute_media_test_"))
		require.Equal(t, "127.0.0.1:56379", input.RedisAddr)
		for _, endpoint := range []string{input.Config.MediaTasks.KIEBaseURL, input.Config.CanonicalWallet.ControlPlaneURL} {
			u, e := url.Parse(endpoint)
			require.NoError(t, e)
			require.Equal(t, "127.0.0.1", u.Hostname())
			require.Equal(t, "http", u.Scheme)
		}
		for _, m := range service.MediaTaskCatalog() {
			if m.ModelID == supplierTestModel {
				require.Empty(t, m.BillingBasis, "child binary must have new producer disabled")
			}
		}
		db, err := sql.Open("postgres", input.DSN)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		rdb := redis.NewClient(&redis.Options{Addr: input.RedisAddr, DB: input.RedisDB})
		defer func() { _ = rdb.Close() }()
		users := &mediaUsers{db: db}
		apiKeys := service.NewAPIKeyService(&mediaKeys{db: db}, users, nil, nil, nil, nil, input.Config)
		keys := service.NewPlatformAPIKeyService(&mediaProjection{db}, apiKeys)
		snapshots := service.NewBillingSnapshotService(input.Config, nil, nil, nil, repository.ProvideBillingSnapshotStore(db))
		leaseStore, ok := repository.NewGatewayCache(rdb).(service.CanonicalWalletLeaseStore)
		require.True(t, ok, "fixture cache must implement canonical lease storage")
		bridge := service.NewCanonicalWalletBridge(input.Config, leaseStore, db, repository.ProvideWalletOutboxStore(db))
		defer bridge.Close()
		svc, err := service.NewMediaTaskService(input.Config, db, bridge, snapshots, keys, apiKeys, users, service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: http.DefaultClient}, input.Config))
		require.NoError(t, err)
		defer svc.Stop()
		require.Eventually(t, func() bool {
			v, e := svc.Get(context.Background(), input.UserID, input.TaskID)
			return e == nil && v.Billing.State == "charged" && v.Billing.ChargedUSD != nil && *v.Billing.ChargedUSD == "0.025"
		}, 20*time.Second, 50*time.Millisecond)
		return
	}
	f := newMediaFixture(t)
	var ready atomic.Bool
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			credits := "4"
			supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/reloaded.mp3")
			return
		}
		supplierResponse(w, r, nil, supplierTestModel, "https://example.test/frozen-first.mp3")
	}
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-compatible-reload", supplierTestInput())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var pending bool
		return f.db.QueryRow(`SELECT query_uncertainty_deadline IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&pending) == nil && pending
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	ready.Store(true)
	var name string
	require.NoError(t, f.db.QueryRow(`SELECT current_database()`).Scan(&name))
	dsn, err := url.Parse(os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN"))
	require.NoError(t, err)
	dsn.Path = "/" + name
	f.cfg.MediaTasks.Enabled = false
	input, err := json.Marshal(readerInput{Config: f.cfg, DSN: dsn.String(), RedisAddr: f.rdb.Options().Addr, RedisDB: f.rdb.Options().DB, TaskID: task.TaskID, UserID: f.userID})
	require.NoError(t, err)
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "reader-state.json")
	require.NoError(t, os.WriteFile(inputPath, input, 0600))
	binary := filepath.Join(dir, "compatible-reader.test")
	build := exec.Command("go", "test", "-c", "-tags", "media_integration", "-ldflags", "-X github.com/Wei-Shaw/sub2api/internal/service.mediaCostPlusPricingMode=disabled", "-o", binary, ".")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	binaryRaw, err := os.ReadFile(binary)
	require.NoError(t, err)
	t.Logf("same-source producer-disabled reader binary SHA256: %x", sha256.Sum256(binaryRaw))
	run := exec.Command(binary, "-test.run", "^TestExternalMediaSupplierCompatibleReaderReload$", "-test.timeout", "40s", "-test.v")
	run.Env = append(os.Environ(), "UNIROUTE_MEDIA_SUPPLIER_READER_STATE="+inputPath)
	output, err = run.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "PASS")
	view, err := f.svc.Get(context.Background(), f.userID, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, "0.025", *view.Billing.ChargedUSD)
	require.Equal(t, "https://example.test/frozen-first.mp3", view.URLs[0].URL)
	require.Equal(t, int64(1), f.creates.Load())
}

func supplierResponse(w http.ResponseWriter, r *http.Request, credits *string, model, result string) {
	data := map[string]any{"taskId": r.URL.Query().Get("taskId"), "model": model, "state": "success", "resultJson": `{"resultUrls":["` + result + `"]}`}
	if credits != nil {
		data["creditsConsumed"] = json.Number(*credits)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": data})
}

func TestExternalMediaSupplierRequiresPositiveConfirmedServerMinimumCap(t *testing.T) {
	f := newMediaFixture(t)
	input := supplierTestInput()
	input.MaxCostUSD = nil
	_, err := f.svc.Create(context.Background(), f.userID, "supplier-no-cap", input)
	require.ErrorContains(t, err, "max_cost_usd is required")
	tiny := "0.00000001"
	input.MaxCostUSD = &tiny
	_, err = f.svc.Create(context.Background(), f.userID, "supplier-tiny-cap", input)
	require.ErrorContains(t, err, "quoted USD cost exceeds")
	zero := "0"
	input.MaxCostUSD = &zero
	_, err = f.svc.Create(context.Background(), f.userID, "supplier-zero-cap", input)
	require.Error(t, err)
	var tasks int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM gateway_media_task`).Scan(&tasks))
	require.Zero(t, tasks)
	require.Zero(t, f.creates.Load())
}

func TestExternalMediaSupplierQwenConsumesActualCreditsWithinConfirmedImageBudget(t *testing.T) {
	f := newMediaFixture(t)
	const model = "qwen/text-to-image"
	credits := "2"
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		supplierResponse(w, r, &credits, model, "https://example.test/qwen.png")
	}
	cap := "0.05"
	var option string
	for _, m := range service.MediaTaskCatalog() {
		if m.ModelID == model {
			option = m.Param.Options[0].Value
			require.Equal(t, "budget_cap", m.BillingBasis)
		}
	}
	require.NotEmpty(t, option)
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-qwen", service.MediaCreateInput{Model: model, Option: option, Prompt: "A colorful garden", MaxCostUSD: &cap})
	require.NoError(t, err)
	view := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "success", view.Status)
	require.Equal(t, "image", view.MediaType)
	require.Equal(t, "0.05", view.Billing.QuotedUSD)
	require.Equal(t, "0.0125", *view.Billing.ChargedUSD)
	require.Equal(t, "0.0375", *view.Billing.ReleasedUSD)
	var actual, captured, usage int64
	require.NoError(t, f.db.QueryRow(`SELECT t.actual_units,b.captured_units,(u.actual_cost*100000000)::bigint FROM gateway_media_task t JOIN api_key_budget_authorization b ON b.authorization_id=t.authorization_id JOIN usage_logs u ON u.request_id=t.id WHERE t.id=$1 AND u.actual_cost=t.actual_units::numeric/100000000 AND u.image_count=1 AND t.fee_evidence->'result'->>'credits_consumed'='2' AND t.fee_evidence->'supplier_pricing'->>'budget_units'='5000000'`, task.TaskID).Scan(&actual, &captured, &usage))
	require.EqualValues(t, 1250000, actual)
	require.Equal(t, actual, captured)
	require.Equal(t, actual, usage)
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaSupplierActualRefundAndOverCapExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name, credits, charge, refund string
		units, loss                   int64
		outbox                        int
	}{
		{"partial-zero-share", "4", "0.025", "0.075", 2500000, 0, 1},
		{"over-cap", "24", "0.1", "", 10000000, 5000000, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMediaFixture(t)
			f.poolFunds(t, 6000000, 0, 6000000)
			f.readHook = func(w http.ResponseWriter, r *http.Request) {
				supplierResponse(w, r, &tc.credits, supplierTestModel, "https://example.test/original.mp3")
			}
			input := supplierTestInput()
			task, err := f.svc.Create(context.Background(), f.userID, "supplier-actual", input)
			require.NoError(t, err)
			view := f.wait(t, task.TaskID, "charged")
			require.Equal(t, tc.charge, *view.Billing.ChargedUSD)
			if tc.refund != "" {
				require.Equal(t, tc.refund, *view.Billing.ReleasedUSD)
			}
			var actual, quoted, budget, captured, loss, events, total int64
			var count, pins int
			require.NoError(t, f.db.QueryRow(`SELECT t.actual_units,t.quoted_units,(s.payload->'pricing'->'media_supplier_pricing'->>'budget_units')::bigint,b.captured_units,(t.fee_evidence->>'over_cap_loss_units')::bigint FROM gateway_media_task t JOIN wallet_billing_snapshot s ON s.id=t.billing_snapshot_id JOIN api_key_budget_authorization b ON b.authorization_id=t.authorization_id WHERE t.id=$1`, task.TaskID).Scan(&actual, &quoted, &budget, &captured, &loss))
			require.Equal(t, tc.units, actual)
			require.EqualValues(t, 10000000, quoted)
			require.Equal(t, quoted, budget)
			require.Equal(t, actual, captured)
			require.Equal(t, tc.loss, loss)
			require.NoError(t, f.db.QueryRow(`SELECT count(*),COALESCE(sum(amount_units),0) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&events, &total))
			require.EqualValues(t, tc.outbox, events)
			require.Equal(t, actual, total)
			require.NoError(t, f.db.QueryRow(`SELECT count(*),min(actual_cost*100000000)::bigint FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&count, &captured))
			require.Equal(t, 1, count)
			require.Equal(t, actual, captured)
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment a JOIN gateway_media_task t ON t.authorization_id=a.parent_authorization_id WHERE t.id=$1 AND a.pin_state='finished' AND a.state='finished' AND NOT a.fee_pending AND a.remainder_payload IS NULL`, task.TaskID).Scan(&pins))
			require.Equal(t, 2, pins)
			replay, err := f.svc.Create(context.Background(), f.userID, "supplier-actual", input)
			require.NoError(t, err)
			require.Equal(t, task.TaskID, replay.TaskID)
			f.svc.Stop()
			f.svc = f.newService(t)
			time.Sleep(1500 * time.Millisecond)
			require.Equal(t, int64(1), f.creates.Load())
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}

func TestExternalMediaSupplierFreeSuccessWaitsForSignedACKAndCompletesAtZero(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 6000000, 0, 6000000)
	f.control.mu.Lock()
	f.control.pinFinishRefused = true
	f.control.mu.Unlock()
	credits := "0"
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/free.mp3")
	}
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-free", supplierTestInput())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return state == "zero_pending"
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	var evidence, actual bool
	require.NoError(t, f.db.QueryRow(`SELECT fee_evidence IS NOT NULL,actual_units IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&evidence, &actual))
	require.False(t, evidence)
	require.False(t, actual)
	f.control.mu.Lock()
	f.control.pinFinishRefused = false
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	view := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "success", view.Status)
	require.Equal(t, "0", *view.Billing.ChargedUSD)
	require.Equal(t, "0.1", *view.Billing.ReleasedUSD)
	require.Equal(t, "0", view.Billing.HeldUSD)
	var ack, events, usage int
	var state string
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment a JOIN gateway_media_task t ON t.authorization_id=a.parent_authorization_id WHERE t.id=$1 AND a.zero_ack_at IS NOT NULL AND a.zero_receipt_signature IS NOT NULL AND a.pin_state='finished' AND a.state='finished'`, task.TaskID).Scan(&ack))
	require.Equal(t, 2, ack)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&events))
	require.Zero(t, events)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1 AND actual_cost=0`, task.TaskID).Scan(&usage))
	require.Equal(t, 1, usage)
	require.NoError(t, f.db.QueryRow(`SELECT state FROM api_key_budget_authorization b JOIN gateway_media_task t ON t.authorization_id=b.authorization_id WHERE t.id=$1`, task.TaskID).Scan(&state))
	require.Equal(t, "released", state)
	require.Equal(t, int64(1), f.creates.Load())
	// Exercise the actual PostgreSQL CHECK, including SQL NULL behavior. A
	// clone avoids update immutability masking the constraint under test.
	var originalRaw []byte
	require.NoError(t, f.db.QueryRow(`SELECT to_jsonb(t) FROM gateway_media_task t WHERE id=$1`, task.TaskID).Scan(&originalRaw))
	var original map[string]any
	require.NoError(t, json.Unmarshal(originalRaw, &original))
	for _, mutation := range []string{"missing-evidence", "legacy-policy", "missing-credits", "negative-credits", "wrong-task", "wrong-model", "missing-proof", "unfinished-pin", "unknown-release", "settling-zero", "zero-quote", "zero-held"} {
		t.Run(mutation, func(t *testing.T) {
			var row map[string]any
			require.NoError(t, json.Unmarshal(originalRaw, &row))
			id := "constraint-" + mutation
			for _, field := range []string{"id", "authorization_id", "provider_task_id", "settlement_event_id", "idempotency_key"} {
				row[field] = id
			}
			result, ok := row["result"].(map[string]any)
			require.True(t, ok, "fixture result must remain a JSON object")
			result["provider_task_id"] = id
			evidence, ok := row["fee_evidence"].(map[string]any)
			require.True(t, ok, "fixture fee evidence must remain a JSON object")
			evidence["provider_task_id"] = id
			evidence["result"] = result
			switch mutation {
			case "missing-evidence":
				row["fee_evidence"] = nil
			case "legacy-policy":
				delete(evidence, "supplier_pricing")
			case "missing-credits":
				delete(result, "credits_consumed")
			case "negative-credits":
				result["credits_consumed"] = "-1"
			case "wrong-task":
				result["provider_task_id"] = "other"
			case "wrong-model":
				result["provider_model"] = "other"
			case "missing-proof":
				row["financial_terminal_proof"] = nil
			case "unfinished-pin":
				row["pin_state"] = "active"
			case "unknown-release":
				row["financial_state"] = "released_unknown"
			case "settling-zero":
				row["status"] = "settling"
			case "zero-quote":
				row["quoted_units"] = 0
			case "zero-held":
				row["held_units"] = 0
				row["lease_id"] = nil
			}
			raw, e := json.Marshal(row)
			require.NoError(t, e)
			_, e = f.db.Exec(`INSERT INTO gateway_media_task SELECT (jsonb_populate_record(NULL::gateway_media_task,$1::jsonb)).*`, string(raw))
			var pgErr *pq.Error
			require.ErrorAs(t, e, &pgErr)
			require.Equal(t, pq.ErrorCode("23514"), pgErr.Code, fmt.Sprint(mutation))
		})
	}
	// The established positive completion guard continues to accept legacy
	// fixed-fee evidence without the new supplier policy.
	positive := original
	for _, field := range []string{"id", "authorization_id", "provider_task_id", "settlement_event_id", "idempotency_key"} {
		positive[field] = "constraint-legacy-positive"
	}
	positive["actual_units"], positive["financial_state"], positive["fee_evidence"] = 1, "charged", nil
	raw, err := json.Marshal(positive)
	require.NoError(t, err)
	_, err = f.db.Exec(`INSERT INTO gateway_media_task SELECT (jsonb_populate_record(NULL::gateway_media_task,$1::jsonb)).*`, string(raw))
	require.NoError(t, err)
}

func TestExternalMediaSupplierFreeSuccessRealWorkerWire(t *testing.T) {
	f, missingFinish := mediaV5WireFixture(t, "enabled")
	missingFinish.Store(true)
	credits := "0"
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/worker-free.mp3")
	}
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-real-worker-free", supplierTestInput())
	require.NoError(t, err)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var pending bool
		return f.db.QueryRow(`SELECT financial_state='zero_pending' AND fee_evidence IS NULL AND actual_units IS NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&pending) == nil && pending
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	missingFinish.Store(false)
	f.svc = f.newService(t)
	view := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "success", view.Status)
	require.Equal(t, "0", *view.Billing.ChargedUSD)
	require.Equal(t, "0.1", *view.Billing.ReleasedUSD)
	var signedZero, closed int
	var returned int64
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment a JOIN gateway_media_task t ON t.authorization_id=a.parent_authorization_id WHERE t.id=$1 AND a.zero_ack_at IS NOT NULL AND a.zero_receipt->>'status'='released' AND a.zero_receipt_signature ~ '^[0-9a-f]{64}$' AND a.pin_state='finished' AND a.state='finished'`, task.TaskID).Scan(&signedZero))
	require.Equal(t, 1, signedZero)
	require.NoError(t, f.db.QueryRow(`SELECT count(*),COALESCE(sum(f.returned_units),0) FROM wallet_funding_freeze f JOIN wallet_authorization_segment a ON a.lease_id=f.lease_id JOIN gateway_media_task t ON t.authorization_id=a.parent_authorization_id WHERE t.id=$1 AND f.closed AND f.return_revision=f.redis_applied_revision`, task.TaskID).Scan(&closed, &returned))
	require.Equal(t, 1, closed)
	require.EqualValues(t, 10000000, returned)
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaSupplierMissingUsagePreservesFirstSuccessAndFixedEpisodeAcrossRestart(t *testing.T) {
	f := newMediaFixture(t)
	var ready atomic.Bool
	var reportFailure atomic.Bool
	var failures atomic.Int64
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			credits := "4"
			supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/later.mp3")
			return
		}
		if reportFailure.Load() {
			failures.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": map[string]any{"taskId": r.URL.Query().Get("taskId"), "model": supplierTestModel, "state": "fail"}})
			return
		}
		supplierResponse(w, r, nil, supplierTestModel, "https://example.test/first.mp3")
	}
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-missing", supplierTestInput())
	require.NoError(t, err)
	var first, deadline time.Time
	require.Eventually(t, func() bool {
		return f.db.QueryRow(`SELECT query_episode_first_failed_at,query_uncertainty_deadline FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&first, &deadline) == nil
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, 15*time.Minute, deadline.Sub(first))
	reportFailure.Store(true)
	require.Eventually(t, func() bool { return failures.Load() > 0 }, 8*time.Second, 50*time.Millisecond)
	var retainedFirst, retainedDeadline time.Time
	var evidence bool
	require.NoError(t, f.db.QueryRow(`SELECT query_episode_first_failed_at,query_uncertainty_deadline,fee_evidence IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&retainedFirst, &retainedDeadline, &evidence))
	require.Equal(t, first, retainedFirst)
	require.Equal(t, deadline, retainedDeadline)
	require.False(t, evidence)
	record := mediaJournalObservation(t, f, task.TaskID)
	require.NotEmpty(t, record["success"])
	require.Empty(t, record["failure"], "a later provider failure cannot overturn the first success or authorize a zero refund")
	f.svc.Stop()
	ready.Store(true)
	f.svc = f.newService(t)
	view := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.025", *view.Billing.ChargedUSD)
	require.Equal(t, "https://example.test/first.mp3", view.URLs[0].URL)
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaSupplierFsyncedUsageWinsFailedPGHandoffAndProviderChange(t *testing.T) {
	f := newMediaFixture(t)
	_, err := f.db.Exec(`CREATE FUNCTION supplier_first_fee_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.fee_evidence IS NOT NULL AND OLD.fee_evidence IS NULL THEN RAISE EXCEPTION 'fixture fee handoff unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER supplier_first_fee_fault BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION supplier_first_fee_fault()`)
	require.NoError(t, err)
	credits := "4"
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/frozen.mp3")
	}
	task, err := f.svc.Create(context.Background(), f.userID, "supplier-wal", supplierTestInput())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		record := mediaJournalObservation(t, f, task.TaskID)
		return len(record["supplier_credits"]) > 0 && len(record["success"]) > 0
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	_, err = f.db.Exec(`DROP TRIGGER supplier_first_fee_fault ON gateway_media_task; DROP FUNCTION supplier_first_fee_fault()`)
	require.NoError(t, err)
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
	credits = "24"
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		supplierResponse(w, r, &credits, supplierTestModel, "https://example.test/changed.mp3")
	}
	f.svc = f.newService(t)
	view := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.025", *view.Billing.ChargedUSD)
	require.Equal(t, "https://example.test/frozen.mp3", view.URLs[0].URL)
	require.Equal(t, int64(1), f.creates.Load())
}
