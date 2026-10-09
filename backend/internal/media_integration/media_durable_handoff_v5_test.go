//go:build media_integration

package media_integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func mediaJournalObservation(t *testing.T, f *mediaFixture, task string) map[string]json.RawMessage {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.readerJournalDir, "media", "*.json"))
	require.NoError(t, err)
	for _, path := range paths {
		raw, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		var record map[string]json.RawMessage
		if json.Unmarshal(raw, &record) != nil {
			continue
		}
		var id string
		_ = json.Unmarshal(record["task"], &id)
		if id == task {
			return record
		}
	}
	return nil
}

func TestExternalMediaV5UnjoinedInitialJournalCrashBeforePGStartRecoversSignedZero(t *testing.T) {
	type processInput struct {
		Config    *config.Config
		DSN       string
		RedisAddr string
		RedisDB   int
	}
	if inputPath := os.Getenv("UNIROUTE_MEDIA_V5_CRASH_PROCESS"); inputPath != "" {
		raw, err := os.ReadFile(inputPath)
		require.NoError(t, err)
		var input processInput
		require.NoError(t, json.Unmarshal(raw, &input))
		dsn, err := url.Parse(input.DSN)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:55432", dsn.Host)
		require.True(t, strings.HasPrefix(dsn.Path, "/uniroute_media_test_"))
		require.Equal(t, "127.0.0.1:56379", input.RedisAddr)
		db, err := sql.Open("postgres", input.DSN)
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{Addr: input.RedisAddr, DB: input.RedisDB})
		users := &mediaUsers{db: db}
		apiKeys := service.NewAPIKeyService(&mediaKeys{db: db}, users, nil, nil, nil, nil, input.Config)
		keys := service.NewPlatformAPIKeyService(&mediaProjection{db}, apiKeys)
		snapshots := service.NewBillingSnapshotService(input.Config, nil, nil, nil, repository.ProvideBillingSnapshotStore(db))
		bridge := service.NewCanonicalWalletBridge(input.Config, repository.NewGatewayCache(rdb).(service.CanonicalWalletLeaseStore), db, repository.ProvideWalletOutboxStore(db))
		_, err = service.NewMediaTaskService(input.Config, db, bridge, snapshots, keys, apiKeys, users, service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: http.DefaultClient}, input.Config))
		require.NoError(t, err)
		// The parent kills this real owner while PG blocks its first send-start.
		// No normal Stop/defer may turn this into an actually returned callback.
		select {}
	}

	f, missingFinish := mediaV5WireFixture(t, "enabled")
	missingFinish.Store(true)
	task := f.create(t, "unjoined-pre-pg-owner-crash", "google/nano-banana", "1:1")
	f.bridge.Close()
	_, err := f.db.Exec(`CREATE FUNCTION media_initial_start_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.write_started_at IS NOT NULL AND OLD.write_started_at IS NULL THEN PERFORM pg_sleep(30); END IF; RETURN NEW; END $$; CREATE TRIGGER media_initial_start_pause BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION media_initial_start_pause()`)
	require.NoError(t, err)
	var databaseName string
	require.NoError(t, f.db.QueryRow(`SELECT current_database()`).Scan(&databaseName))
	dsn, err := url.Parse(os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN"))
	require.NoError(t, err)
	dsn.Path = "/" + databaseName
	input, err := json.Marshal(processInput{Config: f.cfg, DSN: dsn.String(), RedisAddr: f.rdb.Options().Addr, RedisDB: f.rdb.Options().DB})
	require.NoError(t, err)
	inputPath := filepath.Join(t.TempDir(), "isolated-media-owner.json")
	require.NoError(t, os.WriteFile(inputPath, input, 0600))
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.Command(executable, "-test.run=^TestExternalMediaV5UnjoinedInitialJournalCrashBeforePGStartRecoversSignedZero$", "-test.count=1")
	command.Env = append(os.Environ(), "UNIROUTE_MEDIA_V5_CRASH_PROCESS="+inputPath)
	childLog, err := os.Create(filepath.Join(t.TempDir(), "isolated-media-owner.log"))
	require.NoError(t, err)
	command.Stdout, command.Stderr = childLog, childLog
	require.NoError(t, command.Start())
	childJoined := false
	defer func() {
		if !childJoined {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		_ = childLog.Close()
		if t.Failed() {
			raw, _ := os.ReadFile(childLog.Name())
			t.Log(string(raw))
		}
	}()
	var writerPID int
	require.Eventually(t, func() bool {
		record := mediaJournalObservation(t, f, task.TaskID)
		err := f.db.QueryRow(`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='PgSleep' AND query LIKE 'UPDATE gateway_media_task SET write_owner=%'`).Scan(&writerPID)
		return err == nil && string(record["operation"]) == `"write"` && string(record["joined"]) == "false"
	}, 6*time.Second, 20*time.Millisecond)
	record := mediaJournalObservation(t, f, task.TaskID)
	var parent, owner, token, snapshot, provider string
	var quote int64
	var noStart, liveOwner bool
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id,billing_snapshot_id,quoted_units,authorization_token IS NULL AND write_started_at IS NULL AND write_ended_at IS NULL AND provider_task_id IS NULL AND NOT media_journal_pending AND claim_until>now() FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&parent, &snapshot, &quote, &noStart))
	require.True(t, noStart)
	require.NoError(t, json.Unmarshal(record["owner"], &owner))
	require.NoError(t, json.Unmarshal(record["token"], &token))
	require.NoError(t, json.Unmarshal(record["provider"], &provider))
	require.Equal(t, parent+".1", token)
	require.Equal(t, "kie", provider)
	require.JSONEq(t, fmt.Sprintf("%q", snapshot), string(record["snapshot"]))
	require.Equal(t, fmt.Sprint(quote), string(record["quote"]))
	require.NoError(t, f.db.QueryRow(`SELECT NOT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "media-journal-owner:"+owner).Scan(&liveOwner))
	require.True(t, liveOwner, "the actual child PG owner session must still be live")
	sum := sha256.Sum256([]byte(task.TaskID))
	lock, err := os.OpenFile(filepath.Join(f.readerJournalDir, "media", hex.EncodeToString(sum[:])+".json.lock"), os.O_RDWR, 0600)
	require.NoError(t, err)
	lockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	_ = lock.Close()
	require.ErrorIs(t, lockErr, syscall.EWOULDBLOCK, "the actual child must own the WAL read/write lock")
	var allUnstarted bool
	require.NoError(t, f.db.QueryRow(`SELECT bool_and(authorization_token IS NULL AND first_write_at IS NULL AND write_ended_at IS NULL AND state='prepared' AND pin_state='active' AND zero_intent_at IS NULL AND known_fee_units IS NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, parent).Scan(&allUnstarted))
	require.True(t, allUnstarted)
	require.Zero(t, f.creates.Load())

	require.NoError(t, command.Process.Kill())
	require.Error(t, command.Wait(), "SIGKILL must prevent the decorated Create callback from returning")
	childJoined = true
	// A disconnected client does not necessarily interrupt PostgreSQL pg_sleep.
	// End only this deliberately paused backend in this fixture's cloned DB so
	// its uncommitted atomic start rolls back without waiting for the sleep.
	_, err = f.db.Exec(`SELECT pg_terminate_backend($1) WHERE EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=current_database() AND wait_event='PgSleep' AND query LIKE 'UPDATE gateway_media_task SET write_owner=%')`, writerPID)
	require.NoError(t, err)
	_, err = f.db.Exec(`DROP TRIGGER media_initial_start_pause ON gateway_media_task; DROP FUNCTION media_initial_start_pause()`)
	require.NoError(t, err)
	// Flags off still recover a durable original owner. Proved owner completion
	// hands the old 90s query claim to finance immediately.
	f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "off"
	f.cfg.MediaTasks.Enabled = false
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	recoveryStarted := time.Now()
	require.Eventually(t, func() bool {
		var ready bool
		_ = f.db.QueryRow(`SELECT financial_state='zero_pending' AND financial_terminal_proof IS NOT NULL AND authorization_token=$2 AND write_started_at IS NULL AND write_ended_at IS NULL AND accepted_at IS NULL AND provider_task_id IS NULL FROM gateway_media_task WHERE id=$1`, task.TaskID, token).Scan(&ready)
		return ready
	}, 4*time.Second, 20*time.Millisecond)
	record = mediaJournalObservation(t, f, task.TaskID)
	require.Equal(t, "true", string(record["joined"]))
	require.Equal(t, "true", string(record["not_sent"]))
	require.NotEmpty(t, record["write_ended_at"])
	var allZero bool
	var counters, charges int
	require.NoError(t, f.db.QueryRow(`SELECT bool_and(authorization_token=$2 AND first_write_at IS NULL AND known_fee_units=0 AND zero_intent_at IS NOT NULL AND zero_ack_at IS NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, parent, token).Scan(&allZero))
	require.True(t, allZero)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	hold, err := wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, parent)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State, "bare missing finish cannot release the original backing")
	missingFinish.Store(false)
	require.Eventually(t, func() bool {
		var released bool
		_ = f.db.QueryRow(`SELECT financial_state='released_zero' AND financial_released_at IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&released)
		return released
	}, 4*time.Second, 20*time.Millisecond)
	hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, parent)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	require.Less(t, time.Since(recoveryStarted), 8*time.Second, "durable owner completion must release well before the original 90s claim")
	var raw []byte
	var held int64
	var lease, event string
	require.NoError(t, f.db.QueryRow(`SELECT zero_receipt,held_units,lease_id,event_id FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0 AND zero_ack_at IS NOT NULL AND first_write_at IS NULL`, parent).Scan(&raw, &held, &lease, &event))
	var receipt service.WalletTaskPinReceipt
	require.NoError(t, json.Unmarshal(raw, &receipt))
	_, err = service.VerifyWalletTaskPinReceipt(f.cfg.CanonicalWallet.Secret, service.WalletTaskPinReceiptExpected{GatewayJobID: task.TaskID, AuthorizationID: parent, PlatformUserID: f.platformUserID, LeaseID: lease, BillingSnapshotID: snapshot, SettlementEventID: event, HeldUnits: held, AuthorizationKind: "media", AuthorizationToken: token, Status: "released"}, receipt)
	require.NoError(t, err)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, parent).Scan(&counters))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&charges))
	require.Zero(t, counters)
	require.Zero(t, charges)
	require.Zero(t, f.creates.Load(), "neither crash recovery nor flags-off restarts may repeat the paid POST")
}

func TestExternalMediaV5CreateReturnSurvivesFirstPGHandoffFault(t *testing.T) {
	for _, fault := range []string{"returned_id", "no_id", "intermediate_segment"} {
		t.Run(fault, func(t *testing.T) {
			f, _ := mediaV5WireFixture(t, "enabled")
			f.mode.Store(3)
			if fault == "no_id" {
				f.mode.Store(1)
			}
			table := "gateway_media_task"
			condition := "NEW.write_ended_at IS NOT NULL AND OLD.write_ended_at IS NULL"
			if fault == "intermediate_segment" {
				table = "wallet_authorization_segment"
				condition = "NEW.kind='media' AND NEW.write_ended_at IS NOT NULL AND OLD.write_ended_at IS NULL"
			}
			_, err := f.db.Exec(fmt.Sprintf(`CREATE FUNCTION media_owner_handoff_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'isolated first media owner PG handoff fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER media_owner_handoff_fault BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION media_owner_handoff_fault()`, condition, table))
			require.NoError(t, err)
			task := f.create(t, "durable-owner-"+fault, "google/nano-banana", "1:1")
			f.svc = f.newService(t)
			require.Eventually(t, func() bool {
				record := mediaJournalObservation(t, f, task.TaskID)
				return f.creates.Load() == 1 && string(record["joined"]) == "true" && len(record["write_ended_at"]) > 0
			}, 6*time.Second, 25*time.Millisecond)
			var pending, ended bool
			var provider, auth string
			require.NoError(t, f.db.QueryRow(`SELECT media_journal_pending,write_ended_at IS NOT NULL,COALESCE(provider_task_id,''),authorization_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&pending, &ended, &provider, &auth))
			require.True(t, pending)
			require.False(t, ended, "task and every segment owner handoff are one transaction")
			require.Empty(t, provider)
			var startedBound bool
			require.NoError(t, f.db.QueryRow(`SELECT bool_and(authorization_token=(SELECT authorization_token FROM gateway_media_task WHERE id=$2) AND first_write_at IS NOT NULL AND write_ended_at IS NULL AND state='indeterminate') FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, auth, task.TaskID).Scan(&startedBound))
			require.True(t, startedBound, "every real POST segment must carry the exact durable task token before owner completion")
			hold, err := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
			require.NoError(t, err)
			require.Equal(t, "armed", hold.State)
			record := mediaJournalObservation(t, f, task.TaskID)
			if fault != "no_id" {
				require.JSONEq(t, `"vendor-1"`, string(record["provider_id"]))
			}
			require.JSONEq(t, `"kie"`, string(record["provider"]))
			require.NotContains(t, record, "prompt")
			require.NotContains(t, record, "request_payload")
			f.svc.Stop()
			_, err = f.db.Exec(fmt.Sprintf(`DROP TRIGGER media_owner_handoff_fault ON %s; DROP FUNCTION media_owner_handoff_fault()`, table))
			require.NoError(t, err)
			// Recreate the process owner while retaining the named journal volume.
			f.svc = f.newService(t)
			if fault == "no_id" {
				result := f.wait(t, task.TaskID, "released")
				require.Equal(t, "indeterminate", result.Status)
			} else {
				require.Eventually(t, func() bool {
					var ok bool
					_ = f.db.QueryRow(`SELECT write_ended_at IS NOT NULL AND provider_task_id='vendor-1' AND NOT media_journal_pending FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&ok)
					return ok
				}, 6*time.Second, 25*time.Millisecond)
			}
			var allEnded bool
			require.NoError(t, f.db.QueryRow(`SELECT bool_and(write_ended_at IS NOT NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, auth).Scan(&allEnded))
			require.True(t, allEnded)
			require.Equal(t, int64(1), f.creates.Load(), "restart never repeats the paid POST")
		})
	}
}

func TestExternalMediaV5SegmentWriteFenceFaultCannotPOSTAndRecoversSignedZero(t *testing.T) {
	f, missingFinish := mediaV5WireFixture(t, "enabled")
	missingFinish.Store(true)
	_, err := f.db.Exec(`CREATE FUNCTION media_segment_start_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='media' AND NEW.first_write_at IS NOT NULL AND OLD.first_write_at IS NULL THEN RAISE EXCEPTION 'isolated media segment start fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER media_segment_start_fault BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION media_segment_start_fault()`)
	require.NoError(t, err)
	task := f.create(t, "atomic-media-segment-start", "google/nano-banana", "1:1")
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		record := mediaJournalObservation(t, f, task.TaskID)
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return string(record["joined"]) == "true" && string(record["not_sent"]) == "true" && state == "zero_pending"
	}, 5*time.Second, 20*time.Millisecond)
	var auth string
	var noStart bool
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id,write_started_at IS NULL AND accepted_at IS NULL AND provider_task_id IS NULL AND financial_terminal_proof IS NOT NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&auth, &noStart))
	require.True(t, noStart, "failed segment binding rolls back the task's provider-start marker")
	var allZeroPending bool
	require.NoError(t, f.db.QueryRow(`SELECT bool_and(first_write_at IS NULL AND zero_intent_at IS NOT NULL AND zero_ack_at IS NULL AND known_fee_units=0) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, auth).Scan(&allZeroPending))
	require.True(t, allZeroPending, "direct returned not-sent proof is signed zero, never a fabricated provider write")
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	hold, err := wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State, "a missing finish route cannot acknowledge known zero")
	require.Zero(t, f.creates.Load(), "all media segments must bind the exact task token before the paid POST")
	_, err = f.db.Exec(`DROP TRIGGER media_segment_start_fault ON wallet_authorization_segment; DROP FUNCTION media_segment_start_fault()`)
	require.NoError(t, err)
	missingFinish.Store(false)
	f.wait(t, task.TaskID, "released")
	hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, auth)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	var counters, charges int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, auth).Scan(&counters))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&charges))
	require.Zero(t, counters)
	require.Zero(t, charges)
	require.Zero(t, f.creates.Load())
}

func TestExternalMediaV5FirstSuccessSurvivesFirstFeePGFaultAndLater404Deadline(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "vendor-first-success", time.Now().Add(-29*time.Minute-54*time.Second), true)
	var later404 atomic.Bool
	var reads404 atomic.Int64
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		if later404.Load() {
			reads404.Add(1)
			w.WriteHeader(404)
			return
		}
		_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":"success","resultJson":"{}"}}`, r.URL.Query().Get("taskId"))
	}
	_, err := f.db.Exec(`CREATE FUNCTION media_first_success_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.fee_pending_at IS NOT NULL AND OLD.fee_pending_at IS NULL THEN RAISE EXCEPTION 'isolated first success PG transaction fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER media_first_success_fault BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION media_first_success_fault()`)
	require.NoError(t, err)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		record := mediaJournalObservation(t, f, task.TaskID)
		return len(record["success"]) > 0 && string(record["joined"]) == "true"
	}, 4*time.Second, 25*time.Millisecond)
	var pending, absent bool
	require.NoError(t, f.db.QueryRow(`SELECT media_journal_pending,fee_evidence IS NULL AND fee_pending_at IS NULL AND actual_units IS NULL FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&pending, &absent))
	require.True(t, pending)
	require.True(t, absent, "fault must be in the first authoritative fee transaction")
	later404.Store(true)
	time.Sleep(7 * time.Second)
	var counter int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter, "fsynced first success protects funds across the original deadline")
	f.svc.Stop()
	_, err = f.db.Exec(`DROP TRIGGER media_first_success_fault ON gateway_media_task;DROP FUNCTION media_first_success_fault()`)
	require.NoError(t, err)
	f.svc = f.newService(t)
	charged := f.wait(t, task.TaskID, "charged")
	require.Equal(t, "0.0273", *charged.Billing.ChargedUSD)
	var usage, outbox int
	var units int64
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&usage))
	require.NoError(t, f.db.QueryRow(`SELECT count(*),COALESCE(sum(amount_units),0) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&outbox, &units))
	require.Equal(t, 1, usage)
	require.Equal(t, 1, outbox)
	require.Equal(t, int64(2730000), units)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter)
	require.Zero(t, reads404.Load(), "recovery adopts the first success before any later provider read")
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaV5LiveWriteCannotEndFromExpiredClaimAndCompletedNoIDReleasesImmediately(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	started, finish := make(chan struct{}), make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/jobs/createTask" {
			w.WriteHeader(404)
			return
		}
		f.creates.Add(1)
		close(started)
		select {
		case <-finish:
			w.WriteHeader(500)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	f.cfg.MediaTasks.KIEBaseURL = proxy.URL
	task := f.create(t, "actual-live-owner", "google/nano-banana", "1:1")
	f.svc = f.newService(t)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("actual provider POST did not start")
	}
	_, err := f.db.Exec(`UPDATE gateway_media_task SET claim_until=now()-interval '90 seconds' WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	var ended bool
	var state string
	var counter int
	require.NoError(t, f.db.QueryRow(`SELECT write_ended_at IS NOT NULL,financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&ended, &state))
	require.False(t, ended)
	require.Equal(t, "held", state)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counter))
	require.Zero(t, counter)
	at := time.Now()
	close(finish)
	require.Eventually(t, func() bool {
		v, e := f.svc.Get(context.Background(), f.userID, task.TaskID)
		return e == nil && v.Billing.State == "released"
	}, 2*time.Second, 10*time.Millisecond, "actual joined no-ID owner releases without a 90-second claim wait")
	require.Less(t, time.Since(at), 2*time.Second)
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaV5SlowLaneDoesNotStarveOtherReadyLanes(t *testing.T) {
	for _, lane := range []string{"fee", "finance"} {
		t.Run(lane, func(t *testing.T) {
			f, _ := mediaV5WireFixture(t, "enabled")
			fee := mediaV5PreparedTask(t, f, "vendor-ready-fee", time.Now().Add(-2*time.Minute), true)
			feeBacklog := []service.MediaTaskView{fee}
			if lane == "fee" {
				feeBacklog = append(feeBacklog, mediaV5PreparedTask(t, f, "vendor-ready-fee-2", time.Now().Add(-2*time.Minute), true))
			}
			finance := mediaV5PreparedTask(t, f, "", time.Now().Add(-2*time.Minute), true)
			query := mediaV5PreparedTask(t, f, "vendor-ready-query", time.Now().Add(-2*time.Minute), true)
			var reads atomic.Int64
			f.readHook = func(w http.ResponseWriter, r *http.Request) {
				state := "success"
				if r.URL.Query().Get("taskId") == "vendor-ready-query" {
					state = "generating"
					reads.Add(1)
				}
				_, _ = fmt.Fprintf(w, `{"code":200,"data":{"taskId":%q,"state":%q,"resultJson":"{}"}}`, r.URL.Query().Get("taskId"), state)
			}
			_, err := f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now()+interval '1 hour',next_financial_recovery_at=now()+interval '1 hour'`)
			require.NoError(t, err)
			table, condition := "wallet_authorization_segment", "NEW.actual_units>0 AND OLD.actual_units=0"
			if lane == "finance" {
				table = "gateway_media_task"
				condition = "NEW.financial_state='unknown_pending' AND OLD.financial_state='held'"
			}
			_, err = f.db.Exec(fmt.Sprintf(`CREATE FUNCTION media_slow_lane() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN PERFORM pg_sleep(6); END IF; RETURN NEW; END $$;CREATE TRIGGER media_slow_lane BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION media_slow_lane()`, condition, table))
			require.NoError(t, err)
			if lane == "fee" {
				for _, pending := range feeBacklog {
					_, err = f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now() WHERE id=$1`, pending.TaskID)
					require.NoError(t, err)
				}
			} else {
				_, err = f.db.Exec(`UPDATE gateway_media_task SET next_financial_recovery_at=now() WHERE id=$1`, finance.TaskID)
			}
			require.NoError(t, err)
			f.svc = f.newService(t)
			require.Eventually(t, func() bool {
				var sleeping bool
				_ = f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='PgSleep')`).Scan(&sleeping)
				return sleeping
			}, 6*time.Second, 25*time.Millisecond)
			if lane == "fee" {
				require.Eventually(t, func() bool {
					var count int
					err := f.db.QueryRow(`SELECT count(*) FROM gateway_media_task WHERE id IN ($1,$2) AND financial_state='fee_pending'`, feeBacklog[0].TaskID, feeBacklog[1].TaskID).Scan(&count)
					return err == nil && count == 2
				}, time.Second, 10*time.Millisecond, "two authoritative original fees remain ready while the fee lane is blocked")
			}
			_, err = f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now() WHERE id=$1`, query.TaskID)
			require.NoError(t, err)
			if lane == "fee" {
				_, err = f.db.Exec(`UPDATE gateway_media_task SET next_financial_recovery_at=now() WHERE id=$1`, finance.TaskID)
			} else {
				_, err = f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now() WHERE id=$1`, fee.TaskID)
			}
			require.NoError(t, err)
			require.Eventually(t, func() bool { return reads.Load() > 0 }, 3*time.Second, 25*time.Millisecond, "ready query runs while another financial lane is blocked")
			if lane == "fee" {
				require.Eventually(t, func() bool {
					v, e := f.svc.Get(context.Background(), f.userID, finance.TaskID)
					return e == nil && v.Billing.State == "released"
				}, 3*time.Second, 25*time.Millisecond, "ready financial cleanup is independent of the blocked fee lane")
			} else {
				require.Eventually(t, func() bool {
					var state string
					_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, fee.TaskID).Scan(&state)
					return state == "fee_pending" || state == "charged"
				}, 3*time.Second, 25*time.Millisecond, "ready first fee evidence is independent of blocked cleanup")
			}
			f.svc.Stop()
			_, err = f.db.Exec(fmt.Sprintf(`DROP TRIGGER media_slow_lane ON %s;DROP FUNCTION media_slow_lane()`, table))
			require.NoError(t, err)
		})
	}
}

func TestExternalMediaV5Released404ReschedulesOnlyQueryWithinFiveSeconds(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "enabled")
	task := mediaV5PreparedTask(t, f, "vendor-released-404", time.Now().Add(-31*time.Minute), true)
	type providerRead struct {
		sequence int64
		at       time.Time
	}
	var readCount atomic.Int64
	reads := make(chan providerRead, 16)
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		read := providerRead{sequence: readCount.Add(1), at: time.Now()}
		select {
		case reads <- read:
		default:
		}
		w.WriteHeader(404)
	}
	f.svc = f.newService(t)
	f.wait(t, task.TaskID, "released")
	// The one-second worker ticker adds at most one tick, plus 250ms for
	// local PG/HTTP work. Measure consecutive actual GET arrivals themselves.
	const pollInterval = 5 * time.Second
	const runtimeJitter = 1250 * time.Millisecond
	readAfter := func(sequence int64) providerRead {
		timer := time.NewTimer(pollInterval + runtimeJitter)
		defer timer.Stop()
		for {
			select {
			case read := <-reads:
				if read.sequence > sequence {
					return read
				}
			case <-timer.C:
				t.Fatalf("released task did not perform its next actual provider GET within %s", pollInterval+runtimeJitter)
				return providerRead{}
			}
		}
	}
	first := readAfter(readCount.Load())
	second := readAfter(first.sequence)
	gap := second.at.Sub(first.at)
	t.Logf("released 404 actual provider read-to-read gap=%s; five-second interval jitter allowance=%s", gap, runtimeJitter)
	require.LessOrEqual(t, gap, pollInterval+runtimeJitter)
	var live bool
	var state string
	require.Eventually(t, func() bool {
		err := f.db.QueryRow(`SELECT claimed_by IS NOT NULL,financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&live, &state)
		return err == nil && !live
	}, time.Second, 10*time.Millisecond, "each actual returned 404 clears its query claim")
	require.False(t, live, "a returned provider 404 must not leave a 90-second claim")
	require.Equal(t, "released_unknown", state)
	require.Equal(t, int64(1), f.creates.Load())
	var holds int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE expiry_ack_at IS NULL`).Scan(&holds))
	require.Zero(t, holds)
}
