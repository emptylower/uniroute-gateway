//go:build media_integration

package media_integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func fundingReturnRaceControl(t *testing.T, lease, operation string) int {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"lease": lease})
	require.NoError(t, err)
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post("http://127.0.0.1:62071/"+operation, "application/json", bytes.NewReader(raw))
	require.NoError(t, err, "the actual Worker signing-boundary instrumentation is required")
	defer func() { _ = response.Body.Close() }()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var state struct {
		Arrived int `json:"arrived"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&state))
	return state.Arrived
}

func TestExternalFundingDispatchMetadataCancelRestoresImmutablePendingAndUnknownKeys(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.f.bridge.Close()
	x.f.db.SetMaxOpenConns(2)
	require.NoError(t, service.PrepareFundingReturnForIntegration(context.Background(), x.f.bridge, x.f.platformUserID, x.source))
	// Model a future producer's opaque top-level metadata in this isolated row.
	// The existing immutable-request guard remains enabled throughout.
	tx, err := x.f.db.Begin()
	require.NoError(t, err)
	var raw []byte
	require.NoError(t, tx.QueryRow(`SELECT pending_request FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2 FOR UPDATE`, x.f.platformUserID, x.source).Scan(&raw))
	_, err = tx.Exec(`UPDATE wallet_funding_freeze SET pending_request=NULL WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE wallet_funding_freeze SET pending_request=jsonb_set($3::jsonb,'{future_metadata}','{"retained":["signed","opaque"]}'::jsonb) WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source, string(raw))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	readState := func(queryer *sql.Conn) []byte {
		var state []byte
		require.NoError(t, queryer.QueryRowContext(context.Background(), `SELECT row_to_json(f) FROM wallet_funding_freeze f WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&state))
		return state
	}
	dsn, err := url.Parse(os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN"))
	require.NoError(t, err)
	var database string
	require.NoError(t, x.f.db.QueryRow(`SELECT current_database()`).Scan(&database))
	dsn.Path = "/" + database
	observer, err := sql.Open("postgres", dsn.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = observer.Close() })
	observerConn, err := observer.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = observerConn.Close() })
	original := readState(observerConn)
	gate := "isolated-dispatch-mark-gap-" + uuid.NewString()
	_, err = observerConn.ExecContext(context.Background(), `SELECT pg_advisory_lock(hashtextextended($1,0))`, gate)
	require.NoError(t, err)
	// Block only the second metadata UPDATE, after the first NULL write but
	// before reinsert. External readers must continue seeing the original row.
	_, err = x.f.db.Exec(`CREATE FUNCTION isolated_dispatch_mark_gap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.pending_request IS NULL AND NEW.pending_request->>'dispatch_started'='true' THEN PERFORM pg_advisory_xact_lock(hashtextextended('` + gate + `',0)); END IF; RETURN NEW; END $$; CREATE TRIGGER z_isolated_dispatch_mark_gap BEFORE UPDATE ON wallet_funding_freeze FOR EACH ROW EXECUTE FUNCTION isolated_dispatch_mark_gap()`)
	require.NoError(t, err)
	calls := &atomic.Int64{}
	refusal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/v2/wallet/leases/return" {
			calls.Add(1)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(refusal.Close)
	x.f.cfg.CanonicalWallet.ControlPlaneURL = refusal.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	x.f.bridge.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		result <- service.ReplayFundingReturnForIntegration(ctx, x.f.bridge, x.f.platformUserID, x.source)
	}()
	require.Eventually(t, func() bool {
		var waiting int
		return observerConn.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&waiting) == nil && waiting == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.JSONEq(t, string(original), string(readState(observerConn)), "the intermediate NULL is invisible outside its transaction")
	cancel()
	select {
	case err = <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled metadata handoff did not release its session")
	}
	require.Zero(t, calls.Load(), "failed dispatch metadata cannot reach the Worker")
	require.JSONEq(t, string(original), string(readState(observerConn)), "cancellation rolls back pending, money, identity, receipt and unknown metadata")
	var unlocked bool
	require.NoError(t, observerConn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1,0))`, gate).Scan(&unlocked))
	require.True(t, unlocked)
	_, err = x.f.db.Exec(`DROP TRIGGER z_isolated_dispatch_mark_gap ON wallet_funding_freeze; DROP FUNCTION isolated_dispatch_mark_gap()`)
	require.NoError(t, err)
	// The original PG230 guard still rejects replacing the signed financial
	// request/basis, and even a direct metadata replacement remains forbidden.
	for _, statement := range []string{
		`UPDATE wallet_funding_freeze SET pending_request=jsonb_set(pending_request,'{request,gateway_consumed,amount_units}','"1"'::jsonb) WHERE platform_user_id=$1 AND lease_id=$2`,
		`UPDATE wallet_funding_freeze SET pending_request=jsonb_set(pending_request,'{basis,lease,funded_units}','2000000000'::jsonb) WHERE platform_user_id=$1 AND lease_id=$2`,
		`UPDATE wallet_funding_freeze SET pending_request=jsonb_set(pending_request,'{dispatch_started}','true'::jsonb) WHERE platform_user_id=$1 AND lease_id=$2`,
	} {
		_, err = x.f.db.Exec(statement, x.f.platformUserID, x.source)
		require.Error(t, err)
		require.JSONEq(t, string(original), string(readState(observerConn)))
	}
	require.Error(t, service.ReplayFundingReturnForIntegration(context.Background(), x.f.bridge, x.f.platformUserID, x.source), "the injected transport refusal retains the dispatched request")
	var marked []byte
	require.NoError(t, x.f.db.QueryRow(`SELECT pending_request FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&marked))
	var expected []byte
	require.NoError(t, x.f.db.QueryRow(`SELECT jsonb_set(($1::jsonb)->'pending_request','{dispatch_started}','true'::jsonb)`, string(original)).Scan(&expected))
	require.JSONEq(t, string(expected), string(marked), "successful dispatch changes only its metadata and preserves opaque future keys")
	// Cancel a genuinely blocked session-lock acquisition as well. Its database
	// connection must be discarded rather than pooled with a possible lock.
	dispatchGate := "wallet-funding-return-dispatch-v1:" + x.f.platformUserID + ":" + x.source
	_, err = observerConn.ExecContext(context.Background(), `SELECT pg_advisory_lock(hashtextextended($1,0))`, dispatchGate)
	require.NoError(t, err)
	lockCtx, cancelLock := context.WithCancel(context.Background())
	t.Cleanup(cancelLock)
	lockResult := make(chan error, 1)
	go func() {
		release, lockErr := service.LockFundingReturnForIntegration(lockCtx, x.f.bridge, x.f.platformUserID, x.source)
		if release != nil {
			_ = release()
		}
		lockResult <- lockErr
	}()
	require.Eventually(t, func() bool {
		var waiting int
		return observerConn.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&waiting) == nil && waiting == 1
	}, 5*time.Second, 20*time.Millisecond)
	cancelLock()
	select {
	case err = <-lockResult:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled session-lock acquisition did not discard its connection")
	}
	require.NoError(t, observerConn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1,0))`, dispatchGate).Scan(&unlocked))
	require.True(t, unlocked)
	var locks int
	require.NoError(t, observerConn.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&locks))
	require.Zero(t, locks)
	require.Zero(t, x.f.db.Stats().InUse)
}

func TestExternalFundingDispatchCanceledWinnerAndStale409KeepExactReceiptRecovery(t *testing.T) {
	x := newFundingV7Fixture(t)
	fundingV7OriginalSignedTerminals(t, x)
	x.f.bridge.Close()
	// Directly drive the real recovery methods to exclude an unrelated third
	// background RPC rescuing the pending request before the 409 is observed.
	target, err := url.Parse(x.base)
	require.NoError(t, err)
	var mu sync.Mutex
	requests := [][]byte{}
	statuses := []int{}
	var winner service.WalletFundingReturnReceipt
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		var request struct {
			Lease string `json:"lease_id"`
			Mode  string `json:"mode"`
		}
		_ = json.Unmarshal(raw, &request)
		closeSource := r.URL.Path == "/api/internal/v2/wallet/leases/return" && request.Lease == x.source && request.Mode == "close"
		ordinal := 0
		if closeSource {
			mu.Lock()
			requests = append(requests, append([]byte(nil), raw...))
			ordinal = len(requests)
			mu.Unlock()
		}
		// The actual fixture does not abort its Worker Request on client cancel.
		// Retain its response here so the test can verify the committed winner.
		forwardCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		forwarded := r.Clone(forwardCtx)
		forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
		forwarded.Body = io.NopCloser(bytes.NewReader(raw))
		response, forwardErr := http.DefaultClient.Do(forwarded)
		if forwardErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		body, bodyErr := io.ReadAll(response.Body)
		if bodyErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if closeSource {
			mu.Lock()
			statuses = append(statuses, response.StatusCode)
			mu.Unlock()
			if ordinal == 1 && response.StatusCode == http.StatusOK {
				var result struct {
					Data struct {
						Receipt service.WalletFundingReturnReceipt `json:"receipt"`
					} `json:"data"`
				}
				if json.Unmarshal(body, &result) != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				mu.Lock()
				winner = result.Data.Receipt
				mu.Unlock()
				// A's real D1 batch has committed and returned its signed receipt.
				// Only then may B sign and execute its genuinely stale D1 batch.
				control, controlErr := http.Post("http://127.0.0.1:62071/resume-second", "application/json", bytes.NewBufferString(`{"lease":"`+x.source+`"}`))
				if controlErr != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				_ = control.Body.Close()
			}
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(proxy.Close)
	fundingReturnRaceControl(t, x.source, "arm")
	t.Cleanup(func() { fundingReturnRaceControl(t, x.source, "release") })
	x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	x.f.bridge.Close()
	x.f.db.SetMaxOpenConns(2)
	_, err = x.mediaFunding(t, 1100000000)
	require.Error(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, service.CleanupFundingTerminalForIntegration(context.Background(), x.f.bridge, x.f.platformUserID, x.source))
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	t.Cleanup(cancelFirst)
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- service.RecoverFundingTerminalForIntegration(firstCtx, x.f.bridge, x.f.platformUserID, x.source)
	}()
	var premature bool
	var earlyFailure error
	require.Eventually(t, func() bool {
		select {
		case earlyFailure = <-firstResult:
			premature = true
			return true
		default:
		}
		return fundingReturnRaceControl(t, x.source, "state") == 1
	}, 10*time.Second, 20*time.Millisecond)
	require.False(t, premature, "recovery returned before signing barrier: %v", earlyFailure)
	cancelFirst()
	select {
	case err = <-firstResult:
		require.True(t, errors.Is(err, context.Canceled), "first RPC is canceled before D1 commits: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery did not release its connection")
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSecond()
	err = service.ReplayFundingReturnForIntegration(secondCtx, x.f.bridge, x.f.platformUserID, x.source)
	require.ErrorContains(t, err, "funding_conflict", "the stale batch must receive the actual Worker 409")
	var pending []byte
	require.NoError(t, x.f.db.QueryRow(`SELECT pending_request FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&pending))
	require.NotEmpty(t, pending, "a replay's 409 cannot discard the committed winner's receipt identity")
	var saved struct {
		DispatchStarted *bool `json:"dispatch_started"`
	}
	require.NoError(t, json.Unmarshal(pending, &saved))
	require.NotNil(t, saved.DispatchStarted)
	require.True(t, *saved.DispatchStarted)
	require.NoError(t, service.ReplayFundingReturnForIntegration(context.Background(), x.f.bridge, x.f.platformUserID, x.source))
	mu.Lock()
	requestCopies := append([][]byte(nil), requests...)
	statusCopies := append([]int(nil), statuses...)
	winningReceipt := winner
	mu.Unlock()
	require.Len(t, requestCopies, 3)
	require.Equal(t, []int{200, 409, 200}, statusCopies)
	for _, request := range requestCopies[1:] {
		require.Equal(t, sha256.Sum256(requestCopies[0]), sha256.Sum256(request), "all attempts replay the same immutable payload")
	}
	receipt := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
	require.Equal(t, winningReceipt, receipt, "the canceled winner's actual signed receipt is verified and acknowledged")
	require.Equal(t, "close", receipt.Mode)
	require.Equal(t, "0", receipt.CapturedUnits)
	require.Equal(t, "1000000000", receipt.ReturnedAfterUnits)
	var closed bool
	var revision, applied, returned int64
	require.NoError(t, x.f.db.QueryRow(`SELECT closed,return_revision,redis_applied_revision,returned_units FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &revision, &applied, &returned))
	require.True(t, closed)
	require.Equal(t, receipt.ReturnRevision, revision)
	require.Equal(t, revision, applied)
	require.Equal(t, int64(1000000000), returned)
	var cleaned, charged, unknown, locks int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND lease_id=$2 AND zero_ack_at IS NOT NULL AND funding_terminal_cleanup_at IS NOT NULL`, x.f.platformUserID, x.source).Scan(&cleaned))
	require.Equal(t, 129, cleaned)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE platform_user_id=$1`, x.f.platformUserID).Scan(&charged))
	require.Zero(t, charged)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE platform_user_id=$1`, x.f.platformUserID).Scan(&unknown))
	require.Zero(t, unknown)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&locks))
	require.Zero(t, locks, "canceled RPC and recovery leave no session advisory locks in the pool")
	require.Equal(t, int64(1000000000), fundingV7Available(t, x))
}

func TestExternalFundingDispatchTwoConnectionPoolAllowsConcurrentPrepareAndACK(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.f.bridge.Close()
	x.f.db.SetMaxOpenConns(2)
	require.NoError(t, service.PrepareFundingReturnForIntegration(context.Background(), x.f.bridge, x.f.platformUserID, x.source))
	target, err := url.Parse(x.base)
	require.NoError(t, err)
	started, respond := make(chan struct{}), make(chan struct{})
	var once sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/v2/wallet/leases/return" {
			once.Do(func() { close(started) })
			select {
			case <-respond:
			case <-r.Context().Done():
				return
			}
		}
		forwarded := r.Clone(r.Context())
		forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
		response, forwardErr := http.DefaultClient.Do(forwarded)
		if forwardErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(proxy.Close)
	var respondOnce sync.Once
	t.Cleanup(func() { respondOnce.Do(func() { close(respond) }) })
	x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	x.f.bridge.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	replay, prepare := make(chan error, 1), make(chan error, 1)
	go func() {
		replay <- service.ReplayFundingReturnForIntegration(ctx, x.f.bridge, x.f.platformUserID, x.source)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("funding return did not reach its real RPC")
	}
	beforeWait := x.f.db.Stats().WaitCount
	go func() {
		prepare <- service.PrepareFundingReturnForIntegration(ctx, x.f.bridge, x.f.platformUserID, x.source)
	}()
	require.Eventually(t, func() bool { stats := x.f.db.Stats(); return stats.InUse == 2 && stats.WaitCount > beforeWait }, 2*time.Second, 10*time.Millisecond)
	respondOnce.Do(func() { close(respond) })
	for _, result := range []chan error{replay, prepare} {
		select {
		case err = <-result:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("prepare and ACK exhausted the two-connection pool")
		}
	}
	var locks int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&locks))
	require.Zero(t, locks)
	require.Zero(t, x.f.db.Stats().InUse)
	var revision, applied int64
	require.NoError(t, x.f.db.QueryRow(`SELECT return_revision,redis_applied_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&revision, &applied))
	require.Equal(t, int64(1), revision)
	require.Equal(t, revision, applied)
}
