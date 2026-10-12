//go:build budget_integration

package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func budgetFixture(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("BUDGET_TEST_DSN")
	if dsn == "" {
		t.Fatal("BUDGET_TEST_DSN required")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", parsed.Hostname())
	require.Equal(t, "55432", parsed.Port())
	require.True(t, strings.HasPrefix(parsed.Path, "/uniroute_media_test_"))
	schema := "budget_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE; CREATE SCHEMA ` + schema)
	require.NoError(t, err)
	db.Close()
	db, err = sql.Open("postgres", dsn+"&search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); db.Close() })
	_, err = db.Exec(`CREATE TABLE api_keys(id bigint PRIMARY KEY,user_id bigint,status text DEFAULT 'active',deleted_at timestamptz,expires_at timestamptz,quota numeric(20,8) DEFAULT 0,quota_used numeric(20,8) DEFAULT 0,rate_limit_5h numeric(20,8) DEFAULT 0,rate_limit_1d numeric(20,8) DEFAULT 0,rate_limit_7d numeric(20,8) DEFAULT 0,usage_5h numeric(20,8) DEFAULT 0,usage_1d numeric(20,8) DEFAULT 0,usage_7d numeric(20,8) DEFAULT 0,window_5h_start timestamptz,window_1d_start timestamptz,window_7d_start timestamptz,updated_at timestamptz DEFAULT now()); INSERT INTO api_keys(id,user_id,quota,rate_limit_5h) VALUES(1,1,1,1)`)
	require.NoError(t, err)
	raw, err := os.ReadFile("../../migrations/234_api_key_budget.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(raw))
	require.NoError(t, err)
	return db
}
func budgetTx(db *sql.DB, f func(*sql.Tx) error) error {
	tx, e := db.BeginTx(context.Background(), nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = f(tx); e != nil {
		return e
	}
	return tx.Commit()
}
func budgetReserve(db *sql.DB, id, kind string, n int64) error {
	return budgetTx(db, func(tx *sql.Tx) error {
		return ReserveAPIKeyBudgetTx(context.Background(), tx, APIKeyBudgetReservation{AuthorizationID: id, APIKeyID: 1, UserID: 1, SnapshotID: "snapshot", ReservedUnits: n, Kind: kind})
	})
}
func TestPublicMediaBudgetMixedAdmission(t *testing.T) {
	db := budgetFixture(t)
	var wg sync.WaitGroup
	out := make(chan error, 2)
	for i, kind := range []string{"llm", "media"} {
		wg.Add(1)
		go func(i int, k string) {
			defer wg.Done()
			out <- budgetReserve(db, fmt.Sprint("auth_", i), k, 60_000_000)
		}(i, kind)
	}
	wg.Wait()
	close(out)
	accepted := 0
	for err := range out {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrAPIKeyBudgetExceeded)
		}
	}
	require.Equal(t, 1, accepted)
}
func TestPublicMediaBudgetUnknownAndSoftDeletion(t *testing.T) {
	db := budgetFixture(t)
	require.NoError(t, budgetReserve(db, "auth_one", "media", 80_000_000))
	require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return MarkAPIKeyBudgetWrittenTx(context.Background(), tx, "auth_one") }))
	_, e := db.Exec(`UPDATE api_key_budget_authorization SET state='unknown' WHERE authorization_id='auth_one'; UPDATE api_keys SET window_5h_start=now()-interval '6 hours',usage_5h=1`)
	require.NoError(t, e)
	require.ErrorIs(t, budgetReserve(db, "auth_two", "live", 30_000_000), ErrAPIKeyBudgetExceeded)
	_, e = db.Exec(`UPDATE api_keys SET deleted_at=now(),status='disabled'`)
	require.NoError(t, e)
	require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, "auth_one", 70_000_000) }))
	require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, "auth_one", 70_000_000) }))
	var spent, status string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text,status FROM api_keys WHERE id=1`).Scan(&spent, &status))
	require.Equal(t, "0.70000000", spent)
	require.Equal(t, "disabled", status)
	require.ErrorIs(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, "auth_one", 71_000_000) }), ErrAPIKeyBudgetConflict)
}
func TestPublicMediaBudgetFenceStopsWrite(t *testing.T) {
	db := budgetFixture(t)
	require.NoError(t, budgetReserve(db, "auth_fence", "llm", 40_000_000))
	require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return ReleaseAPIKeyBudgetTx(context.Background(), tx, "auth_fence") }))
	require.ErrorIs(t, budgetTx(db, func(tx *sql.Tx) error { return MarkAPIKeyBudgetWrittenTx(context.Background(), tx, "auth_fence") }), ErrAPIKeyBudgetConflict)
	require.NoError(t, budgetReserve(db, "auth_next", "media", 100_000_000))
}

func TestPublicMediaBudgetIndependentNoWriteRecovery(t *testing.T) {
	db := budgetFixture(t)
	require.NoError(t, budgetReserve(db, "auth_crashed", "llm", 60_000_000))
	_, err := db.Exec(`UPDATE api_key_budget_authorization SET created_at=now()-interval '10 minutes',updated_at=now()-interval '10 minutes' WHERE authorization_id='auth_crashed'`)
	require.NoError(t, err)
	require.NoError(t, RecoverAPIKeyBudgets(context.Background(), db, 32))
	require.NoError(t, budgetReserve(db, "auth_recovered", "media", 100_000_000))
	require.ErrorIs(t, budgetTx(db, func(tx *sql.Tx) error { return MarkAPIKeyBudgetWrittenTx(context.Background(), tx, "auth_crashed") }), ErrAPIKeyBudgetConflict)
}
func TestPublicMediaBudgetLiveWindowsNoDoubleCapture(t *testing.T) {
	db := budgetFixture(t)
	for i, n := range []int64{20_000_000, 30_000_000, 10_000_000} {
		auth := fmt.Sprint("auth_window_", i)
		require.NoError(t, budgetReserve(db, auth, "live", 30_000_000))
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, auth, n) }))
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, auth, n) }))
	}
	var spent string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text FROM api_keys WHERE id=1`).Scan(&spent))
	require.Equal(t, "0.60000000", spent)
	require.NoError(t, budgetReserve(db, "auth_media", "media", 40_000_000))
	require.ErrorIs(t, budgetReserve(db, "auth_over", "llm", 1), ErrAPIKeyBudgetExceeded)
}

func budgetHistoricalFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE wallet_billing_snapshot(id text PRIMARY KEY,api_key_id bigint,user_id bigint);
 CREATE TABLE wallet_authorization_segment(parent_authorization_id text,ordinal integer,authorization_id text,api_key_id bigint,billing_snapshot_id text,held_units bigint,kind text,state text,zero_ack_at timestamptz,authorization_token text,first_write_at timestamptz,actual_units bigint DEFAULT 0,terminal_evidence jsonb,settlement_payload jsonb,event_id text,remainder_payload jsonb,updated_at timestamptz DEFAULT now());
 CREATE TABLE gateway_media_task(id text,authorization_id text,api_key_id bigint,user_id bigint,billing_snapshot_id text,quoted_units bigint,financial_state text,authorization_token text,created_at timestamptz DEFAULT now(),actual_units bigint DEFAULT 0);
 CREATE TABLE usage_logs(billing_snapshot_id text);
 CREATE TABLE wallet_settlement_outbox(event_id text,status text);
 INSERT INTO wallet_billing_snapshot VALUES('old-snapshot',1,1)`)
	require.NoError(t, err)
}
func TestPublicMediaBudgetEpochProtectsHistoricalUnknown(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,billing_snapshot_id,held_units,kind,state,authorization_token,first_write_at) VALUES('auth_old',0,'auth_old','old-snapshot',60000000,'llm','expired_unknown','auth_old.1',now()-interval '1 day');
 INSERT INTO gateway_media_task VALUES('old-media','auth_old_media',1,1,'old-snapshot',20000000,'released_unknown','auth_old_media.1',now())`)
	require.NoError(t, err)
	require.ErrorIs(t, budgetReserve(db, "auth_new", "media", 30_000_000), ErrAPIKeyBudgetExceeded)
	require.NoError(t, budgetReserve(db, "auth_new", "media", 20_000_000))
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization WHERE state NOT IN ('captured','released')`).Scan(&n))
	require.Equal(t, 3, n)
}
func TestPublicMediaBudgetAdmissionDoesNotSeedItself(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`UPDATE api_keys SET quota_used=0.9; INSERT INTO gateway_media_task VALUES('new-media','auth_self',1,1,'old-snapshot',20000000,'queued',NULL,now())`)
	require.NoError(t, err)
	require.ErrorIs(t, budgetReserve(db, "auth_self", "media", 20_000_000), ErrAPIKeyBudgetExceeded)
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization`).Scan(&n))
	require.Zero(t, n)
}
func TestPublicMediaBudgetAmbiguousEpochFailsClosed(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,billing_snapshot_id,held_units,kind,state,authorization_token) VALUES('auth_opaque',0,'auth_opaque','missing',60000000,'llm','expired_unknown','auth_opaque.1')`)
	require.NoError(t, err)
	require.ErrorIs(t, budgetReserve(db, "auth_new", "media", 1), ErrAPIKeyBudgetReconciliation)
}

func TestPublicMediaBudgetWriteRecoveryRace(t *testing.T) {
	db := budgetFixture(t)
	for i := 0; i < 20; i++ {
		auth := fmt.Sprintf("auth_race_%d", i)
		require.NoError(t, budgetReserve(db, auth, "llm", 1))
		_, err := db.Exec(`UPDATE api_key_budget_authorization SET updated_at=now()-interval '10 minutes' WHERE authorization_id=$1`, auth)
		require.NoError(t, err)
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			<-start
			done <- budgetTx(db, func(tx *sql.Tx) error { return MarkAPIKeyBudgetWrittenTx(context.Background(), tx, auth) })
		}()
		go func() { <-start; done <- RecoverAPIKeyBudgets(context.Background(), db, 32) }()
		close(start)
		for j := 0; j < 2; j++ {
			err := <-done
			if err != nil {
				require.ErrorIs(t, err, ErrAPIKeyBudgetConflict)
			}
		}
		var state string
		var written bool
		require.NoError(t, db.QueryRow(`SELECT state,written_at IS NOT NULL FROM api_key_budget_authorization WHERE authorization_id=$1`, auth).Scan(&state, &written))
		if state == "released" {
			require.False(t, written)
		} else {
			require.Contains(t, []string{"written", "unknown"}, state)
			require.True(t, written)
		}
	}
}

func TestPublicMediaBudgetEpochReconcilesChargedUnlimitedHistoryOnce(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`UPDATE api_keys SET quota=0,rate_limit_5h=0,quota_used=0.3; INSERT INTO gateway_media_task(id,authorization_id,api_key_id,user_id,billing_snapshot_id,quoted_units,actual_units,financial_state)VALUES('charged','auth_charged',1,1,'old-snapshot',20000000,10000000,'charged')`)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error {
			if err := lockAPIKeyBudget(context.Background(), tx, 1); err != nil {
				return err
			}
			return BackfillAPIKeyBudgetTx(context.Background(), tx, 1)
		}))
	}
	var spent string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text FROM api_keys WHERE id=1`).Scan(&spent))
	require.Equal(t, "0.40000000", spent)
	var units int64
	require.NoError(t, db.QueryRow(`SELECT captured_units FROM api_key_budget_authorization WHERE authorization_id='auth_charged'`).Scan(&units))
	require.Equal(t, int64(10000000), units)
}

func TestPublicMediaBudgetEpochReconcilesOnlyUnmeteredLLMReceipts(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`CREATE TABLE wallet_billing_charge_receipt(request_id text,api_key_id bigint,receipt jsonb,created_at timestamptz DEFAULT now()); UPDATE api_keys SET quota=0,rate_limit_5h=0,quota_used=0.3`)
	require.NoError(t, err)
	for i, quotaCost := range []float64{0, 0.1} {
		auth := fmt.Sprint("auth_receipt_", i)
		binding := WalletBillingBinding{ParentAuthorizationID: auth, BillingSnapshotID: "old-snapshot", FeeUnits: 10000000, Source: "llm_http_usage", PolicyVersion: WalletImmediateReleasePolicyVersion}
		receipt := WalletBillingChargeReceipt{Binding: binding, Command: UsageBillingCommand{APIKeyID: 1, UserID: 1, SettlementCurrency: "USD", WalletBinding: &binding, APIKeyQuotaCost: quotaCost}}
		raw, e := json.Marshal(receipt)
		require.NoError(t, e)
		_, e = db.Exec(`INSERT INTO wallet_billing_charge_receipt(request_id,api_key_id,receipt)VALUES($1,1,$2::jsonb)`, auth, string(raw))
		require.NoError(t, e)
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error {
			if e := lockAPIKeyBudget(context.Background(), tx, 1); e != nil {
				return e
			}
			return BackfillAPIKeyBudgetTx(context.Background(), tx, 1)
		}))
	}
	var spent string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text FROM api_keys`).Scan(&spent))
	require.Equal(t, "0.40000000", spent)
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization WHERE state='captured' AND kind='llm'`).Scan(&n))
	require.Equal(t, 2, n)
}

func TestPublicMediaBudgetHistoricalMediaPreservesCurrentWindows(t *testing.T) {
	db := budgetFixture(t)
	budgetHistoricalFixture(t, db)
	_, err := db.Exec(`UPDATE api_keys SET quota=2,quota_used=0.3,rate_limit_5h=1,rate_limit_1d=1,rate_limit_7d=1,usage_5h=0.35,usage_1d=0.45,usage_7d=0.55,window_5h_start=now(),window_1d_start=date_trunc('day',now()),window_7d_start=date_trunc('day',now()); INSERT INTO gateway_media_task(id,authorization_id,api_key_id,user_id,billing_snapshot_id,quoted_units,actual_units,financial_state,created_at)VALUES('old-media','auth_historical_media',1,1,'old-snapshot',20000000,10000000,'charged',now()-interval '10 days')`)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error {
			if e := lockAPIKeyBudget(context.Background(), tx, 1); e != nil {
				return e
			}
			return BackfillAPIKeyBudgetTx(context.Background(), tx, 1)
		}))
	}
	var spent, w5, w1, w7 string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text,usage_5h::text,usage_1d::text,usage_7d::text FROM api_keys`).Scan(&spent, &w5, &w1, &w7))
	require.Equal(t, "0.40000000", spent)
	require.Equal(t, "0.35000000", w5)
	require.Equal(t, "0.45000000", w1)
	require.Equal(t, "0.55000000", w7)
}

func TestPublicMediaBudgetCapturedZeroReleaseIsIdempotent(t *testing.T) {
	db := budgetFixture(t)
	require.NoError(t, budgetReserve(db, "auth_exact_zero", "llm", 40000000))
	require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, "auth_exact_zero", 0) }))
	for i := 0; i < 2; i++ {
		require.NoError(t, budgetTx(db, func(tx *sql.Tx) error { return ReleaseAPIKeyBudgetTx(context.Background(), tx, "auth_exact_zero") }))
	}
	require.ErrorIs(t, budgetTx(db, func(tx *sql.Tx) error { return CaptureAPIKeyBudgetTx(context.Background(), tx, "auth_exact_zero", 1) }), ErrAPIKeyBudgetConflict)
	require.NoError(t, budgetReserve(db, "auth_after_zero", "media", 100000000))
}
