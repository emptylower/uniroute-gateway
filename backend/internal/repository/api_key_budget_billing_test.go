//go:build budget_integration

package repository

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaBudgetLLMUsageTransaction(t *testing.T) {
	dsn := os.Getenv("BUDGET_TEST_DSN")
	require.NotEmpty(t, dsn)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", u.Hostname())
	require.Equal(t, "55432", u.Port())
	require.True(t, strings.HasPrefix(u.Path, "/uniroute_media_test_"))
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer admin.Close()
	schema := "budget_bill_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(`CREATE SCHEMA ` + schema)
	require.NoError(t, err)
	defer admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	db, err := sql.Open("postgres", dsn+"&search_path="+schema)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE api_keys(id bigint PRIMARY KEY,user_id bigint,status text DEFAULT 'active',deleted_at timestamptz,expires_at timestamptz,quota numeric(20,8) DEFAULT 1,quota_used numeric(20,8) DEFAULT 0,rate_limit_5h numeric(20,8) DEFAULT 1,rate_limit_1d numeric(20,8) DEFAULT 0,rate_limit_7d numeric(20,8) DEFAULT 0,usage_5h numeric(20,8) DEFAULT 0,usage_1d numeric(20,8) DEFAULT 0,usage_7d numeric(20,8) DEFAULT 0,window_5h_start timestamptz,window_1d_start timestamptz,window_7d_start timestamptz,updated_at timestamptz DEFAULT now()); INSERT INTO api_keys(id,user_id) VALUES(1,1);
 CREATE TABLE wallet_authorization_segment(authorization_id text,parent_authorization_id text,ordinal int);
 CREATE TABLE usage_billing_dedup(id bigserial,request_id text,api_key_id bigint,request_fingerprint text,UNIQUE(request_id,api_key_id)); CREATE TABLE usage_billing_dedup_archive(request_id text,api_key_id bigint,request_fingerprint text)`)
	require.NoError(t, err)
	raw, err := os.ReadFile("../../migrations/234_api_key_budget.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(raw))
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, service.ReserveAPIKeyBudgetTx(context.Background(), tx, service.APIKeyBudgetReservation{AuthorizationID: "auth_llm", APIKeyID: 1, UserID: 1, SnapshotID: "snap_llm", ReservedUnits: 80_000_000, Kind: "llm"}))
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`UPDATE api_keys SET status='disabled',deleted_at=now()`)
	require.NoError(t, err)
	repo := &usageBillingRepository{db: db}
	cmd := &service.UsageBillingCommand{APIKeyBudgetAuthorizationID: "auth_llm", RequestID: "req_llm", APIKeyID: 1, UserID: 1, WalletCostUSD: 0.7, APIKeyQuotaCost: 0.7, APIKeyRateLimitCost: 0.7}
	first, err := repo.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.True(t, first.Applied)
	second, err := repo.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.False(t, second.Applied)
	var spent, window, status string
	require.NoError(t, db.QueryRow(`SELECT quota_used::text,usage_5h::text,status FROM api_keys`).Scan(&spent, &window, &status))
	require.Equal(t, "0.70000000", spent)
	require.Equal(t, "0.70000000", window)
	require.Equal(t, "disabled", status)
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization WHERE state='captured' AND captured_units=70000000`).Scan(&n))
	require.Equal(t, 1, n)
}
