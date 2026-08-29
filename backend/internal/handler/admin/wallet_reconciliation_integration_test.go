//go:build integration

package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// test 69's wire leg (redesign §15.5): through the REAL gin chain — the
// repository's own WalletOutboxStore, the real read service, the real
// handler, the real token middleware — the summary is 401 without the
// token, 200 with it, the watermark counts match, and the 5 000-row cap
// pages by the id cursor without skipping or duplicating a row, including
// rows sharing an occurred_at across the boundary (round-1 MAJOR-4,
// round-2 MAJOR-2 — the leg that fails a time cursor).
func TestWalletReconciliationWireThroughGin(t *testing.T) {
	ctx := context.Background()
	// This package's first integration test: it starts its OWN Postgres
	// and Redis containers (the same images the other harnesses use) —
	// the 3.7c shared containers are test-binary-local to
	// internal/service and cannot be reached from here.
	pgContainer, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23",
		tcpostgres.WithDatabase("recon_wire_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })
	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable", "TimeZone=UTC")
	require.NoError(t, err)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))

	redisContainer, err := tcredis.Run(ctx, "redis:8.4-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisContainer.Terminate(ctx) })
	redisHost, err := redisContainer.Host(ctx)
	require.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", redisHost, redisPort.Int())})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	for _, migration := range []string{
		"208_wallet_settlement_outbox.sql",
		"209_wallet_billing_snapshot.sql",
		"211_wallet_live_provisional.sql",
		"212_wallet_outbox_dead_letter_reason.sql",
		"213_wallet_hold_outcome.sql",
		"214_wallet_outbox_split_and_authorization.sql",
		"215_wallet_reconciliation_indexes.sql",
		"216_wallet_outbox_billing_snapshot.sql",
	} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(raw))
		require.NoError(t, err)
	}

	const token = "test-69-wire-token-0123456789abcdef"
	cfg := &config.Config{}
	cfg.CanonicalWallet.ReconciliationReadToken = token

	outboxStore := repository.NewWalletOutboxStore(db)
	gatewayCache := repository.NewGatewayCache(rdb)
	readService, err := service.NewWalletReconciliationReadService(cfg, db, gatewayCache, outboxStore)
	require.NoError(t, err)
	handler := NewWalletReconciliationHandler(readService, cfg)
	// The cache's canonical-wallet writer surface for seeding (the same
	// type assertion the service's wiring performs).
	leaseWriter, ok := any(gatewayCache).(service.CanonicalWalletLeaseStore)
	require.True(t, ok, "the gateway cache implements CanonicalWalletLeaseStore")

	gin.SetMode(gin.TestMode)
	router := gin.New()
	recon := router.Group("/api/v1/admin/wallet/reconciliation")
	recon.Use(middleware.NewWalletReconciliationTokenAuth(token))
	recon.GET("/summary", handler.Summary)
	recon.GET("/watermark", handler.Watermark)

	user := "shipany-user-recon69wire"
	now := time.Now().UTC().Truncate(time.Microsecond)
	since, until := now.Add(-time.Hour), now.Add(time.Hour)
	summaryURL := fmt.Sprintf("/api/v1/admin/wallet/reconciliation/summary?platform_user_id=%s&since=%s&until=%s",
		user, since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339))

	// Seed: one delivered row for U (the Postgres leg of the 200), and a
	// current lease for U in Redis through the REAL cache writer.
	seedOutboxRow(t, db, user, "gwusg_69w_d1", "lease-69w-a", "delivered", "", 1_000000, now)
	require.NoError(t, leaseWriter.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "lease-69w-current", PlatformUserID: user, Currency: "CNY",
		BudgetUnits: 500_000000, ConsumedUnits: 1_000000, ExpiresAt: now.Add(10 * time.Minute),
	}))

	get := func(url, bearer string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	t.Run("401 without the token, 200 with it", func(t *testing.T) {
		rec := get(summaryURL, "")
		require.Equal(t, 401, rec.Code)
		rec = get(summaryURL, "wrong-token-0123456789abcdef0123456789")
		require.Equal(t, 401, rec.Code)

		rec = get(summaryURL, token)
		require.Equal(t, 200, rec.Code)
		var body struct {
			Schema         int    `json:"schema"`
			PlatformUserID string `json:"platform_user_id"`
			Outbox         []struct {
				EventID     string `json:"event_id"`
				AmountUnits string `json:"amount_units"`
				Status      string `json:"status"`
			} `json:"outbox"`
			Redis struct {
				Available      bool    `json:"available"`
				CurrentLeaseID *string `json:"current_lease_id"`
			} `json:"redis"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, 1, body.Schema)
		require.Equal(t, user, body.PlatformUserID)
		require.Len(t, body.Outbox, 1)
		require.Equal(t, "1000000", body.Outbox[0].AmountUnits)
		require.Equal(t, "delivered", body.Outbox[0].Status)
		require.True(t, body.Redis.Available)
		require.NotNil(t, body.Redis.CurrentLeaseID)
		require.Equal(t, "lease-69w-current", *body.Redis.CurrentLeaseID)
	})

	t.Run("watermark counts match", func(t *testing.T) {
		rec := get("/api/v1/admin/wallet/reconciliation/watermark", token)
		require.Equal(t, 200, rec.Code)
		var wm struct {
			Schema     int   `json:"schema"`
			Pending    int64 `json:"pending"`
			InFlight   int64 `json:"in_flight"`
			DeadLetter int64 `json:"dead_letter"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &wm))
		require.Equal(t, 1, wm.Schema)
		// Compared against the direct table at this moment (only U's
		// delivered row exists yet — the truncation seeds come after).
		var pending, inFlight, dead int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT count(*) FILTER (WHERE status = 'pending'),
			       count(*) FILTER (WHERE status = 'in_flight'),
			       count(*) FILTER (WHERE status = 'dead_letter')
			FROM wallet_settlement_outbox`).Scan(&pending, &inFlight, &dead))
		require.Equal(t, int64(pending), wm.Pending)
		require.Equal(t, int64(inFlight), wm.InFlight)
		require.Equal(t, int64(dead), wm.DeadLetter)
	})

	// --- the truncation leg: 5 001 rows for a second user, every row
	// sharing ONE occurred_at — so ids 5 000 and 5 001 straddle the cap
	// boundary with an IDENTICAL timestamp, the case a time cursor
	// mis-pages.
	truncUser := "shipany-user-recon69trunc"
	occurred := now.Add(-30 * time.Minute)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at)
		VALUES ($1, $2, $3, 'CNY', 1000, 'hash-69w', 'pending', $4)`)
	require.NoError(t, err)
	for i := 0; i < 5001; i++ {
		_, err := stmt.ExecContext(ctx, fmt.Sprintf("gwusg_69w_t%04d", i), truncUser, fmt.Sprintf("req-69w-t%04d", i), occurred)
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM wallet_settlement_outbox WHERE platform_user_id = $1`, truncUser)
	})
	t.Logf("test 69 truncation leg seeded 5001 rows for %s, all with occurred_at %s", truncUser, occurred.Format(time.RFC3339Nano))

	truncURL := fmt.Sprintf("/api/v1/admin/wallet/reconciliation/summary?platform_user_id=%s&since=%s&until=%s",
		truncUser, since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339))

	t.Run("page one: truncated at the 5000-row cap with next_after_id", func(t *testing.T) {
		rec := get(truncURL, token)
		require.Equal(t, 200, rec.Code)
		t.Logf("page one response size: %d bytes", rec.Body.Len())
		var page struct {
			Schema      int               `json:"schema"`
			Outbox      []json.RawMessage `json:"outbox"`
			Truncated   bool              `json:"truncated"`
			NextAfterID *int64            `json:"next_after_id"`
			Receivable  struct {
				BalanceShortfallUnits string `json:"balance_shortfall_units"`
				Rows                  int    `json:"rows"`
			} `json:"receivable"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		require.Equal(t, 1, page.Schema)
		require.True(t, page.Truncated)
		require.Len(t, page.Outbox, 5000, "the row cap is exactly 5000")
		require.NotNil(t, page.NextAfterID)

		// The wire's outbox row carries the EVENT id, not the row id; the
		// seeds embed their insertion order in the event id suffix, and
		// insertion order is BIGSERIAL order, so ascending suffixes prove
		// the id ordering. The last row must be t4999 (the 5000th of
		// 5001), and next_after_id must equal that row's numeric id.
		var rowIDByEvent = func(eventID string) int64 {
			var id int64
			require.NoError(t, db.QueryRowContext(ctx, `SELECT id FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&id))
			return id
		}
		eventIDSuffix := func(raw json.RawMessage) string {
			var row struct {
				EventID string `json:"event_id"`
			}
			require.NoError(t, json.Unmarshal(raw, &row))
			return row.EventID
		}
		lastEvent := eventIDSuffix(page.Outbox[4999])
		firstEvent := eventIDSuffix(page.Outbox[0])
		require.Equal(t, "gwusg_69w_t0000", firstEvent, "page one starts at the first row by id")
		require.Equal(t, "gwusg_69w_t4999", lastEvent, "page one ends at the 5000th row by id")
		require.Equal(t, rowIDByEvent("gwusg_69w_t4999"), *page.NextAfterID, "next_after_id is the last returned row's id")
		require.Equal(t, "0", page.Receivable.BalanceShortfallUnits)

		t.Run("page two: the remaining row, not truncated, null cursor", func(t *testing.T) {
			page2URL := fmt.Sprintf("%s&after_id=%d", truncURL, *page.NextAfterID)
			rec2 := get(page2URL, token)
			require.Equal(t, 200, rec2.Code)
			var page2 struct {
				Truncated   bool              `json:"truncated"`
				NextAfterID *int64            `json:"next_after_id"`
				Outbox      []json.RawMessage `json:"outbox"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
			require.False(t, page2.Truncated)
			require.Nil(t, page2.NextAfterID)
			require.Len(t, page2.Outbox, 1, "the remaining 1 of 5001")

			// The boundary leg: rows 5000 and 5001 share an IDENTICAL
			// occurred_at and appear EXACTLY ONCE each across the two
			// pages — the leg that fails a time cursor and passes the id
			// cursor.
			require.Equal(t, "gwusg_69w_t5000", eventIDSuffix(page2.Outbox[0]), "the boundary row after the cap — same occurred_at, next id")
		})
	})
}

func seedOutboxRow(t *testing.T, db *sql.DB, user, eventID, leaseID, status, reason string, amount int64, now time.Time) {
	t.Helper()
	var deliveredAt, deadReason any
	if status == "delivered" {
		deliveredAt = now.Add(-time.Minute)
	}
	if reason != "" {
		deadReason = reason
	}
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at, delivered_at, dead_letter_reason)
		VALUES ($1, $2, $3, $4, 'CNY', $5, 'hash-69w', $6, $7, $8, $9)`,
		eventID, user, leaseID, "req-"+eventID, amount, status, now.Add(-30*time.Minute), deliveredAt, deadReason)
	require.NoError(t, err)
}

// TestWalletReconciliationSummaryOutboxFX (Phase 4.2-G Task 1d): the wire's
// outbox rows carry their billing snapshot's fx — `outbox[].billing_snapshot_id`
// and `outbox[].billing_fx` after `authorization_id` (4.1-G's hand-on
// closing). A delivered row whose snapshot resolves reports the snapshot's
// rate through the same LEFT JOIN the Live rows use; a row without a
// snapshot reports the id as "" and fx as null — never a dropped record.
func TestWalletReconciliationSummaryOutboxFX(t *testing.T) {
	ctx := context.Background()
	pgContainer, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23",
		tcpostgres.WithDatabase("recon_fx_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })
	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable", "TimeZone=UTC")
	require.NoError(t, err)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))

	for _, migration := range []string{
		"208_wallet_settlement_outbox.sql",
		"209_wallet_billing_snapshot.sql",
		"211_wallet_live_provisional.sql",
		"212_wallet_outbox_dead_letter_reason.sql",
		"213_wallet_hold_outcome.sql",
		"214_wallet_outbox_split_and_authorization.sql",
		"215_wallet_reconciliation_indexes.sql",
		"216_wallet_outbox_billing_snapshot.sql",
	} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(raw))
		require.NoError(t, err)
	}

	redisContainer, err := tcredis.Run(ctx, "redis:8.4-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisContainer.Terminate(ctx) })
	redisHost, err := redisContainer.Host(ctx)
	require.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", redisHost, redisPort.Int())})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	const token = "test-76e-fx-token-0123456789abcdef"
	cfg := &config.Config{}
	cfg.CanonicalWallet.ReconciliationReadToken = token
	outboxStore := repository.NewWalletOutboxStore(db)
	gatewayCache := repository.NewGatewayCache(rdb)
	readService, err := service.NewWalletReconciliationReadService(cfg, db, gatewayCache, outboxStore)
	require.NoError(t, err)
	handler := NewWalletReconciliationHandler(readService, cfg)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	recon := router.Group("/api/v1/admin/wallet/reconciliation")
	recon.Use(middleware.NewWalletReconciliationTokenAuth(token))
	recon.GET("/summary", handler.Summary)

	user := "shipany-user-recon76e"
	now := time.Now().UTC().Truncate(time.Microsecond)
	since, until := now.Add(-time.Hour), now.Add(time.Hour)

	// The snapshot: payload->fx->rate is the figure the LEFT JOIN reads.
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_billing_snapshot
			(id, version, user_id, api_key_id, account_id, billing_model, pricing_mode, payload)
		VALUES ('wbs_76e', 1, 42, 7, 9, 'claude-sonnet-4', 'token', $1)`,
		`{"id":"wbs_76e","version":1,"fx":{"rate":"7.2451","source":"bootstrap","at":"2026-08-30T00:00:00Z"},"pricing":{}}`)
	require.NoError(t, err)

	// Two delivered rows for the user: one bound to the snapshot, one
	// without (pre-4.2 or token-less — the NULL-rate leg).
	seedOutboxRow76e(t, db, user, "gwusg_76e_fx", "lease-76e-a", "wbs_76e", 1_000000, now)
	seedOutboxRow76e(t, db, user, "gwusg_76e_plain", "lease-76e-b", "", 2_000000, now)

	url := fmt.Sprintf("/api/v1/admin/wallet/reconciliation/summary?platform_user_id=%s&since=%s&until=%s",
		user, since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code)

	var body struct {
		Schema int `json:"schema"`
		Outbox []struct {
			EventID           string  `json:"event_id"`
			AuthorizationID   string  `json:"authorization_id"`
			BillingSnapshotID string  `json:"billing_snapshot_id"`
			BillingFX         *string `json:"billing_fx"`
		} `json:"outbox"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, 1, body.Schema)
	require.Len(t, body.Outbox, 2, "both rows return — a missing snapshot never drops the record")
	byEvent := map[string]int{}
	for i, row := range body.Outbox {
		byEvent[row.EventID] = i
	}
	require.Contains(t, byEvent, "gwusg_76e_fx")
	require.Contains(t, byEvent, "gwusg_76e_plain")

	fxRow := body.Outbox[byEvent["gwusg_76e_fx"]]
	require.Equal(t, "wbs_76e", fxRow.BillingSnapshotID)
	require.NotNil(t, fxRow.BillingFX, "a resolving snapshot reports its fx rate")
	require.Equal(t, "7.2451", *fxRow.BillingFX)

	plainRow := body.Outbox[byEvent["gwusg_76e_plain"]]
	require.Equal(t, "", plainRow.BillingSnapshotID)
	require.Nil(t, plainRow.BillingFX, "a row without a snapshot reports null fx")
}

func seedOutboxRow76e(t *testing.T, db *sql.DB, user, eventID, leaseID, snapshotID string, amount int64, now time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO wallet_settlement_outbox
			(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at, delivered_at, billing_snapshot_id)
		VALUES ($1, $2, $3, $4, 'CNY', $5, 'hash-76e', 'delivered', $6, $7, NULLIF($8, ''))`,
		eventID, user, leaseID, "req-"+eventID, amount, now.Add(-30*time.Minute), now.Add(-time.Minute), snapshotID)
	require.NoError(t, err)
}
