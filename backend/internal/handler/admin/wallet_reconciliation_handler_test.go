//go:build unit

package admin

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// fakeWalletReconciliationReader is the handler unit test's stand-in for the
// read service: it returns canned summaries/watermarks and records the
// calls, so the wire rendering (decimal strings, schema:1, RFC3339) and the
// parameter validation are tested without any store.
type fakeWalletReconciliationReader struct {
	summary   *service.WalletReconciliationSummary
	watermark *service.WalletReconciliationWatermark
	err       error

	lastUser    string
	lastSince   time.Time
	lastUntil   time.Time
	lastAfterID int64
	calls       int
}

func (f *fakeWalletReconciliationReader) Summary(_ context.Context, platformUserID string, since, until time.Time, afterID int64) (*service.WalletReconciliationSummary, error) {
	f.calls++
	f.lastUser, f.lastSince, f.lastUntil, f.lastAfterID = platformUserID, since, until, afterID
	if f.err != nil {
		return nil, f.err
	}
	return f.summary, nil
}

func (f *fakeWalletReconciliationReader) Watermark(_ context.Context) (*service.WalletReconciliationWatermark, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.watermark, nil
}

func newWalletReconciliationHandlerTestRouter(fake *fakeWalletReconciliationReader) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := &WalletReconciliationHandler{svc: fake}
	r.GET("/api/v1/admin/wallet/reconciliation/summary", h.Summary)
	r.GET("/api/v1/admin/wallet/reconciliation/watermark", h.Watermark)
	return r
}

func walletReconciliationFixtureSummary() *service.WalletReconciliationSummary {
	delivered := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	occurred := time.Date(2026, 8, 29, 9, 59, 0, 0, time.UTC)
	pendingRelease := int64(6_000000)
	leaseID := "srv-lease-9"
	nextAfter := int64(5000)
	settled := "settled"
	gwusg := "gwusg_settled"
	fx := "7.2451"
	return &service.WalletReconciliationSummary{
		PlatformUserID: "shipany-user-7",
		Window: service.WalletReconciliationWindow{
			Since: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			Until: time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC),
		},
		Outbox: []service.WalletReconciliationOutboxRow{{
			EventID: "gwusg_evt_1", LeaseID: "srv-lease-8", GatewayRequestID: "req_1", Currency: "CNY",
			AmountUnits: 12_345678, Status: "delivered", AttemptCount: 1, AuthorizationID: "auth_1",
			OccurredAt: occurred, DeliveredAt: &delivered,
		}, {
			EventID: "gwusg_evt_2", GatewayRequestID: "req_2", Currency: "CNY",
			AmountUnits: 6_000000, Status: "dead_letter", DeadLetterReason: "balance_shortfall",
			ParentEventID: "gwusg_evt_1", SplitDepth: 1, PendingReleaseUnits: &pendingRelease,
			OccurredAt:    occurred,
		}},
		Receivable:  service.WalletReconciliationReceivable{BalanceShortfallUnits: 6_000000, SplitExhaustedUnits: 0, Rows: 1},
		Truncated:   true,
		NextAfterID: &nextAfter,
		Holds: []service.WalletReconciliationHoldRow{{
			AuthorizationID: "auth_1", LeaseID: "srv-lease-8", HeldUnits: 5_000000, Class: "indeterminate",
			ArmedAt: occurred, ClassifiedAt: occurred, Resolution: &settled, ResolvedAt: &delivered, SettlementEventID: &gwusg,
		}},
		Live: []service.WalletReconciliationLiveRow{{
			CallHash: "live_hash_1", AuthorizationID: "auth_live", BillingSnapshotID: "snap_1", BillingFX: &fx,
			Status: "finalized", EstimatedUnits: 7_000000, SettlementEventID: "gwusg_live_final",
			Windows: []service.WalletReconciliationLiveWindowRow{{
				Seq: 1, LeaseID: leaseID, SettledUnits: 3_000000, PendingUnits: 0, OpenedAtMS: 1693302000000,
				EventID: "gwusg_w1",
			}},
		}},
		Redis: service.WalletReconciliationRedisView{
			Available:      true,
			CurrentLeaseID: &leaseID,
			Lease: &service.CanonicalWalletLease{
				LeaseID: leaseID, PlatformUserID: "shipany-user-7", Currency: "CNY",
				BudgetUnits: 500_000000, ConsumedUnits: 9_000000, ExpiresAt: delivered.Add(5 * time.Minute),
			},
			OpenHolds: 2,
		},
	}
}

func TestWalletReconciliationHandlerSummary(t *testing.T) {
	since := "2026-08-01T00:00:00Z"
	until := "2026-08-28T00:00:00Z"

	t.Run("validation is 400", func(t *testing.T) {
		cases := []struct {
			name string
			qs   string
		}{
			{"missing user", "since=" + since + "&until=" + until},
			{"bad user id shape", "platform_user_id=" + strings.Repeat("!", 5) + "&since=" + since + "&until=" + until},
			{"missing since", "platform_user_id=shipany-user-7&until=" + until},
			{"missing until", "platform_user_id=shipany-user-7&since=" + since},
			{"bad since format", "platform_user_id=shipany-user-7&since=not-a-time&until=" + until},
			{"since after until", "platform_user_id=shipany-user-7&since=" + until + "&until=" + since},
			{"window over 31 days", "platform_user_id=shipany-user-7&since=" + since + "&until=2026-10-01T00:00:00Z"},
			{"negative after_id", "platform_user_id=shipany-user-7&since=" + since + "&until=" + until + "&after_id=-1"},
			{"bad after_id", "platform_user_id=shipany-user-7&since=" + since + "&until=" + until + "&after_id=abc"},
		}
		for _, tc := range cases {
			fake := &fakeWalletReconciliationReader{}
			r := newWalletReconciliationHandlerTestRouter(fake)
			req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary?"+tc.qs, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			require.Equal(t, 400, rec.Code, "%s: %s", tc.name, tc.qs)
			require.Zero(t, fake.calls, "%s must not reach the reader", tc.name)
		}
	})

	t.Run("happy path renders the wire: schema 1, decimal-string units, RFC3339 UTC", func(t *testing.T) {
		fake := &fakeWalletReconciliationReader{summary: walletReconciliationFixtureSummary()}
		r := newWalletReconciliationHandlerTestRouter(fake)
		req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary?platform_user_id=shipany-user-7&since="+since+"&until="+until+"&after_id=42", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)

		require.Equal(t, "shipany-user-7", fake.lastUser)
		require.Equal(t, int64(42), fake.lastAfterID)
		require.Equal(t, since, fake.lastSince.UTC().Format(time.RFC3339))
		require.Equal(t, until, fake.lastUntil.UTC().Format(time.RFC3339))

		var body map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		var schema int
		require.NoError(t, json.Unmarshal(body["schema"], &schema))
		require.Equal(t, 1, schema)

		var outbox []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body["outbox"], &outbox))
		require.Len(t, outbox, 2)
		var amount string
		require.NoError(t, json.Unmarshal(outbox[0]["amount_units"], &amount))
		require.Equal(t, "12345678", amount, "units are decimal strings")
		var deliveredAt string
		require.NoError(t, json.Unmarshal(outbox[0]["delivered_at"], &deliveredAt))
		require.Equal(t, "2026-08-29T10:00:00Z", deliveredAt, "timestamps are RFC3339 UTC")
		var occurredAt string
		require.NoError(t, json.Unmarshal(outbox[0]["occurred_at"], &occurredAt))
		require.Equal(t, "2026-08-29T09:59:00Z", occurredAt)
		var pendingRelease string
		require.NoError(t, json.Unmarshal(outbox[1]["pending_release_units"], &pendingRelease))
		require.Equal(t, "6000000", pendingRelease)
		var status, reason string
		require.NoError(t, json.Unmarshal(outbox[1]["status"], &status))
		require.NoError(t, json.Unmarshal(outbox[1]["dead_letter_reason"], &reason))
		require.Equal(t, "dead_letter", status)
		require.Equal(t, "balance_shortfall", reason)

		var receivable struct {
			BalanceShortfallUnits string `json:"balance_shortfall_units"`
			SplitExhaustedUnits   string `json:"split_exhausted_units"`
			Rows                  int    `json:"rows"`
		}
		require.NoError(t, json.Unmarshal(body["receivable"], &receivable))
		require.Equal(t, "6000000", receivable.BalanceShortfallUnits)
		require.Equal(t, "0", receivable.SplitExhaustedUnits)
		require.Equal(t, 1, receivable.Rows)

		var truncated bool
		require.NoError(t, json.Unmarshal(body["truncated"], &truncated))
		require.True(t, truncated)
		var nextAfter int64
		require.NoError(t, json.Unmarshal(body["next_after_id"], &nextAfter))
		require.Equal(t, int64(5000), nextAfter)

		var holds []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body["holds"], &holds))
		require.Len(t, holds, 1)
		var held, resolution, eventID string
		require.NoError(t, json.Unmarshal(holds[0]["held_units"], &held))
		require.NoError(t, json.Unmarshal(holds[0]["resolution"], &resolution))
		require.NoError(t, json.Unmarshal(holds[0]["settlement_event_id"], &eventID))
		require.Equal(t, "5000000", held)
		require.Equal(t, "settled", resolution)
		require.Equal(t, "gwusg_settled", eventID)

		var live []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body["live"], &live))
		require.Len(t, live, 1)
		var fx, est string
		require.NoError(t, json.Unmarshal(live[0]["billing_fx"], &fx))
		require.NoError(t, json.Unmarshal(live[0]["estimated_units"], &est))
		require.Equal(t, "7.2451", fx)
		require.Equal(t, "7000000", est)
		var windows []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(live[0]["windows"], &windows))
		require.Len(t, windows, 1)
		var settledUnits, wEvent string
		require.NoError(t, json.Unmarshal(windows[0]["settled_units"], &settledUnits))
		require.NoError(t, json.Unmarshal(windows[0]["event_id"], &wEvent))
		require.Equal(t, "3000000", settledUnits)
		require.Equal(t, "gwusg_w1", wEvent)

		var redisView struct {
			Available      bool            `json:"available"`
			CurrentLeaseID *string         `json:"current_lease_id"`
			Lease          json.RawMessage `json:"lease"`
			OpenHolds      int             `json:"open_holds"`
		}
		require.NoError(t, json.Unmarshal(body["redis"], &redisView))
		require.True(t, redisView.Available)
		require.NotNil(t, redisView.CurrentLeaseID)
		require.Equal(t, "srv-lease-9", *redisView.CurrentLeaseID)
		require.True(t, strings.Contains(string(redisView.Lease), `"budget_units":"500000000"`), "lease units are decimal strings too: %s", redisView.Lease)
		require.Equal(t, 2, redisView.OpenHolds)

		var window struct {
			Since string `json:"since"`
			Until string `json:"until"`
		}
		require.NoError(t, json.Unmarshal(body["window"], &window))
		require.Equal(t, since, window.Since)
		require.Equal(t, until, window.Until)
	})

	t.Run("a reader error is 503, not 500", func(t *testing.T) {
		fake := &fakeWalletReconciliationReader{err: context.DeadlineExceeded}
		r := newWalletReconciliationHandlerTestRouter(fake)
		req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary?platform_user_id=shipany-user-7&since="+since+"&until="+until, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 503, rec.Code)
	})

	t.Run("redis unavailable is diagnostic only: still 200", func(t *testing.T) {
		summary := walletReconciliationFixtureSummary()
		summary.Redis = service.WalletReconciliationRedisView{Available: false}
		fake := &fakeWalletReconciliationReader{summary: summary}
		r := newWalletReconciliationHandlerTestRouter(fake)
		req := httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/summary?platform_user_id=shipany-user-7&since="+since+"&until="+until, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)
		var body map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		var redisView struct {
			Available      bool    `json:"available"`
			CurrentLeaseID *string `json:"current_lease_id"`
			Lease          *string `json:"lease"`
			OpenHolds      int     `json:"open_holds"`
		}
		require.NoError(t, json.Unmarshal(body["redis"], &redisView))
		require.False(t, redisView.Available)
		require.Nil(t, redisView.CurrentLeaseID)
		require.Nil(t, redisView.Lease)
		require.Zero(t, redisView.OpenHolds)
	})

	t.Run("watermark renders the global shape", func(t *testing.T) {
		maxDelivered := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
		fake := &fakeWalletReconciliationReader{watermark: &service.WalletReconciliationWatermark{
			DeliveredAtMax: &maxDelivered, OutboxIDMax: 987654,
			Pending: 3, InFlight: 2, DeadLetter: 1,
			Receivable: service.WalletReconciliationReceivable{BalanceShortfallUnits: 6_000000, SplitExhaustedUnits: 1_000000, Rows: 2},
		}}
		r := newWalletReconciliationHandlerTestRouter(fake)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/admin/wallet/reconciliation/watermark", nil))
		require.Equal(t, 200, rec.Code)
		var body struct {
			Schema        int    `json:"schema"`
			DeliveredAtMax string `json:"delivered_at_max"`
			OutboxIDMax   int64  `json:"outbox_id_max"`
			Pending       int64  `json:"pending"`
			InFlight      int64  `json:"in_flight"`
			DeadLetter    int64  `json:"dead_letter"`
			Receivable    struct {
				BalanceShortfallUnits string `json:"balance_shortfall_units"`
				SplitExhaustedUnits   string `json:"split_exhausted_units"`
			} `json:"receivable"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, 1, body.Schema)
		require.Equal(t, "2026-08-29T10:00:00Z", body.DeliveredAtMax)
		require.Equal(t, int64(987654), body.OutboxIDMax)
		require.Equal(t, int64(3), body.Pending)
		require.Equal(t, int64(2), body.InFlight)
		require.Equal(t, int64(1), body.DeadLetter)
		require.Equal(t, "6000000", body.Receivable.BalanceShortfallUnits)
		require.Equal(t, "1000000", body.Receivable.SplitExhaustedUnits)
	})

	t.Run("an unwired service is 503, never a panic", func(t *testing.T) {
		h := NewWalletReconciliationHandler(nil, &config.Config{})
		require.Empty(t, h.ReadToken(), "no token configured")
		r := gin.New()
		r.GET("/summary", h.Summary)
		req := httptest.NewRequest("GET", "/summary?platform_user_id=shipany-user-7&since="+since+"&until="+until, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 503, rec.Code)
	})

	t.Run("ReadToken carries the configured token", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.CanonicalWallet.ReconciliationReadToken = walletReconciliationHandlerTestToken
		h := NewWalletReconciliationHandler(nil, cfg)
		require.Equal(t, walletReconciliationHandlerTestToken, h.ReadToken())
	})
}

const walletReconciliationHandlerTestToken = "handler-test-token-0123456789abcd"
