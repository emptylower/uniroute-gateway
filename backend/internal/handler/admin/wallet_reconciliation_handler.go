// Package admin — the Phase 4.1-G reconciliation read handler (redesign
// §15.3): the read-only, token-authenticated view of Sub2API's side of the
// four-way reconciliation. The wire is the plan's contract (units are
// DECIMAL STRINGS, timestamps RFC3339 UTC, schema:1, the redis block
// diagnostic-only); 4.1-S consumes it, so any change is a recorded
// deviation on both sides.
package admin

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// WalletReconciliationReader is the read surface the handler renders —
// *service.WalletReconciliationReadService in production, a fake in unit
// tests.
type WalletReconciliationReader interface {
	Summary(ctx context.Context, platformUserID string, since, until time.Time, afterID int64) (*service.WalletReconciliationSummary, error)
	Watermark(ctx context.Context) (*service.WalletReconciliationWatermark, error)
}

// WalletReconciliationHandler exposes the reconciliation summary and the
// watermark. ReadToken carries the configured bearer token so the route
// registration can mount the group only when a token is deployed (an
// undeployed key is inert — the group is absent, 404).
type WalletReconciliationHandler struct {
	svc   WalletReconciliationReader
	token string
}

// NewWalletReconciliationHandler is the Wire-facing constructor: the
// concrete read service plus the config whose
// canonical_wallet.reconciliation_read_token gates the group's mounting.
func NewWalletReconciliationHandler(svc *service.WalletReconciliationReadService, cfg *config.Config) *WalletReconciliationHandler {
	h := &WalletReconciliationHandler{}
	if svc != nil {
		h.svc = svc
	}
	if cfg != nil {
		h.token = cfg.CanonicalWallet.ReconciliationReadToken
	}
	return h
}

// ReadToken is the configured canonical_wallet.reconciliation_read_token
// ("" when unset — the route registration leaves the group unmounted).
func (h *WalletReconciliationHandler) ReadToken() string { return h.token }

// walletReconciliationMaxWindow is the wire's window bound: since/until
// spans larger than 31 days are refused (400) — the reconciliation pages
// by user, not by unbounded windows.
const walletReconciliationMaxWindow = 31 * 24 * time.Hour

type walletReconciliationWindowDTO struct {
	Since string `json:"since"`
	Until string `json:"until"`
}

type walletReconciliationOutboxRowDTO struct {
	EventID             string  `json:"event_id"`
	LeaseID             string  `json:"lease_id"`
	GatewayRequestID    string  `json:"gateway_request_id"`
	Currency            string  `json:"currency"`
	AmountUnits         string  `json:"amount_units"`
	Status              string  `json:"status"`
	AttemptCount        int     `json:"attempt_count"`
	DeadLetterReason    string  `json:"dead_letter_reason"`
	ParentEventID       string  `json:"parent_event_id"`
	SplitDepth          int     `json:"split_depth"`
	PendingReleaseUnits *string `json:"pending_release_units"`
	AuthorizationID     string  `json:"authorization_id"`
	// Phase 4.2-G (4.1-G's hand-on): the row's own billing snapshot and
	// its fx rate — billing_fx closes 4.1-S's provider_fx_unverified
	// (null is the unverified tail: pre-4.2 rows awaiting the backfill,
	// token-less paths).
	BillingSnapshotID string  `json:"billing_snapshot_id"`
	BillingFX         *string `json:"billing_fx"`
	OccurredAt        string  `json:"occurred_at"`
	DeliveredAt       *string `json:"delivered_at"`
	// RedriveCount (Task 2): the receivable collector's re-drive bound —
	// a window is not final while any row is dead_letter
	// balance_shortfall or pending with redrive_count > 0.
	RedriveCount int `json:"redrive_count"`
}

type walletReconciliationReceivableDTO struct {
	BalanceShortfallUnits string `json:"balance_shortfall_units"`
	SplitExhaustedUnits   string `json:"split_exhausted_units"`
	Rows                  int    `json:"rows"`
}

type walletReconciliationHoldRowDTO struct {
	AuthorizationID   string  `json:"authorization_id"`
	LeaseID           string  `json:"lease_id"`
	HeldUnits         string  `json:"held_units"`
	Class             string  `json:"class"`
	ArmedAt           string  `json:"armed_at"`
	ClassifiedAt      string  `json:"classified_at"`
	Resolution        *string `json:"resolution"`
	ResolvedAt        *string `json:"resolved_at"`
	SettlementEventID *string `json:"settlement_event_id"`
}

type walletReconciliationLiveWindowDTO struct {
	Seq          int    `json:"seq"`
	LeaseID      string `json:"lease_id"`
	SettledUnits string `json:"settled_units"`
	PendingUnits string `json:"pending_units"`
	OpenedAtMS   int64  `json:"opened_at_ms"`
	EventID      string `json:"event_id"`
}

type walletReconciliationLiveRowDTO struct {
	CallHash          string                              `json:"call_hash"`
	AuthorizationID   string                              `json:"authorization_id"`
	BillingSnapshotID string                              `json:"billing_snapshot_id"`
	BillingFX         *string                             `json:"billing_fx"`
	Status            string                              `json:"status"`
	EstimatedUnits    string                              `json:"estimated_units"`
	SettlementEventID string                              `json:"settlement_event_id"`
	Windows           []walletReconciliationLiveWindowDTO `json:"windows"`
}

type walletReconciliationLeaseDTO struct {
	LeaseID        string `json:"lease_id"`
	PlatformUserID string `json:"platform_user_id"`
	Currency       string `json:"currency"`
	BudgetUnits    string `json:"budget_units"`
	ConsumedUnits  string `json:"consumed_units"`
	ExpiresAt      string `json:"expires_at"`
}

type walletReconciliationRedisDTO struct {
	Available      bool                          `json:"available"`
	CurrentLeaseID *string                       `json:"current_lease_id"`
	Lease          *walletReconciliationLeaseDTO `json:"lease"`
	OpenHolds      int                           `json:"open_holds"`
}

type walletReconciliationSummaryDTO struct {
	Schema         int                                `json:"schema"`
	PlatformUserID string                             `json:"platform_user_id"`
	Window         walletReconciliationWindowDTO      `json:"window"`
	Outbox         []walletReconciliationOutboxRowDTO `json:"outbox"`
	Receivable     walletReconciliationReceivableDTO  `json:"receivable"`
	Truncated      bool                               `json:"truncated"`
	NextAfterID    *int64                             `json:"next_after_id"`
	Holds          []walletReconciliationHoldRowDTO   `json:"holds"`
	Live           []walletReconciliationLiveRowDTO   `json:"live"`
	Redis          walletReconciliationRedisDTO       `json:"redis"`
}

type walletReconciliationWatermarkDTO struct {
	Schema         int                               `json:"schema"`
	DeliveredAtMax *string                           `json:"delivered_at_max"`
	OutboxIDMax    int64                             `json:"outbox_id_max"`
	Pending        int64                             `json:"pending"`
	InFlight       int64                             `json:"in_flight"`
	DeadLetter     int64                             `json:"dead_letter"`
	Receivable     walletReconciliationReceivableDTO `json:"receivable"`
}

func walletReconciliationUnits(v int64) string { return strconv.FormatInt(v, 10) }

func walletReconciliationTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func walletReconciliationTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	v := walletReconciliationTime(*t)
	return &v
}

func walletReconciliationUnitsPtr(v *int64) *string {
	if v == nil {
		return nil
	}
	s := walletReconciliationUnits(*v)
	return &s
}

func walletReconciliationReceivableDTOFrom(r service.WalletReconciliationReceivable) walletReconciliationReceivableDTO {
	return walletReconciliationReceivableDTO{
		BalanceShortfallUnits: walletReconciliationUnits(r.BalanceShortfallUnits),
		SplitExhaustedUnits:   walletReconciliationUnits(r.SplitExhaustedUnits),
		Rows:                  r.Rows,
	}
}

// Summary renders GET /api/v1/admin/wallet/reconciliation/summary — the
// four-leg read model for ONE platform user over [since, until), resuming
// after the id cursor. Validation failures are 400; store failures are
// 503; Redis is diagnostic and never fails the request.
func (h *WalletReconciliationHandler) Summary(c *gin.Context) {
	rawUser := strings.TrimSpace(c.Query("platform_user_id"))
	if rawUser == "" {
		walletReconciliationBadRequest(c, "platform_user_id is required")
		return
	}
	platformUserID, err := service.NormalizePlatformUserID(rawUser)
	if err != nil {
		walletReconciliationBadRequest(c, "platform_user_id: unknown user id shape")
		return
	}
	since, err := time.Parse(time.RFC3339, strings.TrimSpace(c.Query("since")))
	if err != nil {
		walletReconciliationBadRequest(c, "since must be RFC3339")
		return
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(c.Query("until")))
	if err != nil {
		walletReconciliationBadRequest(c, "until must be RFC3339")
		return
	}
	if since.After(until) {
		walletReconciliationBadRequest(c, "since must not be after until")
		return
	}
	if until.Sub(since) > walletReconciliationMaxWindow {
		walletReconciliationBadRequest(c, "window must not exceed 31 days")
		return
	}
	var afterID int64
	if raw := strings.TrimSpace(c.Query("after_id")); raw != "" {
		afterID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || afterID < 0 {
			walletReconciliationBadRequest(c, "after_id must be a non-negative integer")
			return
		}
	}
	if h == nil || h.svc == nil {
		walletReconciliationUnavailable(c)
		return
	}
	summary, err := h.svc.Summary(c.Request.Context(), platformUserID, since, until, afterID)
	if err != nil {
		walletReconciliationUnavailable(c)
		return
	}

	dto := walletReconciliationSummaryDTO{
		Schema:         1,
		PlatformUserID: summary.PlatformUserID,
		Window:         walletReconciliationWindowDTO{Since: walletReconciliationTime(summary.Window.Since), Until: walletReconciliationTime(summary.Window.Until)},
		Outbox:         make([]walletReconciliationOutboxRowDTO, 0, len(summary.Outbox)),
		Receivable:     walletReconciliationReceivableDTOFrom(summary.Receivable),
		Truncated:      summary.Truncated,
		NextAfterID:    summary.NextAfterID,
		Holds:          make([]walletReconciliationHoldRowDTO, 0, len(summary.Holds)),
		Live:           make([]walletReconciliationLiveRowDTO, 0, len(summary.Live)),
	}
	for _, e := range summary.Outbox {
		dto.Outbox = append(dto.Outbox, walletReconciliationOutboxRowDTO{
			EventID: e.EventID, LeaseID: e.LeaseID, GatewayRequestID: e.GatewayRequestID, Currency: e.Currency,
			AmountUnits: walletReconciliationUnits(e.AmountUnits), Status: e.Status, AttemptCount: e.AttemptCount,
			DeadLetterReason: e.DeadLetterReason, ParentEventID: e.ParentEventID, SplitDepth: e.SplitDepth,
			PendingReleaseUnits: walletReconciliationUnitsPtr(e.PendingReleaseUnits), AuthorizationID: e.AuthorizationID,
			BillingSnapshotID: e.BillingSnapshotID, BillingFX: e.BillingFX, RedriveCount: e.RedriveCount,
			OccurredAt: walletReconciliationTime(e.OccurredAt), DeliveredAt: walletReconciliationTimePtr(e.DeliveredAt),
		})
	}
	for _, hold := range summary.Holds {
		dto.Holds = append(dto.Holds, walletReconciliationHoldRowDTO{
			AuthorizationID: hold.AuthorizationID, LeaseID: hold.LeaseID, HeldUnits: walletReconciliationUnits(hold.HeldUnits),
			Class: hold.Class, ArmedAt: walletReconciliationTime(hold.ArmedAt), ClassifiedAt: walletReconciliationTime(hold.ClassifiedAt),
			Resolution: hold.Resolution, ResolvedAt: walletReconciliationTimePtr(hold.ResolvedAt), SettlementEventID: hold.SettlementEventID,
		})
	}
	for _, rec := range summary.Live {
		row := walletReconciliationLiveRowDTO{
			CallHash: rec.CallHash, AuthorizationID: rec.AuthorizationID, BillingSnapshotID: rec.BillingSnapshotID,
			BillingFX: rec.BillingFX, Status: rec.Status, EstimatedUnits: walletReconciliationUnits(rec.EstimatedUnits),
			SettlementEventID: rec.SettlementEventID,
			Windows:           make([]walletReconciliationLiveWindowDTO, 0, len(rec.Windows)),
		}
		for _, w := range rec.Windows {
			row.Windows = append(row.Windows, walletReconciliationLiveWindowDTO{
				Seq: w.Seq, LeaseID: w.LeaseID, SettledUnits: walletReconciliationUnits(w.SettledUnits),
				PendingUnits: walletReconciliationUnits(w.PendingUnits), OpenedAtMS: w.OpenedAtMS, EventID: w.EventID,
			})
		}
		dto.Live = append(dto.Live, row)
	}
	dto.Redis = walletReconciliationRedisDTO{
		Available:      summary.Redis.Available,
		CurrentLeaseID: summary.Redis.CurrentLeaseID,
		OpenHolds:      summary.Redis.OpenHolds,
	}
	if summary.Redis.Lease != nil {
		dto.Redis.Lease = &walletReconciliationLeaseDTO{
			LeaseID: summary.Redis.Lease.LeaseID, PlatformUserID: summary.Redis.Lease.PlatformUserID,
			Currency: summary.Redis.Lease.Currency, BudgetUnits: walletReconciliationUnits(summary.Redis.Lease.BudgetUnits),
			ConsumedUnits: walletReconciliationUnits(summary.Redis.Lease.ConsumedUnits),
			ExpiresAt:     walletReconciliationTime(summary.Redis.Lease.ExpiresAt),
		}
	}
	c.JSON(http.StatusOK, dto)
}

// Watermark renders GET /api/v1/admin/wallet/reconciliation/watermark —
// the global high-water mark and the global receivable.
func (h *WalletReconciliationHandler) Watermark(c *gin.Context) {
	if h == nil || h.svc == nil {
		walletReconciliationUnavailable(c)
		return
	}
	wm, err := h.svc.Watermark(c.Request.Context())
	if err != nil {
		walletReconciliationUnavailable(c)
		return
	}
	c.JSON(http.StatusOK, walletReconciliationWatermarkDTO{
		Schema:         1,
		DeliveredAtMax: walletReconciliationTimePtr(wm.DeliveredAtMax),
		OutboxIDMax:    wm.OutboxIDMax,
		Pending:        wm.Pending,
		InFlight:       wm.InFlight,
		DeadLetter:     wm.DeadLetter,
		Receivable:     walletReconciliationReceivableDTOFrom(wm.Receivable),
	})
}

func walletReconciliationBadRequest(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": message})
}

func walletReconciliationUnavailable(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reconciliation store unavailable"})
}
