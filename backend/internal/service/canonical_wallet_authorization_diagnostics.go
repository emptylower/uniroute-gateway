package service

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

type walletAuthorizationDiagnosticKey struct{}

// This request-local record observes existing operations only. No error text,
// wire payload, endpoint, credential or authorization write token is retained.
type walletAuthorizationDiagnostic struct {
	mu                          sync.Mutex
	started, stageStarted       time.Time
	stage                       string
	poolKnown                   bool
	poolTotal, poolHeadroom     int64
	leaseCount                  int
	controlStatus               int
	controlReason, controlStage string
	controlElapsed              time.Duration
	controlHeadroom             *int64
}

func newWalletAuthorizationDiagnostic(ctx context.Context) (context.Context, *walletAuthorizationDiagnostic) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	d := &walletAuthorizationDiagnostic{started: now, stageStarted: now, stage: "handle_create"}
	return context.WithValue(ctx, walletAuthorizationDiagnosticKey{}, d), d
}

func walletAuthorizationDiagnosticFromContext(ctx context.Context) *walletAuthorizationDiagnostic {
	if ctx == nil {
		return nil
	}
	d, _ := ctx.Value(walletAuthorizationDiagnosticKey{}).(*walletAuthorizationDiagnostic)
	return d
}

func walletAuthorizationStage(ctx context.Context, stage string) {
	if d := walletAuthorizationDiagnosticFromContext(ctx); d != nil {
		d.mu.Lock()
		d.stage, d.stageStarted = stage, time.Now()
		d.mu.Unlock()
	}
}

func walletAuthorizationPool(ctx context.Context, leases []CanonicalWalletLease) {
	d := walletAuthorizationDiagnosticFromContext(ctx)
	if d == nil {
		return
	}
	var total, headroom int64
	for _, lease := range leases {
		var err error
		total, err = AddUnits(total, lease.BudgetUnits)
		if err != nil {
			return
		}
		headroom, err = AddUnits(headroom, lease.RemainingUnits())
		if err != nil {
			return
		}
	}
	d.mu.Lock()
	d.poolKnown, d.poolTotal, d.poolHeadroom, d.leaseCount = true, total, headroom, len(leases)
	d.mu.Unlock()
}

func walletAuthorizationControlStart(ctx context.Context) func() {
	d := walletAuthorizationDiagnosticFromContext(ctx)
	if d == nil {
		return func() {}
	}
	start := time.Now()
	d.mu.Lock()
	d.controlStatus, d.controlReason, d.controlStage = 0, "", d.stage
	d.controlHeadroom = nil
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		d.controlElapsed = time.Since(start)
		d.mu.Unlock()
	}
}

func walletAuthorizationControlResponse(ctx context.Context, status int, reason string, headroom *int64) {
	if d := walletAuthorizationDiagnosticFromContext(ctx); d != nil {
		d.mu.Lock()
		d.controlStatus, d.controlReason = status, safeWalletControlReason(reason)
		d.controlHeadroom = headroom
		d.mu.Unlock()
	}
}

func safeWalletControlReason(reason string) string {
	switch reason {
	case "", "lease_cap_reached", "insufficient_balance", "lease_contention", "over_capture", "lease_missing", "lease_expired", "lease_exhausted", "policy_mismatch", "invalid_request", "unauthorized", "forbidden", "reconciliation_required", "internal_error", "lease_owner_mismatch":
		return reason
	default:
		return "other"
	}
}

func walletAuthorizationErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	for _, entry := range []struct {
		err   error
		class string
	}{
		{context.DeadlineExceeded, "deadline"}, {context.Canceled, "canceled"},
		{ErrCanonicalWalletBalanceShortfall, "balance_shortfall"}, {ErrCanonicalWalletLeaseCapReached, "lease_cap_reached"},
		{ErrCanonicalWalletLeaseContention, "lease_contention"}, {ErrCanonicalWalletControlPlaneIncompatible, "control_incompatible"},
		{ErrCanonicalUSDWalletPolicy, "usd_policy"}, {ErrCanonicalWalletLeaseGrantBelowAmount, "grant_below_bound"},
		{ErrCanonicalWalletLeaseMissing, "lease_missing"}, {ErrCanonicalWalletLeaseExpired, "lease_expired"},
		{ErrCanonicalWalletLeaseExhausted, "lease_exhausted"}, {ErrCanonicalWalletHoldMissing, "hold_missing"},
		{ErrCanonicalWalletHoldNotArmed, "hold_not_armed"}, {ErrCanonicalWalletUnitsOverflow, "units_overflow"},
		{ErrCanonicalWalletUnitsNegative, "units_negative"}, {ErrEstimateUnbounded, "estimate_unbounded"},
		{ErrModelPricingUnavailable, "pricing_unavailable"}, {sql.ErrNoRows, "database_no_rows"},
		{sql.ErrTxDone, "database_transaction_done"},
	} {
		if errors.Is(err, entry.err) {
			return entry.class
		}
	}
	var status *canonicalWalletStatusError
	if errors.As(err, &status) {
		return "control_http"
	}
	var network interface {
		error
		Timeout() bool
		Temporary() bool
	}
	if errors.As(err, &network) {
		if network.Timeout() {
			return "network_timeout"
		}
		return "network"
	}
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	if errors.As(err, &syntax) || errors.As(err, &mismatch) {
		return "json_decode"
	}
	return "other"
}

func safeWalletDiagnosticModel(model string) string {
	if len(model) > 128 {
		return "redacted"
	}
	family := strings.ToLower(model)
	known := false
	for _, prefix := range []string{"gpt-", "o1", "o3", "o4", "codex", "claude-", "grok-", "gemini-", "deepseek", "glm-", "kimi-", "qwen", "llama", "mistral", "minimax", "jev-"} {
		known = known || strings.HasPrefix(family, prefix)
	}
	if model == "" {
		return ""
	}
	if !known {
		return "redacted"
	}
	for _, c := range model {
		allowed := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.'
		if !allowed {
			return "redacted"
		}
	}
	return model
}

func safeWalletDiagnosticID(id, prefix string) string {
	if id == "" {
		return ""
	}
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+32 {
		return "redacted"
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(id, prefix)); err != nil {
		return "redacted"
	}
	return id
}

func (d *walletAuthorizationDiagnostic) log(ctx context.Context, h *AuthorizationHandle, in AuthorizeInput, reason AuthorizationRefusalReason, cause error, enforce bool) {
	d.mu.Lock()
	fields := []zap.Field{
		zap.String("component", "service.authorization"), zap.String("stage", d.stage),
		zap.String("reason", string(reason)), zap.Bool("enforce", enforce),
		zap.String("error_class", walletAuthorizationErrorClass(cause)),
		zap.Int64("elapsed_ms", time.Since(d.started).Milliseconds()), zap.Int64("stage_elapsed_ms", time.Since(d.stageStarted).Milliseconds()),
		zap.Bool("pool_known", d.poolKnown), zap.Int64("pool_total_units", d.poolTotal),
		zap.Int64("pool_headroom_units", d.poolHeadroom), zap.Int("lease_count", d.leaseCount),
		zap.Int("control_http_status", d.controlStatus), zap.String("control_reason", d.controlReason),
		zap.String("control_stage", d.controlStage), zap.Int64("control_elapsed_ms", d.controlElapsed.Milliseconds()),
		zap.Int("input_tokens_upper_bound", in.Estimate.InputTokensUpperBound), zap.Int("request_max_output_tokens", in.Estimate.MaxOutputTokens),
	}
	if d.controlHeadroom != nil {
		fields = append(fields, zap.Int64("control_headroom_units", *d.controlHeadroom))
	}
	d.mu.Unlock()
	if h != nil {
		fields = append(fields, zap.String("authorization_id", safeWalletDiagnosticID(h.ID, "auth_")), zap.String("snapshot_id", safeWalletDiagnosticID(h.SnapshotID, "bsnap_")), zap.Int64("estimated_units", h.EstimatedUnits), zap.Int("segment_count", len(h.Segments)))
	}
	if in.Snapshot != nil {
		fields = append(fields, zap.String("billing_model", safeWalletDiagnosticModel(in.Snapshot.BillingModel)), zap.Int("model_max_output_tokens", in.Snapshot.Pricing.MaxOutputTokens))
	}
	var status *canonicalWalletStatusError
	if errors.As(cause, &status) && status != nil {
		fields = append(fields, zap.Int("cause_http_status", status.Status), zap.String("cause_control_reason", safeWalletControlReason(status.Reason)))
		if status.HeadroomUnits != nil {
			fields = append(fields, zap.Int64("cause_control_headroom_units", *status.HeadroomUnits))
		}
	}
	log := logger.FromContext(ctx)
	log.Warn("wallet authorization refused", fields...)
}
