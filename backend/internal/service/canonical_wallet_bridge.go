package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrCanonicalWalletLeaseMissing          = errors.New("canonical wallet lease is missing")
	ErrCanonicalWalletLeaseExpired          = errors.New("canonical wallet lease is expired")
	ErrCanonicalWalletLeaseExhausted        = errors.New("canonical wallet lease is exhausted")
	ErrCanonicalWalletLeaseCurrencyMismatch = errors.New("canonical wallet lease currency mismatch")
	ErrCanonicalWalletReservationConflict   = errors.New("canonical wallet event was reserved against a different lease")
)

const (
	canonicalWalletLeaseScope         = "wallet:lease"
	canonicalWalletSettlementScope    = "wallet:settlement"
	canonicalWalletCNYMicrosPerUnit   = 1_000_000
	canonicalWalletCNYMicrosPerCredit = 10_000
)

type CanonicalWalletLease struct {
	LeaseID        string    `json:"lease_id"`
	PlatformUserID string    `json:"platform_user_id"`
	Currency       string    `json:"currency"`
	BudgetMicros   int64     `json:"budget_micros"`
	ConsumedMicros int64     `json:"consumed_micros"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (l CanonicalWalletLease) RemainingMicros() int64 {
	remaining := l.BudgetMicros - l.ConsumedMicros
	if remaining < 0 {
		return 0
	}
	return remaining
}

type CanonicalWalletReservation struct {
	Lease     CanonicalWalletLease
	Duplicate bool
}

// CanonicalWalletLeaseStore is implemented by the Redis-backed gateway cache.
// Reserve must be atomic and idempotent for the supplied event ID.
type CanonicalWalletLeaseStore interface {
	InstallCanonicalWalletLease(ctx context.Context, lease CanonicalWalletLease) error
	GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*CanonicalWalletLease, error)
	ReserveCanonicalWalletLease(ctx context.Context, platformUserID, currency, eventID string, amountMicros int64, now time.Time) (*CanonicalWalletReservation, error)
}

type CanonicalWalletSettlementEvent struct {
	EventID                 string    `json:"event_id"`
	GatewayRequestID        string    `json:"gateway_request_id"`
	PlatformUserID          string    `json:"platform_user_id"`
	LeaseID                 string    `json:"lease_id"`
	Currency                string    `json:"currency"`
	AmountMicros            int64     `json:"amount_micros"`
	LocalBalanceAfterMicros *int64    `json:"local_balance_after_micros,omitempty"`
	OccurredAt              time.Time `json:"occurred_at"`
}

type CanonicalWalletSettlementResult struct {
	Accepted               bool   `json:"accepted"`
	Duplicate              bool   `json:"duplicate"`
	CanonicalBalanceMicros *int64 `json:"canonical_balance_micros,omitempty"`
}

type canonicalWalletLeaseRequest struct {
	PlatformUserID      string `json:"platform_user_id"`
	Currency            string `json:"currency"`
	RequestedMicros     int64  `json:"requested_micros"`
	RequestedTTLSeconds int    `json:"requested_ttl_seconds"`
}

type canonicalWalletControlPlane interface {
	AcquireLease(ctx context.Context, request canonicalWalletLeaseRequest) (*CanonicalWalletLease, error)
	SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error)
}

type canonicalWalletHTTPClient struct {
	cfg    config.CanonicalWalletConfig
	client *http.Client
	now    func() time.Time
}

func newCanonicalWalletHTTPClient(cfg config.CanonicalWalletConfig, client *http.Client) *canonicalWalletHTTPClient {
	if client == nil {
		client = &http.Client{Timeout: time.Duration(cfg.RequestTimeoutMS) * time.Millisecond}
	}
	return &canonicalWalletHTTPClient{cfg: cfg, client: client, now: func() time.Time { return time.Now().UTC() }}
}

func (c *canonicalWalletHTTPClient) AcquireLease(ctx context.Context, request canonicalWalletLeaseRequest) (*CanonicalWalletLease, error) {
	var lease CanonicalWalletLease
	windowSeconds := int64(c.cfg.LeaseTTLSeconds)
	if windowSeconds <= 0 {
		windowSeconds = 60
	}
	window := c.now().Unix() / windowSeconds
	leaseKeyRaw := fmt.Sprintf("v1|%s|%s|%d", strings.TrimSpace(request.PlatformUserID), NormalizeUserBillingCurrency(request.Currency), window)
	leaseKeyHash := sha256.Sum256([]byte(leaseKeyRaw))
	idempotencyKey := "gwlease_" + hex.EncodeToString(leaseKeyHash[:])
	if err := c.doJSON(ctx, http.MethodPost, "/api/internal/v1/wallet/leases/acquire", canonicalWalletLeaseScope, idempotencyKey, request, &lease); err != nil {
		return nil, err
	}
	if strings.TrimSpace(lease.LeaseID) == "" || strings.TrimSpace(lease.PlatformUserID) != strings.TrimSpace(request.PlatformUserID) || lease.BudgetMicros <= 0 || lease.ExpiresAt.IsZero() {
		return nil, errors.New("control plane returned an invalid canonical wallet lease")
	}
	lease.Currency = NormalizeUserBillingCurrency(lease.Currency)
	if lease.Currency != NormalizeUserBillingCurrency(request.Currency) {
		return nil, ErrCanonicalWalletLeaseCurrencyMismatch
	}
	return &lease, nil
}

func (c *canonicalWalletHTTPClient) SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	var result CanonicalWalletSettlementResult
	if err := c.doJSON(ctx, http.MethodPost, "/api/internal/v1/wallet/settlements", canonicalWalletSettlementScope, event.EventID, event, &result); err != nil {
		return nil, err
	}
	if !result.Accepted && !result.Duplicate {
		return nil, errors.New("control plane rejected canonical wallet settlement")
	}
	return &result, nil
}

func (c *canonicalWalletHTTPClient) doJSON(ctx context.Context, method, path, scope, idempotencyKey string, requestBody, responseBody any) error {
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("encode canonical wallet request: %w", err)
	}
	assertion, err := c.serviceAssertion(scope)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.ControlPlaneURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build canonical wallet request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+assertion)
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("call canonical wallet control plane: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read canonical wallet response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("canonical wallet control plane returned status %d", resp.StatusCode)
	}
	if len(body) == 0 || responseBody == nil {
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) == nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		body = envelope.Data
	}
	if err := json.Unmarshal(body, responseBody); err != nil {
		return fmt.Errorf("decode canonical wallet response: %w", err)
	}
	return nil
}

func (c *canonicalWalletHTTPClient) serviceAssertion(scope string) (string, error) {
	now := c.now()
	claims := jwt.MapClaims{
		"iss":   c.cfg.Issuer,
		"aud":   c.cfg.Audience,
		"sub":   "sub2api-gateway",
		"iat":   now.Unix(),
		"exp":   now.Add(30 * time.Second).Unix(),
		"jti":   uuid.NewString(),
		"scope": scope,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = c.cfg.Version
	signed, err := token.SignedString([]byte(c.cfg.Secret))
	if err != nil {
		return "", fmt.Errorf("sign canonical wallet assertion: %w", err)
	}
	return signed, nil
}

type canonicalWalletMetrics struct {
	queued              atomic.Int64
	queueDropped        atomic.Int64
	leaseAcquireOK      atomic.Int64
	leaseAcquireError   atomic.Int64
	reserveOK           atomic.Int64
	reserveError        atomic.Int64
	settlementOK        atomic.Int64
	settlementError     atomic.Int64
	balanceMismatch     atomic.Int64
	missingPlatformID   atomic.Int64
	unsupportedCurrency atomic.Int64
}

var canonicalWalletBridgeMetrics canonicalWalletMetrics

func CanonicalWalletBridgeStats() map[string]int64 {
	m := &canonicalWalletBridgeMetrics
	return map[string]int64{
		"queued": m.queued.Load(), "queue_dropped": m.queueDropped.Load(),
		"lease_acquire_ok": m.leaseAcquireOK.Load(), "lease_acquire_error": m.leaseAcquireError.Load(),
		"reserve_ok": m.reserveOK.Load(), "reserve_error": m.reserveError.Load(),
		"settlement_ok": m.settlementOK.Load(), "settlement_error": m.settlementError.Load(),
		"balance_mismatch": m.balanceMismatch.Load(), "missing_platform_user_id": m.missingPlatformID.Load(),
		"unsupported_currency": m.unsupportedCurrency.Load(),
	}
}

type CanonicalWalletBridge struct {
	cfg     config.CanonicalWalletConfig
	store   CanonicalWalletLeaseStore
	control canonicalWalletControlPlane
	queue   chan CanonicalWalletSettlementEvent
}

func NewCanonicalWalletBridge(cfg *config.Config, store CanonicalWalletLeaseStore) *CanonicalWalletBridge {
	if cfg == nil || cfg.CanonicalWallet.Mode == "" || cfg.CanonicalWallet.Mode == config.CanonicalWalletModeDisabled {
		return nil
	}
	return newCanonicalWalletBridge(cfg.CanonicalWallet, store, newCanonicalWalletHTTPClient(cfg.CanonicalWallet, nil))
}

func newCanonicalWalletBridge(cfg config.CanonicalWalletConfig, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane) *CanonicalWalletBridge {
	b := &CanonicalWalletBridge{cfg: cfg, store: store, control: control, queue: make(chan CanonicalWalletSettlementEvent, cfg.SettlementQueueSize)}
	for i := 0; i < cfg.SettlementWorkers; i++ {
		go b.runWorker()
	}
	return b
}

// ObserveSettlement is a bounded, non-blocking shadow hook. It never returns an
// error to the existing billing path.
func (b *CanonicalWalletBridge) ObserveSettlement(event CanonicalWalletSettlementEvent) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled || event.AmountMicros <= 0 {
		return
	}
	event.PlatformUserID = strings.TrimSpace(event.PlatformUserID)
	if event.PlatformUserID == "" {
		canonicalWalletBridgeMetrics.missingPlatformID.Add(1)
		return
	}
	event.Currency = NormalizeUserBillingCurrency(event.Currency)
	if event.Currency != CurrencyCNY {
		canonicalWalletBridgeMetrics.unsupportedCurrency.Add(1)
		return
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency, event.AmountMicros)
	}
	select {
	case b.queue <- event:
		canonicalWalletBridgeMetrics.queued.Add(1)
	default:
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet shadow queue full", "event_id", event.EventID, "platform_user_id", event.PlatformUserID)
	}
}

// CheckAndReserve is the future request-preflight state machine. Shadow mode
// observes a denial but always allows the request; enforce mode fails closed.
// It is intentionally not wired into gateway admission in this migration.
func (b *CanonicalWalletBridge) CheckAndReserve(ctx context.Context, event CanonicalWalletSettlementEvent) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency, event.AmountMicros)
	}
	_, err := b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountMicros)
	if err == nil {
		_, err = b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, event.Currency, event.EventID, event.AmountMicros, time.Now().UTC())
	}
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *CanonicalWalletBridge) runWorker() {
	for event := range b.queue {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
		b.processSettlement(ctx, event)
		cancel()
	}
}

func (b *CanonicalWalletBridge) processSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) {
	lease, err := b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountMicros)
	if err != nil {
		canonicalWalletBridgeMetrics.leaseAcquireError.Add(1)
		slog.Warn("canonical wallet shadow lease unavailable", "event_id", event.EventID, "platform_user_id", event.PlatformUserID, "error", err)
		return
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, event.Currency, event.EventID, event.AmountMicros, time.Now().UTC())
	if err != nil {
		canonicalWalletBridgeMetrics.reserveError.Add(1)
		slog.Warn("canonical wallet shadow reservation failed", "event_id", event.EventID, "lease_id", lease.LeaseID, "error", err)
		return
	}
	canonicalWalletBridgeMetrics.reserveOK.Add(1)
	event.LeaseID = reservation.Lease.LeaseID
	result, err := b.control.SubmitSettlement(ctx, event)
	if err != nil {
		canonicalWalletBridgeMetrics.settlementError.Add(1)
		slog.Warn("canonical wallet shadow settlement failed", "event_id", event.EventID, "lease_id", event.LeaseID, "error", err)
		return
	}
	canonicalWalletBridgeMetrics.settlementOK.Add(1)
	attrs := []any{"event_id", event.EventID, "lease_id", event.LeaseID, "duplicate", result.Duplicate, "amount_micros", event.AmountMicros}
	if result.CanonicalBalanceMicros != nil && event.LocalBalanceAfterMicros != nil {
		delta := *result.CanonicalBalanceMicros - *event.LocalBalanceAfterMicros
		if delta != 0 {
			canonicalWalletBridgeMetrics.balanceMismatch.Add(1)
			attrs = append(attrs, "balance_delta_micros", delta, "canonical_balance_micros", *result.CanonicalBalanceMicros, "local_balance_after_micros", *event.LocalBalanceAfterMicros)
		}
	}
	slog.Info("canonical wallet shadow settlement observed", attrs...)
}

func (b *CanonicalWalletBridge) ensureLease(ctx context.Context, platformUserID, currency string, amountMicros int64) (*CanonicalWalletLease, error) {
	if b.store == nil || b.control == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	currency = NormalizeUserBillingCurrency(currency)
	lease, err := b.store.GetCanonicalWalletLease(ctx, platformUserID)
	if err == nil && lease != nil && lease.Currency == currency && lease.ExpiresAt.After(time.Now().UTC()) && lease.RemainingMicros() >= amountMicros {
		return lease, nil
	}
	requested := b.cfg.LeaseBudgetMicros
	if amountMicros > requested {
		requested = amountMicros
	}
	lease, err = b.control.AcquireLease(ctx, canonicalWalletLeaseRequest{
		PlatformUserID: platformUserID, Currency: currency, RequestedMicros: requested, RequestedTTLSeconds: b.cfg.LeaseTTLSeconds,
	})
	if err != nil {
		return nil, err
	}
	if err := b.store.InstallCanonicalWalletLease(ctx, *lease); err != nil {
		return nil, err
	}
	canonicalWalletBridgeMetrics.leaseAcquireOK.Add(1)
	return lease, nil
}

func CanonicalWalletSettlementEventID(requestID, platformUserID, currency string, amountMicros int64) string {
	raw := fmt.Sprintf("v1|%s|%s|%s|%d", strings.TrimSpace(requestID), strings.TrimSpace(platformUserID), NormalizeUserBillingCurrency(currency), amountMicros)
	sum := sha256.Sum256([]byte(raw))
	return "gwusg_" + hex.EncodeToString(sum[:])
}

func canonicalWalletMicros(amount float64) (int64, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) || amount < 0 || amount > float64(math.MaxInt64)/1_000_000 {
		return 0, errors.New("canonical wallet amount is invalid")
	}
	return int64(math.Round(amount * canonicalWalletCNYMicrosPerUnit)), nil
}

func observeCanonicalWalletSettlement(bridge *CanonicalWalletBridge, requestID string, user *User, cost *CostBreakdown, subscriptionBilling, billingApplied bool, billingResult *UsageBillingApplyResult) {
	if bridge == nil || user == nil || cost == nil || subscriptionBilling || !billingApplied || cost.ActualCost <= 0 {
		return
	}
	amountMicros, err := canonicalWalletMicros(cost.ActualCost)
	if err != nil || amountMicros <= 0 {
		return
	}
	localBalance := user.Balance - cost.ActualCost
	if billingResult != nil && billingResult.NewBalance != nil {
		localBalance = *billingResult.NewBalance
	}
	localBalanceAfter, balanceErr := canonicalWalletMicros(localBalance)
	var localBalanceAfterPtr *int64
	if balanceErr == nil {
		localBalanceAfterPtr = &localBalanceAfter
	}
	currency := NormalizeUserBillingCurrency(user.BillingCurrency)
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: requestID, PlatformUserID: user.PlatformUserID, Currency: currency,
		AmountMicros: amountMicros, LocalBalanceAfterMicros: localBalanceAfterPtr, OccurredAt: time.Now().UTC(),
	})
}
