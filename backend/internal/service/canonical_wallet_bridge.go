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
	canonicalWalletLeaseScope      = "wallet:lease"
	canonicalWalletSettlementScope = "wallet:settlement"
)

type CanonicalWalletLease struct {
	LeaseID        string    `json:"lease_id"`
	PlatformUserID string    `json:"platform_user_id"`
	Currency       string    `json:"currency"`
	BudgetUnits    int64     `json:"budget_units"`
	ConsumedUnits  int64     `json:"consumed_units"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (l CanonicalWalletLease) RemainingUnits() int64 {
	remaining, err := SubUnits(l.BudgetUnits, l.ConsumedUnits)
	if err != nil {
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
	// GetCanonicalWalletLease resolves the user's CURRENT (most recently
	// issued, non-expired) lease — used when admitting a brand-new request.
	GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*CanonicalWalletLease, error)
	// GetCanonicalWalletLeaseByID resolves one specific lease regardless of
	// whether it is still the user's current lease — used to inspect a
	// lease an in-flight reservation was already anchored to.
	GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*CanonicalWalletLease, error)
	// ReserveCanonicalWalletLease reserves against the EXPLICIT leaseID the
	// caller supplies — a fresh request supplies the ID it just got from
	// GetCanonicalWalletLease; a retry supplies the ID it used the first
	// time (service.CanonicalWalletSettlementEvent.LeaseID), never "whatever
	// is current now".
	ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*CanonicalWalletReservation, error)
}

type CanonicalWalletSettlementEvent struct {
	EventID                string    `json:"event_id"`
	GatewayRequestID       string    `json:"gateway_request_id"`
	PlatformUserID         string    `json:"platform_user_id"`
	LeaseID                string    `json:"lease_id"`
	Currency               string    `json:"currency"`
	AmountUnits            int64     `json:"amount_units"`
	LocalBalanceAfterUnits *int64    `json:"local_balance_after_units,omitempty"`
	OccurredAt             time.Time `json:"occurred_at"`
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

// canonicalWalletLeaseWireResponse matches ShipAny's actual current
// response shape (still *_micros, unchanged by this phase) — kept separate
// from the internal CanonicalWalletLease type so the rest of this codebase
// can move to cny-e8-v1 *_units without silently breaking this one HTTP
// boundary. `consumed_micros` must be read from the wire too — a reinstall
// of an EXISTING, partially-consumed lease (e.g. after a Redis restart
// evicts the key) would otherwise default ConsumedUnits to 0, discarding
// real consumption history.
type canonicalWalletLeaseWireResponse struct {
	LeaseID        string    `json:"lease_id"`
	PlatformUserID string    `json:"platform_user_id"`
	Currency       string    `json:"currency"`
	BudgetMicros   int64     `json:"budget_micros"`
	ConsumedMicros int64     `json:"consumed_micros"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (c *canonicalWalletHTTPClient) AcquireLease(ctx context.Context, request canonicalWalletLeaseRequest) (*CanonicalWalletLease, error) {
	var wire canonicalWalletLeaseWireResponse
	windowSeconds := int64(c.cfg.LeaseTTLSeconds)
	if windowSeconds <= 0 {
		windowSeconds = 60
	}
	window := c.now().Unix() / windowSeconds
	leaseKeyRaw := fmt.Sprintf("v1|%s|%s|%d", strings.TrimSpace(request.PlatformUserID), NormalizeUserBillingCurrency(request.Currency), window)
	leaseKeyHash := sha256.Sum256([]byte(leaseKeyRaw))
	idempotencyKey := "gwlease_" + hex.EncodeToString(leaseKeyHash[:])
	if err := c.doJSON(ctx, http.MethodPost, "/api/internal/v1/wallet/leases/acquire", canonicalWalletLeaseScope, idempotencyKey, request, &wire); err != nil {
		return nil, err
	}
	if strings.TrimSpace(wire.LeaseID) == "" || strings.TrimSpace(wire.PlatformUserID) != strings.TrimSpace(request.PlatformUserID) || wire.BudgetMicros <= 0 || wire.ConsumedMicros < 0 || wire.ConsumedMicros > wire.BudgetMicros || wire.ExpiresAt.IsZero() {
		return nil, errors.New("control plane returned an invalid canonical wallet lease")
	}
	// Reject an unsupported currency outright on BOTH sides of the
	// comparison — coercing both with NormalizeUserBillingCurrency would
	// independently force invalid values to CNY and the two coerced values
	// would match, masking a real mismatch instead of catching it.
	currency, err := RequireCNYBillingCurrency(wire.Currency)
	if err != nil {
		return nil, fmt.Errorf("control plane returned an unsupported currency: %w", err)
	}
	if _, err := RequireCNYBillingCurrency(request.Currency); err != nil {
		return nil, fmt.Errorf("requested an unsupported currency: %w", err)
	}
	if currency != strings.ToUpper(strings.TrimSpace(request.Currency)) {
		return nil, ErrCanonicalWalletLeaseCurrencyMismatch
	}
	// ShipAny's route still speaks the OLD 1,000,000-per-CNY scale — convert
	// to cny-e8-v1 units (100,000,000 per CNY) at this one boundary.
	// Multiplying UP in scale is always exact (no precision loss going from
	// a coarser to a finer unit) — the lossy rounding boundary is the OTHER
	// direction, in ensureLease below.
	budgetUnits, err := MulUnits(wire.BudgetMicros, 100)
	if err != nil {
		return nil, fmt.Errorf("convert control plane lease budget to cny-e8-v1 units: %w", err)
	}
	consumedUnits, err := MulUnits(wire.ConsumedMicros, 100)
	if err != nil {
		return nil, fmt.Errorf("convert control plane lease consumption to cny-e8-v1 units: %w", err)
	}
	return &CanonicalWalletLease{
		LeaseID: wire.LeaseID, PlatformUserID: strings.TrimSpace(request.PlatformUserID), Currency: currency,
		BudgetUnits: budgetUnits, ConsumedUnits: consumedUnits, ExpiresAt: wire.ExpiresAt,
	}, nil
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
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled || event.AmountUnits <= 0 {
		return
	}
	event.PlatformUserID = strings.TrimSpace(event.PlatformUserID)
	if event.PlatformUserID == "" {
		canonicalWalletBridgeMetrics.missingPlatformID.Add(1)
		return
	}
	// Strict boundary: reject a non-CNY currency outright instead of
	// coercing it to CNY first (which would make the check below unable to
	// ever observe the invalid value).
	currency, err := RequireCNYBillingCurrency(event.Currency)
	if err != nil {
		canonicalWalletBridgeMetrics.unsupportedCurrency.Add(1)
		return
	}
	event.Currency = currency
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency)
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
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency)
	}
	lease, err := b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountUnits)
	if err == nil {
		_, err = b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, time.Now().UTC())
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
	lease, err := b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountUnits)
	if err != nil {
		canonicalWalletBridgeMetrics.leaseAcquireError.Add(1)
		slog.Warn("canonical wallet shadow lease unavailable", "event_id", event.EventID, "platform_user_id", event.PlatformUserID, "error", err)
		return
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, time.Now().UTC())
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
	attrs := []any{"event_id", event.EventID, "lease_id", event.LeaseID, "duplicate", result.Duplicate, "amount_units", event.AmountUnits}
	if result.CanonicalBalanceMicros != nil && event.LocalBalanceAfterUnits != nil {
		// result.CanonicalBalanceMicros is still ShipAny's wire-level micros
		// scale (renamed/converted in Task 4's SubmitSettlement fix); convert
		// x100 (exact) so both sides of the drift comparison are cny-e8-v1
		// units — comparing across the two scales directly would report a
		// spurious 100x "mismatch" on every settlement.
		canonicalBalanceUnits, convErr := MulUnits(*result.CanonicalBalanceMicros, 100)
		if convErr == nil {
			delta := canonicalBalanceUnits - *event.LocalBalanceAfterUnits
			if delta != 0 {
				canonicalWalletBridgeMetrics.balanceMismatch.Add(1)
				attrs = append(attrs, "balance_delta_units", delta, "canonical_balance_units", canonicalBalanceUnits, "local_balance_after_units", *event.LocalBalanceAfterUnits)
			}
		}
	}
	slog.Info("canonical wallet shadow settlement observed", attrs...)
}

func (b *CanonicalWalletBridge) ensureLease(ctx context.Context, platformUserID, currency string, amountUnits int64) (*CanonicalWalletLease, error) {
	if b.store == nil || b.control == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	lease, err := b.store.GetCanonicalWalletLease(ctx, platformUserID)
	if err == nil && lease != nil && lease.Currency == currency && lease.ExpiresAt.After(time.Now().UTC()) && lease.RemainingUnits() >= amountUnits {
		return lease, nil
	}
	// Interim until Task 5's config rename (LeaseBudgetMicros ->
	// LeaseBudgetUnits with a rescaled default): the configured budget is
	// still expressed in ShipAny's wire-level micros, so the comparison and
	// the ceiling conversion happen in micros here.
	requestedMicros := b.cfg.LeaseBudgetMicros
	// Ceiling division: cny-e8-v1 units -> ShipAny's coarser wire-level
	// micros scale (1 micro = 100 units). Rounding up means the lease
	// ACTUALLY requested is never smaller than amountUnits demanded —
	// at most 99 units (0.99 millionths of a CNY) of extra headroom is
	// requested, never a shortfall.
	amountMicros := (amountUnits + 99) / 100
	if amountMicros > requestedMicros {
		requestedMicros = amountMicros
	}
	lease, err = b.control.AcquireLease(ctx, canonicalWalletLeaseRequest{
		PlatformUserID: platformUserID, Currency: currency, RequestedMicros: requestedMicros, RequestedTTLSeconds: b.cfg.LeaseTTLSeconds,
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

func CanonicalWalletSettlementEventID(requestID, platformUserID, currency string) string {
	raw := fmt.Sprintf("v1|%s|%s|%s", strings.TrimSpace(requestID), strings.TrimSpace(platformUserID), NormalizeUserBillingCurrency(currency))
	sum := sha256.Sum256([]byte(raw))
	return "gwusg_" + hex.EncodeToString(sum[:])
}

// canonicalWalletUnitsFromCNY converts a CNY float64 to cny-e8-v1 units
// (1 CNY = 100,000,000 units — canonicalWalletUnitsPerCNY from
// canonical_wallet_units.go; do NOT redeclare it here). Rejects non-finite
// and negative inputs, and rejects any scaled value at or beyond int64's
// ceiling BEFORE converting — a bare int64(...) cast of an out-of-range
// float64 is implementation-defined behavior in Go, not a panic.
func canonicalWalletUnitsFromCNY(amount float64) (int64, error) {
	if amount < 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, errors.New("canonical wallet amount must be a finite, non-negative number")
	}
	scaled := amount * canonicalWalletUnitsPerCNY
	if scaled >= math.MaxInt64 {
		return 0, ErrCanonicalWalletUnitsOverflow
	}
	return int64(math.Round(scaled)), nil
}

// RequireCNYBillingCurrency is the STRICT counterpart to
// NormalizeUserBillingCurrency, exported so other packages (repository,
// same as CanonicalWalletLeaseStore's other cross-package uses) and other
// call sites in this package can reject an invalid currency outright
// instead of silently treating it as CNY. NormalizeUserBillingCurrency
// coerces ANY unrecognized value to CNY (confirmed by reading currency.go's
// normalizeBillingCurrencyOrDefault), so a check performed AFTER that
// coercion can never observe an invalid currency — every genuine
// admission/reservation/settlement boundary must reject BEFORE coercing.
func RequireCNYBillingCurrency(value string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	if normalized != CurrencyCNY {
		return "", fmt.Errorf("cny-e8-v1 requires CNY, got %q", value)
	}
	return normalized, nil
}

func observeCanonicalWalletSettlement(bridge *CanonicalWalletBridge, requestID string, user *User, cost *CostBreakdown, subscriptionBilling, billingApplied bool, billingResult *UsageBillingApplyResult) {
	if bridge == nil || user == nil || cost == nil || subscriptionBilling || !billingApplied || cost.ActualCost <= 0 {
		return
	}
	amountUnits, err := canonicalWalletUnitsFromCNY(cost.ActualCost)
	if err != nil || amountUnits <= 0 {
		return
	}
	localBalance := user.Balance - cost.ActualCost
	if billingResult != nil && billingResult.NewBalance != nil {
		localBalance = *billingResult.NewBalance
	}
	localBalanceAfterUnits, balanceErr := canonicalWalletUnitsFromCNY(localBalance)
	var localBalanceAfterPtr *int64
	if balanceErr == nil {
		localBalanceAfterPtr = &localBalanceAfterUnits
	}
	// Pass the raw BillingCurrency through and let ObserveSettlement's own
	// RequireCNYBillingCurrency be the single authoritative boundary —
	// coercing here first would force every value to "CNY" before that
	// check ever runs, so it could never reject a real non-CNY user.
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: requestID, PlatformUserID: user.PlatformUserID, Currency: user.BillingCurrency,
		AmountUnits: amountUnits, LocalBalanceAfterUnits: localBalanceAfterPtr, OccurredAt: time.Now().UTC(),
	})
}
