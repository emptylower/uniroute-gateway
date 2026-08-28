package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
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
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrCanonicalWalletLeaseMissing          = errors.New("canonical wallet lease is missing")
	ErrCanonicalWalletLeaseExpired          = errors.New("canonical wallet lease is expired")
	ErrCanonicalWalletLeaseExhausted        = errors.New("canonical wallet lease is exhausted")
	ErrCanonicalWalletLeaseCurrencyMismatch = errors.New("canonical wallet lease currency mismatch")
	ErrCanonicalWalletReservationConflict   = errors.New("canonical wallet event was reserved against a different lease")
	ErrCanonicalWalletOutboxClaimLost       = errors.New("canonical wallet outbox row is no longer claimed by this dispatcher")
	// ErrCanonicalWalletBalanceShortfall: the control plane granted a lease whose
	// remaining budget is below the amount being authorized now — both acquire
	// routes clamp to available balance by design. Rejected client-side and
	// unconditionally (spec §2.0.1 step (3)); 3.4a decides on the issuance row
	// whether the grant was clamped (terminal) or merely undersized (one advance).
	ErrCanonicalWalletBalanceShortfall = errors.New("canonical wallet lease granted below the amount being authorized")
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
	// AuthorizationToken / AuthorizationID (Phase 3.3): json:"-" — the outbox row
	// payload is unchanged in 3.3 (3.5 adds the column and the wire field; changing
	// the payload now would make a redelivered pre-3.3 row a payload conflict).
	AuthorizationToken string `json:"-"`
	AuthorizationID    string `json:"-"`
}

type CanonicalWalletSettlementResult struct {
	Accepted              bool   `json:"accepted"`
	Duplicate             bool   `json:"duplicate"`
	CanonicalBalanceUnits *int64 `json:"-"`
}

// CanonicalWalletOutboxEvent carries every field the outbox dispatcher needs
// to reconstruct the original settlement event for delivery — including
// GatewayRequestID/OccurredAt (SubmitSettlement requires them) and
// LocalBalanceAfterUnits (the drift-detection signal the pre-outbox inline
// path already had).
type CanonicalWalletOutboxEvent struct {
	ID                     int64
	EventID                string
	PlatformUserID         string
	LeaseID                string
	GatewayRequestID       string
	Currency               string
	AmountUnits            int64
	LocalBalanceAfterUnits *int64
	OccurredAt             time.Time
	AttemptCount           int
}

// CanonicalWalletOutboxStore is implemented by repository.WalletOutboxStore.
// Defined here (same pattern as CanonicalWalletLeaseStore) so the bridge can
// depend on the outbox without a service<->repository import cycle.
type CanonicalWalletOutboxStore interface {
	InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event CanonicalWalletSettlementEvent) error
	// Every claim/resolve method carries an opaque workerID claim token.
	// A dispatcher may only resolve rows it currently owns — see
	// WalletOutboxStore's implementations and runOutboxDispatcher below.
	ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]CanonicalWalletOutboxEvent, error)
	MarkOutboxEventDelivered(ctx context.Context, id int64, workerID string) error
	MarkOutboxEventFailed(ctx context.Context, id int64, workerID string, simulatedNow time.Time) error
	// BindOutboxEventLease durably records WHICH lease this event's
	// reservation is anchored to, and is called BEFORE the reservation is
	// attempted — never after. The Redis reservation marker is keyed by
	// (platform_user_id, event_id) and remembers the lease the reservation
	// landed on; a retry that reserves against "whatever lease is current
	// now" gets a cross-lease conflict on every attempt until the row
	// dead-letters. Binding first means no crash point can leave a
	// reservation whose lease the row does not know. Passing an empty
	// leaseID releases the binding — only legal when the reservation itself
	// proved no marker exists on that lease (see deliverOutboxEvent).
	// Ownership-guarded like every other resolve method: a row this worker
	// no longer owns returns ErrCanonicalWalletOutboxClaimLost.
	BindOutboxEventLease(ctx context.Context, id int64, workerID, leaseID string) error
	// ReclaimStaleInFlightEvents recovers rows a crashed dispatcher left
	// stuck in_flight.
	ReclaimStaleInFlightEvents(ctx context.Context, staleAfter time.Duration) (int64, error)
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

// canonicalWalletSettlementWireRequest matches ShipAny's actual current
// request shape for POST /api/internal/v1/wallet/settlements (still
// *_micros, unchanged by this phase) — kept separate from the internal
// CanonicalWalletSettlementEvent type for the same reason
// canonicalWalletLeaseWireResponse is kept separate from CanonicalWalletLease.
type canonicalWalletSettlementWireRequest struct {
	PlatformUserID          string `json:"platform_user_id"`
	EventID                 string `json:"event_id"`
	LeaseID                 string `json:"lease_id"`
	Currency                string `json:"currency"`
	AmountMicros            int64  `json:"amount_micros"`
	LocalBalanceAfterMicros *int64 `json:"local_balance_after_micros,omitempty"`
	GatewayRequestID        string `json:"gateway_request_id,omitempty"`
	OccurredAt              string `json:"occurred_at"`
}

// canonicalWalletSettlementWireResponse matches ShipAny's actual current
// response shape (still canonical_balance_micros, unchanged by this phase).
type canonicalWalletSettlementWireResponse struct {
	Accepted               bool   `json:"accepted"`
	Duplicate              bool   `json:"duplicate"`
	CanonicalBalanceMicros *int64 `json:"canonical_balance_micros,omitempty"`
}

func (c *canonicalWalletHTTPClient) SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	// Ceiling division, same direction and same reasoning as ensureLease's
	// requestedMicros conversion: rounding up means ShipAny is never told
	// to settle LESS than the lease actually reserved for this event, only
	// possibly up to 99 units (0.99 millionths of a CNY) more.
	amountMicros := (event.AmountUnits + 99) / 100
	var localBalanceAfterMicros *int64
	if event.LocalBalanceAfterUnits != nil {
		v := (*event.LocalBalanceAfterUnits + 99) / 100
		localBalanceAfterMicros = &v
	}
	wireRequest := canonicalWalletSettlementWireRequest{
		PlatformUserID: event.PlatformUserID, EventID: event.EventID, LeaseID: event.LeaseID, Currency: event.Currency,
		AmountMicros: amountMicros, LocalBalanceAfterMicros: localBalanceAfterMicros,
		GatewayRequestID: event.GatewayRequestID, OccurredAt: event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	var wireResponse canonicalWalletSettlementWireResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/internal/v1/wallet/settlements", canonicalWalletSettlementScope, event.EventID, wireRequest, &wireResponse); err != nil {
		return nil, err
	}
	if !wireResponse.Accepted && !wireResponse.Duplicate {
		return nil, errors.New("control plane rejected canonical wallet settlement")
	}
	result := &CanonicalWalletSettlementResult{Accepted: wireResponse.Accepted, Duplicate: wireResponse.Duplicate}
	if wireResponse.CanonicalBalanceMicros != nil {
		// Multiplying UP in scale (micros -> cny-e8-v1 units) is always
		// exact, the same non-lossy direction already established in
		// AcquireLease's wire conversion — the lossy rounding boundary is
		// only ever the OTHER direction (units -> the wire's coarser micros
		// scale, handled above with ceiling division).
		balanceUnits, err := MulUnits(*wireResponse.CanonicalBalanceMicros, 100)
		if err != nil {
			return nil, fmt.Errorf("convert control plane canonical balance to cny-e8-v1 units: %w", err)
		}
		result.CanonicalBalanceUnits = &balanceUnits
	}
	return result, nil
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
	queued                atomic.Int64
	queueDropped          atomic.Int64
	leaseAcquireOK        atomic.Int64
	leaseGrantBelowAmount atomic.Int64
	leaseGrantExpired     atomic.Int64
	leaseAcquireError     atomic.Int64
	leaseBindError        atomic.Int64
	reserveOK             atomic.Int64
	reserveError          atomic.Int64
	settlementOK          atomic.Int64
	settlementError       atomic.Int64
	balanceMismatch       atomic.Int64
	missingPlatformID     atomic.Int64
	unsupportedCurrency   atomic.Int64
}

var canonicalWalletBridgeMetrics canonicalWalletMetrics

func CanonicalWalletBridgeStats() map[string]int64 {
	m := &canonicalWalletBridgeMetrics
	return map[string]int64{
		"queued": m.queued.Load(), "queue_dropped": m.queueDropped.Load(),
		"lease_acquire_ok": m.leaseAcquireOK.Load(), "lease_acquire_error": m.leaseAcquireError.Load(),
		"lease_grant_below_amount": m.leaseGrantBelowAmount.Load(), "lease_grant_expired": m.leaseGrantExpired.Load(),
		"lease_bind_error": m.leaseBindError.Load(),
		"reserve_ok":       m.reserveOK.Load(), "reserve_error": m.reserveError.Load(),
		"settlement_ok": m.settlementOK.Load(), "settlement_error": m.settlementError.Load(),
		"balance_mismatch": m.balanceMismatch.Load(), "missing_platform_user_id": m.missingPlatformID.Load(),
		"unsupported_currency": m.unsupportedCurrency.Load(),
	}
}

type CanonicalWalletBridge struct {
	cfg      config.CanonicalWalletConfig
	store    CanonicalWalletLeaseStore
	control  canonicalWalletControlPlane
	outboxDB *sql.DB
	outbox   CanonicalWalletOutboxStore
	workerID string
	// observedForTest (Phase 3.3a): test-only hook invoked at the top of
	// ObserveSettlement so the token's arrival can be asserted in-process.
	observedForTest func(CanonicalWalletSettlementEvent)
}

func NewCanonicalWalletBridge(cfg *config.Config, store CanonicalWalletLeaseStore, outboxDB *sql.DB, outbox CanonicalWalletOutboxStore) *CanonicalWalletBridge {
	if cfg == nil || cfg.CanonicalWallet.Mode == "" || cfg.CanonicalWallet.Mode == config.CanonicalWalletModeDisabled {
		return nil
	}
	return newCanonicalWalletBridge(cfg.CanonicalWallet, store, newCanonicalWalletHTTPClient(cfg.CanonicalWallet, nil), outboxDB, outbox)
}

func newCanonicalWalletBridge(cfg config.CanonicalWalletConfig, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane, outboxDB *sql.DB, outbox CanonicalWalletOutboxStore) *CanonicalWalletBridge {
	b := &CanonicalWalletBridge{
		cfg:      cfg,
		store:    store,
		control:  control,
		outboxDB: outboxDB,
		outbox:   outbox,
		// This dispatcher instance's opaque claim token — generated once
		// per bridge, never per tick, so every row this instance claims is
		// resolvable only by this same instance.
		workerID: "sub2api-wallet-dispatcher-" + uuid.NewString(),
	}
	go b.runOutboxDispatcher()
	return b
}

// ObserveSettlement durably records the settlement event in the Postgres
// outbox (its own transaction, committed synchronously before returning) —
// replacing the previous in-memory bounded channel, which silently dropped
// events when full or on crash. Once this returns, the event WILL
// eventually be delivered (at-least-once, surviving a crash). It never
// returns an error to the existing billing path.
//
// Disclosed limit: a BeginTx/Insert/Commit failure here still loses the
// event — logged and counted in queueDropped, but not retried, because the
// durability store itself is what failed. Same class of gap the in-memory
// channel had, now bounded to "Postgres itself is down or the write
// genuinely failed" instead of "an ordinary burst filled a fixed buffer."
func (b *CanonicalWalletBridge) ObserveSettlement(event CanonicalWalletSettlementEvent) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled || event.AmountUnits <= 0 {
		return
	}
	if b.observedForTest != nil {
		b.observedForTest(event)
	}
	if b.outbox == nil || b.outboxDB == nil {
		// Several of this file's own tests construct a bridge with a nil
		// outbox because they don't exercise ObserveSettlement — guard BOTH
		// fields here, since the very next statement dereferences outboxDB.
		return
	}
	event.PlatformUserID = strings.TrimSpace(event.PlatformUserID)
	if event.PlatformUserID == "" {
		canonicalWalletBridgeMetrics.missingPlatformID.Add(1)
		return
	}
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
	// Phase 3.3a: in shadow mode the bridge logs the token with the event id
	// when the settlement is enqueued — the durable join between a wallet
	// event and the write that produced it (debug-level, token non-empty only).
	if event.AuthorizationToken != "" {
		logger.LegacyPrintf("service.authorization", "shadow: settlement %s carries authorization token %s (authorization %s)", event.EventID, event.AuthorizationToken, event.AuthorizationID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox begin failed", "event_id", event.EventID, "error", err)
		return
	}
	if err := b.outbox.InsertOutboxEventTx(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox insert failed", "event_id", event.EventID, "error", err)
		return
	}
	if err := tx.Commit(); err != nil {
		canonicalWalletBridgeMetrics.queueDropped.Add(1)
		slog.Warn("canonical wallet outbox commit failed", "event_id", event.EventID, "error", err)
		return
	}
	canonicalWalletBridgeMetrics.queued.Add(1)
}

// CheckAndReserve is the real request-preflight state machine. Shadow mode
// observes a denial but always allows the request; enforce mode fails closed.
// A retry carrying an explicit event.LeaseID resolves against THAT lease —
// never silently against whatever lease is current now.
func (b *CanonicalWalletBridge) CheckAndReserve(ctx context.Context, event CanonicalWalletSettlementEvent) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	if event.EventID == "" {
		event.EventID = CanonicalWalletSettlementEventID(event.GatewayRequestID, event.PlatformUserID, event.Currency)
	}
	var lease *CanonicalWalletLease
	var err error
	if strings.TrimSpace(event.LeaseID) != "" {
		// A retry supplies the exact lease id it was reserved against the
		// first time — resolve THAT lease specifically.
		lease, err = b.store.GetCanonicalWalletLeaseByID(ctx, event.PlatformUserID, event.LeaseID)
	} else {
		lease, err = b.ensureLease(ctx, event.PlatformUserID, event.Currency, event.AmountUnits)
	}
	if err != nil {
		if b.cfg.Mode == config.CanonicalWalletModeShadow {
			return true, nil
		}
		return false, err
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, time.Now().UTC())
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	event.LeaseID = reservation.Lease.LeaseID
	return true, nil
}

// EnsureCanonicalWalletHeadroom atomically extends an already-admitted
// request's reservation by additionalUnits — used when a streaming
// response's actual cost is running ahead of the original pre-authorized
// estimate. Returns false (never an error for the ordinary insufficient-
// budget case) when the lease cannot cover the extension, so the caller's
// only correct response is to stop generation, not retry.
//
// Each distinct top-up attempt gets its own identity via the caller-supplied
// incrementing topUpSequence (a FIXED per-request event id would make a
// second, larger top-up a silent duplicate of the first); a genuine retry
// (same sequence, same amount) still hits the duplicate path. The caller
// must pass the explicit leaseID it received from its own CheckAndReserve
// call — resolving "whatever is current" would anchor the extension to the
// wrong lease after a renewal. No production caller yet (Phase 3's in-stream
// overrun work).
func (b *CanonicalWalletBridge) EnsureCanonicalWalletHeadroom(ctx context.Context, gatewayRequestID, platformUserID, leaseID, currency string, topUpSequence int, additionalUnits int64) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	topUpEventID := CanonicalWalletSettlementEventID(
		fmt.Sprintf("%s:topup:%d", gatewayRequestID, topUpSequence), platformUserID, currency,
	)
	_, err := b.store.ReserveCanonicalWalletLease(ctx, platformUserID, leaseID, currency, topUpEventID, additionalUnits, time.Now().UTC())
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if errors.Is(err, ErrCanonicalWalletLeaseExhausted) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Mode reports the bridge's configured mode — exported so callers outside
// this package's own methods (billing_cache_service.go, same `service`
// package but a different file) have a stable accessor rather than reaching
// into the unexported cfg field directly.
func (b *CanonicalWalletBridge) Mode() string {
	if b == nil {
		return config.CanonicalWalletModeDisabled
	}
	return b.cfg.Mode
}

// HasCanonicalWalletHeadroom is a READ-ONLY admission gate — it does not
// reserve anything and does not touch the Redis reservation script at all.
// It deliberately does NOT call ensureLease: ensureLease can synchronously
// call ShipAny's AcquireLease and INSTALL a brand-new lease when none
// exists or the current one is exhausted, and ShipAny's issueShadowLease
// only reads available credits and inserts a lease row (it does not
// atomically transfer credits into a leased ledger) — so repeatedly calling
// such a check against an exhausted lease could mint overlapping shadow
// budgets across concurrent requests. A pure GetCanonicalWalletLease read
// never writes anything. If no lease has ever been issued for this user
// (or Redis evicted it), there is no data to check and this method fails
// CLOSED in enforce mode rather than fabricating one — pre-warming leases
// at purchase/recharge/key-creation is later-phase work.
func (b *CanonicalWalletBridge) HasCanonicalWalletHeadroom(ctx context.Context, platformUserID, currency string) (bool, error) {
	if b == nil || b.cfg.Mode == config.CanonicalWalletModeDisabled {
		return true, nil
	}
	lease, err := b.store.GetCanonicalWalletLease(ctx, platformUserID)
	if b.cfg.Mode == config.CanonicalWalletModeShadow {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if lease.Currency != currency || !lease.ExpiresAt.After(time.Now().UTC()) {
		return false, nil
	}
	return lease.RemainingUnits() > 0, nil
}

// runOutboxDispatcher polls for durably-persisted pending events instead of
// draining an in-memory channel, so a crash mid-delivery leaves the event
// claimable again on restart instead of gone.
// walletOutboxDispatchBatch bounds how many events one tick claims. It is
// deliberately small: staleAfter (below) must provably exceed the worst-case
// time the LAST event in a batch waits behind its predecessors, and that
// worst case grows linearly with this number.
const walletOutboxDispatchBatch = 10

func (b *CanonicalWalletBridge) runOutboxDispatcher() {
	if b.outbox == nil || b.outboxDB == nil {
		return // several of this file's own tests construct a bridge with a nil outbox (they don't exercise ObserveSettlement) — starting the ticker loop unconditionally would eventually dereference it. "Start only when BOTH are non-nil" is enforced in this one place.
	}
	perAttempt := time.Duration(b.cfg.RequestTimeoutMS) * time.Millisecond
	// staleAfter must exceed the worst case for the LAST event in a batch:
	// it waits behind (batch-1) predecessors, each bounded by perAttempt,
	// then takes up to perAttempt itself — i.e. batch*perAttempt — plus
	// margin for the claim/reclaim round-trips themselves. 2x that bound is
	// the margin used here.
	staleAfter := 2 * time.Duration(walletOutboxDispatchBatch) * perAttempt
	ticker := time.NewTicker(perAttempt)
	defer ticker.Stop()
	for range ticker.C {
		// Reclaim stale in_flight rows before claiming fresh pending ones,
		// so a row a crashed dispatcher instance abandoned mid-delivery
		// becomes eligible for ClaimPendingOutboxEvents again in this same
		// tick rather than staying stuck until some future tick happens to
		// run this first.
		reclaimCtx, cancelReclaim := context.WithTimeout(context.Background(), perAttempt)
		if reclaimed, err := b.outbox.ReclaimStaleInFlightEvents(reclaimCtx, staleAfter); err != nil {
			slog.Warn("canonical wallet outbox stale-claim reclaim failed", "error", err)
		} else if reclaimed > 0 {
			slog.Warn("canonical wallet outbox reclaimed stale in_flight events", "count", reclaimed)
		}
		cancelReclaim()

		claimCtx, cancelClaim := context.WithTimeout(context.Background(), perAttempt)
		events, err := b.outbox.ClaimPendingOutboxEvents(claimCtx, b.workerID, walletOutboxDispatchBatch)
		cancelClaim()
		if err != nil {
			slog.Warn("canonical wallet outbox claim failed", "error", err)
			continue
		}
		for _, e := range events {
			// One fresh context PER EVENT — a slow delivery must not consume
			// the time budget of the events queued behind it.
			eventCtx, cancelEvent := context.WithTimeout(context.Background(), perAttempt)
			b.deliverOutboxEvent(eventCtx, e)
			cancelEvent()
		}
	}
}

func (b *CanonicalWalletBridge) deliverOutboxEvent(ctx context.Context, e CanonicalWalletOutboxEvent) {
	event := CanonicalWalletSettlementEvent{
		EventID: e.EventID, GatewayRequestID: e.GatewayRequestID, PlatformUserID: e.PlatformUserID,
		LeaseID: e.LeaseID, Currency: e.Currency, AmountUnits: e.AmountUnits,
		LocalBalanceAfterUnits: e.LocalBalanceAfterUnits, OccurredAt: e.OccurredAt,
	}
	lease, err := b.resolveOutboxEventLease(ctx, e)
	if err != nil {
		canonicalWalletBridgeMetrics.leaseAcquireError.Add(1)
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, time.Now().UTC())
		return
	}
	// Bind BEFORE reserving, never after. Binding afterwards leaves a crash
	// window in which a reservation exists in Redis but the row does not know
	// its lease — the retry then reserves against a different lease and
	// conflicts forever.
	if lease.LeaseID != e.LeaseID {
		if err := b.outbox.BindOutboxEventLease(ctx, e.ID, b.workerID, lease.LeaseID); err != nil {
			if errors.Is(err, ErrCanonicalWalletOutboxClaimLost) {
				// Another dispatcher owns this row now and will deliver it.
				// Not this call's outcome to report — do NOT mark it failed,
				// that would burn an attempt against the other worker's claim.
				return
			}
			canonicalWalletBridgeMetrics.leaseBindError.Add(1)
			_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, time.Now().UTC())
			return
		}
	}
	reservation, err := b.store.ReserveCanonicalWalletLease(ctx, event.PlatformUserID, lease.LeaseID, event.Currency, event.EventID, event.AmountUnits, time.Now().UTC())
	if err != nil {
		// A bound lease that has since gone missing, expired, or run out can
		// never accept this event. The Lua script checks the event's
		// reservation marker BEFORE it checks the budget, so any of these
		// three outcomes proves no marker for this event exists on that lease
		// — releasing the binding is safe, and lets the next attempt acquire
		// a fresh lease instead of retrying the same dead one until
		// dead-letter. A cross-lease CONFLICT is deliberately excluded: there
		// the marker does exist and points elsewhere, so the binding must
		// stand.
		if canonicalWalletLeaseBindingIsStale(err) {
			_ = b.outbox.BindOutboxEventLease(ctx, e.ID, b.workerID, "")
		}
		canonicalWalletBridgeMetrics.reserveError.Add(1)
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, time.Now().UTC())
		return
	}
	canonicalWalletBridgeMetrics.reserveOK.Add(1)
	event.LeaseID = reservation.Lease.LeaseID
	result, err := b.control.SubmitSettlement(ctx, event)
	if err != nil {
		canonicalWalletBridgeMetrics.settlementError.Add(1)
		_ = b.outbox.MarkOutboxEventFailed(ctx, e.ID, b.workerID, time.Now().UTC())
		return
	}
	canonicalWalletBridgeMetrics.settlementOK.Add(1)
	// Balance-mismatch (drift) detection, restored from the pre-outbox
	// inline path — now comparing in the internal *Units scale on both
	// sides (SubmitSettlement converts the wire response's
	// canonical_balance_micros to CanonicalBalanceUnits before returning).
	attrs := []any{"event_id", event.EventID, "lease_id", event.LeaseID, "duplicate", result.Duplicate, "amount_units", event.AmountUnits}
	if result.CanonicalBalanceUnits != nil && event.LocalBalanceAfterUnits != nil {
		delta := *result.CanonicalBalanceUnits - *event.LocalBalanceAfterUnits
		if delta != 0 {
			canonicalWalletBridgeMetrics.balanceMismatch.Add(1)
			attrs = append(attrs, "balance_delta_units", delta, "canonical_balance_units", *result.CanonicalBalanceUnits, "local_balance_after_units", *event.LocalBalanceAfterUnits)
		}
	}
	slog.Info("canonical wallet settlement delivered", attrs...)
	_ = b.outbox.MarkOutboxEventDelivered(ctx, e.ID, b.workerID)
}

// canonicalWalletLeaseBindingIsStale reports whether a reservation failure
// against an ALREADY-BOUND lease proves that binding can never succeed, so it
// must be released. See the call site for why conflict is excluded.
func canonicalWalletLeaseBindingIsStale(err error) bool {
	return errors.Is(err, ErrCanonicalWalletLeaseMissing) ||
		errors.Is(err, ErrCanonicalWalletLeaseExpired) ||
		errors.Is(err, ErrCanonicalWalletLeaseExhausted)
}

// resolveOutboxEventLease returns the lease this event's reservation must be
// anchored to: the lease already bound to the row if it is still usable,
// otherwise a freshly acquired one.
//
// Falling back to a fresh lease when the bound one is gone or expired is
// safe, and this is the reason: the reservation marker is written with
// `SET KEYS[2] <lease_id>` followed by `PEXPIREAT KEYS[2] expires_at` — the
// SAME absolute deadline the lease hash itself carries. Both keys expire
// against one clock, Redis's, so a dead lease implies a dead marker and
// there is nothing left for a fresh lease's reservation to collide with.
//
// This was not always true, and the history is worth keeping: the marker
// used to get a RELATIVE `PX expires_at - gateway_now`, which Redis applied
// at its own execution instant. That shifted the marker's deadline by
// `redis_now - gateway_now` — round-trip latency plus clock offset — so a
// marker COULD outlive its lease, and did whenever that shift was positive,
// which is the ordinary case. Inside such a window this fallback rebound the
// event to a fresh lease while the old marker still pointed at the previous
// one, so every retry hit a cross-lease conflict until the marker finally
// expired. Retries recovered on their own after that; the row dead-lettered
// only when the attempt budget ran out first — which is exactly what made it
// a money bug rather than a delay. Do not reintroduce a relative TTL here.
func (b *CanonicalWalletBridge) resolveOutboxEventLease(ctx context.Context, e CanonicalWalletOutboxEvent) (*CanonicalWalletLease, error) {
	if e.LeaseID == "" {
		return b.ensureLease(ctx, e.PlatformUserID, e.Currency, e.AmountUnits)
	}
	if b.store == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, e.PlatformUserID, e.LeaseID)
	if err != nil && !errors.Is(err, ErrCanonicalWalletLeaseMissing) {
		// A real transport failure — do NOT silently fall through to a new
		// lease, or a Redis blip would rebind an event that still has a live
		// marker somewhere.
		return nil, err
	}
	if err == nil && lease != nil && lease.Currency == e.Currency && lease.ExpiresAt.After(time.Now().UTC()) {
		return lease, nil
	}
	return b.ensureLease(ctx, e.PlatformUserID, e.Currency, e.AmountUnits)
}

func (b *CanonicalWalletBridge) ensureLease(ctx context.Context, platformUserID, currency string, amountUnits int64) (*CanonicalWalletLease, error) {
	if b.store == nil || b.control == nil {
		return nil, errors.New("canonical wallet bridge dependencies unavailable")
	}
	lease, err := b.store.GetCanonicalWalletLease(ctx, platformUserID)
	if err == nil && lease != nil && lease.Currency == currency && lease.ExpiresAt.After(time.Now().UTC()) && lease.RemainingUnits() >= amountUnits {
		return lease, nil
	}
	// The configured budget is cny-e8-v1 units; convert to ShipAny's
	// wire-level micros scale with ceiling division at this one boundary —
	// rounding up means the lease ACTUALLY requested is never smaller than
	// requestedUnits demanded: at most 99 units (0.99 millionths of a CNY)
	// of extra headroom is requested, never a shortfall.
	requestedUnits := b.cfg.LeaseBudgetUnits
	if amountUnits > requestedUnits {
		requestedUnits = amountUnits
	}
	requestedMicros := (requestedUnits + 99) / 100
	lease, err = b.control.AcquireLease(ctx, canonicalWalletLeaseRequest{
		PlatformUserID: platformUserID, Currency: currency, RequestedMicros: requestedMicros, RequestedTTLSeconds: b.cfg.LeaseTTLSeconds,
	})
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, ErrCanonicalWalletLeaseMissing // a (nil, nil) grant was a latent nil deref one line later; name it
	}
	now := time.Now().UTC()
	if !lease.ExpiresAt.After(now) {
		canonicalWalletBridgeMetrics.leaseGrantExpired.Add(1)
		return nil, fmt.Errorf("%w: lease %s expired at %s", ErrCanonicalWalletLeaseExpired, lease.LeaseID, lease.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	if lease.RemainingUnits() < amountUnits {
		canonicalWalletBridgeMetrics.leaseGrantBelowAmount.Add(1)
		// Dispatcher consequence: on the outbox path (resolveOutboxEventLease)
		// this error fails the delivery attempt like any other ensureLease
		// error today — retried on the backoff and moved to dead_letter after
		// walletOutboxMaxAttempts. The terminal balance_shortfall dead-letter
		// classification on the first occurrence is 3.4a's (it needs the
		// issuance row to tell clamped from undersized).
		return nil, fmt.Errorf("%w: granted %d units, %d required", ErrCanonicalWalletBalanceShortfall, lease.RemainingUnits(), amountUnits)
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

func observeCanonicalWalletSettlement(bridge *CanonicalWalletBridge, requestID string, user *User, cost *CostBreakdown, subscriptionBilling, billingApplied bool, billingResult *UsageBillingApplyResult, authorizationToken, authorizationID string) {
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
		AuthorizationToken: authorizationToken, AuthorizationID: authorizationID,
	})
}
