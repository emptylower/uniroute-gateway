//go:build integration && e2e

// Phase 3.8-G (redesign §14.1): the activation suite. The real Sub2API
// bridge, authorizer, dispatcher, ws_v2 harness and the 3.7b Live fixture
// against the REAL ShipAny (Shape A: `pnpm dev` on sqlite, started per stage
// by 3.8-S's scripts/wallet-e2e.sh with WALLET_E2E_STAGE set) — the §3 fake
// is not used anywhere in this suite. Seeding and inspection go through
// ShipAny's own scripts via os/exec; this suite never edits ShipAny's files
// or database directly. Every ShipAny-side assertion logs the row ids it
// read, so the completion record is reproducible against the seed.
//
// THE THROWAWAY-REDIS RULE (redesign §14.2, a rule — not a note): tests 61
// and 63 start their own Redis with tcredis.Run directly, NEVER through
// SharedTestRedisClientForTest. The shared instance is process-lifetime
// under the one-live-claim guard (3.7c); stopping it mid-suite breaks every
// later test. Do not "helpfully" consolidate the two onto the shared
// instance — the outage/data-loss tests MUST be able to stop and flush their
// own container without poisoning the run.
//
// The suite skips cleanly (one line) when WALLET_E2E_CONTROL_PLANE_URL or
// WALLET_E2E_SECRET is unset, so the ordinary -tags=integration sweep never
// sees it (the e2e tag guards the file).
package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// ---------------------------------------------------------------------------
// The environment
// ---------------------------------------------------------------------------

// e2eWalletEnv reads the two required variables and skips the suite with a
// one-line reason when they are unset (the ordinary integration sweep runs
// with them unset by construction).
func e2eWalletEnv(t *testing.T) (url, secret string) {
	t.Helper()
	url = strings.TrimSpace(os.Getenv("WALLET_E2E_CONTROL_PLANE_URL"))
	secret = strings.TrimSpace(os.Getenv("WALLET_E2E_SECRET"))
	if url == "" || secret == "" {
		t.Skip("Phase 3.8 activation suite: WALLET_E2E_CONTROL_PLANE_URL and WALLET_E2E_SECRET are unset (the real ShipAny disposable environment is not running)")
	}
	return url, secret
}

// e2eStage returns the driver's stage (routes_off | routes_on); empty when
// run by hand without it.
func e2eStage() string { return strings.TrimSpace(os.Getenv("WALLET_E2E_STAGE")) }

// e2eRequireStage skips unless the driver is running the named stage.
func e2eRequireStage(t *testing.T, want string) {
	t.Helper()
	if got := e2eStage(); got != want {
		t.Skipf("Phase 3.8: this half runs in the %s stage (WALLET_E2E_STAGE=%q)", want, got)
	}
	e2eWalletEnv(t)
}

// e2eUserID mints a fresh ShipAny platform user per test — no cross-test
// state (3.8-S's seed script is idempotent per user, and the fresh-user rule
// makes it irrelevant).
func e2eUserID(test string) string {
	return "wallet-e2e-" + test + "-" + uuid.NewString()
}

// ---------------------------------------------------------------------------
// ShipAny's own scripts — seed and inspect through os/exec (round-1 ruling
// (e)): 60 s CommandContext, cmd.Dir = WALLET_E2E_SHIPANY_DIR, stdout and
// stderr captured SEPARATELY, non-zero exit fatal with the full command
// line, the exit code and stderr verbatim. A missing pnpm in an environment
// that set WALLET_E2E_* is a setup error, not an absent environment — it
// FAILS, never skips.
// ---------------------------------------------------------------------------

// e2eShipAnyDir returns the ShipAny checkout (for scripts/with-env.ts and
// friends), failing when unset.
func e2eShipAnyDir(t *testing.T) string {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv("WALLET_E2E_SHIPANY_DIR"))
	if dir == "" {
		t.Fatal("Phase 3.8: WALLET_E2E_SHIPANY_DIR is unset — the suite seeds and inspects ShipAny through its own scripts")
	}
	if _, err := os.Stat(filepath.Join(dir, "scripts", "with-env.ts")); err != nil {
		t.Fatalf("Phase 3.8: %s/scripts/with-env.ts not readable: %v", dir, err)
	}
	return dir
}

// e2eShipAnyScriptEnv builds the child environment. The seed/inspect
// children MUST read the SAME database the stage's dev server serves: the
// driver writes .env.e2e.<stage> before starting this suite and deletes it
// after, and with-env.ts would otherwise fall back to .env.development
// (data/local.db — the wrong database entirely).
func e2eShipAnyScriptEnv(t *testing.T, dir string) []string {
	t.Helper()
	env := os.Environ()
	name := "ENV_FILE"
	if _, ok := os.LookupEnv("ENV_FILE"); ok {
		return env
	}
	stage := e2eStage()
	file := ".env.e2e." + stage
	if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
		t.Fatalf("Phase 3.8: neither ENV_FILE is set nor %s exists in %s — the seed/inspect children would read the wrong database; the driver writes the stage env file, a by-hand run must create it (or export ENV_FILE)", file, dir)
	}
	return append(env, name+"="+file)
}

// e2eRunShipAny runs one of ShipAny's own package scripts (wallet:e2e:seed
// / wallet:e2e:inspect — both wrap scripts/with-env.ts, so ENV_FILE still
// selects the stage database) and returns its stdout.
func e2eRunShipAny(t *testing.T, script string, args ...string) string {
	t.Helper()
	dir := e2eShipAnyDir(t)
	if _, err := exec.LookPath("pnpm"); err != nil {
		t.Fatalf("Phase 3.8: pnpm is not on PATH — seeding/inspection goes through ShipAny's own tsx scripts (%v)", err)
	}
	argv := append([]string{script}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pnpm", argv...)
	cmd.Dir = dir
	cmd.Env = e2eShipAnyScriptEnv(t, dir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Phase 3.8: `pnpm %s` (dir %s) failed: %v\n--- stderr ---\n%s", strings.Join(argv, " "), dir, err, stderr.String())
	}
	return stdout.String()
}

// e2eLastJSONObject scans stdout from the end for the first line that is a
// JSON object — with-env.ts prints its own banner lines to stdout before the
// child runs, so the contract is "the LAST JSON line on stdout", with real
// diagnostics still routed to stderr only.
func e2eLastJSONObject(t *testing.T, stdout, origin string) []byte {
	t.Helper()
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
			return []byte(trimmed)
		}
	}
	t.Fatalf("Phase 3.8: %s printed no JSON object line on stdout (with-env banner only?): %q", origin, stdout)
	return nil
}

// The 3.8-S inspect contract (schema 1), field for field — decimal unit
// strings on the wire, parsed to int64 here.
type shipanyLeaseView struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	BudgetUnits   string `json:"budget_units"`
	CapturedUnits string `json:"captured_units"`
	ReleasedUnits string `json:"released_units"`
	ExpiresAt     string `json:"expires_at"`
}

type shipanyCloseView struct {
	LeaseID         string  `json:"lease_id"`
	Reason          string  `json:"reason"`
	ReleasedUnits   string  `json:"released_units"`
	CreditedCredits int64   `json:"credited_credits"`
	RemainderUnits  string  `json:"remainder_units"`
	WriteOffUnits   string  `json:"write_off_units"`
	WriteOffReason  *string `json:"write_off_reason"`
	ClosedAt        string  `json:"closed_at"`
}

type shipanyCreditView struct {
	ID               string `json:"id"`
	Status           string `json:"status"`
	RemainingCredits int64  `json:"remaining_credits"`
}

type shipanyState struct {
	Schema  int                 `json:"schema"`
	User    string              `json:"user"`
	Balance int64               `json:"balance"`
	Leases  []shipanyLeaseView  `json:"leases"`
	Closes  []shipanyCloseView  `json:"closes"`
	Credits []shipanyCreditView `json:"credits"`
}

func parseShipanyUnits(t *testing.T, field, raw string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, "ShipAny %s %q is not an int64", field, raw)
	return v
}

// budgetUnits/capturedUnits/releasedUnits parse the decimal strings once.
func (l shipanyLeaseView) budgetUnits(t *testing.T) int64 {
	return parseShipanyUnits(t, "budget_units", l.BudgetUnits)
}
func (l shipanyLeaseView) capturedUnits(t *testing.T) int64 {
	return parseShipanyUnits(t, "captured_units", l.CapturedUnits)
}
func (l shipanyLeaseView) releasedUnits(t *testing.T) int64 {
	return parseShipanyUnits(t, "released_units", l.ReleasedUnits)
}

func (s shipanyState) leaseByID(t *testing.T, id string) shipanyLeaseView {
	t.Helper()
	for _, l := range s.Leases {
		if l.ID == id {
			return l
		}
	}
	t.Fatalf("ShipAny lease %s not found among %v", id, s.leaseIDs())
	return shipanyLeaseView{}
}

func (s shipanyState) leaseIDs() []string {
	ids := make([]string, 0, len(s.Leases))
	for _, l := range s.Leases {
		ids = append(ids, l.ID)
	}
	return ids
}

func (s shipanyState) creditIDs() []string {
	ids := make([]string, 0, len(s.Credits))
	for _, c := range s.Credits {
		ids = append(ids, c.ID)
	}
	return ids
}

// e2eSeed grants N credits to a platform user through ShipAny's own seed
// script (idempotent per user — 3.8-S Task 1). The plan's literal argv ran
// the script through bare `tsx`, which cannot load the `cloudflare:` shim
// the script's import graph needs — ShipAny's own package script
// (`wallet:e2e:seed`) wraps it with the loader (deviation: recorded).
func e2eSeed(t *testing.T, user string, credits int64) {
	t.Helper()
	stdout := e2eRunShipAny(t, "wallet:e2e:seed", "--", "--user", user, "--credits", strconv.FormatInt(credits, 10))
	var out struct {
		Schema  int    `json:"schema"`
		User    string `json:"user"`
		Credits int64  `json:"credits"`
	}
	require.NoError(t, json.Unmarshal(e2eLastJSONObject(t, stdout, "wallet-e2e-seed"), &out))
	require.Equal(t, 1, out.Schema, "the seed script's schema contract is 1 (3.8-S Task 1); got %d — the two plans version their wire by this field", out.Schema)
	require.Equal(t, user, out.User)
	require.Equal(t, credits, out.Credits)
	t.Logf("[shipany] seeded user=%s credits=%d", user, credits)
}

// e2eInspect reads one platform user's wallet state through ShipAny's own
// inspection script.
func e2eInspect(t *testing.T, user string) shipanyState {
	t.Helper()
	stdout := e2eRunShipAny(t, "wallet:e2e:inspect", "--", "--user", user)
	var state shipanyState
	require.NoError(t, json.Unmarshal(e2eLastJSONObject(t, stdout, "wallet-e2e-inspect"), &state))
	require.Equal(t, 1, state.Schema, "the inspect script's schema contract is 1 (3.8-S Task 1); got %d", state.Schema)
	require.Equal(t, user, state.User)
	return state
}

// e2eLogState records the row ids an assertion read — the record's
// reproducibility contract (redesign §14.4).
func e2eLogState(t *testing.T, state shipanyState) {
	t.Helper()
	t.Logf("[shipany] user=%s balance=%d credit_ids=%v lease_ids=%v closes=%d",
		state.User, state.Balance, state.creditIDs(), state.leaseIDs(), len(state.Closes))
}

// ---------------------------------------------------------------------------
// The bridge: p34bDispatcherBridge's construction with the control plane
// pointed at the REAL ShipAny (3.7c's shared containers, closed at cleanup).
// ---------------------------------------------------------------------------

// e2eWalletConfig is the Phase-34 test config with ShipAny's assertion
// identity (the v2 defaults 3.8-S ships: sub2api-gateway /
// shipany-control-plane / v1), a 300 ms request timeout and the 500 CNY-cent
// lease budget every Phase-34 fixture uses.
func e2eWalletConfig(t *testing.T, mode, holds string) config.CanonicalWalletConfig {
	t.Helper()
	cfg := p34bHoldsConfig(mode)
	cfg.Holds = holds
	cfg.Issuer = "sub2api-gateway"
	cfg.Audience = "shipany-control-plane"
	cfg.Version = "v1"
	cfg.ControlPlaneURL, cfg.Secret = e2eWalletEnv(t)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 100, 300
	cfg.LeaseBudgetUnits = 500_000_000
	// The gateway REQUESTS the ttl; ShipAny honors it clamped to [30, 3600]
	// (lease-ensure.ts:770-776). The driver's routes_on stage runs ShipAny at
	// its minimum 30 — test 55 requests the same through WALLET_E2E_LEASE_TTL.
	if raw := strings.TrimSpace(os.Getenv("WALLET_E2E_LEASE_TTL")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 30 || v > 3600 {
			panic(fmt.Sprintf("WALLET_E2E_LEASE_TTL must be seconds in [30, 3600], got %q", raw))
		}
		cfg.LeaseTTLSeconds = v
	}
	return cfg
}

// e2eBridge builds a REAL bridge (store on the shared Redis, dispatcher and
// reaper on the shared Postgres outbox) against the real ShipAny.
func e2eBridge(t *testing.T, mode, holds string) (*CanonicalWalletBridge, *sql.DB, *redis.Client) {
	t.Helper()
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	cfg := e2eWalletConfig(t, mode, holds)
	client := newCanonicalWalletHTTPClient(cfg, nil)
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)
	t.Cleanup(b.Close)
	return b, db, rdb
}

// e2eBridgeOn builds a further bridge over stores an earlier e2eBridge call
// in the SAME test already claimed (one shared-Redis claim per test — 3.7c's
// guard; a second claim mid-test is fatal by design).
func e2eBridgeOn(t *testing.T, mode, holds string, rdb *redis.Client, db *sql.DB) *CanonicalWalletBridge {
	t.Helper()
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}
	cfg := e2eWalletConfig(t, mode, holds)
	client := newCanonicalWalletHTTPClient(cfg, nil)
	b := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)
	t.Cleanup(b.Close)
	return b
}

// e2eCatalogPricing loads the deployment's real pricing catalog (the same
// fallback file Initialize() loads) so Freeze prices arbitrary models with
// their real maxima — the nil-pricing fixture would fall back to the
// hand-written table and bound the wrong numbers.
func e2eCatalogPricing(t *testing.T) *PricingService {
	t.Helper()
	svc := NewPricingService(&config.Config{}, nil)
	require.NoError(t, svc.loadPricingData(filepath.Join("..", "..", "resources", "model-pricing", "model_prices_and_context_window.json")))
	return svc
}

// e2eSnapshotFixture is NewSnapshotTestFixtureForTest's shape with the real
// catalog behind the billing service (see e2eCatalogPricing).
func e2eSnapshotFixture(t *testing.T) (*BillingSnapshotService, *APIKey, *User, *Account, *BillingService, *ModelPricingResolver, *ExchangeRateService) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	billing := NewBillingService(&config.Config{}, e2eCatalogPricing(t))
	cs := NewChannelService(&LiveRestartChannelRepoStub{}, nil, nil, nil)
	resolver := NewModelPricingResolver(cs, billing)
	fx := NewExchangeRateService(cfg)
	svc := NewBillingSnapshotService(cfg, resolver, billing, fx, nil)
	svc.now = func() time.Time { return time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC) }
	group := &Group{ID: 7, RateMultiplier: 1.5, ImageRateIndependent: true, ImageRateMultiplier: 2.5}
	gid := int64(7)
	user := &User{ID: 42, BillingCurrency: "CNY"}
	apiKey := &APIKey{ID: 11, GroupID: &gid, Group: group, User: user}
	rate := 1.25
	account := &Account{
		ID:             99,
		RateMultiplier: &rate,
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Concurrency:    2,
		Credentials:    map[string]any{"access_token": "test-token"},
	}
	return svc, apiKey, user, account, billing, resolver, fx
}

// e2eAuthorize drives the real authorization point (the authorizer + the
// e2e bridge + the catalog snapshot fixture) for a ShipAny platform user,
// returning the handle or the refusal for the test to assert.
func e2eAuthorize(t *testing.T, ctx context.Context, b *CanonicalWalletBridge, holds, user, body string) (*AuthorizationHandle, error) {
	t.Helper()
	snapshots, apiKey, _, account, _, _, _ := e2eSnapshotFixture(t)
	snap, err := snapshots.Freeze(ctx, FreezeInput{APIKey: apiKey, User: apiKey.User, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	require.NoError(t, err)
	require.NotNil(t, snap)
	apiKey.User.PlatformUserID = user
	apiKey.User.BillingCurrency = "CNY"
	cfg := &config.Config{}
	cfg.CanonicalWallet = e2eWalletConfig(t, b.Mode(), holds)
	auth := NewCanonicalWalletAuthorizer(cfg, b, snapshots)
	return auth.Authorize(ctx, AuthorizeInput{Snapshot: snap, Estimate: EstimateInputFromRequestBody([]byte(body), EstimateInputOptions{}), User: apiKey.User})
}

// e2eWaitOutboxStatus polls the outbox row (the file idiom; no sleeps beyond
// the poll interval) with a caller-chosen deadline — ShipAny dev on sqlite
// can be slower than the 30 s the Phase-34 helper assumes.
func e2eWaitOutboxStatus(t *testing.T, ctx context.Context, db *sql.DB, requestID, want string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		var status string
		err := db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, requestID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status, reason sql.NullString
	_ = db.QueryRowContext(ctx, `SELECT status, dead_letter_reason FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, requestID).Scan(&status, &reason)
	t.Fatalf("outbox row %s never reached status %s (last: status=%q dead_letter_reason=%q)", requestID, want, status.String, reason.String)
}

// e2eWaitOutboxEventStatus is the event-id form (the Live window events are
// known by their event id — gwusg_<hash> — not their request id).
func e2eWaitOutboxEventStatus(t *testing.T, ctx context.Context, db *sql.DB, eventID, want string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		var status string
		err := db.QueryRowContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status, reason sql.NullString
	_ = db.QueryRowContext(ctx, `SELECT status, dead_letter_reason FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&status, &reason)
	t.Fatalf("outbox event %s never reached status %s (last: status=%q dead_letter_reason=%q)", eventID, want, status.String, reason.String)
}

// e2eOutboxRow is the row shape the suite asserts on.
type e2eOutboxRow struct {
	Status           string
	AttemptCount     int
	DeadLetterReason sql.NullString
}

func e2eOutboxRowByRequest(t *testing.T, ctx context.Context, db *sql.DB, requestID string) e2eOutboxRow {
	t.Helper()
	var row e2eOutboxRow
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT status, attempt_count, dead_letter_reason FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, requestID,
	).Scan(&row.Status, &row.AttemptCount, &row.DeadLetterReason))
	return row
}

// ---------------------------------------------------------------------------
// The smoke — the first real settlement (Task 3 Step 2)
// ---------------------------------------------------------------------------

// TestPhase38Smoke proves the pair end to end once: a seeded user, one
// authorization in shadow against the real ensure route, one settlement
// delivered through the real settlements route — the first time the two real
// services settle money.
func TestPhase38Smoke(t *testing.T) {
	e2eRequireStage(t, "routes_on")
	ctx := context.Background()
	user := e2eUserID("smoke")
	e2eSeed(t, user, 100_000)

	b, db, _ := e2eBridge(t, config.CanonicalWalletModeShadow, "off")
	h, err := e2eAuthorize(t, ctx, b, "off", user, `{"max_tokens":64}`)
	require.NoError(t, err)
	require.NotNil(t, h)
	require.Nil(t, h.Refusal, "shadow admits everywhere")

	state := e2eInspect(t, user)
	e2eLogState(t, state)
	require.Len(t, state.Leases, 1, "one ensure → one lease (ids read: %v)", state.leaseIDs())
	lease := state.Leases[0]
	require.Equal(t, "active", lease.Status)
	require.Equal(t, int64(500_000_000), lease.budgetUnits(t), "the requested budget (5 CNY at 1e8 units/CNY) was granted")
	t.Logf("[shipany] smoke lease id=%s budget=%d", lease.ID, lease.budgetUnits(t))

	const amount = int64(30_000_000) // 0.3 CNY
	reqID := "phase38-smoke-" + lease.ID
	b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: reqID, PlatformUserID: user, Currency: "CNY",
		AmountUnits: amount, OccurredAt: time.Now().UTC(),
	})
	e2eWaitOutboxStatus(t, ctx, db, reqID, "delivered", 60*time.Second)

	state = e2eInspect(t, user)
	e2eLogState(t, state)
	require.Equal(t, amount, state.leaseByID(t, lease.ID).capturedUnits(t),
		"the real settlements route captured the delivered amount on lease %s", lease.ID)
	t.Logf("[shipany] smoke settled: lease=%s captured=%d — the first real settlement", lease.ID, amount)
}

// ---------------------------------------------------------------------------
// Tests 54–57 — the activation order (redesign §14.4)
// ---------------------------------------------------------------------------

// Test 54, routes_off half: with the ledger routes off ShipAny answers 503
// on every wallet route — §11.5 classifies it transient, so the outbox row
// retries with attempt_count climbing and NEVER dead-letters, and an enforce
// authorization is refused lease_unavailable (fail-closed on a transient
// control-plane failure — test 45(a)'s arm on the real wire).
func TestPhase38RoutesOffTransient(t *testing.T) {
	e2eRequireStage(t, "routes_off")
	ctx := context.Background()
	user := e2eUserID("t54-off")
	e2eSeed(t, user, 100_000)

	b, db0, rdb0 := e2eBridge(t, config.CanonicalWalletModeShadow, "off")
	h, err := e2eAuthorize(t, ctx, b, "off", user, `{"max_tokens":64}`)
	require.NoError(t, err)
	require.NotNil(t, h)
	require.Nil(t, h.Refusal, "shadow admits everywhere — even through a 503")

	reqID := "phase38-t54-off-" + user
	b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: reqID, PlatformUserID: user, Currency: "CNY",
		AmountUnits: 10_000_000, OccurredAt: time.Now().UTC(),
	})
	// Poll for THREE delivery attempts (the backoff after each failure is
	// 2^n s, so ~6–8 s), then assert the row is still retrying: transient,
	// not dead-lettered — the §11.5 row for the 503.
	end := time.Now().Add(30 * time.Second)
	for time.Now().Before(end) {
		row := e2eOutboxRowByRequest(t, ctx, db0, reqID)
		if row.AttemptCount >= 3 {
			require.NotEqual(t, "dead_letter", row.Status,
				"503 is transient (§11.5): three attempts in, the row must still be retrying")
			require.False(t, row.DeadLetterReason.Valid, "no dead-letter reason may be set on a transient classification")
			t.Logf("test 54 (routes_off): row %s attempt_count=%d status=%s — transient, retrying", reqID, row.AttemptCount, row.Status)
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().After(end) {
			row := e2eOutboxRowByRequest(t, ctx, db0, reqID)
			t.Fatalf("outbox row %s reached only attempt_count=%d (status %s) in 30 s — expected 3 transient attempts", reqID, row.AttemptCount, row.Status)
		}
	}

	// Enforce on the same 503: refused, fail-closed — on the SAME claimed
	// stores (one shared-Redis claim per test).
	enforce := e2eBridgeOn(t, config.CanonicalWalletModeEnforce, "off", rdb0, db0)
	_, err = e2eAuthorize(t, ctx, enforce, "off", user, `{"max_tokens":64}`)
	require.Error(t, err, "enforce refuses when the control plane is unreachable (fail-closed)")
	refused, ok := AsAuthorizationRefused(err)
	require.True(t, ok, "the refusal carries the client-visible shape, got %v", err)
	require.Equal(t, AuthorizationRefusalLeaseUnavailable, refused.Reason,
		"the 503 maps to lease_unavailable (transient) — never a silent admit in enforce")

	// No ShipAny row was created by anything above: the routes are off.
	state := e2eInspect(t, user)
	e2eLogState(t, state)
	require.Empty(t, state.Leases, "no lease row can exist with the ledger routes off (ids read: %v)", state.leaseIDs())
}

// Test 54, routes_on half: the same flow with the routes on — ensure 200,
// the settlement delivers, ShipAny captures (§14.4: "503 transient before;
// ensure 200 after").
func TestPhase38RoutesOnDelivers(t *testing.T) {
	e2eRequireStage(t, "routes_on")
	ctx := context.Background()
	user := e2eUserID("t54-on")
	e2eSeed(t, user, 100_000)

	b, db, _ := e2eBridge(t, config.CanonicalWalletModeShadow, "off")
	h, err := e2eAuthorize(t, ctx, b, "off", user, `{"max_tokens":64}`)
	require.NoError(t, err)
	require.Nil(t, h.Refusal)
	state := e2eInspect(t, user)
	require.Len(t, state.Leases, 1, "ensure 200 → one active lease (ids read: %v)", state.leaseIDs())
	lease := state.Leases[0]

	reqID := "phase38-t54-on-" + user
	b.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: reqID, PlatformUserID: user, Currency: "CNY",
		AmountUnits: 10_000_000, OccurredAt: time.Now().UTC(),
	})
	e2eWaitOutboxStatus(t, ctx, db, reqID, "delivered", 60*time.Second)
	state = e2eInspect(t, user)
	e2eLogState(t, state)
	require.Equal(t, int64(10_000_000), state.leaseByID(t, lease.ID).capturedUnits(t),
		"delivered and captured on lease %s", lease.ID)
	t.Logf("[shipany] test 54 (routes_on): lease=%s captured=10000000", lease.ID)
}

// e2eSweep drives ShipAny's real sweep route with the sweep secret.
func e2eSweep(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	url, _ := e2eWalletEnv(t)
	secret := strings.TrimSpace(os.Getenv("WALLET_E2E_SWEEP_SECRET"))
	require.NotEmpty(t, secret, "WALLET_E2E_SWEEP_SECRET is required for the sweep test (the driver sets it)")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/api/internal/v2/wallet/leases/sweep", strings.NewReader(`{"limit":100}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "sweep route answered %s: %s", resp.Status, string(body))
	var envelope struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "sweep response: %s", string(body))
	require.Equal(t, 0, envelope.Code, "sweep route refused: %s (%v)", envelope.Message, envelope.Data)
	return envelope.Data
}

// Test 55 — activation step 2: the sweep flag on with the routes on; a
// grace-expired lease is closed by the REAL sweep route driven with the
// sweep secret; released_units returns to the grant. Opt-in via
// WALLET_E2E_SLOW=1 (it waits out a real grace at ShipAny's minimum stage
// settings, ~5.5 min under the driver) — 3.8's acceptance requires one
// WALLET_E2E_SLOW=1 run with this test green, recorded with its duration.
func TestPhase38SweepClosesAndRefunds(t *testing.T) {
	e2eRequireStage(t, "routes_on")
	if os.Getenv("WALLET_E2E_SLOW") != "1" {
		t.Skip("test 55 waits out a real settle grace (~5.5 min at the driver's minimum stage settings); opt in with WALLET_E2E_SLOW=1")
	}
	started := time.Now()
	ctx := context.Background()
	user := e2eUserID("t55")
	e2eSeed(t, user, 1000)
	grace := 300 * time.Second
	if raw := strings.TrimSpace(os.Getenv("WALLET_E2E_SETTLE_GRACE")); raw != "" {
		v, err := strconv.Atoi(raw)
		require.NoError(t, err, "WALLET_E2E_SETTLE_GRACE must be seconds")
		grace = time.Duration(v) * time.Second
	}
	// The plan's TTL source: ShipAny's GATEWAY_WALLET_LEASE_TTL_SECONDS for
	// the stage — the driver sets its minimum 30 (ShipAny honors the
	// gateway's requested ttl only down to 30, lease-ensure.ts:600/:770).
	if os.Getenv("WALLET_E2E_LEASE_TTL") == "" {
		t.Setenv("WALLET_E2E_LEASE_TTL", "30")
	}
	leaseTTL, err := strconv.Atoi(os.Getenv("WALLET_E2E_LEASE_TTL"))
	require.NoError(t, err)

	b, _, rdb := e2eBridge(t, config.CanonicalWalletModeEnforce, "off")
	h, err2 := e2eAuthorize(t, ctx, b, "off", user, `{"max_tokens":64}`)
	require.NoError(t, err2)
	require.Nil(t, h.Refusal)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	lease, err2 := store.GetCanonicalWalletLease(ctx, user)
	require.NoError(t, err2)
	// The TTL the wait budgets below is real: ShipAny honored the requested
	// 30 s floor (a 300 s lease would blow the 10 m package budget).
	require.WithinDuration(t, time.Now().UTC().Add(time.Duration(leaseTTL)*time.Second), lease.ExpiresAt, 15*time.Second,
		"ShipAny honored the requested lease TTL %d s", leaseTTL)
	state := e2eInspect(t, user)
	e2eLogState(t, state)
	shipanyLease := state.leaseByID(t, lease.LeaseID)
	require.Equal(t, "active", shipanyLease.Status)
	budget := shipanyLease.budgetUnits(t)
	seededBalance := state.Balance
	t.Logf("[shipany] test 55: lease=%s budget=%d balance=%d — waiting out expiry %s + grace %s",
		lease.LeaseID, budget, seededBalance, lease.ExpiresAt.Format(time.RFC3339), grace)

	// Poll past expires_at + settle_grace (ShipAny's sweep only closes leases
	// whose expires_at is older than now − grace — lease-close.ts's cutoff).
	cutoff := lease.ExpiresAt.Add(grace)
	for time.Now().Before(cutoff) {
		time.Sleep(500 * time.Millisecond)
	}

	// Drive the real sweep route until it closes the lease (the route is
	// idempotent; retrying covers clock skew between the two processes).
	closedTotal := 0.0
	end := time.Now().Add(60 * time.Second)
	for time.Now().Before(end) {
		data := e2eSweep(t, ctx)
		closedTotal, _ = data["closed"].(float64)
		state = e2eInspect(t, user)
		if l := state.leaseByID(t, lease.LeaseID); l.Status == "closed" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	state = e2eInspect(t, user)
	e2eLogState(t, state)
	l := state.leaseByID(t, lease.LeaseID)
	require.Equal(t, "closed", l.Status, "the sweep closed lease %s (closed=%v)", lease.LeaseID, closedTotal)
	require.Equal(t, budget, l.releasedUnits(t), "released_units == budget − captured (nothing captured)")
	require.Len(t, state.Closes, 1, "one wallet_lease_close row (ids read: closes on %s)", state.User)
	require.Equal(t, "grace_sweep", state.Closes[0].Reason)
	require.Equal(t, lease.LeaseID, state.Closes[0].LeaseID)
	require.Equal(t, budget, parseShipanyUnits(t, "released_units", state.Closes[0].ReleasedUnits))
	// The user's balance is restored by the credited amount: nothing was
	// captured, so the released budget returns in full.
	require.Equal(t, seededBalance+budget/1_000_000, state.Balance,
		"balance restored by the credited amount (credit ids %v)", state.creditIDs())
	t.Logf("[shipany] test 55: lease=%s closed by the real sweep; balance %d → %d (duration %s)",
		lease.LeaseID, seededBalance, state.Balance, time.Since(started).Round(time.Second))
}

// ---------------------------------------------------------------------------
// The service fixture — one OpenAIGatewayService over the e2e bridge (the
// 3.7b liveWindowFixture assembly with the §3 fake replaced by the real
// ShipAny): the ws_v2 harness (56b/57/64) and the Live fixture (56c/57) are
// the same service with a different fake upstream dialer.
// ---------------------------------------------------------------------------

type e2eServiceFixture struct {
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc
	svc        *OpenAIGatewayService
	bridge     *CanonicalWalletBridge
	store      CanonicalWalletLeaseStore
	liveStore  LiveCallStore
	snapshots  *BillingSnapshotService
	authorizer *CanonicalWalletAuthorizer
	user       *User
	apiKey     *APIKey
	account    *Account
	db         *sql.DB
	rdb        *redis.Client
	conn       *liveTestFrameConn // the Live sideband fake
	wsUpstream *stagedPassthroughConn
	cfg        *config.Config
	callSeq    atomic.Int64
}

// newE2EServiceFixture seeds the platform user, builds the real stores, the
// snapshot service over the REAL catalog and store, the bridge against the
// real ShipAny, and the service. surface selects the fake upstream the
// service dials: "ws" (the staged passthrough conn) or "live" (the sideband
// frame conn).
func newE2EServiceFixture(t *testing.T, mode, holds, surface, platformUser string, credits int64) *e2eServiceFixture {
	t.Helper()
	ResetAuthorizationMetricsForTest()
	ResetLiveWindowMetricsForTest()
	EnableOpenAIAdvancedSchedulerForTest()
	e2eSeed(t, platformUser, credits)
	ctxBg := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctxBg)
	db := startCanonicalWalletTestPostgres(t, ctxBg)
	for _, migration := range []string{"209_wallet_billing_snapshot.sql", "211_wallet_live_provisional.sql"} {
		content, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		require.NoError(t, err)
		_, err = db.ExecContext(ctxBg, string(content))
		require.NoError(t, err)
	}

	snapCfg := &config.Config{}
	snapCfg.Default.RateMultiplier = 1
	snapCfg.CanonicalWallet.BillingSnapshotMode = "record"
	snapCfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	billing := NewBillingService(&config.Config{}, e2eCatalogPricing(t))
	cs := NewChannelService(&LiveRestartChannelRepoStub{}, nil, nil, nil)
	resolver := NewModelPricingResolver(cs, billing)
	fx := NewExchangeRateService(snapCfg)
	snapshots := NewBillingSnapshotService(snapCfg, resolver, billing, fx, NewBillingSnapshotStoreForTest(t, db))

	group := &Group{ID: 7, RateMultiplier: 1.5, ImageRateIndependent: true, ImageRateMultiplier: 2.5}
	gid := int64(7)
	user := &User{ID: 42, BillingCurrency: "CNY", PlatformUserID: platformUser}
	apiKey := &APIKey{ID: 11, GroupID: &gid, Group: group, User: user}
	rate := 1.25
	account := &Account{
		ID: 99, RateMultiplier: &rate, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Concurrency: 4, Credentials: map[string]any{"access_token": "test-access-token", "chatgpt_account_id": "acct_test"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	// The passthrough lifecycle config carries the ws_v2 gateway flags
	// (timeouts, ingress mode router) the ws surface needs; the Live surface
	// tolerates them (3.7b's fixture ran on a bare config).
	cfg := passthroughLifecycleConfig()
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet = e2eWalletConfig(t, mode, holds)
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.CanonicalWallet.LeaseTTLSeconds = 30 // the driver's routes_on stage TTL
	cfg.CanonicalWallet.LiveWindowMinSeconds = 5
	cfg.CanonicalWallet.LiveControllerTakeoverSeconds = 2
	cfg.JWT.Secret = "test-jwt-secret-32-bytes-long!!!"
	cfg.Gateway.Live.MaxSessionDurationSeconds = 60

	cache := NewRealGatewayCacheForTest(t, rdb)
	store := cache.(CanonicalWalletLeaseStore)
	liveStore := cache.(LiveCallStore)
	outbox := &outboxStoreForTest{db: db}
	client := newCanonicalWalletHTTPClient(cfg.CanonicalWallet, nil)
	bridge := newCanonicalWalletBridge(cfg.CanonicalWallet, store, client, db, outbox, 0, nil)
	authorizer := NewCanonicalWalletAuthorizer(cfg, bridge, snapshots)
	provStore := LiveProvisionalStore(newLiveProvisionalStore(db))

	f := &e2eServiceFixture{
		t: t, ctx: ctx, cancel: cancel,
		svc: nil, bridge: bridge, store: store, snapshots: snapshots, authorizer: authorizer,
		user: user, apiKey: apiKey, account: account, db: db, rdb: rdb,
		liveStore: liveStore, conn: newLiveTestFrameConn(), wsUpstream: newStagedPassthroughConn(), cfg: cfg,
	}
	var dialer openAIWSClientDialer
	if surface == "live" {
		liveDialer := &liveTestDialer{conn: f.conn}
		dialer = &p37bFakeOnlyDialer{inner: liveDialer, fake: f.conn, t: t}
	} else {
		dialer = &stagedPassthroughDialer{conn: f.wsUpstream}
	}
	f.svc = &OpenAIGatewayService{
		cfg:                       cfg,
		cache:                     cache,
		concurrencyService:        NewConcurrencyService(&liveTestConcurrencyCache{}),
		liveProvisional:           provStore,
		httpUpstream:              &e2eLiveHTTPUpstream{fixture: f},
		openaiWSPassthroughDialer: dialer,
		openaiScheduler:           &LiveRestartSchedulerStub{Account: account},
		accountRepo:               &liveTestAccountRepo{account: account},
		liveAttestation:           liveAttestationStub{header: `{"v":1,"s":0,"t":"v1.test"}`},
		liveAttestationCipher:     newLiveAttestationCipher(cfg),
		resolver:                  resolver,
		billingService:            billing,
		exchangeRates:             fx,
		billingSnapshotSettler:    billingSnapshotSettler{snapshots: snapshots},
		authorizer:                authorizer,
		canonicalWallet:           bridge,
		deferredService:           NewDeferredService(nil, nil, time.Second),
		usageBillingRepo:          &openAIRecordUsageBillingRepoStub{},
		usageLogRepo:              &liveTestUsageRepo{},
	}
	prev := liveObserverContextProvider
	liveObserverContextProvider = func() context.Context { return f.ctx }
	t.Cleanup(func() {
		liveObserverContextProvider = prev
		cancel()
		bridge.Close()
	})
	return f
}

// e2eLiveHTTPUpstream answers each CreateLiveCall with a DISTINCT call id
// (the 3.7b shape).
type e2eLiveHTTPUpstream struct{ fixture *e2eServiceFixture }

func (h *e2eLiveHTTPUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	id := h.fixture.callSeq.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Location": {"/backend-api/codex/call_p38e2e_" + strconv.FormatInt(id, 10)}},
		Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
	}, nil
}

func (h *e2eLiveHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return h.Do(req, proxyURL, accountID, accountConcurrency)
}

// e2eWSHarness — the passthrough harness (openai_ws_v2_passthrough_
// authorization_test.go's shape) with the REAL authorizer on the e2e bridge
// and the production 4402 mapping (openai_gateway_handler.go:2006).
type e2eWSHarness struct {
	server     *httptest.Server
	serverErr  chan error
	clientConn *coderws.Conn
	authCalls  chan int
	handles    sync.Map
	f          *e2eServiceFixture
}

func newE2EWSHarness(t *testing.T, f *e2eServiceFixture, model string) *e2eWSHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	svc := f.svc
	harness := &e2eWSHarness{
		serverErr: make(chan error, 1),
		authCalls: make(chan int, 16),
		handles:   sync.Map{},
		f:         f,
	}
	hooks := &OpenAIWSIngressHooks{
		AuthorizeTurn: func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
			harness.authCalls <- turn
			snap, err := f.snapshots.Freeze(f.ctx, FreezeInput{
				APIKey: f.apiKey, User: f.user, Account: f.account,
				RequestedModel: model, BillingModel: model, Family: BillingFamilyOpenAI,
			})
			if err != nil {
				return nil, err
			}
			h, err := f.authorizer.Authorize(f.ctx, AuthorizeInput{Snapshot: snap, Estimate: estimate, User: f.user})
			if err != nil {
				// The production mapping (openai_gateway_handler.go:2006):
				// 4402 with the named refusal reason.
				refused, _ := AsAuthorizationRefused(err)
				reason := AuthorizationRefusedWSCloseReason
				if refused != nil {
					reason += ": " + string(refused.Reason)
				}
				return h, NewOpenAIWSClientCloseError(coderws.StatusCode(AuthorizationRefusedWSCloseStatus), reason, err)
			}
			harness.handles.Store(turn, h)
			return h, nil
		},
	}
	account := passthroughLifecycleAccount()
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			harness.serverErr <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		msgType, firstMessage, err := ReadOpenAIWSClientMessage(r.Context(), conn, 3*time.Second, coderws.StatusPolicyViolation, "missing first response.create message")
		if err != nil {
			harness.serverErr <- err
			return
		}
		if msgType != coderws.MessageText {
			harness.serverErr <- errors.New("first message was not text")
			return
		}
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		ginCtx.Request = req
		proxyErr := svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
		if proxyErr != nil {
			// The unit harness's close handling: a refusal must reach the
			// client as a proper CLOSE frame (4402 + reason), not an EOF.
			var closeErr *OpenAIWSClientCloseError
			if errors.As(proxyErr, &closeErr) {
				_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
			}
		}
		harness.serverErr <- proxyErr
	}))
	return harness
}

// dial connects a client and sends the first frame.
func (h *e2eWSHarness) dial(t *testing.T, firstMessage string) {
	t.Helper()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(h.server.URL, "http"), nil)
	require.NoError(t, err)
	h.clientConn = clientConn
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	require.NoError(t, clientConn.Write(writeCtx, coderws.MessageText, []byte(firstMessage)))
}

func (h *e2eWSHarness) waitCall(t *testing.T) int {
	t.Helper()
	select {
	case turn := <-h.authCalls:
		return turn
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for AuthorizeTurn call")
		return 0
	}
}

func (h *e2eWSHarness) writeClient(t *testing.T, payload string) {
	t.Helper()
	writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, h.clientConn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func (h *e2eWSHarness) readClient(t *testing.T) ([]byte, error) {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, payload, err := h.clientConn.Read(readCtx)
	return payload, err
}

func (h *e2eWSHarness) waitUpstreamWrite(t *testing.T) []byte {
	t.Helper()
	select {
	case payload := <-h.f.wsUpstream.writes:
		return payload
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream write")
		return nil
	}
}

func (h *e2eWSHarness) closeClientAndWait(t *testing.T) {
	t.Helper()
	_ = h.clientConn.Close(coderws.StatusNormalClosure, "done")
	select {
	case serverErr := <-h.serverErr:
		if serverErr != nil && !errors.Is(serverErr, context.Canceled) {
			t.Logf("e2e ws session ended with: %v", serverErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the ws session to end")
	}
}

// settleTurn mirrors the handler's post-turn settle: RecordUsage with the
// turn's usage and the turn's authorization id — the path that makes the
// settlement flow through the REAL bridge to the REAL settlements route.
func (h *e2eWSHarness) settleTurn(t *testing.T, turn int, requestID string, in, out int) {
	t.Helper()
	val, ok := h.handles.Load(turn)
	require.True(t, ok, "turn %d was authorized", turn)
	handle := val.(*AuthorizationHandle)
	require.NoError(t, h.f.svc.RecordUsage(h.f.ctx, &OpenAIRecordUsageInput{
		Result: &OpenAIForwardResult{
			RequestID: requestID, Model: "gpt-5.1", Stream: true,
			Usage: OpenAIUsage{InputTokens: in, OutputTokens: out},
		},
		APIKey: h.f.apiKey, User: h.f.user, Account: h.f.account,
		AuthorizationID: handle.ID, AuthorizationToken: handle.ID + ".1",
	}))
}

// Test 56 — activation step 3: shadow + holds ON settles on all three
// surfaces against the real routes.
func TestPhase38ShadowSettlesAllSurfaces(t *testing.T) {
	e2eRequireStage(t, "routes_on")
	ctx := context.Background()

	t.Run("http-hold-armed-converted-captured", func(t *testing.T) {
		user := e2eUserID("t56-http")
		e2eSeed(t, user, 100_000)
		b, db, rdb := e2eBridge(t, config.CanonicalWalletModeShadow, "on")
		h, err := e2eAuthorize(t, ctx, b, "on", user, `{"max_tokens":64}`)
		require.NoError(t, err)
		require.True(t, h.HoldArmed, "holds on arms at the authorization point")
		require.Equal(t, h.EstimatedUnits, h.HeldUnits)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		ids, err := store.ListCanonicalWalletHolds(ctx, user, 100)
		require.NoError(t, err)
		require.Len(t, ids, 1, "holds:{user} has one member: %v", ids)
		state := e2eInspect(t, user)
		require.Len(t, state.Leases, 1)
		leaseID := state.Leases[0].ID

		reqID := "phase38-t56-http-" + user
		const amount = int64(1_000_000)
		b.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: reqID, PlatformUserID: user, Currency: "CNY",
			AmountUnits: amount, OccurredAt: time.Now().UTC(), AuthorizationID: h.ID,
		})
		e2eWaitOutboxStatus(t, ctx, db, reqID, "delivered", 60*time.Second)
		hold, err := store.GetCanonicalWalletHold(ctx, user, h.ID)
		require.NoError(t, err)
		require.Equal(t, "settled", hold.State, "the settlement converted the armed hold")
		state = e2eInspect(t, user)
		e2eLogState(t, state)
		require.Equal(t, amount, state.leaseByID(t, leaseID).capturedUnits(t),
			"ShipAny captured the converted hold's amount on lease %s", leaseID)
		t.Logf("[shipany] test 56(a): lease=%s captured=%d hold=%s settled", leaseID, amount, h.ID)
	})

	t.Run("ws-two-turns-two-event-ids", func(t *testing.T) {
		user := e2eUserID("t56-ws")
		f := newE2EServiceFixture(t, config.CanonicalWalletModeShadow, "on", "ws", user, 100_000)
		harness := newE2EWSHarness(t, f, "gpt-5.1")
		defer harness.server.Close()

		// Turn 1: response.create without previous_response_id.
		harness.dial(t, `{"type":"response.create","model":"gpt-5.1","max_output_tokens":512}`)
		require.Equal(t, 1, harness.waitCall(t))
		_ = harness.waitUpstreamWrite(t)
		harness.f.wsUpstream.Send(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.1"}}`)
		harness.f.wsUpstream.Send(`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.1","usage":{"input_tokens":120,"output_tokens":30,"total_tokens":150}}}`)
		for {
			ev, err := harness.readClient(t)
			require.NoError(t, err)
			if gjsonGet(ev, "type") == "response.completed" {
				break
			}
		}
		req1 := "phase38-t56-ws-t1-" + user
		harness.settleTurn(t, 1, req1, 120, 30)

		// Turn 2 with previous_response_id.
		harness.writeClient(t, `{"type":"response.create","previous_response_id":"resp_1","max_output_tokens":512}`)
		require.Equal(t, 2, harness.waitCall(t))
		_ = harness.waitUpstreamWrite(t)
		harness.f.wsUpstream.Send(`{"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.1","usage":{"input_tokens":200,"output_tokens":40,"total_tokens":240}}}`)
		for {
			ev, err := harness.readClient(t)
			require.NoError(t, err)
			if gjsonGet(ev, "type") == "response.completed" {
				break
			}
		}
		req2 := "phase38-t56-ws-t2-" + user
		harness.settleTurn(t, 2, req2, 200, 40)
		harness.closeClientAndWait(t)

		e2eWaitOutboxStatus(t, f.ctx, f.db, req1, "delivered", 60*time.Second)
		e2eWaitOutboxStatus(t, f.ctx, f.db, req2, "delivered", 60*time.Second)
		// Two DISTINCT event ids delivered; ShipAny captured both on the lease.
		var captured1, captured2 int64
		require.NoError(t, f.db.QueryRowContext(f.ctx,
			`SELECT amount_units FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, req1).Scan(&captured1))
		require.NoError(t, f.db.QueryRowContext(f.ctx,
			`SELECT amount_units FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, req2).Scan(&captured2))
		require.Greater(t, captured1, int64(0))
		require.Greater(t, captured2, int64(0))
		state := e2eInspect(t, user)
		e2eLogState(t, state)
		require.NotEmpty(t, state.Leases)
		var total int64
		for _, l := range state.Leases {
			total += l.capturedUnits(t)
		}
		require.Equal(t, captured1+captured2, total,
			"two captures under two event ids (%s, %s) landed on ShipAny's leases %v", req1, req2, state.leaseIDs())
		t.Logf("[shipany] test 56(b): leases=%v captured=%d+%d under event ids %s, %s",
			state.leaseIDs(), captured1, captured2, req1, req2)
	})

	t.Run("live-one-window-closed-by-usage", func(t *testing.T) {
		user := e2eUserID("t56-live")
		f := newE2ELiveFixture(t, config.CanonicalWalletModeShadow, "on", user, 100_000)
		created, err := f.svc.CreateLiveCall(f.ctx, &LiveCallRequest{
			SDP:     "v=0\r\n",
			Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096,"instructions":"p38 e2e live window session"}`),
		}, LiveCallIdentity{
			APIKeyID: f.apiKey.ID, UserID: f.user.ID, APIKey: f.apiKey, User: f.user,
			GroupID: f.apiKey.GroupID, BillingCurrency: "CNY", RateMultiplier: 1.0, BillingModel: "claude-sonnet-4",
		}, 5)
		require.NoError(t, err)
		require.NotNil(t, created)
		callHash := hashLiveCallID(created.CallID)

		// Pump usage past the window's held estimate E_w; with floor 5 s the
		// window closes shortly after (clock (ii)), settling exactly its
		// accrued usage under the window event id.
		rec := f.provisionalRecord(t, callHash)
		eW := rec.EstimatedUnits
		require.Greater(t, eW, int64(0), "the session's held estimate is finite")
		tokensPerFrame := int(float64(eW)/f.unitsPerOutputToken(t, callHash)) + 1000
		for i := 0; i < 3; i++ {
			f.pumpUsage(fmt.Sprintf("resp-t56-live-%d", i), 0, tokensPerFrame)
		}
		// Poll for window 2 (window 1 closed and settled, re-authorized).
		end := time.Now().Add(45 * time.Second)
		var advanced *LiveProvisionalRecord
		for time.Now().Before(end) {
			rec = f.provisionalRecord(t, callHash)
			if len(rec.Windows) >= 2 {
				advanced = rec
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		require.NotNil(t, advanced, "the window closed by usage and the session re-authorized (windows: %+v)", rec.Windows)
		// liveWindowRequestID(callHash, 1) is the BARE callHash (window ids
		// take the :window:N suffix only from seq 2 — openai_live.go:128).
		eventID := CanonicalWalletSettlementEventID(callHash, user, "CNY")
		e2eWaitOutboxEventStatus(f.t, f.ctx, f.db, eventID, "delivered", 60*time.Second)

		state := e2eInspect(t, user)
		e2eLogState(t, state)
		var captured int64
		for _, l := range state.Leases {
			captured += l.capturedUnits(t)
		}
		require.Greater(t, captured, int64(0), "the window's event was delivered and captured on ShipAny (leases %v)", state.leaseIDs())
		// The re-authorization's ensure is visible as one lease (reused) or
		// two (issued). Window 1's own LeaseID is recorded empty on the
		// initial window, so reused-vs-issued is decided from ShipAny's rows
		// and matched against window 2's lease.
		w2 := advanced.Windows[1]
		var w2OnShipAny bool
		for _, l := range state.Leases {
			if l.ID == w2.LeaseID {
				w2OnShipAny = true
			}
		}
		require.True(t, w2OnShipAny, "window 2's lease %s is one of ShipAny's rows %v", w2.LeaseID, state.leaseIDs())
		outcome := "issued"
		if len(state.Leases) == 1 {
			outcome = "reused"
		}
		t.Logf("[shipany] test 56(c): leases=%v captured=%d; window2 lease=%s (%s)",
			state.leaseIDs(), captured, w2.LeaseID, outcome)
	})
}

// newE2ELiveFixture is the ws fixture with the Live sideband dialer.
func newE2ELiveFixture(t *testing.T, mode, holds, platformUser string, credits int64) *e2eServiceFixture {
	t.Helper()
	return newE2EServiceFixture(t, mode, holds, "live", platformUser, credits)
}

// The Live pump helpers (the 3.7b shapes, local to the e2e fixture).
func (f *e2eServiceFixture) provisionalRecord(t *testing.T, callHash string) *LiveProvisionalRecord {
	t.Helper()
	rec, err := f.svc.liveProvisional.GetByCallHash(context.Background(), callHash)
	require.NoError(t, err)
	return rec
}

func (f *e2eServiceFixture) unitsPerOutputToken(t *testing.T, callHash string) float64 {
	t.Helper()
	rec, err := f.liveStore.GetLiveCall(context.Background(), callHash)
	require.NoError(t, err)
	return rec.OutputPricePerToken * rec.ExchangeRate * rec.RateMultiplier * float64(canonicalWalletUnitsPerCNY)
}

func (f *e2eServiceFixture) pumpUsage(responseID string, inputTokens, outputTokens int) {
	payload := fmt.Sprintf(`{"type":"response.done","response":{"id":%q,"usage":{"input_tokens":%d,"output_tokens":%d}}}`, responseID, inputTokens, outputTokens)
	f.conn.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(payload)}
}

// gjsonGet is a one-liner so the import reads clearly at the call sites.
func gjsonGet(payload []byte, path string) string {
	return gjson.GetBytes(payload, path).String()
}

// Test 57 — activation step 4: enforce + enforce_ready; an unfunded user is
// refused on all three surfaces with the named shapes; a funded user is
// admitted on all three.
func TestPhase38EnforceRefusesUnfunded(t *testing.T) {
	e2eRequireStage(t, "routes_on")
	ctx := context.Background()

	t.Run("http-refused-and-funded-admitted", func(t *testing.T) {
		b, _, _ := e2eBridge(t, config.CanonicalWalletModeEnforce, "off")
		broke := e2eUserID("t57-http-broke")
		// No seed: balance 0 (the seed script cannot grant zero).
		_, err := e2eAuthorize(t, ctx, b, "off", broke, `{"max_tokens":64}`)
		require.Error(t, err)
		refused, ok := AsAuthorizationRefused(err)
		require.True(t, ok)
		require.Equal(t, AuthorizationRefusalBalanceShortfall, refused.Reason,
			"the named terminal shape for an unfunded user (AuthorizationRefusedError balance_shortfall)")
		state := e2eInspect(t, broke)
		e2eLogState(t, state)
		require.Empty(t, state.Leases, "no lease was issued (ids read: %v)", state.leaseIDs())

		funded := e2eUserID("t57-http-funded")
		e2eSeed(t, funded, 1000)
		h, err := e2eAuthorize(t, ctx, b, "off", funded, `{"max_tokens":64}`)
		require.NoError(t, err)
		require.Nil(t, h.Refusal)
		state = e2eInspect(t, funded)
		require.Len(t, state.Leases, 1, "the funded user's ensure issued (ids read: %v)", state.leaseIDs())
	})

	t.Run("ws-4402-named-shape-no-upstream-write", func(t *testing.T) {
		broke := e2eUserID("t57-ws-broke")
		f := newE2EServiceFixture(t, config.CanonicalWalletModeEnforce, "off", "ws", broke, 1)
		harness := newE2EWSHarness(t, f, "gpt-5.1")
		defer harness.server.Close()
		harness.dial(t, `{"type":"response.create","model":"gpt-5.1","max_output_tokens":512}`)
		require.Equal(t, 1, harness.waitCall(t))
		_, err := harness.readClient(t)
		var closeErr coderws.CloseError
		require.ErrorAs(t, err, &closeErr, "the refused turn closes the client connection")
		require.Equal(t, coderws.StatusCode(AuthorizationRefusedWSCloseStatus), closeErr.Code, "4402")
		require.Contains(t, closeErr.Reason, AuthorizationRefusedWSCloseReason)
		require.Contains(t, closeErr.Reason, string(AuthorizationRefusalBalanceShortfall), "the named shape rides the close reason")
		select {
		case payload := <-harness.f.wsUpstream.writes:
			t.Fatalf("no upstream write may happen on a refused turn, got: %s", string(payload))
		case <-time.After(200 * time.Millisecond):
		}
		select {
		case serverErr := <-harness.serverErr:
			require.ErrorIs(t, serverErr, ErrAuthorizationRefused)
		case <-time.After(5 * time.Second):
			t.Fatal("the session did not exit after the refusal")
		}
	})

	t.Run("ws-funded-admitted", func(t *testing.T) {
		funded := e2eUserID("t57-ws-funded")
		f2 := newE2EServiceFixture(t, config.CanonicalWalletModeEnforce, "off", "ws", funded, 100_000)
		h2 := newE2EWSHarness(t, f2, "gpt-5.1")
		defer h2.server.Close()
		h2.dial(t, `{"type":"response.create","model":"gpt-5.1","max_output_tokens":512}`)
		require.Equal(t, 1, h2.waitCall(t))
		_ = h2.waitUpstreamWrite(t)
		h2.f.wsUpstream.Send(`{"type":"response.completed","response":{"id":"resp_f1","model":"gpt-5.1","usage":{"input_tokens":50,"output_tokens":10}}}`)
		for {
			ev, err := h2.readClient(t)
			require.NoError(t, err)
			if gjsonGet(ev, "type") == "response.completed" {
				break
			}
		}
		h2.closeClientAndWait(t)
		val, ok := h2.handles.Load(1)
		require.True(t, ok)
		require.Nil(t, val.(*AuthorizationHandle).Refusal, "the funded user's turn was admitted")
	})

	t.Run("live-refused-402-shape", func(t *testing.T) {
		broke := e2eUserID("t57-live-broke")
		f := newE2ELiveFixture(t, config.CanonicalWalletModeEnforce, "on", broke, 1)
		_, err := f.svc.CreateLiveCall(f.ctx, &LiveCallRequest{
			SDP:     "v=0\r\n",
			Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096,"instructions":"p38 e2e refused live session"}`),
		}, LiveCallIdentity{
			APIKeyID: f.apiKey.ID, UserID: f.user.ID, APIKey: f.apiKey, User: f.user,
			GroupID: f.apiKey.GroupID, BillingCurrency: "CNY", RateMultiplier: 1.0, BillingModel: "claude-sonnet-4",
		}, 5)
		require.Error(t, err, "the unfunded user's Live POST is refused at its authorization point")
		refused, ok := AsAuthorizationRefused(err)
		require.True(t, ok, "the refusal is the 402-mapped shape (3.3c: AuthorizationRefusedError), got %v", err)
		require.Equal(t, AuthorizationRefusalBalanceShortfall, refused.Reason)
		state := e2eInspect(t, broke)
		e2eLogState(t, state)
		require.Empty(t, state.Leases, "no lease (ids read: %v)", state.leaseIDs())
	})

	t.Run("live-funded-admitted", func(t *testing.T) {
		funded := e2eUserID("t57-live-funded")
		f2 := newE2ELiveFixture(t, config.CanonicalWalletModeEnforce, "on", funded, 100_000)
		created, err := f2.svc.CreateLiveCall(f2.ctx, &LiveCallRequest{
			SDP:     "v=0\r\n",
			Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096,"instructions":"p38 e2e funded live session"}`),
		}, LiveCallIdentity{
			APIKeyID: f2.apiKey.ID, UserID: f2.user.ID, APIKey: f2.apiKey, User: f2.user,
			GroupID: f2.apiKey.GroupID, BillingCurrency: "CNY", RateMultiplier: 1.0, BillingModel: "claude-sonnet-4",
		}, 5)
		require.NoError(t, err, "the funded user's Live session is admitted")
		require.NotNil(t, created)
		state := e2eInspect(t, funded)
		require.NotEmpty(t, state.Leases, "the session's ensure issued (ids read: %v)", state.leaseIDs())
	})
}
