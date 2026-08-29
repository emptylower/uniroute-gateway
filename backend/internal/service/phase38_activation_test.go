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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
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
