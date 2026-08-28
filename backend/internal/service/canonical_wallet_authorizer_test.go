//go:build unit

package service

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// The fixture reuses 3.2's newSnapshotTestFixture (billing_snapshot_freeze_test.go:14),
// which returns a BillingSnapshotService, an APIKey with User, and an Account, and
// the bridge stubs from canonical_wallet_bridge_test.go.
func newAuthorizerFixture(t *testing.T, mode string) (*CanonicalWalletAuthorizer, *BillingSnapshot, *APIKey, *canonicalWalletControlStub, *canonicalWalletStoreStub) {
	t.Helper()
	snapshots, apiKey, _, account := newSnapshotTestFixture(t)
	snap, err := snapshots.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: apiKey.User, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	require.NoError(t, err)
	require.NotNil(t, snap)
	apiKey.User.PlatformUserID = "platform-user-1"
	apiKey.User.BillingCurrency = "CNY"
	bridge, store, control := newBridgeForEnsureLeaseTest(t)
	control.lease = CanonicalWalletLease{LeaseID: "lease-1", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(5 * time.Minute)}
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = mode
	cfg.CanonicalWallet.RequestTimeoutMS = 300
	cfg.CanonicalWallet.LeaseBudgetUnits = 500_000_000
	return NewCanonicalWalletAuthorizer(cfg, bridge, snapshots), snap, apiKey, control, store
}

func estimateFor(body string) EstimateInput {
	return EstimateInputFromRequestBody([]byte(body), EstimateInputOptions{})
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func itoaTest(i int) string { return strconv.Itoa(i) }

func TestAuthorizeShadowMintsEstimatesAndEnsuresALease(t *testing.T) {
	resetAuthorizationMetricsForTest()
	auth, snap, apiKey, _, store := newAuthorizerFixture(t, config.CanonicalWalletModeShadow)
	h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"model":"claude-sonnet-4","max_tokens":64,"messages":[]}`), User: apiKey.User})
	require.NoError(t, err)
	require.NotEmpty(t, h.ID)
	require.Equal(t, snap.ID, h.SnapshotID)
	require.Greater(t, h.EstimatedUnits, int64(0))
	require.Equal(t, "lease-1", h.LeaseID)
	require.Nil(t, h.Refusal)
	require.Equal(t, 1, store.installCalls)
	require.Equal(t, 0, store.reserveCalls, "no hold is armed in 3.3")
}

func TestAuthorizeDisabledModeMintsOnly(t *testing.T) {
	auth, snap, apiKey, control, _ := newAuthorizerFixture(t, config.CanonicalWalletModeDisabled)
	h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{}`), User: apiKey.User})
	require.NoError(t, err)
	require.NotEmpty(t, h.ID)
	require.Equal(t, int64(0), h.EstimatedUnits)
	require.Equal(t, "", h.LeaseID)
	require.Equal(t, 0, control.ensureCalls)
}

func TestAuthorizeNilAuthorizerMintsOnly(t *testing.T) {
	var auth *CanonicalWalletAuthorizer
	h, err := auth.Authorize(context.Background(), AuthorizeInput{})
	require.NoError(t, err)
	require.NotEmpty(t, h.ID)
}

func TestAuthorizeShadowNeverRefuses(t *testing.T) {
	resetAuthorizationMetricsForTest()
	auth, snap, apiKey, control, _ := newAuthorizerFixture(t, config.CanonicalWalletModeShadow)
	t.Run("missing snapshot", func(t *testing.T) {
		h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: nil, User: apiKey.User})
		require.NoError(t, err)
		require.Nil(t, h.Refusal)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().SnapshotMissing)
	})
	t.Run("balance shortfall", func(t *testing.T) {
		control.lease = CanonicalWalletLease{LeaseID: "lease-2", Currency: "CNY", BudgetUnits: 1, ExpiresAt: time.Now().Add(time.Minute)}
		h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User})
		require.NoError(t, err)
		require.Nil(t, h.Refusal)
		require.Equal(t, "", h.LeaseID)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().BalanceShortfall)
	})
	t.Run("control plane down", func(t *testing.T) {
		control.leaseErr = errors.New("503")
		h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User})
		require.NoError(t, err)
		require.Nil(t, h.Refusal)
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().LeaseUnavailable)
	})
}

func TestAuthorizeEnforceRefusesWithTheNamedReason(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(in *AuthorizeInput, control *canonicalWalletControlStub)
		reason AuthorizationRefusalReason
	}{
		{"missing snapshot", func(in *AuthorizeInput, _ *canonicalWalletControlStub) { in.Snapshot = nil }, AuthorizationRefusalSnapshotMissing},
		{"missing identity", func(in *AuthorizeInput, _ *canonicalWalletControlStub) { in.User = &User{} }, AuthorizationRefusalIdentityMissing},
		{"non-CNY", func(in *AuthorizeInput, _ *canonicalWalletControlStub) {
			u := *in.User
			u.BillingCurrency = "USD"
			in.User = &u
		}, AuthorizationRefusalCurrency},
		{"balance shortfall", func(_ *AuthorizeInput, c *canonicalWalletControlStub) {
			c.lease = CanonicalWalletLease{LeaseID: "l", Currency: "CNY", BudgetUnits: 1, ExpiresAt: time.Now().Add(time.Minute)}
		}, AuthorizationRefusalBalanceShortfall},
		{"control plane down", func(_ *AuthorizeInput, c *canonicalWalletControlStub) { c.leaseErr = errors.New("503") }, AuthorizationRefusalLeaseUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth, snap, apiKey, control, _ := newAuthorizerFixture(t, config.CanonicalWalletModeEnforce)
			in := AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User}
			tc.mutate(&in, control)
			h, err := auth.Authorize(context.Background(), in)
			require.True(t, errors.Is(err, ErrAuthorizationRefused))
			refused, _ := AsAuthorizationRefused(err)
			require.Equal(t, tc.reason, refused.Reason)
			require.NotNil(t, h, "the handle is minted so the refusal carries an id")
			require.Same(t, refused, h.Refusal)
			require.Equal(t, h.ID, refused.AuthorizationID)
		})
	}
}

func TestAuthorizeEnforceAdmitsWhenLeaseCoversTheEstimate(t *testing.T) {
	auth, snap, apiKey, _, _ := newAuthorizerFixture(t, config.CanonicalWalletModeEnforce)
	h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User})
	require.NoError(t, err)
	require.Nil(t, h.Refusal)
	require.Equal(t, "lease-1", h.LeaseID)
}

func TestAuthorizeRequestsExactlyTheEstimate(t *testing.T) {
	auth, snap, apiKey, control, _ := newAuthorizerFixture(t, config.CanonicalWalletModeShadow)
	h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User})
	require.NoError(t, err)
	// Phase 3.4 (v2 wire): the ask is the amount itself as min_headroom_units
	// plus the CONFIGURED lease budget as requested_budget_units — the server
	// takes the max (§3 step 4), so the client no longer computes it.
	require.Equal(t, h.EstimatedUnits, control.lastEnsure.MinHeadroomUnits)
	require.Equal(t, int64(500_000_000), control.lastEnsure.RequestedBudgetUnits)
}

func TestEstimateInputFromRequestBody(t *testing.T) {
	body := `{"model":"gpt-5","input":"hello","max_output_tokens":123,"service_tier":"flex"}`
	in := EstimateInputFromRequestBody([]byte(body), EstimateInputOptions{ImageCount: 2, ImageSize: "1024x1024"})
	require.Equal(t, len(body), in.InputTokensUpperBound)
	require.Equal(t, 123, in.MaxOutputTokens)
	require.Equal(t, "flex", in.ServiceTier)
	require.Equal(t, 2, in.ImageCount)
	require.Equal(t, "1024x1024", in.ImageSize)
	require.Equal(t, ContinuationNone, in.Continuation)
	require.Equal(t, 77, EstimateInputFromRequestBody([]byte(`{"max_tokens":77}`), EstimateInputOptions{}).MaxOutputTokens)
	require.Equal(t, 9, EstimateInputFromRequestBody([]byte(`{"max_completion_tokens":9}`), EstimateInputOptions{}).MaxOutputTokens)
	require.Equal(t, 5, EstimateInputFromRequestBody([]byte(`{"generationConfig":{"maxOutputTokens":5}}`), EstimateInputOptions{}).MaxOutputTokens)
	require.Equal(t, 0, EstimateInputFromRequestBody([]byte(`{"max_tokens":-1}`), EstimateInputOptions{}).MaxOutputTokens)
	// The bound is the MAX over every present key: a compatibility body can carry both, and
	// openai_gateway_forward.go:444 deletes max_completion_tokens only for API-key / non-OpenAI
	// accounts, so on an OpenAI OAuth account both reach the wire.
	require.Equal(t, 100000, EstimateInputFromRequestBody([]byte(`{"max_tokens":16,"max_completion_tokens":100000}`), EstimateInputOptions{}).MaxOutputTokens)
	require.Equal(t, 0, EstimateInputFromRequestBody(nil, EstimateInputOptions{}).InputTokensUpperBound)
}

// The differential invariant through the authorization point (index 3.3 exit, last
// clause): for statically bounded attempts, final settled units <= authorized units.
// Reuses 3.2's corpus machinery: settledUnitsForTest (billing_snapshot_estimator_test.go:31)
// and the snapshot fixture. Draw request bodies and usages within the request's bound.
func TestAuthorizeEstimateDominatesSettlement(t *testing.T) {
	auth, snap, apiKey, _, _ := newAuthorizerFixture(t, config.CanonicalWalletModeShadow)
	rng := rand.New(rand.NewSource(3)) // the same inline seeding 3.2's corpus tests use
	for i := 0; i < 500; i++ {
		maxOut := 1 + rng.Intn(4000)
		bodyLen := 16 + rng.Intn(20000)
		body := make([]byte, bodyLen)
		copy(body, []byte(`{"max_tokens":`+itoaTest(maxOut)+`,"input":"`))
		h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, Estimate: EstimateInputFromRequestBody(body, EstimateInputOptions{}), User: apiKey.User})
		require.NoError(t, err)
		usage := UsageTokens{InputTokens: rng.Intn(bodyLen + 1), OutputTokens: rng.Intn(maxOut + 1)}
		cost, err := auth.snapshots.billing.CalculateCostFromSnapshot(snap, SnapshotSettlementInput{Tokens: usage})
		require.NoError(t, err)
		settled := settledUnitsForTest(t, auth.snapshots, snap, cost)
		require.LessOrEqual(t, settled, h.EstimatedUnits, "draw %d: settled %d > authorized %d", i, settled, h.EstimatedUnits)
	}
}

// Phase 3.3a Task 10 Step 1b: the shadow-mode request-path cost of Authorize.
// The REAL bound is canonical_wallet.request_timeout_ms (default 300 ms), which
// these stub fixtures cannot show — they measure the in-process authorizer +
// bridge-stub overhead only, to sit beside 3.2's ≈3 µs freeze figure.
func benchmarkAuthorize(b *testing.B, leaseOnStore bool) {
	snapshots, apiKey, _, account := newSnapshotTestFixture(b)
	snap, err := snapshots.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: apiKey.User, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	if err != nil {
		b.Fatal(err)
	}
	apiKey.User.PlatformUserID = "platform-user-1"
	apiKey.User.BillingCurrency = "CNY"

	store := &canonicalWalletStoreStub{}
	if leaseOnStore {
		// Current lease covers any estimate this body produces → the fast path:
		// one store read, no EnsureLease, no install.
		store.lease = &CanonicalWalletLease{LeaseID: "lease-hit", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(time.Hour)}
	} else {
		// Review note M3: keep the store empty on every iteration — without
		// this the first install would turn iterations 2..N into hits.
		store.neverPersist = true
	}
	control := &canonicalWalletControlStub{}
	if !leaseOnStore {
		control.lease = CanonicalWalletLease{LeaseID: "lease-miss", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(time.Hour)}
	}
	bridgeCfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	bridgeCfg.LeaseBudgetUnits = 500_000_000
	bridge := newCanonicalWalletBridge(bridgeCfg, store, control, nil, nil, 0)
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeShadow
	cfg.CanonicalWallet.RequestTimeoutMS = 300
	cfg.CanonicalWallet.LeaseBudgetUnits = 500_000_000
	auth := NewCanonicalWalletAuthorizer(cfg, bridge, snapshots)
	in := AuthorizeInput{Snapshot: snap, Estimate: estimateFor(`{"model":"claude-sonnet-4","max_tokens":64,"messages":[]}`), User: apiKey.User}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := auth.Authorize(context.Background(), in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeLeaseHit(b *testing.B)  { benchmarkAuthorize(b, true) }
func BenchmarkAuthorizeLeaseMiss(b *testing.B) { benchmarkAuthorize(b, false) }

// Phase 3.3a Task 10 Step 1 coverage: the guard branches and facades.

func TestAuthorizationHandleRemainingGuardBranches(t *testing.T) {
	resetAuthorizationMetricsForTest()
	// RecordOutcome for an unknown token must be a silent no-op.
	h, err := newAuthorizationHandle("shadow")
	require.NoError(t, err)
	h.RecordOutcome("auth_unknown.99", AuthorizationOutcomeResult, nil)
	require.Empty(t, h.Writes())
	// LastWriteToken with no writes.
	fresh, err := newAuthorizationHandle("shadow")
	require.NoError(t, err)
	require.Equal(t, "", fresh.LastWriteToken())
	// The typed-nil refusal error's Error/Unwrap are safe.
	var refused *AuthorizationRefusedError
	require.NotEmpty(t, refused.Error())
	require.Nil(t, refused.Unwrap())
	// describeUpstreamRequest handles a nil request.
	require.Equal(t, "<nil request>", describeUpstreamRequest(nil))
	// Abandoned on a nil receiver.
	var nilHandle *AuthorizationHandle
	require.Equal(t, "", nilHandle.Abandoned())
	// AuthorizationIDOf with a real handle.
	require.Equal(t, h.ID, AuthorizationIDOf(h))
	// WithNonBillableUpstream tolerates a nil context; the mark stays readable.
	marked := WithNonBillableUpstream(nil, NonBillableProbe)
	reason, ok := NonBillableUpstreamFromContext(marked)
	require.True(t, ok)
	require.Equal(t, NonBillableProbe, reason)
	// The mark reader tolerates a nil context.
	got, ok := NonBillableUpstreamFromContext(nil)
	require.False(t, ok)
	require.Empty(t, got)
	// WithAuthorizationHandle attaches through a nil context too.
	withHandle := WithAuthorizationHandle(nil, h)
	require.Same(t, h, AuthorizationHandleFromContext(withHandle))
	// NewAuthorizingHTTPUpstream with a nil cfg degrades to disabled mode; a
	// write through it must pass straight through and not touch the context.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	inner := &transportUpstream{transport: http.DefaultTransport}
	dec := NewAuthorizingHTTPUpstream(inner, nil)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader(`{}`))
	require.NoError(t, err)
	resp, err := dec.Do(req, "", 1, 1)
	require.NoError(t, err)
	_ = resp.Body.Close()
	decCfg := &config.Config{}
	decCfg.CanonicalWallet.Mode = config.CanonicalWalletModeDisabled
	require.NotNil(t, NewAuthorizingHTTPUpstream(inner, decCfg))
	_, err = NewAuthorizingHTTPUpstream(inner, decCfg).Do(req, "", 1, 1)
	require.NoError(t, err)
	// requestTimeout falls back to 300 ms without a configured timeout.
	auth := &CanonicalWalletAuthorizer{cfg: &config.Config{}}
	require.Equal(t, 300*time.Millisecond, auth.requestTimeout())
	var nilAuth *CanonicalWalletAuthorizer
	require.Equal(t, 300*time.Millisecond, nilAuth.requestTimeout())
}

func TestAuthorizeBillableAttemptFacadesAreNilSafe(t *testing.T) {
	// Services built by tests without an authorizer degrade to token-only handles.
	gw := &GatewayService{}
	h, err := gw.AuthorizeBillableAttempt(context.Background(), nil, nil, EstimateInput{})
	require.NoError(t, err)
	require.NotEmpty(t, h.ID)
	require.Nil(t, h.Refusal)

	ogw := &OpenAIGatewayService{}
	h2, err := ogw.AuthorizeBillableAttempt(context.Background(), nil, nil, EstimateInput{})
	require.NoError(t, err)
	require.NotEmpty(t, h2.ID)
	// userOfAPIKey derives the user from the key when one exists.
	require.Nil(t, userOfAPIKey(nil))
	key := &APIKey{User: &User{PlatformUserID: "p1"}}
	require.Same(t, key.User, userOfAPIKey(key))
}

func TestEnsureLeaseRejectsMissingDependenciesAndNilGrant(t *testing.T) {
	// A bridge without its store/control dependencies fails closed.
	broken := &CanonicalWalletBridge{}
	_, err := broken.ensureLease(context.Background(), "user-1", "CNY", 1, canonicalWalletLeasePurposeAuthorize, "")
	require.Error(t, err)

	// A (nil, nil) grant from the control plane is named, not a nil deref.
	b, store, control := newBridgeForEnsureLeaseTest(t)
	control.nilLease = true
	_, err = b.ensureLease(context.Background(), "user-1", "CNY", 1, canonicalWalletLeasePurposeAuthorize, "")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing)
	require.Equal(t, 0, store.installCalls)
}

func TestPhase34ProtoAuthorizeMapsCapReachedAndContention(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason AuthorizationRefusalReason
	}{
		{ErrCanonicalWalletLeaseCapReached, AuthorizationRefusalLeaseCapReached},
		{ErrCanonicalWalletLeaseContention, AuthorizationRefusalLeaseUnavailable},
		{ErrCanonicalWalletBalanceShortfall, AuthorizationRefusalBalanceShortfall},
	} {
		auth, snap, apiKey, stub, _ := newAuthorizerFixture(t, config.CanonicalWalletModeEnforce)
		stub.leaseErr = tc.err
		_, err := auth.Authorize(context.Background(), AuthorizeInput{
			Snapshot: snap, Estimate: estimateFor(`{"max_tokens":64}`), User: apiKey.User,
		})
		refused, ok := AsAuthorizationRefused(err)
		require.True(t, ok, "%v", tc.err)
		require.Equal(t, tc.reason, refused.Reason)
	}
}
