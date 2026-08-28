//go:build unit

package service

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
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
	require.Equal(t, 0, control.acquireCalls)
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
		{"non-CNY", func(in *AuthorizeInput, _ *canonicalWalletControlStub) { u := *in.User; u.BillingCurrency = "USD"; in.User = &u }, AuthorizationRefusalCurrency},
		{"balance shortfall", func(_ *AuthorizeInput, c *canonicalWalletControlStub) { c.lease = CanonicalWalletLease{LeaseID: "l", Currency: "CNY", BudgetUnits: 1, ExpiresAt: time.Now().Add(time.Minute)} }, AuthorizationRefusalBalanceShortfall},
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
	// ensureLease requests max(lease_budget_units, amount) in micros with ceiling division.
	want := (max64(500_000_000, h.EstimatedUnits) + 99) / 100
	require.Equal(t, want, control.lastRequest.RequestedMicros)
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
