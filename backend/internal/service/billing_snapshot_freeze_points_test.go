//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreezeBillingSnapshotModeRules(t *testing.T) {
	svc, apiKey, _, account := newSnapshotTestFixture(t)
	// the two fields the facade reads (gateway_service.go:691, :711) plus the embedded settler
	gw := &GatewayService{billingSnapshotSettler: billingSnapshotSettler{snapshots: svc, billing: svc.billing}, billingService: svc.billing, resolver: svc.resolver}

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "record"
	before := BillingSnapshotMetricsSnapshot().FreezeError
	snap, err := gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", false, 0, 0)
	require.NoError(t, err, "an ungated site never refuses in record mode")
	require.Nil(t, snap)
	require.Equal(t, before+1, BillingSnapshotMetricsSnapshot().FreezeError)

	_, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", true, 0, 0)
	require.ErrorIs(t, err, ErrModelPricingUnavailable, "a gated site refuses exactly as EnsureModelPricing did")

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	_, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", false, 0, 0)
	require.Error(t, err, "settle mode needs a snapshot")

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "off"
	snap, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "claude-sonnet-4", "claude-sonnet-4", true, 0, 0)
	require.NoError(t, err)
	require.Nil(t, snap, "off mode validates and returns nil")
}

// Seven of the eleven freeze rows (1, 2, 3, 4, 5, 8, 11) go through the
// OpenAI facade — OpenAIGatewayHandler.gatewayService is *OpenAIGatewayService
// (internal/handler/openai_gateway_handler.go:32) — and it alone does the
// shadow→credential resolution the OpenAI differential rests on.
func TestOpenAIFreezeBillingSnapshotModeRulesAndShadowAccount(t *testing.T) {
	svc, apiKey, _, account := newSnapshotTestFixture(t)
	// Field names per openai_gateway_service.go:389-416. apiKeyWithFreshGroupMediaPricing
	// nil-guards channelService/groupRepo (openai_gateway_usage.go:613-615), so they stay nil.
	gw := &OpenAIGatewayService{billingSnapshotSettler: billingSnapshotSettler{snapshots: svc, billing: svc.billing}, billingService: svc.billing, resolver: svc.resolver}

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "record"
	before := BillingSnapshotMetricsSnapshot().FreezeError
	snap, err := gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", false)
	require.NoError(t, err, "an ungated site never refuses in record mode")
	require.Nil(t, snap)
	require.Equal(t, before+1, BillingSnapshotMetricsSnapshot().FreezeError)
	_, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", true)
	require.ErrorIs(t, err, ErrModelPricingUnavailable, "a gated site refuses exactly as EnsureModelPricing did")
	svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	_, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, account, nil, "no-such-model", "no-such-model", false)
	require.Error(t, err, "settle mode needs a snapshot")

	// A shadow account (IsShadow = ParentAccountID != nil, account.go:2872): the
	// long-context flag must come from the credential account behind it.
	svc.cfg.CanonicalWallet.BillingSnapshotMode = "record"
	parent := int64(100)
	shadow := &Account{ID: 99, Platform: PlatformOpenAI, ParentAccountID: &parent}
	// resolveCredentialAccount rejects a parent that is not OpenAI OAuth (credential_shadow.go:29-31; IsOpenAIOAuth = IsOpenAI && Type == AccountTypeOAuth, account.go:1257-1259)
	credential := &Account{ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{openAILongContextBillingEnabledKey: true}}
	gw.accountRepo = &shadowSkipTestRepo{account: credential} // account_credential_shadow_skip_test.go:17-28 (//go:build unit, package service): embeds AccountRepository, GetByID matches on id — the only method resolveCredentialAccount calls (credential_shadow.go:13-32)
	snap, err = gw.FreezeBillingSnapshot(context.Background(), apiKey, shadow, nil, "claude-sonnet-4", "claude-sonnet-4", true)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.True(t, snap.Flags.LongContextBillingEnabled)
	require.Equal(t, int64(99), snap.AccountID, "AccountID is the selected (shadow) account; only the flags come from the credential")
}

// BenchmarkFreezeBillingSnapshot measures the request-path cost the generic
// facade adds per attempt (p50 read from -benchtime output; recorded in the
// completion record as the 3.3 authorization path's baseline cost).
func BenchmarkFreezeBillingSnapshot(b *testing.B) {
	svc, apiKey, _, account := newSnapshotTestFixture(&testing.T{})
	gw := &GatewayService{billingSnapshotSettler: billingSnapshotSettler{snapshots: svc, billing: svc.billing}, billingService: svc.billing, resolver: svc.resolver}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gw.FreezeBillingSnapshot(ctx, apiKey, account, nil, "claude-sonnet-4", "claude-sonnet-4", false, 0, 0)
	}
}

// BenchmarkFreezeBillingSnapshotWSRow3 / Row4 measure the two WS sites
// separately: row 3 is the handshake's validation-only freeze, row 4 the
// per-turn carried freeze (including turn 1).
func BenchmarkFreezeBillingSnapshotWSRow3(b *testing.B) {
	svc, apiKey, _, account := newSnapshotTestFixture(&testing.T{})
	gw := &OpenAIGatewayService{billingSnapshotSettler: billingSnapshotSettler{snapshots: svc, billing: svc.billing}, billingService: svc.billing, resolver: svc.resolver}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gw.FreezeBillingSnapshot(ctx, apiKey, account, nil, "claude-sonnet-4", "claude-sonnet-4", true)
	}
}

func BenchmarkFreezeBillingSnapshotWSRow4(b *testing.B) {
	svc, apiKey, _, account := newSnapshotTestFixture(&testing.T{})
	gw := &OpenAIGatewayService{billingSnapshotSettler: billingSnapshotSettler{snapshots: svc, billing: svc.billing}, billingService: svc.billing, resolver: svc.resolver}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gw.FreezeBillingSnapshot(ctx, apiKey, account, nil, "claude-sonnet-4", "claude-sonnet-4", false)
	}
}
