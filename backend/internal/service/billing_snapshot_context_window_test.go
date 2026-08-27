//go:build unit

package service

import (
	"os"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// The bundled LiteLLM data carries max_input_tokens on 188 of 198 entries
// (checked 2026-08-27); nothing on the pricing side parsed them until Phase
// 3.2. Production loads go parsePricingData → LiteLLMRawEntry → hand-copied
// LiteLLMModelPricing (pricing_service.go:421-500), so this test MUST go
// through parsePricingData: a direct json.Unmarshal into LiteLLMModelPricing
// would pass with the field missing from LiteLLMRawEntry and the window would
// be 0 in production. (The catalog side parses a governance-mutable window —
// litellm_catalog_adapter.go contextWindowFrom — which the snapshot must NOT use.)
func TestLiteLLMModelPricingParsesContextWindow(t *testing.T) {
	raw, err := os.ReadFile("../../resources/model-pricing/model_prices_and_context_window.json")
	require.NoError(t, err)
	pricingSvc := &PricingService{}
	entries, err := pricingSvc.parsePricingData(raw) // the production parser (pricing_service_test.go:218 uses the same call)
	require.NoError(t, err)
	gpt5, ok := entries["gpt-5"]
	require.True(t, ok)
	require.Equal(t, 272000, gpt5.MaxInputTokens)
	require.Equal(t, 128000, gpt5.MaxOutputTokens)
	withWindow := 0
	for _, e := range entries {
		if e.MaxInputTokens > 0 {
			withWindow++
		}
	}
	require.GreaterOrEqual(t, withWindow, 150, "the bundled data must keep carrying context windows for most models")
}

func TestModelPricingCarriesContextWindowFromLiteLLM(t *testing.T) {
	// pricing_service_test.go seeds PricingService with this inline literal
	// (:96, :189, :263) — the map value is a POINTER; there is no seeding helper.
	pricingSvc := &PricingService{pricingData: map[string]*LiteLLMModelPricing{
		"probe-model": {InputCostPerToken: 1e-6, OutputCostPerToken: 2e-6, MaxInputTokens: 4096, MaxOutputTokens: 1024},
	}}
	svc := NewBillingService(&config.Config{}, pricingSvc)
	pricing, err := svc.GetModelPricing("probe-model")
	require.NoError(t, err)
	require.Equal(t, 4096, pricing.MaxInputTokens)
	require.Equal(t, 1024, pricing.MaxOutputTokens)
}

func TestModelPricingFallbackHasNoContextWindow(t *testing.T) {
	svc := newTestBillingService() // fallback prices only
	pricing, err := svc.GetModelPricing("claude-sonnet-4")
	require.NoError(t, err)
	require.Equal(t, 0, pricing.MaxInputTokens, "fallback prices carry no window; the estimator must treat 0 as unknown")
}
