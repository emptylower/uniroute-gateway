//go:build unit

package service

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMediaSupplierBudgetMatchesQuoteAndCatalog(t *testing.T) {
	for model, expected := range map[string]string{
		"qwen/text-to-image":               "0.05",
		"google/gemini-3-8-flash-lite-tts": "0.01225", "google/gemini-3-8-flash-tts": "0.0175",
		"google/gemini-3-1-flash-tts": "0.0385", "google/gemini-2-5-pro-tts": "0.0385",
	} {
		t.Run(model, func(t *testing.T) {
			m, option, units, err := resolveMediaQuoteUnchecked(model, "")
			require.NoError(t, err)
			require.Equal(t, expected, mediaUSD(units))
			require.Equal(t, expected, m.PriceUSD)
			require.Equal(t, "budget_cap", m.BillingBasis)
			for _, value := range m.OptionPriceUSD {
				require.Equal(t, expected, value)
			}
			_, _, normalized, _, err := normalizeMediaCreateUnchecked(MediaCreateInput{Model: model, Prompt: "Hello", Option: option})
			require.NoError(t, err)
			require.Equal(t, units, normalized)
			data, err := json.Marshal(m)
			require.NoError(t, err)
			require.NotContains(t, string(data), "markup")
			require.NotContains(t, string(data), "credit_usd")
			require.NotContains(t, string(data), "supplier")
		})
	}
}

func TestMediaSupplierProducerDisabledKeepsLegacyBudget(t *testing.T) {
	prior := mediaCostPlusPricingMode
	mediaCostPlusPricingMode = "disabled"
	defer func() { mediaCostPlusPricingMode = prior }()
	_, _, units, err := resolveMediaQuoteUnchecked("google/gemini-3-8-flash-lite-tts", "")
	require.NoError(t, err)
	require.Equal(t, "0.012", mediaUSD(units))
	require.Nil(t, newMediaSupplierPricing("google/gemini-3-8-flash-lite-tts", units))
	// A disabled producer still reads the amount frozen by the new producer.
	credits := "1"
	actual, loss, err := mediaSupplierActualUnits(&FrozenMediaSupplierPricing{PolicyVersion: mediaCostPlusPolicyVersion, CreditUSD: "0.005", MarkupBPS: 2500, BudgetUnits: 1225000, SourceVersion: "sha256:source"}, &credits, 1225000)
	require.NoError(t, err)
	require.Equal(t, int64(625000), actual)
	require.Zero(t, loss)
}

// The disabled producer remains a tested rollback path. Existing catalog
// fixtures pin its original provider fields and exact pre-change prices.
func useLegacyMediaPriceProducer(t *testing.T) {
	prior := mediaCostPlusPricingMode
	mediaCostPlusPricingMode = "disabled"
	t.Cleanup(func() { mediaCostPlusPricingMode = prior })
}

// This sentinel observes the original linker value, without changing globals.
func TestMediaSupplierRollbackLinkedMode(t *testing.T) {
	want := os.Getenv("SEO_DAY2_EXPECT_MEDIA_PRODUCER_MODE")
	if want == "" {
		t.Skip("release-only linker proof requires an explicit expected mode")
	}
	require.Equal(t, "disabled", want)
	require.Equal(t, want, mediaCostPlusPricingMode)
	for _, model := range []string{"google/gemini-3-8-flash-lite-tts", "qwen/text-to-image"} {
		m, _, units, err := resolveMediaQuoteUnchecked(model, "")
		require.NoError(t, err)
		require.NotEqual(t, "budget_cap", m.BillingBasis)
		require.Nil(t, newMediaSupplierPricing(model, units))
	}
	_, _, units, err := resolveMediaQuoteUnchecked("nano-banana-2-lite", "")
	require.NoError(t, err)
	require.Equal(t, "0.0273", mediaUSD(units))
	for credits, amount := range map[string]int64{"0": 0, "1": 625000, "100": 1225000} {
		actual, loss, err := mediaSupplierActualUnits(&FrozenMediaSupplierPricing{PolicyVersion: mediaCostPlusPolicyVersion, CreditUSD: "0.005", MarkupBPS: 2500, BudgetUnits: 1225000, SourceVersion: "sha256:source"}, &credits, 1225000)
		require.NoError(t, err)
		require.Equal(t, amount, actual)
		if credits == "100" {
			require.Positive(t, loss)
		} else {
			require.Zero(t, loss)
		}
	}
}
