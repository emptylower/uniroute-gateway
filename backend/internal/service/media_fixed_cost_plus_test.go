//go:build unit

package service

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMediaFixedCostPlusQuoteUsesSameRequestCost(t *testing.T) {
	total := 0
	for _, model := range MediaTaskCatalog() {
		costs, ok := mediaFixedCostOptions[model.ModelID]
		if !ok {
			continue
		}
		require.Equal(t, "per_request", model.Unit)
		require.Equal(t, "fixed_quote", model.BillingBasis)
		for _, option := range model.Param.Options {
			total++
			expected := costs[option.Value].QuotedUnits
			require.Equal(t, mediaUSD(expected), model.OptionPriceUSD[option.Value])
			_, _, quoted, err := resolveMediaQuoteUnchecked(model.ModelID, option.Value)
			require.NoError(t, err)
			require.Equal(t, expected, quoted)
			_, _, created, _, err := normalizeMediaCreateUnchecked(MediaCreateInput{Model: model.ModelID, Option: option.Value, Prompt: "a colorful garden"})
			require.NoError(t, err)
			require.Equal(t, expected, created)
		}
		bytes, err := json.Marshal(model)
		require.NoError(t, err)
		for _, private := range []string{"supplier_usd", "markup_bps", "source_sku", "source_sha256"} {
			require.NotContains(t, string(bytes), private)
		}
	}
	require.Equal(t, 232, total)
}

func TestMediaFixedCostPlusNativeUnitsAndSingleRounding(t *testing.T) {
	cases := []struct{ model, option, want string }{
		{"nano-banana-2-lite", "1:1", "0.025"},
		{"veo-3-1", "4", "0.375"}, {"veo-3-1", "8", "0.375"},
		{"kling-2.6/text-to-video", "5", "0.34375"},
		{"bytedance/seedance-1.5-pro", "480p:5", "0.0546875"},
		{"bytedance/seedance-2-fast", "480p:5", "0.365625"},
		{"google/gemini-omni-flash-1-1", "720p:4", "0.28125"},
		{"google/gemini-omni-flash-1-1", "720p:8", "0.46875"},
	}
	for _, c := range cases {
		_, _, units, input, err := normalizeMediaCreateUnchecked(MediaCreateInput{Model: c.model, Option: c.option, Prompt: "a colorful garden"})
		require.NoError(t, err)
		require.Equal(t, c.want, mediaUSD(units))
		if c.model == "veo-3-1" {
			require.Equal(t, "veo3_fast", input["model"])
			require.Equal(t, "720p", input["resolution"])
			require.Equal(t, "TEXT_2_VIDEO", input["generation_type"])
		}
	}
}
