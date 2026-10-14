//go:build unit

package service

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPublicMediaPricesUsesActualProducerCatalogIncludingCompatibleRole(t *testing.T) {
	prior := mediaCostPlusPricingMode
	defer func() { mediaCostPlusPricingMode = prior }()
	var hashes []string
	for _, mode := range []string{"enabled", "disabled"} {
		mediaCostPlusPricingMode = mode
		snapshot := NewPublicMediaPriceSnapshot(time.Now())
		require.Equal(t, "default_retail_media_quotes", snapshot.PriceScope)
		require.NotEmpty(t, snapshot.Models)
		for _, model := range snapshot.Models {
			for _, option := range model.Param.Options {
				_, _, units, err := resolveMediaQuoteUnchecked(model.ModelID, option.Value)
				require.NoError(t, err)
				require.Equal(t, mediaUSD(units), model.OptionQuoteUSD[option.Value])
			}
		}
		found := false
		for _, model := range snapshot.Models {
			if model.ModelID == "nano-banana-2-lite" {
				found = true
				if mode == "enabled" {
					require.Equal(t, "0.025", model.OptionQuoteUSD["1:1"])
				} else {
					require.Equal(t, "0.0273", model.OptionQuoteUSD["1:1"])
				}
				require.Equal(t, "nano-banana-2-lite:source-228", model.OptionReferenceSKU["1:1"])
			}
		}
		require.True(t, found)
		data, err := json.Marshal(snapshot)
		require.NoError(t, err)
		for _, private := range []string{"supplier_usd", "credit_usd", "markup_bps", "account_id", "kie-cost-plus-25"} {
			require.NotContains(t, string(data), private)
		}
		hashes = append(hashes, snapshot.SnapshotVersion)
	}
	require.NotEqual(t, hashes[0], hashes[1])
}
