package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaCostCap(t *testing.T) {
	for _, s := range []string{"", "0", "0.00000001", "0001.20000000", "92233720368.54775807"} {
		_, _, err := normalizeMediaCostCap(s)
		require.NoError(t, err, s)
	}
	for _, s := range []string{"-1", "+1", "1e2", ".1", "1.", " 1", "1.000000001", "92233720368.54775808", "99999999999999999999"} {
		_, _, err := normalizeMediaCostCap(s)
		require.Error(t, err, s)
	}
	a, n, err := normalizeMediaCostCap("0001.20000000")
	require.NoError(t, err)
	require.Equal(t, int64(120000000), a)
	require.Equal(t, "1.2", n)
}
func TestPublicMediaEligibility(t *testing.T) {
	platform := "key-1"
	key := &APIKey{ID: 1, UserID: 1, PlatformKeyID: &platform, RoutingMode: APIKeyRoutingModeAutoChannels, Status: StatusActive, User: &User{ID: 1, Status: StatusActive, PlatformUserID: "u-1"}}
	cfg := &config.Config{}
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	require.NoError(t, validatePublicMediaKey(key, cfg, false))
	key.Status = StatusAPIKeyQuotaExhausted
	key.Quota = 1
	key.QuotaUsed = 1
	require.NoError(t, validatePublicMediaKey(key, cfg, false))
	require.Error(t, validatePublicMediaKey(key, cfg, true))
	key.Status = StatusActive
	key.Quota = 0
	key.ChannelIDs = []int64{1}
	require.Error(t, validatePublicMediaKey(key, cfg, false))
	key.ChannelIDs = nil
	key.PlatformKeyID = nil
	require.Error(t, validatePublicMediaKey(key, cfg, false))
}
func TestPublicMediaQueuedBilling(t *testing.T) {
	r := &mediaTaskRecord{PublicAPI: true, Status: "queued", FinancialState: "held", QuotedUnits: 123}
	require.Equal(t, "queued", mediaTaskView(r).Billing.State)
	r.Status = "authorizing"
	require.Equal(t, "authorizing", mediaTaskView(r).Billing.State)
	r.Status = "indeterminate"
	r.FinancialState = "released_unknown"
	require.Equal(t, "released_unknown", mediaTaskView(r).Billing.State)
	cost := int64(123)
	r.Status = "completed"
	r.FinancialState = "charged"
	r.ActualUnits = &cost
	r.HeldUnits = 123
	require.Equal(t, "0", mediaTaskView(r).Billing.HeldUSD)
}

func TestPublicMediaCostCapRejectsJSONNull(t *testing.T) {
	for _, body := range []string{`{"model":"nano-banana-2-lite","max_cost_usd":null}`, `{"max_cost_usd":12}`, `{"max_cost_usd":true}`, `{"max_cost_usd":""}`} {
		var input MediaCreateInput
		require.Error(t, json.Unmarshal([]byte(body), &input), body)
	}
	var in MediaCreateInput
	require.NoError(t, json.Unmarshal([]byte(`{"max_cost_usd":"0.02730000"}`), &in))
	require.Equal(t, "0.02730000", *in.MaxCostUSD)
}

func TestPublicMediaOmniPromptUnicodeBounds(t *testing.T) {
	for _, prompt := range []string{"中", strings.Repeat("中", 20000)} {
		in, _, _, _, err := normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-omni-flash-1-1", Prompt: prompt})
		require.NoError(t, err)
		require.Equal(t, prompt, in.Prompt)
	}
	_, _, _, _, err := normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-omni-flash-1-1", Prompt: strings.Repeat("中", 20001)})
	require.Error(t, err)
	_, _, _, _, err = normalizeMediaCreate(MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "中"})
	require.Error(t, err)
}

func TestPublicMediaMusicAdvertisesRequestPricing(t *testing.T) {
	count := 0
	for _, model := range PublicMediaTaskCatalog() {
		if model.MediaKind != "music" {
			continue
		}
		count++
		require.Equal(t, "per_request", model.Unit)
		quote, err := mediaQuote(model.ModelID, model.Param.Options[0].Value)
		require.NoError(t, err)
		require.Equal(t, model.OptionPriceUSD[model.Param.Options[0].Value], quote.QuotedUSD, "music quote is one request regardless of delivered track count")
	}
	require.Positive(t, count)
}
