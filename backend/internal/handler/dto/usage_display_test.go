package dto

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageChannelTypeUsesActualSupplierInsteadOfGroupOrRate(t *testing.T) {
	tests := []struct {
		name    string
		account *service.Account
		want    string
	}{
		{"no account", nil, ""},
		{"direct Anthropic default", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey}, "official"},
		{"direct Google", &service.Account{Platform: service.PlatformGemini, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://generativelanguage.googleapis.com"}}, "official"},
		{"Kie", &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.kie.ai"}}, "cloud-vendor"},
		{"AirCode", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.aicodewith.ai/v1"}}, "cloud-vendor"},
		{"proxy with official protocol", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://proxy.example"}}, ""},
		{"spoof host", &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.openai.com.example"}}, ""},
		{"untrusted HTTP", &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "http://api.openai.com"}}, ""},
		{"host belongs to another provider", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.openai.com"}}, ""},
		{"OAuth custom proxy", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeOAuth, Extra: map[string]any{"custom_base_url_enabled": true, "custom_base_url": "https://proxy.example"}}, ""},
		{"built-in OpenAI OAuth", &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}, "official"},
		{"Bedrock", &service.Account{Platform: service.PlatformAnthropic, Type: service.AccountTypeBedrock}, "cloud-vendor"},
		{"unknown platform", &service.Account{Platform: "custom", Type: service.AccountTypeAPIKey}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &service.UsageLog{Account: tt.account, Group: &service.Group{Name: "Official discount 20%", Platform: service.PlatformAnthropic}, RateMultiplier: 1, RequestType: service.RequestTypeSync}
			require.Equal(t, tt.want, UsageLogFromService(log).ChannelType)
		})
	}
}

func TestUsageDisplayMediaContextDoesNotExposeTaskOrAccountInternals(t *testing.T) {
	seconds := 8
	log := &service.UsageLog{Model: "veo-3-1", ActualCost: 0.375, TotalCost: 0.375, RateMultiplier: 1, VideoCount: 1, VideoDurationSeconds: &seconds,
		MediaPriceContext: &service.UsageMediaPriceContext{ModelID: "veo-3-1", Option: "8", ReferenceSKUVerified: true},
		Account:           &service.Account{ID: 9, Name: "private upstream", Credentials: map[string]any{"api_key": "private-key", "base_url": "https://private.example"}},
	}
	result := UsageLogFromService(log)
	require.Equal(t, "cloud-vendor", result.ChannelType)
	require.NotNil(t, result.MediaPriceContext)
	require.Equal(t, "8", result.MediaPriceContext.Option)
	require.NotEmpty(t, result.MediaPriceContext.CurrentReferenceSKU)
	require.Equal(t, 0.375, result.ActualCost)
	require.Equal(t, 1.0, result.RateMultiplier)
	require.Equal(t, 8, *result.VideoDurationSeconds)
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	for _, forbidden := range []string{"private-key", "private upstream", "private.example", "supplier_pricing", "provider_task_id", "request_payload", "account_rate_multiplier"} {
		require.NotContains(t, string(raw), forbidden)
	}
}

func TestUsageMediaLegacyGenericDoesNotBorrowCurrentEngineReference(t *testing.T) {
	result := UsageLogFromService(&service.UsageLog{MediaPriceContext: &service.UsageMediaPriceContext{ModelID: "veo-3-1", Option: "8", ReferenceSKUVerified: false}})
	require.NotNil(t, result.MediaPriceContext)
	require.False(t, result.MediaPriceContext.ReferenceSKUVerified)
	require.Empty(t, result.MediaPriceContext.CurrentReferenceSKU)
	// Supplier channel is known from the owner-checked task even when its old SKU is not comparable.
	require.Equal(t, "cloud-vendor", result.ChannelType)
}

func TestUsageMediaAPICatalogProvidesExactManufacturerWithoutInferringFromSupplier(t *testing.T) {
	for _, tc := range []struct{ model, want string }{
		{"ai-music-api/generate-v6-mini", "Suno"},
		{"ai-music-api/generate-v6-wild", "Suno"},
		{"seedream/5-flash-text-to-image", "ByteDance"},
		{"unknown-private-alias", ""},
	} {
		t.Run(tc.model, func(t *testing.T) {
			result := UsageLogFromService(&service.UsageLog{MediaPriceContext: &service.UsageMediaPriceContext{ModelID: tc.model, Option: "1:1", ReferenceSKUVerified: true}})
			require.Equal(t, tc.want, result.MediaPriceContext.ModelProvider)
		})
	}
}
