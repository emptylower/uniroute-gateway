//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type publicPricesHandlerFake struct {
	calls int
	err   error
}

func (f *publicPricesHandlerFake) ListText(context.Context, int64, time.Time) ([]service.TextModelCatalogItem, error) {
	panic("must not load authenticated catalog")
}
func (f *publicPricesHandlerFake) QuoteChannelCosts(context.Context, int64, time.Time, string) (service.ChannelCostQuote, error) {
	panic("must not quote user prices")
}
func (f *publicPricesHandlerFake) PublicPrices(_ context.Context, now time.Time) (service.PublicModelPriceSnapshot, error) {
	f.calls++
	return service.NewPublicModelPriceSnapshot([]service.PublicModelPrice{{ID: "claude-sonnet-4-5", Channel: "discounted", Currency: "USD", InputPricePerMillionUSD: "0.6", OutputPricePerMillionUSD: "3", ContextScope: "standard"}}, now), f.err
}

func TestPublicPricesHandlerAnonymousContractAndFeatureGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name, query          string
		enabled              bool
		status, calls, items int
	}{
		{"anonymous default", "", true, 200, 1, 1},
		{"identity currency ignored", "?user_id=99&currency=CNY", true, 200, 1, 1},
		{"invalid category", "?category=image", true, 400, 0, 0},
		{"disabled", "", false, 200, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			enabled := "false"
			if test.enabled {
				enabled = "true"
			}
			settings := service.NewSettingService(&settingHandlerPublicRepoStub{values: map[string]string{service.SettingKeyAvailableChannelsEnabled: enabled}}, &config.Config{})
			catalog := &publicPricesHandlerFake{}
			h := &ModelCatalogHandler{catalog: catalog, settingService: settings}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices"+test.query, nil)
			c.Request.Header.Set("Authorization", "Bearer irrelevant-client-token")
			h.ListPublicPrices(c)
			require.Equal(t, test.status, recorder.Code)
			require.Equal(t, test.calls, catalog.calls)
			if test.status != 200 {
				return
			}
			var envelope struct {
				Data service.PublicModelPriceSnapshot `json:"data"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
			require.Len(t, envelope.Data.Items, test.items)
			require.Equal(t, "default_retail_standard_tokens", envelope.Data.PriceScope)
			require.Regexp(t, `^sha256:[a-f0-9]{64}$`, envelope.Data.SnapshotVersion)
			require.False(t, envelope.Data.GeneratedAt.IsZero())
			require.Nil(t, envelope.Data.EffectiveAt)
			if test.items > 0 {
				require.Equal(t, "USD", envelope.Data.Items[0].Currency)
			}
		})
	}
}

func TestPublicPricesHandlerPropagatesServiceFailure(t *testing.T) {
	settings := service.NewSettingService(&settingHandlerPublicRepoStub{values: map[string]string{service.SettingKeyAvailableChannelsEnabled: "true"}}, &config.Config{})
	h := &ModelCatalogHandler{catalog: &publicPricesHandlerFake{err: errors.New("catalog unavailable")}, settingService: settings}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices", nil)
	h.ListPublicPrices(c)
	require.Equal(t, 500, recorder.Code)
}
