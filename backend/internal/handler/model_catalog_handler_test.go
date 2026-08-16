package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestListChannelCostsAcceptsCurrencyAndDisplayCurrencyAlias(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2
	fx := service.NewExchangeRateService(cfg)
	catalog := service.NewModelCatalogService(nil, nil, nil, nil, fx)
	handler := NewModelCatalogHandler(catalog, nil)

	tests := []struct {
		name       string
		query      string
		currency   string
		rate       float64
		rateSource string
	}{
		{name: "canonical USD", query: "currency=USD", currency: service.CurrencyUSD, rate: 1, rateSource: "identity"},
		{name: "canonical CNY", query: "currency=CNY", currency: service.CurrencyCNY, rate: 7.2, rateSource: "bootstrap_config"},
		{name: "display currency alias", query: "display_currency=USD", currency: service.CurrencyUSD, rate: 1, rateSource: "identity"},
		{name: "canonical takes precedence", query: "currency=USD&display_currency=CNY", currency: service.CurrencyUSD, rate: 1, rateSource: "identity"},
		{name: "legacy default remains CNY", query: "", currency: service.CurrencyCNY, rate: 7.2, rateSource: "bootstrap_config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/models/channel-costs?"+tt.query, nil)
			ctx.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 42})

			handler.ListChannelCosts(ctx)

			require.Equal(t, http.StatusOK, recorder.Code)
			var envelope struct {
				Data struct {
					BaseCurrency  string                           `json:"base_currency"`
					QuoteCurrency string                           `json:"quote_currency"`
					ExchangeRate  float64                          `json:"exchange_rate"`
					Rate          float64                          `json:"rate"`
					RateSource    string                           `json:"rate_source"`
					Groups        []service.RoutingGroupModelCosts `json:"groups"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
			require.Equal(t, service.CurrencyUSD, envelope.Data.BaseCurrency)
			require.Equal(t, tt.currency, envelope.Data.QuoteCurrency)
			require.Equal(t, tt.rate, envelope.Data.ExchangeRate)
			require.Equal(t, envelope.Data.ExchangeRate, envelope.Data.Rate)
			require.Equal(t, tt.rateSource, envelope.Data.RateSource)
			require.NotNil(t, envelope.Data.Groups)
		})
	}
}

func TestListChannelCostsRejectsUnsupportedCurrency(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewModelCatalogHandler(nil, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/models/channel-costs?currency=EUR", nil)
	ctx.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 42})

	handler.ListChannelCosts(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var envelope response.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Contains(t, envelope.Message, "allowed values are CNY and USD")
}
