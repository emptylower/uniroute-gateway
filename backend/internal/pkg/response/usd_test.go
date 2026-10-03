package response

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUSDResponsesUsePlainDecimalsAndPreserveNonMoneyIntegers(t *testing.T) {
	result, err := usdResponseData(map[string]any{"cost_usd": "1e-8", "base_cost_usd": "0.123456789", "items": []any{map[string]any{"actual_cost_usd": "1.25", "requests": json.Number("9007199254740993")}}, "optional_usd": nil})
	require.NoError(t, err)
	data, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "0.00000001", data["cost_usd"])
	require.Equal(t, "0.123456789", data["base_cost_usd"])
	require.Nil(t, data["optional_usd"])
	items, ok := data["items"].([]any)
	require.True(t, ok)
	row, ok := items[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "1.25", row["actual_cost_usd"])
	require.Equal(t, json.Number("9007199254740993"), row["requests"])
}

func TestUSDResponsePreservesSubE8UnitPricesAndHistoricalPrecision(t *testing.T) {
	result, err := usdResponseData(map[string]any{"input_price_usd": 1e-9, "actual_cost_usd": "0.0000000001", "quota_usd": "100"})
	require.NoError(t, err)
	data, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "0.000000001", data["input_price_usd"])
	require.Equal(t, "0.0000000001", data["actual_cost_usd"])
	require.Equal(t, "100", data["quota_usd"])
}

func TestBindUSDJSONRejectsNonDecimalAmountsAndPreservesLowPrices(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		body  string
		valid bool
	}{
		{`{"quota_usd":"0.00000001"}`, true},
		{`{"quota_usd":1}`, false},
		{`{"quota":1}`, false},
		{`{"rate_multiplier_cny":1}`, false},
		{`{"quota_usd":"1e1"}`, false},
		{`{"quota_usd":"-1"}`, false},
		{`{"quota_usd":"0.000000001"}`, false},
		{`{"input_price_usd":"0.000000001"}`, true},
		{`{"quota_usd":null}`, true},
	} {
		t.Run(tt.body, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			var request struct {
				Quota *float64 `json:"quota_usd,string"`
				Price *float64 `json:"input_price_usd,string"`
			}
			err := BindUSDJSON(c, &request)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
