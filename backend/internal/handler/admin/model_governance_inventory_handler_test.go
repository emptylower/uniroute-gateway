package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type inventoryListerStub struct {
	items []service.InventoryItem
	err   error
}

func (s inventoryListerStub) List(context.Context) ([]service.InventoryItem, error) {
	return s.items, s.err
}

func TestModelGovernanceInventoryHandlerReturnsInventory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	want := []service.InventoryItem{{
		AccountID: 1, TargetPlatform: service.PlatformOpenAI,
		UpstreamModelID: "model", BillingCurrency: "USD",
	}}
	router := gin.New()
	router.GET("/inventory", (&ModelGovernanceInventoryHandler{service: inventoryListerStub{items: want}}).List)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/inventory", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	var got response.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &got))
	encoded, err := json.Marshal(got.Data)
	require.NoError(t, err)
	var items []service.InventoryItem
	require.NoError(t, json.Unmarshal(encoded, &items))
	require.Equal(t, want, items)
	require.Equal(t, service.PlatformOpenAI, items[0].TargetPlatform)
}

func TestModelGovernanceInventoryHandlerPropagatesRepositoryFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/inventory", (&ModelGovernanceInventoryHandler{service: inventoryListerStub{err: errors.New("inventory unavailable")}}).List)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/inventory", nil))
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}
