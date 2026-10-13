package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPublicModelPricesRouteDoesNotRequireJWT(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterPublicModelPriceRoutes(router.Group("/api/v1"), &handler.Handlers{ModelCatalog: &handler.ModelCatalogHandler{}}, nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices?category=text", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"price_scope":"default_retail_standard_tokens"`)
	require.Contains(t, recorder.Body.String(), `"items":[]`)
}

func TestPublicModelPricesRouteAbsentWithUnwiredHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, handlers := range []*handler.Handlers{nil, {}} {
		router := gin.New()
		RegisterPublicModelPriceRoutes(router.Group("/api/v1"), handlers, nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices", nil))
		require.Equal(t, http.StatusNotFound, recorder.Code)
	}
}
