package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPublicModelPricesRouteDoesNotRequireJWT(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterAuthRoutes(router.Group("/api/v1"), &handler.Handlers{Auth: &handler.AuthHandler{}, Setting: &handler.SettingHandler{}, ModelCatalog: &handler.ModelCatalogHandler{}}, servermiddleware.JWTAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusUnauthorized) }), servermiddleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }), nil, nil, nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/models/public-prices?category=text", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"price_scope":"default_retail_standard_tokens"`)
	require.Contains(t, recorder.Body.String(), `"items":[]`)
}
