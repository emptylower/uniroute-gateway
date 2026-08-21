package admin

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestModelGovernanceConnectionHandlerDesignateRequiresHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewModelGovernanceConnectionHandler(nil, nil, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/gateway-admin/1/connections/1/designate-aggregator", nil)
	handler.DesignateAggregator(c)
	require.Equal(t, http.StatusBadRequest, w.Code)

	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/gateway-admin/1/connections/1/designate-aggregator", bytes.NewBuffer([]byte(`{}`)))
	c.Request.Header.Set("Idempotency-Key", "key-1")
	c.Request.Header.Set("If-Match", "1")
	c.Request.Header.Set("X-Evidence-Ref", "ev1")
	handler.DesignateAggregator(c)
	// Without assertion, should be 401 or 500 (missing service)
	require.Contains(t, []int{http.StatusUnauthorized, http.StatusInternalServerError}, w.Code)
}

func TestModelGovernanceConnectionHandlerReuseRequiresHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewModelGovernanceConnectionHandler(nil, nil, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/gateway-admin/1/connections/1/reuse", bytes.NewBuffer([]byte(`{}`)))
	c.Request.Header.Set("Content-Type", "application/json")
	handler.ReuseAggregatorConnection(c)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
