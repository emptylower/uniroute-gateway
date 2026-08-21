package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func TestModelGovernanceProbeHandlerProbeRequiresIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeProbeRepoForHandler{installed: true}
	svc := service.NewAccountEndpointProbeService(repo, nil)
	handler := NewModelGovernanceProbeHandler(svc)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/gateway-admin/1/accounts/1/endpoint-probe", nil)
	handler.Probe(c)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestModelGovernanceProbeHandlerRejectsClientModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeProbeRepoForHandler{installed: true}
	svc := service.NewAccountEndpointProbeService(repo, nil)
	handler := NewModelGovernanceProbeHandler(svc)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/internal/v1/gateway-admin/1/accounts/1/endpoint-probe?model=gpt-4", nil)
	c.Request.Header.Set("Idempotency-Key", "key-123")
	// Need to set assertion context – handler will check assertion and return 401 if missing
	handler.Probe(c)
	// Without assertion, should be 401 or 400 for model
	// Our handler rejects client model before assertion, so 400
	require.Equal(t, http.StatusBadRequest, w.Code)
}

type fakeProbeRepoForHandler struct {
	installed bool
}

func (f *fakeProbeRepoForHandler) FindLatestValid(ctx context.Context, accountID int64, now time.Time) (*service.AccountEndpointProbe, error) {
	return nil, nil
}
func (f *fakeProbeRepoForHandler) FindByKey(ctx context.Context, a int64, c int64, p service.GovernanceProvider, pr service.AccountProtocol, e string, cv, cf int64) (*service.AccountEndpointProbe, error) {
	return nil, nil
}
func (f *fakeProbeRepoForHandler) Insert(ctx context.Context, p *service.AccountEndpointProbe) error { return nil }
func (f *fakeProbeRepoForHandler) InvalidateOnConfigChange(ctx context.Context, a int64) error { return nil }

func init() {
	_ = context.Background
	_ = time.Now
}
