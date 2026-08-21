package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelQuarantineHandler_Quarantine(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeStore := &fakeQuarantineStore{}
	svc := service.NewModelQuarantineService(fakeStore, nil, nil)
	handler := NewModelQuarantineHandler(svc)
	router := gin.New()
	router.POST("/quarantine", handler.Quarantine)

	body := map[string]any{
		"batch_id": "batch-1", "idempotency_key": "idem-1",
		"expected_registry_version": 5, "channel_versions": map[int64]int64{10: 3},
		"items": []map[string]any{{"AccountID": 1, "ChannelID": 10, "CanonicalModelID": "claude-opus-4-6"}},
	}
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/quarantine", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

type fakeQuarantineStore struct {
	service.ModelAuthorizationStore
}

func (f *fakeQuarantineStore) RecomputeBatch(ctx context.Context, input service.RecomputeInput) error {
	return nil
}
func (f *fakeQuarantineStore) Decision(ctx context.Context, key service.ModelAuthorizationKey) (service.ModelAuthorizationDecision, error) {
	return service.ModelAuthorizationDecision{}, nil
}
func (f *fakeQuarantineStore) IsChannelModelEligible(ctx context.Context, channelID int64, canonical string) (bool, error) {
	return true, nil
}
func (f *fakeQuarantineStore) IsCanonicalEligible(ctx context.Context, canonical string) (bool, error) {
	return true, nil
}
