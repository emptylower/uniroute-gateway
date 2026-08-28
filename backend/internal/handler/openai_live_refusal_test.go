//go:build unit

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveCreateErrorMapsAuthorizationRefusedTo402(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reasons := []service.AuthorizationRefusalReason{
		service.AuthorizationRefusalBalanceShortfall,
		service.AuthorizationRefusalLeaseUnavailable,
		service.AuthorizationRefusalLiveStoreUnavailable,
		service.AuthorizationRefusalSnapshotMissing,
	}

	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/backend-api/codex/realtime/calls", nil)

			refusal := &service.AuthorizationRefusedError{
				Reason:          reason,
				AuthorizationID: "auth_live_test",
			}

			h := &OpenAIGatewayHandler{}
			h.writeLiveCreateError(c, refusal)

			require.Equal(t, service.AuthorizationRefusedHTTPStatus, w.Code)
			require.Equal(t, http.StatusPaymentRequired, w.Code)

			var parsed map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &parsed))
			errorObj, ok := parsed["error"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, service.AuthorizationRefusedErrorType, errorObj["type"])
			assert.Contains(t, errorObj["message"], service.AuthorizationRefusedMessage)
			assert.Contains(t, errorObj["message"], string(reason))
			assert.NotContains(t, w.Body.String(), "Live upstream request failed")
		})
	}
}
