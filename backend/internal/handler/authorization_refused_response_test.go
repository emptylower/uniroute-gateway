//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testAuthorizationRefusal = &service.AuthorizationRefusedError{Reason: service.AuthorizationRefusalBalanceShortfall, AuthorizationID: "auth_test"}

// The refusal response contract (spec §2.0): a refusal surfaced through the
// forward-error fallback path maps to 402 with the named error type and the
// reason in the message — never a 502, never "Upstream request failed".
func TestAuthorizationRefusalResponseShape(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("OpenAI handler writes 402 wallet_authorization_refused", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)

		h := &OpenAIGatewayHandler{}
		wrote := h.ensureForwardErrorResponseFor(c, testAuthorizationRefusal, false)

		require.True(t, wrote)
		require.Equal(t, service.AuthorizationRefusedHTTPStatus, w.Code)
		require.Equal(t, http.StatusPaymentRequired, w.Code)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &parsed))
		errorObj := parsed["error"].(map[string]any)
		assert.Equal(t, service.AuthorizationRefusedErrorType, errorObj["type"])
		assert.Contains(t, errorObj["message"], service.AuthorizationRefusedMessage)
		assert.Contains(t, errorObj["message"], string(service.AuthorizationRefusalBalanceShortfall))
		assert.NotContains(t, w.Body.String(), "Upstream request failed")
	})

	t.Run("generic handler writes 402 wallet_authorization_refused", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

		h := &GatewayHandler{}
		wrote := h.ensureForwardErrorResponseFor(c, testAuthorizationRefusal, false)

		require.True(t, wrote)
		require.Equal(t, http.StatusPaymentRequired, w.Code)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &parsed))
		errorObj := parsed["error"].(map[string]any)
		assert.Equal(t, service.AuthorizationRefusedErrorType, errorObj["type"])
		assert.NotContains(t, w.Body.String(), "Upstream request failed")
	})

	t.Run("non-refusal errors still delegate to the generic fallback", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

		h := &GatewayHandler{}
		wrote := h.ensureForwardErrorResponseFor(c, errors.New("dial failure"), false)

		require.True(t, wrote)
		require.Equal(t, http.StatusBadGateway, w.Code)
		assert.Contains(t, w.Body.String(), "Upstream request failed")
	})

	t.Run("wrapped refusals match too", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

		h := &GatewayHandler{}
		err := errors.Join(testAuthorizationRefusal)
		wrote := h.ensureForwardErrorResponseFor(c, err, false)
		require.True(t, wrote)
		require.Equal(t, http.StatusPaymentRequired, w.Code)
	})
}

// The settling goroutine's captured token: a handle with one settled write in
// the request context yields its token; an absent context yields "".
func TestAuthorizationTokenCaptureForCyberPolicyPath(t *testing.T) {
	h := &service.AuthorizationHandle{ID: "auth_cyber", Mode: "shadow"}
	tok := h.MintWriteToken()
	h.RecordOutcome(tok, service.AuthorizationOutcomeResult, nil)

	ctx := service.WithAuthorizationHandle(context.Background(), h)
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request = c.Request.WithContext(ctx)

	captured := service.AuthorizationHandleFromContext(c.Request.Context())
	require.Equal(t, tok, service.AuthorizationTokenOf(captured))
	require.Equal(t, h.ID, service.AuthorizationIDOf(captured))

	// No handle in context — both fields read "".
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	require.Equal(t, "", service.AuthorizationTokenOf(service.AuthorizationHandleFromContext(c2.Request.Context())))
	require.Equal(t, "", service.AuthorizationIDOf(service.AuthorizationHandleFromContext(c2.Request.Context())))
}
