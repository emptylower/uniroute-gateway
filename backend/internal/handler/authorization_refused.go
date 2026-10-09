package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// refusedReasonForLog extracts the refusal reason string from an authorization
// refusal error for surfaces without a dedicated error-type slot (Gemini's
// googleError writes it into error.message).
func refusedReasonForLog(err error) string {
	if refused, ok := service.AsAuthorizationRefused(err); ok {
		return string(refused.Reason)
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

// ensureForwardErrorResponseFor maps a wallet authorization refusal to its named
// client-visible shape (spec §2.0: never a 502, never a failover) and delegates
// everything else to ensureForwardErrorResponse.
func (h *OpenAIGatewayHandler) ensureForwardErrorResponseFor(c *gin.Context, err error, streamStarted bool) bool {
	if status, retry, ok := service.WalletRiskRefusalDetails(err); ok {
		if c == nil || c.Writer == nil || service.IsResponseCommitted(c) {
			return false
		}
		c.Header("Retry-After", strconv.Itoa(retry))
		h.handleStreamingAwareError(c, status, "wallet_risk_refused", err.Error(), streamStarted || c.Writer.Written())
		return true
	}
	if refused, ok := service.AsAuthorizationRefused(err); ok {
		if c == nil || c.Writer == nil || service.IsResponseCommitted(c) {
			return false
		}
		h.handleStreamingAwareError(c, service.AuthorizationRefusedHTTPStatus, service.AuthorizationRefusedErrorType, service.AuthorizationRefusedMessage+": "+string(refused.Reason), streamStarted || c.Writer.Written())
		return true
	}
	return h.ensureForwardErrorResponse(c, streamStarted)
}

func (h *GatewayHandler) ensureForwardErrorResponseFor(c *gin.Context, err error, streamStarted bool) bool {
	if status, retry, ok := service.WalletRiskRefusalDetails(err); ok {
		if c == nil || c.Writer == nil || service.IsResponseCommitted(c) {
			return false
		}
		c.Header("Retry-After", strconv.Itoa(retry))
		h.handleStreamingAwareError(c, status, "wallet_risk_refused", err.Error(), streamStarted || c.Writer.Written())
		return true
	}
	if refused, ok := service.AsAuthorizationRefused(err); ok {
		if c == nil || c.Writer == nil || service.IsResponseCommitted(c) {
			return false
		}
		h.handleStreamingAwareError(c, service.AuthorizationRefusedHTTPStatus, service.AuthorizationRefusedErrorType, service.AuthorizationRefusedMessage+": "+string(refused.Reason), streamStarted || c.Writer.Written())
		return true
	}
	return h.ensureForwardErrorResponse(c, streamStarted)
}

// Initial prehold refusal sites must call this before their legacy 402 path.
func writeWalletRiskResponse(c *gin.Context, err error) bool {
	status, retry, ok := service.WalletRiskRefusalDetails(err)
	if !ok || c == nil || c.Writer == nil {
		return false
	}
	c.Header("Retry-After", strconv.Itoa(retry))
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"type": "wallet_risk_refused", "message": err.Error()}})
	return true
}
