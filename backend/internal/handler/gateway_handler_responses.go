package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// Responses handles OpenAI Responses API endpoint for Anthropic platform groups.
// POST /v1/responses
// This converts Responses API requests to Anthropic format, forwards to Anthropic
// upstream, and converts responses back to Responses format.
func (h *GatewayHandler) Responses(c *gin.Context) {
	streamStarted := false

	requestStart := time.Now()

	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.responsesErrorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.responsesErrorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.gateway.responses",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)

	// Read request body
	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.responsesErrorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}

	if len(body) == 0 {
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}

	setOpsRequestContext(c, "", false)

	// Validate JSON
	if !gjson.ValidBytes(body) {
		logRequestBodyParseFailure(reqLog, body, nil)
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}

	// Extract model and stream using gjson (like OpenAI handler)
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String || modelResult.String() == "" {
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	reqModel := modelResult.String()
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !compositeTargetPlatformResolved(c, apiKey, reqModel) {
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by composite groups")
		return
	}
	reqStream, ok := parseOpenAICompatibleStream(body)
	if !ok {
		h.responsesErrorResponse(c, http.StatusBadRequest, "invalid_request_error", invalidStreamFieldTypeMessage)
		return
	}
	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", reqStream))

	setOpsRequestContext(c, reqModel, reqStream)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(reqStream, false)))
	requestCtx := c.Request.Context()
	if service.IsImageGenerationIntentForPlatform("/v1/responses", reqModel, body, openAICompatibleRequestPlatform(c.Request.Context(), apiKey)) {
		requestCtx = service.WithOpenAIImageGenerationIntent(requestCtx)
	}

	// Claude Code only restriction:
	// /v1/responses is never a Claude Code endpoint.
	// When claude_code_only is enabled, this endpoint is rejected.
	// The existing service-layer checkClaudeCodeRestriction handles degradation
	// to fallback groups when the Forward path calls SelectAccountForModelWithExclusions.
	// Here we just reject at handler level since /v1/responses clients can't be Claude Code.
	if apiKey.Group != nil && apiKey.Group.ClaudeCodeOnly {
		h.responsesErrorResponse(c, http.StatusForbidden, "permission_error",
			"This group is restricted to Claude Code clients (/v1/messages only)")
		return
	}

	if decision := h.checkSecurityAudit(c, reqLog, apiKey, subject, service.ContentModerationProtocolOpenAIResponses, reqModel, body); decision != nil && !decision.AllowNextStage {
		h.responsesSecurityAuditError(c, decision)
		return
	}

	// Error passthrough binding
	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	candidates, err := h.channelCandidates(requestCtx, apiKey, reqModel)
	if err != nil {
		h.responsesErrorResponse(c, http.StatusServiceUnavailable, "server_error", err.Error())
		return
	}

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())

	userReleaseFunc, err := h.concurrencyHelper.AcquireUserSlotWithWait(c, subject.UserID, subject.Concurrency, reqStream, &streamStarted)
	if err != nil {
		reqLog.Warn("gateway.responses.user_slot_acquire_failed", zap.Error(err))
		h.handleConcurrencyError(c, err, "user", streamStarted)
		return
	}
	userReleaseFunc = wrapReleaseOnDone(c.Request.Context(), userReleaseFunc)
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	// Parse request for session hash
	bodyRef := service.NewRequestBodyRef(body)
	parsedReq, _ := service.ParseGatewayRequest(bodyRef, "responses")
	if parsedReq == nil {
		parsedReq = &service.ParsedRequest{Model: reqModel, Stream: reqStream, Body: bodyRef}
	}
	parsedReq.SessionContext = &service.SessionContext{
		ClientIP:  ip.GetClientIP(c),
		UserAgent: c.GetHeader("User-Agent"),
		APIKeyID:  apiKey.ID,
	}
	sessionHash := h.gatewayService.GenerateSessionHash(parsedReq)

	var lastChannelErr error
	for candidateIndex, candidate := range candidates {
		candidateStartedAt := time.Now()
		routedKey := candidate.Apply(apiKey)
		applyRoutedCandidateContext(c, routedKey)
		requestCtx = c.Request.Context()
		channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(requestCtx, routedKey.GroupID, reqModel)
		billingModel := reqModel
		if channelMapping.Mapped {
			billingModel = channelMapping.MappedModel
		}
		candidateSubscription, err := routedCandidateSubscription(requestCtx, h.apiKeyService, routedKey, subscription)
		if err != nil {
			h.responsesErrorResponse(c, http.StatusServiceUnavailable, "server_error", "Unable to validate channel subscription")
			return
		}
		if err := h.billingCacheService.CheckBillingEligibility(requestCtx, routedKey.User, routedKey, routedKey.Group, candidateSubscription, service.QuotaPlatform(requestCtx, routedKey)); err != nil {
			reqLog.Info("gateway.responses.billing_check_failed", zap.Int64("candidate_group_id", candidate.Group.ID), zap.Error(err))
			if isRecoverableChannelBillingError(err) && candidateIndex < len(candidates)-1 {
				lastChannelErr = err
				continue
			}
			status, code, message, retryAfter := billingErrorDetails(err)
			if retryAfter > 0 {
				c.Header("Retry-After", strconv.Itoa(retryAfter))
			}
			h.responsesErrorResponse(c, status, code, message)
			return
		}
		if err := h.gatewayService.EnsureModelPricing(requestCtx, routedKey, billingModel); err != nil {
			lastChannelErr = err
			if candidateIndex < len(candidates)-1 {
				continue
			}
			h.responsesErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Model pricing is not configured")
			return
		}

		fs := NewFailoverState(h.maxAccountSwitches, false)
		for {
			if requestCtx.Err() != nil {
				return
			}
			selection, err := h.gatewayService.SelectAccountWithLoadAwareness(requestCtx, routedKey.GroupID, sessionHash, reqModel, fs.FailedAccountIDs, "", int64(0))
			if err != nil {
				if len(fs.FailedAccountIDs) == 0 {
					lastChannelErr = err
					break
				}
				if errors.Is(lastChannelErr, service.ErrModelPricingUnavailable) {
					break
				}
				action := fs.HandleSelectionExhausted(requestCtx)
				switch action {
				case FailoverContinue:
					continue
				case FailoverCanceled:
					failoverClientGone(c)
					return
				default:
					lastChannelErr = fs.LastFailoverErr
				}
				break
			}
			if selection == nil {
				lastChannelErr = service.ErrNoChannelRoutingCandidate
				break
			}
			account := selection.Account
			if err := h.gatewayService.EnsureModelPricing(requestCtx, routedKey, account.GetMappedModel(billingModel)); err != nil {
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				fs.FailedAccountIDs[account.ID] = struct{}{}
				lastChannelErr = err
				reqLog.Warn("gateway.responses.model_pricing_unavailable", zap.Int64("account_id", account.ID), zap.Error(err))
				continue
			}
			setOpsSelectedAccount(c, account.ID, account.Platform)

			accountReleaseFunc := selection.ReleaseFunc
			if !selection.Acquired {
				if selection.WaitPlan == nil {
					lastChannelErr = service.ErrNoChannelRoutingCandidate
					break
				}
				accountReleaseFunc, err = h.concurrencyHelper.AcquireAccountSlotWithWaitTimeout(
					c, account.ID, selection.WaitPlan.MaxConcurrency, selection.WaitPlan.Timeout,
					reqStream, &streamStarted,
				)
				if err != nil {
					reqLog.Warn("gateway.responses.account_slot_acquire_failed", zap.Int64("account_id", account.ID), zap.Error(err))
					h.handleConcurrencyError(c, err, "account", streamStarted)
					return
				}
			}
			accountReleaseFunc = wrapReleaseOnDone(c.Request.Context(), accountReleaseFunc)

			writerSizeBeforeForward := c.Writer.Size()
			forwardBody := body
			if channelMapping.Mapped {
				forwardBody = h.gatewayService.ReplaceModelInBody(body, channelMapping.MappedModel)
			}
			var result *service.ForwardResult
			setActualUpstreamEndpoint(c, "")
			if shouldUseAntigravityCompat(account) {
				if h.antigravityGatewayService == nil {
					h.responsesErrorResponse(c, http.StatusBadGateway, "upstream_error", "Antigravity compatibility service is not configured")
					if accountReleaseFunc != nil {
						accountReleaseFunc()
					}
					return
				}
				setActualUpstreamEndpoint(c, EndpointAntigravityGenerateContent)
				result, err = h.antigravityGatewayService.ForwardAsResponses(requestCtx, c, account, forwardBody, parsedReq)
			} else {
				result, err = h.gatewayService.ForwardAsResponses(requestCtx, c, account, forwardBody, parsedReq)
			}
			if accountReleaseFunc != nil {
				accountReleaseFunc()
			}

			if err != nil {
				var failoverErr *service.UpstreamFailoverError
				if errors.As(err, &failoverErr) {
					if c.Writer.Size() != writerSizeBeforeForward {
						h.handleResponsesFailoverExhausted(c, failoverErr, true)
						return
					}
					action := fs.HandleFailoverError(requestCtx, h.gatewayService, account.ID, account.Platform, account.GetPoolModeRetryCount(), failoverErr)
					switch action {
					case FailoverContinue:
						continue
					case FailoverExhausted:
						lastChannelErr = fs.LastFailoverErr
					case FailoverCanceled:
						failoverClientGone(c)
						return
					}
					if lastChannelErr != nil {
						break
					}
				}
				upstreamErrorAlreadyCommunicated := gatewayForwardErrorAlreadyCommunicated(c, writerSizeBeforeForward, err)
				wroteFallback := false
				if !upstreamErrorAlreadyCommunicated {
					wroteFallback = h.ensureForwardErrorResponse(c, streamStarted)
				}
				reqLog.Error("gateway.responses.forward_failed",
					zap.Int64("account_id", account.ID),
					zap.Bool("fallback_error_response_written", wroteFallback),
					zap.Bool("upstream_error_response_already_written", upstreamErrorAlreadyCommunicated),
					zap.Error(err),
				)
				return
			}

			userAgent := c.GetHeader("User-Agent")
			clientIP := ip.GetClientIP(c)
			requestPayloadHash := service.HashUsageRequestPayload(body)
			inboundEndpoint := GetInboundEndpoint(c)
			upstreamEndpoint := GetUpstreamEndpoint(c, account.Platform)
			quotaPlatform := service.QuotaPlatform(c.Request.Context(), routedKey)
			sessionID := service.ExtractClientSessionID(c)
			h.submitUsageRecordTask(c.Request.Context(), func(ctx context.Context) {
				if err := h.gatewayService.RecordUsage(ctx, &service.RecordUsageInput{
					Result: result, QuotaPlatform: quotaPlatform, APIKey: routedKey, User: routedKey.User,
					Account: account, Subscription: candidateSubscription, InboundEndpoint: inboundEndpoint,
					UpstreamEndpoint: upstreamEndpoint, UserAgent: userAgent, IPAddress: clientIP,
					RequestPayloadHash: requestPayloadHash, APIKeyService: h.apiKeyService, SessionID: sessionID,
					ChannelUsageFields: routedChannelUsageFields(c, channelMapping, reqModel, result.UpstreamModel, candidate.ChannelID),
				}); err != nil {
					reqLog.Error("gateway.responses.record_usage_failed", zap.Int64("account_id", account.ID), zap.Error(err))
				}
			})
			reqLog.Info("gateway.responses.channel_succeeded",
				zap.Int64("channel_id", candidate.ChannelID), zap.Int64("group_id", candidate.Group.ID),
				zap.Int64("account_id", account.ID), zap.Int64("duration_ms", time.Since(candidateStartedAt).Milliseconds()),
				zap.Float64("effective_multiplier", candidate.EffectiveMultiplier))
			return
		}
		if lastChannelErr != nil && candidateIndex < len(candidates)-1 && c.Writer.Size() <= 0 {
			reqLog.Info("gateway.responses.channel_failover",
				zap.Int64("channel_id", candidate.ChannelID), zap.Int64("group_id", candidate.Group.ID),
				zap.Int("candidate_index", candidateIndex), zap.Int64("duration_ms", time.Since(candidateStartedAt).Milliseconds()),
				zap.Error(lastChannelErr))
			continue
		}
		break
	}
	if requestCtx.Err() != nil || c.Writer.Size() > 0 {
		return
	}
	if errors.Is(lastChannelErr, service.ErrModelPricingUnavailable) {
		h.responsesErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Model pricing is not configured")
		return
	}
	if failoverErr, ok := lastChannelErr.(*service.UpstreamFailoverError); ok {
		h.handleResponsesFailoverExhausted(c, failoverErr, streamStarted)
		return
	}
	h.responsesErrorResponse(c, http.StatusBadGateway, "server_error", "All enabled channels exhausted")
}

// responsesErrorResponse writes an error in OpenAI Responses API format.
func (h *GatewayHandler) responsesErrorResponse(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"code":    code,
			"message": message,
		},
	})
}

// handleResponsesFailoverExhausted writes a failover-exhausted error in Responses format.
func (h *GatewayHandler) handleResponsesFailoverExhausted(c *gin.Context, lastErr *service.UpstreamFailoverError, streamStarted bool) {
	if streamStarted {
		return // Can't write error after stream started
	}
	if lastErr != nil {
		copyFailoverRetryAfter(c, lastErr.ResponseHeaders)
	}
	if lastErr != nil && lastErr.IsCredentialFailure() {
		status, message := credentialFailoverClientResponse(lastErr)
		h.responsesErrorResponse(c, status, "server_error", message)
		return
	}
	statusCode := http.StatusBadGateway
	if lastErr != nil && lastErr.StatusCode > 0 {
		statusCode = lastErr.StatusCode
	}
	if lastErr != nil && service.IsOpenAISilentRefusalErrorBody(lastErr.ResponseBody) {
		service.SetOpsUpstreamError(c, statusCode, service.OpenAISilentRefusalClientMessage(), "")
		h.responsesErrorResponse(c, http.StatusBadGateway, "upstream_error", service.OpenAISilentRefusalClientMessage())
		return
	}
	h.responsesErrorResponse(c, statusCode, "server_error", "All available accounts exhausted")
}
