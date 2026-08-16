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

// ChatCompletions handles OpenAI Chat Completions API endpoint for Anthropic platform groups.
// POST /v1/chat/completions
// This converts Chat Completions requests to Anthropic format (via Responses format chain),
// forwards to Anthropic upstream, and converts responses back to Chat Completions format.
func (h *GatewayHandler) ChatCompletions(c *gin.Context) {
	streamStarted := false

	requestStart := time.Now()

	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.chatCompletionsErrorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.chatCompletionsErrorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.gateway.chat_completions",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)

	// Read request body
	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.chatCompletionsErrorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}

	if len(body) == 0 {
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}

	setOpsRequestContext(c, "", false)

	// Validate JSON
	if !gjson.ValidBytes(body) {
		logRequestBodyParseFailure(reqLog, body, nil)
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}

	// Extract model and stream
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String || modelResult.String() == "" {
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	reqModel := modelResult.String()
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !compositeTargetPlatformResolved(c, apiKey, reqModel) {
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by composite groups")
		return
	}
	reqStream, ok := parseOpenAICompatibleStream(body)
	if !ok {
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", invalidStreamFieldTypeMessage)
		return
	}
	if service.IsGPTImageGenerationModel(reqModel) {
		h.chatCompletionsErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "This model is not supported on the Chat Completions endpoint")
		return
	}
	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", reqStream))

	setOpsRequestContext(c, reqModel, reqStream)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(reqStream, false)))

	// /v1/chat/completions is not a Claude Code endpoint. Keep this legacy
	// restriction ahead of billing and concurrency acquisition.
	if apiKey.Group != nil && apiKey.Group.ClaudeCodeOnly {
		h.chatCompletionsErrorResponse(c, http.StatusForbidden, "permission_error",
			"This group is restricted to Claude Code clients (/v1/messages only)")
		return
	}

	if decision := h.checkSecurityAudit(c, reqLog, apiKey, subject, service.ContentModerationProtocolOpenAIChat, reqModel, body); decision != nil && !decision.AllowNextStage {
		h.openAISecurityAuditError(c, decision)
		return
	}

	// Error passthrough binding
	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	candidates, err := h.channelCandidates(c.Request.Context(), apiKey, reqModel)
	if err != nil {
		h.chatCompletionsErrorResponse(c, http.StatusServiceUnavailable, "server_error", err.Error())
		return
	}

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())

	userReleaseFunc, err := h.concurrencyHelper.AcquireUserSlotWithWait(c, subject.UserID, subject.Concurrency, reqStream, &streamStarted)
	if err != nil {
		reqLog.Warn("gateway.cc.user_slot_acquire_failed", zap.Error(err))
		h.handleConcurrencyError(c, err, "user", streamStarted)
		return
	}
	userReleaseFunc = wrapReleaseOnDone(c.Request.Context(), userReleaseFunc)
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	// Parse request for session hash
	bodyRef := service.NewRequestBodyRef(body)
	parsedReq, _ := service.ParseGatewayRequest(bodyRef, "chat_completions")
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
		channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(c.Request.Context(), routedKey.GroupID, reqModel)
		billingModel := reqModel
		if channelMapping.Mapped {
			billingModel = channelMapping.MappedModel
		}
		if routedKey.Group != nil && routedKey.Group.ClaudeCodeOnly {
			continue
		}
		candidateSubscription, err := routedCandidateSubscription(c.Request.Context(), h.apiKeyService, routedKey, subscription)
		if err != nil {
			h.chatCompletionsErrorResponse(c, http.StatusServiceUnavailable, "server_error", "Unable to validate channel subscription")
			return
		}
		if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), routedKey.User, routedKey, routedKey.Group, candidateSubscription, service.QuotaPlatform(c.Request.Context(), routedKey)); err != nil {
			reqLog.Info("gateway.cc.billing_check_failed", zap.Int64("candidate_group_id", candidate.Group.ID), zap.Error(err))
			if isRecoverableChannelBillingError(err) && candidateIndex < len(candidates)-1 {
				lastChannelErr = err
				continue
			}
			status, code, message, retryAfter := billingErrorDetails(err)
			if retryAfter > 0 {
				c.Header("Retry-After", strconv.Itoa(retryAfter))
			}
			h.chatCompletionsErrorResponse(c, status, code, message)
			return
		}
		if err := h.gatewayService.EnsureModelPricing(c.Request.Context(), routedKey, billingModel); err != nil {
			lastChannelErr = err
			if candidateIndex < len(candidates)-1 {
				continue
			}
			h.chatCompletionsErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Model pricing is not configured")
			return
		}

		groupPlatform := effectiveAPIKeyPlatform(c, routedKey)
		selectionSessionHash := sessionHash
		if groupPlatform == service.PlatformGemini && selectionSessionHash != "" {
			selectionSessionHash = "gemini:" + selectionSessionHash
		}
		fs := NewFailoverState(h.maxAccountSwitches, false)
		if groupPlatform == service.PlatformGemini {
			fs = NewFailoverState(h.maxAccountSwitchesGemini, false)
		}

		for {
			if c.Request.Context().Err() != nil {
				return
			}
			selection, err := h.gatewayService.SelectAccountWithLoadAwareness(c.Request.Context(), routedKey.GroupID, selectionSessionHash, reqModel, fs.FailedAccountIDs, "", int64(0))
			if err != nil {
				if len(fs.FailedAccountIDs) == 0 {
					lastChannelErr = err
					break
				}
				if errors.Is(lastChannelErr, service.ErrModelPricingUnavailable) {
					break
				}
				action := fs.HandleSelectionExhausted(c.Request.Context())
				switch action {
				case FailoverContinue:
					continue
				case FailoverCanceled:
					failoverClientGone(c)
					return
				default:
					lastChannelErr = fs.LastFailoverErr
					break
				}
				break
			}
			if selection == nil {
				break
			}
			account := selection.Account
			if err := h.gatewayService.EnsureModelPricing(c.Request.Context(), routedKey, account.GetMappedModel(billingModel)); err != nil {
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				fs.FailedAccountIDs[account.ID] = struct{}{}
				lastChannelErr = err
				reqLog.Warn("gateway.cc.model_pricing_unavailable", zap.Int64("account_id", account.ID), zap.Error(err))
				continue
			}
			setOpsSelectedAccount(c, account.ID, account.Platform)

			// 4. Acquire account concurrency slot
			accountReleaseFunc := selection.ReleaseFunc
			if !selection.Acquired {
				if selection.WaitPlan == nil {
					lastChannelErr = service.ErrNoChannelRoutingCandidate
					break
				}
				accountReleaseFunc, err = h.concurrencyHelper.AcquireAccountSlotWithWaitTimeout(
					c,
					account.ID,
					selection.WaitPlan.MaxConcurrency,
					selection.WaitPlan.Timeout,
					reqStream,
					&streamStarted,
				)
				if err != nil {
					reqLog.Warn("gateway.cc.account_slot_acquire_failed", zap.Int64("account_id", account.ID), zap.Error(err))
					h.handleConcurrencyError(c, err, "account", streamStarted)
					return
				}
			}
			accountReleaseFunc = wrapReleaseOnDone(c.Request.Context(), accountReleaseFunc)

			if groupPlatform == service.PlatformGemini && account.Platform != service.PlatformGemini {
				if accountReleaseFunc != nil {
					accountReleaseFunc()
				}
				fs.FailedAccountIDs[account.ID] = struct{}{}
				continue
			}

			// 5. Forward request
			writerSizeBeforeForward := c.Writer.Size()
			forwardBody := body
			if channelMapping.Mapped {
				forwardBody = h.gatewayService.ReplaceModelInBody(body, channelMapping.MappedModel)
			}
			var result *service.ForwardResult
			setActualUpstreamEndpoint(c, "")
			if account.Platform == service.PlatformGemini {
				if h.geminiCompatService == nil {
					h.chatCompletionsErrorResponse(c, http.StatusBadGateway, "upstream_error", "Gemini compatibility service is not configured")
					if accountReleaseFunc != nil {
						accountReleaseFunc()
					}
					return
				}
				result, err = h.geminiCompatService.ForwardAsChatCompletions(c.Request.Context(), c, account, forwardBody)
			} else if shouldUseAntigravityCompat(account) {
				if h.antigravityGatewayService == nil {
					h.chatCompletionsErrorResponse(c, http.StatusBadGateway, "upstream_error", "Antigravity compatibility service is not configured")
					if accountReleaseFunc != nil {
						accountReleaseFunc()
					}
					return
				}
				setActualUpstreamEndpoint(c, EndpointAntigravityGenerateContent)
				result, err = h.antigravityGatewayService.ForwardAsChatCompletions(c.Request.Context(), c, account, forwardBody, parsedReq)
			} else {
				result, err = h.gatewayService.ForwardAsChatCompletions(c.Request.Context(), c, account, forwardBody, parsedReq)
			}

			if accountReleaseFunc != nil {
				accountReleaseFunc()
			}

			if err != nil {
				var failoverErr *service.UpstreamFailoverError
				if errors.As(err, &failoverErr) {
					if c.Writer.Size() != writerSizeBeforeForward {
						h.handleCCFailoverExhausted(c, failoverErr, true)
						return
					}
					action := fs.HandleFailoverError(c.Request.Context(), h.gatewayService, account.ID, account.Platform, account.GetPoolModeRetryCount(), failoverErr)
					switch action {
					case FailoverContinue:
						continue
					case FailoverExhausted:
						lastChannelErr = fs.LastFailoverErr
						break
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
				reqLog.Error("gateway.cc.forward_failed",
					zap.Int64("account_id", account.ID),
					zap.Bool("fallback_error_response_written", wroteFallback),
					zap.Bool("upstream_error_response_already_written", upstreamErrorAlreadyCommunicated),
					zap.Error(err),
				)
				return
			}

			// 6. Record usage
			userAgent := c.GetHeader("User-Agent")
			clientIP := ip.GetClientIP(c)
			requestPayloadHash := service.HashUsageRequestPayload(body)
			inboundEndpoint := GetInboundEndpoint(c)
			upstreamEndpoint := GetUpstreamEndpoint(c, account.Platform)

			quotaPlatform := service.QuotaPlatform(c.Request.Context(), routedKey)
			sessionID := service.ExtractClientSessionID(c)
			h.submitUsageRecordTask(c.Request.Context(), func(ctx context.Context) {
				if err := h.gatewayService.RecordUsage(ctx, &service.RecordUsageInput{
					Result:             result,
					QuotaPlatform:      quotaPlatform,
					APIKey:             routedKey,
					User:               routedKey.User,
					Account:            account,
					Subscription:       candidateSubscription,
					InboundEndpoint:    inboundEndpoint,
					UpstreamEndpoint:   upstreamEndpoint,
					UserAgent:          userAgent,
					IPAddress:          clientIP,
					RequestPayloadHash: requestPayloadHash,
					APIKeyService:      h.apiKeyService,
					SessionID:          sessionID,
					ChannelUsageFields: routedChannelUsageFields(c, channelMapping, reqModel, result.UpstreamModel, candidate.ChannelID),
				}); err != nil {
					reqLog.Error("gateway.cc.record_usage_failed",
						zap.Int64("account_id", account.ID),
						zap.Error(err),
					)
				}
			})
			reqLog.Info("gateway.cc.channel_succeeded",
				zap.Int64("channel_id", candidate.ChannelID), zap.Int64("group_id", candidate.Group.ID),
				zap.Int64("account_id", account.ID), zap.Int64("duration_ms", time.Since(candidateStartedAt).Milliseconds()),
				zap.Float64("effective_multiplier", candidate.EffectiveMultiplier))
			return
		}
		if lastChannelErr != nil && candidateIndex < len(candidates)-1 && c.Writer.Size() <= 0 {
			reqLog.Info("gateway.cc.channel_failover",
				zap.Int64("channel_id", candidate.ChannelID),
				zap.Int64("group_id", candidate.Group.ID),
				zap.Int("candidate_index", candidateIndex),
				zap.Int64("duration_ms", time.Since(candidateStartedAt).Milliseconds()),
				zap.Error(lastChannelErr),
			)
			continue
		}
		break
	}
	if c.Request.Context().Err() != nil || c.Writer.Size() > 0 {
		return
	}
	if errors.Is(lastChannelErr, service.ErrModelPricingUnavailable) {
		h.chatCompletionsErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Model pricing is not configured")
		return
	}
	if failoverErr, ok := lastChannelErr.(*service.UpstreamFailoverError); ok {
		h.handleCCFailoverExhausted(c, failoverErr, streamStarted)
		return
	}
	h.chatCompletionsErrorResponse(c, http.StatusBadGateway, "server_error", "All enabled channels exhausted")
}

// chatCompletionsErrorResponse writes an error in OpenAI Chat Completions format.
func (h *GatewayHandler) chatCompletionsErrorResponse(c *gin.Context, status int, errType, message string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"type":    errType,
			"message": message,
		},
	})
}

// handleCCFailoverExhausted writes a failover-exhausted error in CC format.
func (h *GatewayHandler) handleCCFailoverExhausted(c *gin.Context, lastErr *service.UpstreamFailoverError, streamStarted bool) {
	if streamStarted {
		return
	}
	if lastErr != nil {
		copyFailoverRetryAfter(c, lastErr.ResponseHeaders)
	}
	if lastErr != nil && lastErr.IsCredentialFailure() {
		status, message := credentialFailoverClientResponse(lastErr)
		h.chatCompletionsErrorResponse(c, status, "server_error", message)
		return
	}
	statusCode := http.StatusBadGateway
	if lastErr != nil && lastErr.StatusCode > 0 {
		statusCode = lastErr.StatusCode
	}
	if lastErr != nil && service.IsOpenAISilentRefusalErrorBody(lastErr.ResponseBody) {
		service.SetOpsUpstreamError(c, statusCode, service.OpenAISilentRefusalClientMessage(), "")
		h.chatCompletionsErrorResponse(c, http.StatusBadGateway, "upstream_error", service.OpenAISilentRefusalClientMessage())
		return
	}
	h.chatCompletionsErrorResponse(c, statusCode, "server_error", "All available accounts exhausted")
}
