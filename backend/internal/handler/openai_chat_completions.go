package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// ChatCompletions handles OpenAI Chat Completions API requests.
// POST /v1/chat/completions
func (h *OpenAIGatewayHandler) ChatCompletions(c *gin.Context) {
	streamStarted := false
	defer h.recoverResponsesPanic(c, &streamStarted)

	requestStart := time.Now()

	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.openai_gateway.chat_completions",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)

	if !h.ensureResponsesDependencies(c, reqLog) {
		return
	}

	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.errorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}

	if !gjson.ValidBytes(body) {
		logRequestBodyParseFailure(reqLog, body, nil)
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}

	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String || modelResult.String() == "" {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	reqModel := modelResult.String()
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !openAICompatibleTextTargetAllowed(c, apiKey, reqModel) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by this OpenAI-compatible endpoint for composite groups")
		return
	}
	reqStream, ok := parseOpenAICompatibleStream(body)
	if !ok {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", invalidStreamFieldTypeMessage)
		return
	}
	if service.IsGPTImageGenerationModel(reqModel) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "This model is not supported on the Chat Completions endpoint")
		return
	}

	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", reqStream))

	setOpsRequestContext(c, reqModel, reqStream)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(reqStream, false)))

	if decision := h.checkSecurityAudit(c, reqLog, apiKey, subject, service.ContentModerationProtocolOpenAIChat, reqModel, body); decision != nil && !decision.AllowNextStage {
		h.openAISecurityAuditError(c, decision)
		return
	}
	if h.rejectIfCyberSessionBlocked(c, apiKey, body, reqModel, cyberBlockFormatChat) {
		return
	}

	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	candidates, err := h.channelCandidates(c.Request.Context(), apiKey, reqModel)
	if err != nil {
		h.errorResponse(c, http.StatusServiceUnavailable, "server_error", err.Error())
		return
	}

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
	routingStart := time.Now()

	userReleaseFunc, acquired := h.acquireResponsesUserSlot(c, subject.UserID, subject.Concurrency, reqStream, &streamStarted, reqLog)
	if !acquired {
		return
	}
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	sessionHash := h.gatewayService.GenerateSessionHash(c, body)
	promptCacheKey := h.gatewayService.ExtractSessionID(c, body)

	var lastChannelErr error
	var finalFailoverErr *service.UpstreamFailoverError
	for candidateIndex, candidate := range candidates {
		candidateStartedAt := time.Now()
		routedKey := candidate.Apply(apiKey)
		applyRoutedCandidateContext(c, routedKey)
		candidateSubscription, err := routedCandidateSubscription(c.Request.Context(), h.apiKeyService, routedKey, subscription)
		if err != nil {
			h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "server_error", "Unable to validate channel subscription", streamStarted)
			return
		}
		if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), routedKey.User, routedKey, routedKey.Group, candidateSubscription, routedKey.Group.Platform); err != nil {
			reqLog.Info("openai_chat_completions.billing_eligibility_check_failed", zap.Int64("candidate_group_id", candidate.Group.ID), zap.Error(err))
			if isRecoverableChannelBillingError(err) && candidateIndex < len(candidates)-1 {
				lastChannelErr = err
				continue
			}
			status, code, message, retryAfter := billingErrorDetails(err)
			if retryAfter > 0 {
				c.Header("Retry-After", strconv.Itoa(retryAfter))
			}
			h.handleStreamingAwareError(c, status, code, message, streamStarted)
			return
		}

		candidateBody := body
		if routedKey.Group != nil && routedKey.Group.Platform == service.PlatformOpenAI {
			if cappedBody, changed := service.ApplyOpenAIReasoningEffortPolicy(candidateBody, routedKey.Group.MaxReasoningEffort, routedKey.Group.ReasoningEffortMappings); changed {
				candidateBody = cappedBody
			}
		}
		channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(c.Request.Context(), routedKey.GroupID, reqModel)
		billingModel := reqModel
		if channelMapping.Mapped {
			billingModel = channelMapping.MappedModel
		}
		if err := h.gatewayService.EnsureModelPricing(c.Request.Context(), routedKey, billingModel); err != nil {
			lastChannelErr = err
			if candidateIndex < len(candidates)-1 {
				continue
			}
			h.errorResponse(c, http.StatusServiceUnavailable, "api_error", "Model pricing is not configured")
			return
		}
		requestPlatform := openAICompatibleRequestPlatform(c.Request.Context(), routedKey)
		candidateSessionHash := sessionHash
		maxAccountSwitches := h.maxAccountSwitches
		switchCount := 0
		failedAccountIDs := make(map[int64]struct{})
		sameAccountRetryCount := make(map[int64]int)
		var lastFailoverErr *service.UpstreamFailoverError
		var oauth429FailoverState service.OpenAIOAuth429FailoverState
		lastChannelErr = nil

		for {
			if failoverClientGone(c) {
				return
			}
			reqLog.Debug("openai_chat_completions.account_selecting", zap.Int("excluded_account_count", len(failedAccountIDs)))
			selection, scheduleDecision, err := h.gatewayService.SelectAccountWithSchedulerForCapability(
				c.Request.Context(),
				routedKey.GroupID,
				"",
				candidateSessionHash,
				reqModel,
				failedAccountIDs,
				service.OpenAIUpstreamTransportAny,
				service.OpenAIEndpointCapabilityChatCompletions,
				false,
				false,
				true,
				requestPlatform,
			)
			if err != nil {
				if failoverClientGone(c) {
					reqLog.Info("openai_chat_completions.account_select_aborted_client_disconnected", zap.Error(err))
					return
				}
				reqLog.Warn("openai_chat_completions.account_select_failed",
					zap.Error(openAICompatibleSelectionErrorForLog(err, requestPlatform)),
					zap.Int("excluded_account_count", len(failedAccountIDs)),
				)
				if len(failedAccountIDs) == 0 {
					cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, routedKey, reqModel, reqModel)
					if !cls.ModelNotFound {
						markOpsRoutingCapacityLimitedIfNoAvailable(c, err)
					}
					lastChannelErr = err
					break
				} else {
					if lastFailoverErr != nil {
						lastChannelErr = lastFailoverErr
						finalFailoverErr = lastFailoverErr
					} else {
						lastChannelErr = err
					}
					break
				}
			}
			if selection == nil || selection.Account == nil {
				cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, routedKey, reqModel, reqModel)
				if !cls.ModelNotFound {
					markOpsRoutingCapacityLimited(c)
				}
				lastChannelErr = service.ErrNoChannelRoutingCandidate
				break
			}
			account := selection.Account
			if err := h.gatewayService.EnsureModelPricing(c.Request.Context(), routedKey, account.GetMappedModel(billingModel)); err != nil {
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				failedAccountIDs[account.ID] = struct{}{}
				lastChannelErr = err
				continue
			}
			candidateSessionHash = ensureOpenAIPoolModeSessionHash(candidateSessionHash, account)
			reqLog.Debug("openai_chat_completions.account_selected", zap.Int64("account_id", account.ID), zap.String("account_name", account.Name))
			_ = scheduleDecision
			setOpsSelectedAccount(c, account.ID, account.Platform)

			accountReleaseFunc, acquired := h.acquireResponsesAccountSlot(c, routedKey.GroupID, candidateSessionHash, selection, reqStream, &streamStarted, reqLog)
			if !acquired {
				return
			}

			service.SetOpsLatencyMs(c, service.OpsRoutingLatencyMsKey, time.Since(routingStart).Milliseconds())
			forwardStart := time.Now()

			forwardBody := candidateBody
			if channelMapping.Mapped {
				forwardBody = h.gatewayService.ReplaceModelInBody(candidateBody, channelMapping.MappedModel)
			}
			writerSizeBeforeForward := c.Writer.Size()
			result, err := func() (*service.OpenAIForwardResult, error) {
				defer func() {
					if accountReleaseFunc != nil {
						accountReleaseFunc()
					}
				}()
				return h.gatewayService.ForwardAsChatCompletions(c.Request.Context(), c, account, forwardBody, promptCacheKey, "")
			}()
			cyberBlockKeyChat := ""
			if service.GetOpsCyberPolicy(c) != nil {
				cyberBlockKeyChat = service.CyberSessionBlockKey(apiKey.ID, c, body)
			}
			h.recordCyberPolicyIfMarked(c, routedKey, account, candidateSubscription, reqModel, err != nil, cyberBlockKeyChat, routedChannelUsageFields(c, channelMapping, reqModel, "", candidate.ChannelID), service.HashUsageRequestPayload(body))

			forwardDurationMs := time.Since(forwardStart).Milliseconds()
			upstreamLatencyMs, _ := getContextInt64(c, service.OpsUpstreamLatencyMsKey)
			responseLatencyMs := forwardDurationMs
			if upstreamLatencyMs > 0 && forwardDurationMs > upstreamLatencyMs {
				responseLatencyMs = forwardDurationMs - upstreamLatencyMs
			}
			service.SetOpsLatencyMs(c, service.OpsResponseLatencyMsKey, responseLatencyMs)
			if err == nil && result != nil && result.FirstTokenMs != nil {
				service.SetOpsLatencyMs(c, service.OpsTimeToFirstTokenMsKey, int64(*result.FirstTokenMs))
			}
			if err != nil {
				if result != nil && result.ImageCount > 0 {
					reqLog.Warn("openai_chat_completions.forward_partial_error_with_image_result",
						zap.Int64("account_id", account.ID),
						zap.Int("image_count", result.ImageCount),
						zap.Error(err),
					)
				} else {
					var failoverErr *service.UpstreamFailoverError
					if errors.As(err, &failoverErr) {
						if failoverClientGone(c) {
							reqLog.Info("openai_chat_completions.failover_aborted_client_disconnected",
								zap.Int64("account_id", account.ID),
								zap.Int("upstream_status", failoverErr.StatusCode),
							)
							return
						}
						if c.Writer.Size() != writerSizeBeforeForward {
							h.handleFailoverExhausted(c, failoverErr, true)
							return
						}
						if failoverErr.ShouldReportAccountScheduleFailure() {
							h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), false, nil)
						}
						if !failoverErr.ShouldRetryNextAccount() {
							h.handleFailoverExhausted(c, failoverErr, streamStarted)
							return
						}
						// Pool mode: retry on the same account
						if failoverErr.RetryableOnSameAccount {
							retryLimit := account.GetPoolModeRetryCount()
							if sameAccountRetryCount[account.ID] < retryLimit {
								sameAccountRetryCount[account.ID]++
								reqLog.Warn("openai_chat_completions.pool_mode_same_account_retry",
									zap.Int64("account_id", account.ID),
									zap.Int("upstream_status", failoverErr.StatusCode),
									zap.Int("retry_limit", retryLimit),
									zap.Int("retry_count", sameAccountRetryCount[account.ID]),
								)
								select {
								case <-c.Request.Context().Done():
									return
								case <-time.After(sameAccountRetryDelay):
								}
								continue
							}
						}
						h.gatewayService.RecordOpenAIAccountSwitch()
						failedAccountIDs[account.ID] = struct{}{}
						lastFailoverErr = failoverErr
						if switchCount >= maxAccountSwitches {
							lastChannelErr = failoverErr
							finalFailoverErr = failoverErr
							break
						}
						switchCount++
						if h.gatewayService.ShouldStopOpenAIOAuth429Failover(account, failoverErr.StatusCode, switchCount, &oauth429FailoverState) {
							lastChannelErr = failoverErr
							finalFailoverErr = failoverErr
							break
						}
						reqLog.Warn("openai_chat_completions.upstream_failover_switching",
							zap.Int64("account_id", account.ID),
							zap.Int("upstream_status", failoverErr.StatusCode),
							zap.Int("switch_count", switchCount),
							zap.Int("max_switches", maxAccountSwitches),
						)
						continue
					}
					h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), false, nil)
					upstreamErrorAlreadyCommunicated := openAIForwardErrorAlreadyCommunicated(c, writerSizeBeforeForward, err)
					wroteFallback := false
					if !upstreamErrorAlreadyCommunicated {
						wroteFallback = h.ensureOpenAIStreamReadErrorResponse(c, err, streamStarted)
						if !wroteFallback {
							wroteFallback = h.ensureForwardErrorResponse(c, streamStarted)
						}
					}
					reqLog.Warn("openai_chat_completions.forward_failed",
						zap.Int64("account_id", account.ID),
						zap.Bool("fallback_error_response_written", wroteFallback),
						zap.Bool("upstream_error_response_already_written", upstreamErrorAlreadyCommunicated),
						zap.Error(err),
					)
					return
				}
			}
			if result != nil {
				h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), true, result.FirstTokenMs)
			} else {
				h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), true, nil)
			}

			userAgent := c.GetHeader("User-Agent")
			clientIP := ip.GetClientIP(c)
			inboundEndpoint := GetInboundEndpoint(c)
			upstreamEndpoint := resolveOpenAIUpstreamEndpoint(c, account, result)
			quotaPlatform := routedKey.Group.Platform
			sessionID := service.ExtractClientSessionID(c)

			cyberBlocked := service.GetOpsCyberPolicy(c) != nil
			h.submitOpenAIUsageRecordTask(c.Request.Context(), result, func(ctx context.Context) {
				if err := h.gatewayService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{
					Result:             result,
					APIKey:             routedKey,
					User:               routedKey.User,
					Account:            account,
					Subscription:       candidateSubscription,
					InboundEndpoint:    inboundEndpoint,
					UpstreamEndpoint:   upstreamEndpoint,
					UserAgent:          userAgent,
					IPAddress:          clientIP,
					APIKeyService:      h.apiKeyService,
					QuotaPlatform:      quotaPlatform,
					SessionID:          sessionID,
					ChannelUsageFields: routedChannelUsageFields(c, channelMapping, reqModel, result.UpstreamModel, candidate.ChannelID),
					CyberBlocked:       cyberBlocked,
				}); err != nil {
					logger.L().With(
						zap.String("component", "handler.openai_gateway.chat_completions"),
						zap.Int64("user_id", subject.UserID),
						zap.Int64("api_key_id", apiKey.ID),
						zap.Any("group_id", routedKey.GroupID),
						zap.String("model", reqModel),
						zap.Int64("account_id", account.ID),
					).Error("openai_chat_completions.record_usage_failed", zap.Error(err))
				}
			})
			reqLog.Debug("openai_chat_completions.request_completed",
				zap.Int64("account_id", account.ID),
				zap.Int("switch_count", switchCount),
				zap.Int64("channel_id", candidate.ChannelID),
				zap.Int64("group_id", candidate.Group.ID),
				zap.Int64("candidate_duration_ms", time.Since(candidateStartedAt).Milliseconds()),
				zap.Float64("effective_multiplier", candidate.EffectiveMultiplier),
			)
			return
		}
		if lastChannelErr != nil && candidateIndex < len(candidates)-1 && c.Writer.Size() <= 0 {
			reqLog.Info("openai_chat_completions.channel_failover",
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
	if finalFailoverErr != nil {
		h.handleFailoverExhausted(c, finalFailoverErr, streamStarted)
		return
	}
	h.handleStreamingAwareError(c, http.StatusBadGateway, "api_error", "All enabled channels exhausted", streamStarted)
}

// resolveOpenAIUpstreamEndpoint returns the actual upstream endpoint for an
// OpenAI-compatible account. A forwarding result is authoritative because a
// single inbound route may choose raw Chat or a Responses bridge at runtime.
// The account-based derivation remains as a fallback for existing callers and
// forwarding paths that do not report their endpoint yet.
func resolveOpenAIUpstreamEndpoint(c *gin.Context, account *service.Account, result *service.OpenAIForwardResult) string {
	if result != nil {
		if endpoint := strings.TrimSpace(result.UpstreamEndpoint); endpoint != "" {
			return endpoint
		}
	}
	if endpoint := service.GetActualOpenAIUpstreamEndpoint(c); endpoint != "" {
		return endpoint
	}
	if account != nil && account.Type == service.AccountTypeAPIKey &&
		!openai_compat.ShouldUseResponsesAPI(account.Extra) {
		return EndpointChatCompletions
	}
	return GetUpstreamEndpoint(c, account.Platform)
}
