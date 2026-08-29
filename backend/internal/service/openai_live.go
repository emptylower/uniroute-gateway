package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	coderws "github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const (
	defaultLiveMaxSessionDuration = time.Hour
	liveLeaseRefreshInterval      = 20 * time.Second
	liveRedisOperationTimeout     = 3 * time.Second
	liveClosedRecordTTL           = 24 * time.Hour
	liveObserverPollInterval      = 250 * time.Millisecond
	liveUpstreamBodyLimit         = 2 << 20
	liveProvisionalWriteTimeout   = 300 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Phase 3.7b (redesign §13.2.1–§13.2.4): the Live window clock's counters.
// Package-level atomics in this file (the file the plan's constraint 2 names
// for them), the same shape as authorizationMetrics; deltas only in tests.
// ---------------------------------------------------------------------------

type LiveWindowMetrics struct {
	Closed        int64
	Idle          int64
	SettleRetry   int64
	ReauthRetry   int64
	Refused       int64
	RefusedShadow int64
	// IdleFinalized (§13.2.3): a finalization whose last window carried a
	// zero remainder — all usage already settled by earlier windows.
	IdleFinalized int64
}

var liveWindowMetrics struct {
	closed, idle, settleRetry, reauthRetry, refused, refusedShadow, idleFinalized atomic.Int64
}

func LiveWindowMetricsSnapshot() LiveWindowMetrics {
	m := &liveWindowMetrics
	return LiveWindowMetrics{
		Closed:        m.closed.Load(),
		Idle:          m.idle.Load(),
		SettleRetry:   m.settleRetry.Load(),
		ReauthRetry:   m.reauthRetry.Load(),
		Refused:       m.refused.Load(),
		RefusedShadow: m.refusedShadow.Load(),
		IdleFinalized: m.idleFinalized.Load(),
	}
}

func ResetLiveWindowMetricsForTest() {
	m := &liveWindowMetrics
	m.closed.Store(0)
	m.idle.Store(0)
	m.settleRetry.Store(0)
	m.reauthRetry.Store(0)
	m.refused.Store(0)
	m.refusedShadow.Store(0)
	m.idleFinalized.Store(0)
}

// liveObserverContextProvider (Phase 3.7b Task 4): production observers run
// for the process lifetime (redesign §13.2.6 — the loops end with the
// process); nil means context.Background(). Tests inject a cancellable
// context so every observer they start stops at cleanup — the same honest
// crash simulation test 52 uses (a cancelled loop stops WITHOUT releasing
// the controller or heartbeating).
var liveObserverContextProvider func() context.Context

func liveObserverCtx() context.Context {
	if liveObserverContextProvider != nil {
		return liveObserverContextProvider()
	}
	return context.Background()
}

// liveLeaseTTL / liveExpirySkew / liveWindowFloor (§13.2.1): the clock's
// three config reads — the lease horizon (i), the expiry skew subtracted from
// it, and the floor that gates clock (ii).
func (s *OpenAIGatewayService) liveLeaseTTL() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.CanonicalWallet.LeaseTTLSeconds > 0 {
		return time.Duration(s.cfg.CanonicalWallet.LeaseTTLSeconds) * time.Second
	}
	return 300 * time.Second
}

func (s *OpenAIGatewayService) liveExpirySkew() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.CanonicalWallet.ExpirySkewMarginMS > 0 {
		return time.Duration(s.cfg.CanonicalWallet.ExpirySkewMarginMS) * time.Millisecond
	}
	return 0
}

func (s *OpenAIGatewayService) liveWindowFloor() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.CanonicalWallet.LiveWindowMinSeconds > 0 {
		return time.Duration(s.cfg.CanonicalWallet.LiveWindowMinSeconds) * time.Second
	}
	return 20 * time.Second
}

// liveWindowRequestID (§13.2.2/§13.2.3): window 1's gateway request id is the
// session's own call hash — TODAY's id, so a session in flight across a
// deploy of this phase cannot double-settle its first window; windows ≥ 2
// append :window:n. Phase 4's reconciliation must know window 1's id is the
// bare hash, not :window:1.
func liveWindowRequestID(callHash string, seq int) string {
	if seq <= 1 {
		return callHash
	}
	return fmt.Sprintf("%s:window:%d", callHash, seq)
}

type liveUsageDelta struct {
	inputTokens     int
	outputTokens    int
	cacheReadTokens int
}

type liveUsageFallbackBucket struct {
	mu        sync.Mutex
	responses map[string]liveUsageDelta
}

var (
	chatGPTLiveCallsURL        = "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"
	chatGPTLiveSidebandBaseURL = "wss://chatgpt.com/backend-api/codex"
)

type liveFrameConn interface {
	ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error)
	WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error
	Close() error
}

func liveSidebandReadError(err error) error {
	if coderws.CloseStatus(err) == coderws.StatusNormalClosure {
		return ErrLiveCallNotFound
	}
	return err
}

func hashLiveCallID(callID string) string {
	sum := sha256.Sum256([]byte(callID))
	return hex.EncodeToString(sum[:])
}

func liveGroupID(groupID *int64) int64 {
	if groupID == nil {
		return 0
	}
	return *groupID
}

func liveOptionalID(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	result := value
	return &result
}

func (s *OpenAIGatewayService) liveStore() (LiveCallStore, error) {
	if s == nil || s.cache == nil {
		return nil, ErrLiveUnavailable
	}
	store, ok := s.cache.(LiveCallStore)
	if !ok {
		return nil, ErrLiveUnavailable
	}
	return store, nil
}

func (s *OpenAIGatewayService) liveConcurrencyCache() (LiveConcurrencyCache, error) {
	if s == nil || s.concurrencyService == nil || s.concurrencyService.cache == nil {
		return nil, ErrLiveUnavailable
	}
	cache, ok := s.concurrencyService.cache.(LiveConcurrencyCache)
	if !ok {
		return nil, ErrLiveUnavailable
	}
	return cache, nil
}

func (s *OpenAIGatewayService) liveMaxSessionDuration() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.Live.MaxSessionDurationSeconds > 0 {
		return time.Duration(s.cfg.Gateway.Live.MaxSessionDurationSeconds) * time.Second
	}
	return defaultLiveMaxSessionDuration
}

func ValidateLiveCallRequest(request *LiveCallRequest) error {
	if request == nil || strings.TrimSpace(request.SDP) == "" {
		return errors.New("sdp is required")
	}
	if len(request.Session) == 0 || !json.Valid(request.Session) {
		return errors.New("session must be valid JSON")
	}
	var sessionObject map[string]json.RawMessage
	if err := json.Unmarshal(request.Session, &sessionObject); err != nil {
		return errors.New("session must be a JSON object")
	}
	if sessionObject == nil {
		return errors.New("session must be a JSON object")
	}
	return nil
}

// CreateLiveCall 创建 Frameless 会话。调用方须在调用期间持有普通用户槽位；
// 调度器持有的普通账号槽位会被同一个 Live 租约原子接替。
func (s *OpenAIGatewayService) CreateLiveCall(
	ctx context.Context,
	request *LiveCallRequest,
	identity LiveCallIdentity,
	userMaxConcurrency int,
) (*LiveCallCreated, error) {
	if err := ValidateLiveCallRequest(request); err != nil {
		return nil, err
	}
	store, err := s.liveStore()
	if err != nil {
		return nil, err
	}
	liveCache, err := s.liveConcurrencyCache()
	if err != nil {
		return nil, err
	}
	billingModel := strings.TrimSpace(identity.BillingModel)
	if billingModel == "" {
		billingModel = strings.TrimSpace(gjson.GetBytes(request.Session, "model").String())
	}
	if billingModel == "" {
		return nil, fmt.Errorf("%w: live model is empty", ErrModelPricingUnavailable)
	}
	if s.resolver == nil || s.billingService == nil || s.exchangeRates == nil || s.usageBillingRepo == nil || s.usageLogRepo == nil {
		return nil, errors.New("live billing dependencies are unavailable")
	}
	resolved := s.resolver.Resolve(ctx, PricingInput{Model: billingModel, GroupID: identity.GroupID})
	if !resolvedPricingIsBillable(resolved) || resolved.Mode != BillingModeToken || resolved.BasePricing == nil {
		return nil, fmt.Errorf("%w for live model: %s", ErrModelPricingUnavailable, billingModel)
	}
	fx, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, identity.BillingCurrency)
	if !ok {
		fx, err = s.exchangeRates.Snapshot(ctx, CurrencyUSD, identity.BillingCurrency)
		if err != nil {
			return nil, err
		}
	}
	pricing := resolved.BasePricing
	attestation, attestationCiphertext, err := s.prepareLiveAttestation(ctx)
	if err != nil {
		return nil, err
	}

	excluded := make(map[int64]struct{})
	var lastErr error
	for attempt := 0; attempt <= 3; attempt++ {
		selection, _, selectErr := s.SelectAccountWithSchedulerForCapability(
			ctx,
			identity.GroupID,
			"",
			uuid.NewString(),
			"",
			excluded,
			OpenAIUpstreamTransportHTTPSSE,
			OpenAIEndpointCapabilityLive,
			false,
			false,
			false,
		)
		if selectErr != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, selectErr
		}
		if selection == nil || selection.Account == nil || !selection.Acquired {
			if selection != nil && selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
			return nil, ErrLiveConcurrencyFull
		}

		account := selection.Account
		leaseID := generateRequestID()
		acquired, acquireErr := liveCache.AcquireLiveLease(
			ctx,
			account.ID,
			account.Concurrency,
			identity.UserID,
			userMaxConcurrency,
			identity.APIKeyID,
			leaseID,
			true,
		)
		if acquireErr != nil || !acquired {
			selection.ReleaseFunc()
			if acquireErr != nil {
				return nil, acquireErr
			}
			return nil, ErrLiveConcurrencyFull
		}

		// Phase 3.2 freeze point (Live): after the Live lease is acquired and
		// before the SDP POST — the post-selection point where the pricing
		// basis becomes computable (spec §2.1). Record mode: a failed freeze is
		// counted and the call continues with snap == nil; settle mode releases
		// the selection AND the Live concurrency lease (already held here) and
		// returns the error, exactly as the createErr branch below does.
		var snap *BillingSnapshot
		if s.snapshots != nil && s.snapshots.Mode() != BillingSnapshotModeOff {
			billingAccount := account
			if account.IsShadow() {
				resolvedCredential, credErr := resolveCredentialAccount(ctx, s.accountRepo, account)
				if credErr != nil {
					selection.ReleaseFunc()
					s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
					return nil, credErr
				}
				billingAccount = resolvedCredential
			}
			var freezeErr error
			snap, freezeErr = s.snapshots.Freeze(ctx, FreezeInput{
				APIKey: identity.APIKey, User: identity.User, Account: account, BillingAccount: billingAccount,
				RequestedModel: billingModel, BillingModel: billingModel, Family: BillingFamilyLive,
				ResolveUserGroupRate: s.ResolveUserGroupRateMultiplier,
			})
			if freezeErr != nil {
				snap, freezeErr = s.snapshots.freezeOutcome(nil, freezeErr, billingModel)
				if freezeErr != nil {
					selection.ReleaseFunc()
					s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
					return nil, freezeErr
				}
			}
		}

		// Phase 3.3c: authorize this POST attempt (each attempt is its own
		// authorization — spec §2.0), write the durable provisional record BEFORE
		// the upstream write (spec §2.3, shadow/enforce only — constraint 8), and
		// hand the handle to the request so the decorated port authorizes the POST.
		authHandle, authErr := s.AuthorizeBillableAttempt(ctx, snap, identity.APIKey, EstimateInputFromRequestBody(request.Session, EstimateInputOptions{}))
		if authErr != nil {
			selection.ReleaseFunc()
			s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
			return nil, authErr // the handler maps a refusal to 402 (Task 4); shouldFailoverLiveCreateError returns false for it
		}
		platformUserID := ""
		if identity.User != nil {
			platformUserID = identity.User.PlatformUserID
		}
		provisional := &LiveProvisionalRecord{
			Token: authHandle.ID, AuthorizationID: authHandle.ID, PlatformUserID: platformUserID,
			UserID: identity.UserID, APIKeyID: identity.APIKeyID, AccountID: account.ID,
			BillingCurrency: identity.BillingCurrency, BillingSnapshotID: snapshotIDOf(snap),
			EstimatedUnits: authHandle.EstimatedUnits, Status: LiveProvisionalStatusProvisional,
			Windows:   []LiveWindow{{WindowSeq: 1, LeaseID: "", Token: authHandle.ID, OpenedAtMS: time.Now().UnixMilli()}},
			CreatedAt: time.Now().UTC(),
		}
		rowWritten, rowErr := s.saveLiveProvisional(ctx, provisional, authHandle)
		if rowErr != nil {
			selection.ReleaseFunc()
			s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
			return nil, rowErr
		}
		attemptCtx := WithAuthorizationHandle(ctx, authHandle)
		created, createErr := s.createUpstreamLiveCall(attemptCtx, account, request, attestation, snap)
		selection.ReleaseFunc()
		if createErr != nil {
			s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
			if rowWritten {
				s.abortLiveProvisional(authHandle.ID, createErr)
			}
			if !s.shouldFailoverLiveCreateError(createErr) {
				return nil, createErr
			}
			excluded[account.ID] = struct{}{}
			lastErr = createErr
			continue
		}

		now := time.Now()
		model := strings.TrimSpace(gjson.GetBytes(request.Session, "model").String())
		if model == "" {
			model = "gpt-live"
		}
		// When a snapshot was frozen, the record's per-token prices come from it
		// so the record and the snapshot cannot disagree. (Base is nil for a
		// per-request/image resolve; Live's freeze re-resolves after selection.)
		recordPricing := pricing
		if snap != nil && snap.Pricing.Base != nil {
			recordPricing = snap.Pricing.Base
		}
		var billingSnapshotID string
		if snap != nil {
			billingSnapshotID = snap.ID
		}
		record := &LiveCallRecord{
			CallID:                 created.CallID,
			CallHash:               hashLiveCallID(created.CallID),
			AccountID:              account.ID,
			APIKeyID:               identity.APIKeyID,
			UserID:                 identity.UserID,
			GroupID:                liveGroupID(identity.GroupID),
			SubscriptionID:         liveGroupID(identity.SubscriptionID),
			LeaseID:                leaseID,
			Model:                  billingModel,
			BillingCurrency:        identity.BillingCurrency,
			RateMultiplier:         identity.RateMultiplier,
			GroupRateMultiplier:    identity.GroupRateMultiplier,
			AccountRateMultiplier:  account.BillingRateMultiplier(),
			ExchangeRate:           fx.Rate,
			ExchangeRateSource:     fx.Source,
			ExchangeRateAsOf:       fx.AsOf,
			APIKeyQuota:            identity.APIKeyQuota,
			RateLimit5h:            identity.RateLimit5h,
			RateLimit1d:            identity.RateLimit1d,
			RateLimit7d:            identity.RateLimit7d,
			SubscriptionBilling:    identity.SubscriptionBilling,
			InputPricePerToken:     recordPricing.InputPricePerToken,
			OutputPricePerToken:    recordPricing.OutputPricePerToken,
			CacheReadPricePerToken: recordPricing.CacheReadPricePerToken,
			BillingSnapshotID:      billingSnapshotID,
			AuthorizationToken:     AuthorizationTokenOf(authHandle),
			AuthorizationID:        authHandle.ID,
			PlatformUserID:         platformUserID,
			CreatedAt:              now,
			ExpiresAt:              now.Add(s.liveMaxSessionDuration()),
			Controller:             LiveControllerPending,
			UserAgent:              identity.UserAgent,
			IPAddress:              identity.IPAddress,
			InboundEndpoint:        identity.InboundEndpoint,
			AttestationCiphertext:  attestationCiphertext,
		}
		mappingTTL := s.liveMaxSessionDuration() + 5*time.Minute
		if saveErr := store.SaveLiveCall(ctx, record, mappingTTL); saveErr != nil {
			s.releaseLiveLease(account.ID, identity.UserID, identity.APIKeyID, leaseID)
			if rowWritten {
				s.abortLiveProvisional(authHandle.ID, saveErr)
			}
			return nil, fmt.Errorf("save live call mapping: %w", saveErr)
		}
		if rowWritten {
			s.activateLiveProvisional(authHandle.ID, record.CallHash)
		}
		created.Account = account
		go s.observeLiveCall(liveObserverCtx(), record.CallHash)
		return created, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrLiveUnavailable
}

func (s *OpenAIGatewayService) shouldFailoverLiveCreateError(err error) bool {
	if errors.Is(err, ErrAuthorizationRefused) {
		return false
	}
	var upstreamErr *UpstreamFailoverError
	if !errors.As(err, &upstreamErr) {
		// 凭证读取和网络传输错误都可能只影响当前账号或代理。
		return true
	}
	return s.shouldFailoverOpenAIUpstreamResponse(
		upstreamErr.StatusCode,
		"",
		upstreamErr.ResponseBody,
	)
}

func (s *OpenAIGatewayService) createUpstreamLiveCall(
	ctx context.Context,
	account *Account,
	request *LiveCallRequest,
	attestation string,
	snap *BillingSnapshot,
) (*LiveCallCreated, error) {
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "access_token", err)
		return nil, err
	}
	body, err := json.Marshal(struct {
		SDP     string          `json:"sdp"`
		Session json.RawMessage `json:"session"`
	}{
		SDP:     request.SDP,
		Session: request.Session,
	})
	if err != nil {
		return nil, err
	}
	reqCtx := WithHTTPUpstreamRedirectsDisabled(WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileOpenAI))
	upstreamReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, chatGPTLiveCallsURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	authHeaders, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "authentication_headers", err)
		return nil, err
	}
	for key, values := range authHeaders {
		for _, value := range values {
			upstreamReq.Header.Add(key, value)
		}
	}
	upstreamReq.Host = "chatgpt.com"
	if err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, s.accountRepo, upstreamReq.Header, account); err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "account_headers", err)
		return nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Accept", "application/sdp")
	upstreamReq.Header.Set(liveAttestationHeader, attestation)
	applyLiveUpstreamIdentityHeaders(upstreamReq.Header)

	// Live is the one family whose record must be durable before the write
	// (spec §2.3): persist the snapshot synchronously right before the SDP
	// POST. Errors are counted, never fatal, in 3.2.
	if snap != nil && s.snapshots != nil {
		if persistErr := s.snapshots.Persist(ctx, snap); persistErr != nil {
			billingSnapshotMetrics.persistError.Add(1)
		}
	}

	resp, err := s.httpUpstream.Do(upstreamReq, resolveAccountProxyURL(account), account.ID, account.Concurrency)
	// Phase 3.3a: a refusal is terminal (Live is authorized at 3.3c — the
	// short-circuit still goes in now so a refused write is never wrapped).
	if refused, refusedOK := AsAuthorizationRefused(err); refusedOK {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, refused
	}
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "upstream_transport", err)
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, liveUpstreamBodyLimit+1))
	if readErr != nil {
		return nil, readErr
	}
	if len(responseBody) > liveUpstreamBodyLimit {
		return nil, errors.New("live upstream response is too large")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logLiveUpstreamFailure(ctx, account.ID, resp.StatusCode, resp.Header, responseBody)
		return nil, &UpstreamFailoverError{
			StatusCode:      resp.StatusCode,
			ResponseBody:    responseBody,
			ResponseHeaders: resp.Header.Clone(),
		}
	}
	callID, err := liveCallIDFromLocation(resp.Header.Get("Location"))
	if err != nil {
		return nil, err
	}
	return &LiveCallCreated{
		SDP:      responseBody,
		CallID:   callID,
		Location: resp.Header.Get("Location"),
	}, nil
}

func logLiveCreateStageFailure(ctx context.Context, accountID int64, stage string, err error) {
	logger.FromContext(ctx).Warn(
		"OpenAI Live 创建阶段失败",
		zap.Int64("account_id", accountID),
		zap.String("stage", stage),
		zap.String("error_type", fmt.Sprintf("%T", err)),
	)
}

func logLiveUpstreamFailure(
	ctx context.Context,
	accountID int64,
	statusCode int,
	headers http.Header,
	body []byte,
) {
	errorType := strings.TrimSpace(gjson.GetBytes(body, "error.type").String())
	errorCode := strings.TrimSpace(gjson.GetBytes(body, "error.code").String())
	errorMessage := strings.TrimSpace(gjson.GetBytes(body, "error.message").String())
	if errorType == "" {
		errorType = strings.TrimSpace(gjson.GetBytes(body, "type").String())
	}
	if errorCode == "" {
		errorCode = strings.TrimSpace(gjson.GetBytes(body, "code").String())
	}
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(gjson.GetBytes(body, "message").String())
	}
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(gjson.GetBytes(body, "detail").String())
	}

	logger.FromContext(ctx).Warn(
		"OpenAI Live 上游拒绝请求",
		zap.Int64("account_id", accountID),
		zap.Int("upstream_status_code", statusCode),
		zap.String("upstream_error_type", truncateOpenAIWSLogValue(errorType, 120)),
		zap.String("upstream_error_code", truncateOpenAIWSLogValue(errorCode, 120)),
		zap.String("upstream_error_message", truncateOpenAIWSLogValue(errorMessage, 300)),
		zap.String("upstream_content_type", truncateOpenAIWSLogValue(headers.Get("Content-Type"), 120)),
		zap.String("upstream_server", truncateOpenAIWSLogValue(headers.Get("Server"), 120)),
		zap.String("upstream_cf_mitigated", truncateOpenAIWSLogValue(headers.Get("Cf-Mitigated"), 120)),
		zap.String("upstream_cf_ray", truncateOpenAIWSLogValue(headers.Get("Cf-Ray"), 120)),
		zap.String("upstream_request_id", truncateOpenAIWSLogValue(headers.Get("X-Request-Id"), 120)),
	)
}

func liveCallIDFromLocation(location string) (string, error) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", errors.New("live upstream response has no Location")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse live Location: %w", err)
	}
	callID := strings.TrimSpace(path.Base(strings.TrimSuffix(parsed.Path, "/")))
	if callID == "" || callID == "." || callID == "codex" {
		return "", errors.New("live upstream Location has no call id")
	}
	return callID, nil
}

func applyLiveUpstreamIdentityHeaders(headers http.Header) {
	headers.Set("OpenAI-Alpha", "quicksilver=v2")
	ensureCodexIdentityHeaders(headers)
	enforceCodexIdentityHeaders(headers)
	if strings.TrimSpace(headers.Get("session-id")) == "" {
		headers.Set("session-id", uuid.NewString())
	}
	if strings.TrimSpace(headers.Get("thread-id")) == "" {
		headers.Set("thread-id", uuid.NewString())
	}
	// Realtime/Live 不使用 Responses 的实验头。
	headers.Del("OpenAI-Beta")
}

func (s *OpenAIGatewayService) liveSidebandHeaders(
	ctx context.Context,
	account *Account,
	record *LiveCallRecord,
) (http.Header, error) {
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	headers, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		return nil, err
	}
	if err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, s.accountRepo, headers, account); err != nil {
		return nil, err
	}
	attestation, err := s.decryptLiveAttestation(record)
	if err != nil {
		return nil, err
	}
	headers.Set(liveAttestationHeader, attestation)
	applyLiveUpstreamIdentityHeaders(headers)
	return headers, nil
}

func (s *OpenAIGatewayService) dialLiveSideband(ctx context.Context, record *LiveCallRecord) (liveFrameConn, error) {
	account, err := s.accountRepo.GetByID(ctx, record.AccountID)
	if err != nil {
		return nil, err
	}
	if account == nil || !account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityLive) {
		return nil, ErrLiveUnavailable
	}
	headers, err := s.liveSidebandHeaders(ctx, account, record)
	if err != nil {
		return nil, err
	}
	target := strings.TrimRight(chatGPTLiveSidebandBaseURL, "/") + "/" + url.PathEscape(record.CallID)
	conn, status, _, err := s.getOpenAIWSPassthroughDialer().Dial(ctx, target, headers, resolveAccountProxyURL(account))
	if err != nil {
		return nil, fmt.Errorf("dial live sideband (status %d): %w", status, err)
	}
	raw, ok := conn.(liveFrameConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("live sideband transport does not support raw frames")
	}
	return raw, nil
}

func (s *OpenAIGatewayService) GetLiveCallForIdentity(
	ctx context.Context,
	callID string,
	identity LiveCallIdentity,
) (*LiveCallRecord, error) {
	store, err := s.liveStore()
	if err != nil {
		return nil, err
	}
	record, err := store.GetLiveCall(ctx, hashLiveCallID(callID))
	if err != nil {
		return nil, err
	}
	if record.CallID != callID ||
		record.APIKeyID != identity.APIKeyID ||
		record.UserID != identity.UserID ||
		record.GroupID != liveGroupID(identity.GroupID) {
		return nil, ErrLiveIdentityMismatch
	}
	if record.Controller == LiveControllerClosed {
		return nil, ErrLiveCallNotFound
	}
	return record, nil
}

// ProxyLiveSideband 让认证后的客户端接管控制连接；媒体始终不经过这里。
func (s *OpenAIGatewayService) ProxyLiveSideband(
	ctx context.Context,
	record *LiveCallRecord,
	downstream *coderws.Conn,
) error {
	if record == nil || downstream == nil {
		return ErrLiveCallNotFound
	}
	store, err := s.liveStore()
	if err != nil {
		return err
	}
	owner := uuid.NewString()
	claimed, err := store.ClaimLiveController(ctx, record.CallHash, LiveControllerProxy, owner)
	if err != nil {
		return err
	}
	if !claimed {
		return ErrLiveControllerChanged
	}

	// observer 轮询到接管状态后会关闭旧控制连接；同一个 call 可重新加入。
	time.Sleep(liveObserverPollInterval)
	upstream, err := s.dialLiveSideband(ctx, record)
	if err != nil {
		_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
		go s.observeLiveCall(liveObserverCtx(), record.CallHash)
		return err
	}
	defer func() { _ = upstream.Close() }()
	downstream.SetReadLimit(openAIWSMessageReadLimitBytes)

	proxyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	go func() {
		for {
			messageType, payload, readErr := downstream.Read(proxyCtx)
			if readErr != nil {
				errCh <- readErr
				return
			}
			if writeErr := upstream.WriteFrame(WithNonBillableUpstream(proxyCtx, NonBillableLiveSideband), messageType, payload); writeErr != nil {
				errCh <- writeErr
				return
			}
		}
	}()
	go func() {
		for {
			messageType, payload, readErr := upstream.ReadFrame(proxyCtx)
			if readErr != nil {
				errCh <- liveSidebandReadError(readErr)
				return
			}
			s.accumulateLiveUsage(record, payload)
			if writeErr := downstream.Write(proxyCtx, messageType, payload); writeErr != nil {
				errCh <- writeErr
				return
			}
			if messageType == coderws.MessageText {
				eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
				if eventType == "session.closed" || eventType == "session.ended" {
					errCh <- ErrLiveCallNotFound
					return
				}
			}
		}
	}()

	runErr := s.runLiveController(proxyCtx, record, upstream, errCh)
	cancel()
	_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
	if liveSessionEnded(runErr) || !time.Now().Before(record.ExpiresAt) {
		s.finalizeLiveCall(record)
		return runErr
	}
	go s.observeLiveCall(liveObserverCtx(), record.CallHash)
	return runErr
}

// liveSessionEnded 判断控制连接的退出原因是否意味着会话已终结（应 finalize：写
// usage log 并释放租约），而不是可以交给 observer 重连的临时错误。
//
// ErrLiveUnavailable 在控制循环里只会来自租约续租失败。RefreshLiveLease 的 Lua 在
// leaseID 被 GC 后不会重新写入，重连也拿不回并发槽 —— 若按临时错误重试，会话会以
// 约 1 秒一轮的节奏空转到 ExpiresAt，期间持着上游连接却不计入任何并发限制。
func liveSessionEnded(err error) bool {
	return errors.Is(err, ErrLiveCallNotFound) ||
		errors.Is(err, ErrLiveUnavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (s *OpenAIGatewayService) runLiveController(
	ctx context.Context,
	record *LiveCallRecord,
	upstream liveFrameConn,
	errCh <-chan error,
) error {
	refreshTicker := time.NewTicker(liveLeaseRefreshInterval)
	defer refreshTicker.Stop()
	maxTimer := time.NewTimer(time.Until(record.ExpiresAt))
	defer maxTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case err := <-errCh:
			return err
		case <-maxTimer.C:
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = upstream.WriteFrame(WithNonBillableUpstream(closeCtx, NonBillableLiveSideband), coderws.MessageText, []byte(`{"type":"session.close"}`))
			cancel()
			return context.DeadlineExceeded
		case <-refreshTicker.C:
			if !s.refreshLiveLease(record) {
				return ErrLiveUnavailable
			}
		}
	}
}

// liveControllerTakeoverInterval (Phase 3.7b, §13.2.5): the grace after
// which a stale observer's claim may be taken over — config
// canonical_wallet.live_controller_takeover_seconds (default 15 s).
func (s *OpenAIGatewayService) liveControllerTakeoverInterval() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.CanonicalWallet.LiveControllerTakeoverSeconds > 0 {
		return time.Duration(s.cfg.CanonicalWallet.LiveControllerTakeoverSeconds) * time.Second
	}
	return 15 * time.Second
}

// observeLiveCall (Phase 3.7b, §13.2.5): claims the observer controller and
// runs the session. A refused claim no longer returns — the loop retries the
// claim every live_controller_takeover_seconds through
// TakeOverLiveObserver(stale_before = now − grace), so any live instance
// that knows the call hash becomes the observer within one grace of the old
// one dying. The ctx is the loop's lifetime: cancelled, the loop (and the
// connection under it) stops WITHOUT releasing the controller or
// heartbeating — the exact crash shape the takeover exists for. Production
// call sites pass context.Background() (the loops end with the process, as
// 13.2.6 says of the bridge); tests cancel theirs at cleanup.
func (s *OpenAIGatewayService) observeLiveCall(ctx context.Context, callHash string) {
	store, err := s.liveStore()
	if err != nil {
		return
	}
	owner := uuid.NewString()
	takeover := s.liveControllerTakeoverInterval()
	claimed, claimErr := store.ClaimLiveController(ctx, callHash, LiveControllerObserver, owner)
	if claimErr != nil {
		return
	}
	if !claimed {
		// Loop on the takeover interval until the current observer goes stale
		// or the call ends (record gone, closed, expired) or ctx is done.
		timer := time.NewTimer(takeover)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			record, getErr := store.GetLiveCall(ctx, callHash)
			if getErr != nil || record.Controller == LiveControllerClosed {
				return
			}
			if !time.Now().Before(record.ExpiresAt) {
				return
			}
			taken, takeErr := store.TakeOverLiveObserver(ctx, callHash, owner, time.Now().Add(-takeover))
			if takeErr != nil {
				return
			}
			if taken {
				claimed = true
				break
			}
			timer.Reset(takeover)
		}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		record, getErr := store.GetLiveCall(ctx, callHash)
		if getErr != nil || record.Controller != LiveControllerObserver {
			return
		}
		if !time.Now().Before(record.ExpiresAt) {
			s.finalizeLiveCall(record)
			return
		}
		upstream, dialErr := s.dialLiveSideband(ctx, record)
		if dialErr != nil {
			if !s.waitForLiveObserverRetry(record) {
				return
			}
			continue
		}
		runErr := s.runLiveObserverConnection(ctx, record, upstream, owner)
		_ = upstream.Close()
		if errors.Is(runErr, ErrLiveControllerChanged) {
			return
		}
		if liveSessionEnded(runErr) {
			s.finalizeLiveCall(record)
			return
		}
		if !s.waitForLiveObserverRetry(record) {
			return
		}
	}
}

// runLiveObserverConnection (Phase 3.7b, §13.2.5): the observer's connection
// loop. On every controller tick it first heartbeats (as owner) and then
// re-reads the controller state — an owner mismatch is
// ErrLiveControllerChanged even while the controller string is unchanged
// (the displaced observer's exit). ctx is derived from observeLiveCall's, so
// a cancelled observer exits cleanly: a cancelled ReadFrame surfaces as a
// plain return, never an escalated read error.
func (s *OpenAIGatewayService) runLiveObserverConnection(ctx context.Context, record *LiveCallRecord, upstream liveFrameConn, owner string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	frameCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			messageType, payload, err := upstream.ReadFrame(ctx)
			if err != nil {
				select {
				case errCh <- liveSidebandReadError(err):
				case <-ctx.Done():
				}
				return
			}
			if messageType == coderws.MessageText {
				select {
				case frameCh <- payload:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	refreshTicker := time.NewTicker(liveLeaseRefreshInterval)
	defer refreshTicker.Stop()
	controllerTicker := time.NewTicker(liveObserverPollInterval)
	defer controllerTicker.Stop()
	maxTimer := time.NewTimer(time.Until(record.ExpiresAt))
	defer maxTimer.Stop()
	store, _ := s.liveStore()
	// §13.2.1: the window clock's cached state — loaded lazily on the first
	// tick, reloaded after every advance and every lost CAS.
	var liveClockState *liveWindowState
	for {
		select {
		case payload := <-frameCh:
			s.accumulateLiveUsage(record, payload)
			eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
			if eventType == "session.closed" || eventType == "session.ended" {
				return ErrLiveCallNotFound
			}
		case err := <-errCh:
			// A cancelled cleanup is a clean loop exit, not a read error
			// (round-2 note): never log or escalate it.
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case <-controllerTicker.C:
			// §13.2.5: heartbeat first (one owner-checked HSET), then the
			// state read on the same tick — a foreign owner or a proxy/closed
			// controller displaces this observer at its next tick.
			if store != nil {
				_ = store.HeartbeatLiveController(ctx, record.CallHash, owner, time.Now().UTC())
				state, stateErr := store.GetLiveControllerState(ctx, record.CallHash)
				if stateErr != nil {
					return stateErr
				}
				if state.Controller != LiveControllerObserver || state.Owner != owner {
					return ErrLiveControllerChanged
				}
			}
			// §13.2.1: only the observer loop runs the window clock, after the
			// heartbeat, on this same 250 ms tick.
			if next, clockErr := s.maybeCloseLiveWindow(ctx, record, upstream, liveClockState); next != nil {
				liveClockState = next
			} else if clockErr != nil {
				return clockErr
			}
		case <-refreshTicker.C:
			if !s.refreshLiveLease(record) {
				return ErrLiveUnavailable
			}
		case <-maxTimer.C:
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = upstream.WriteFrame(WithNonBillableUpstream(closeCtx, NonBillableLiveSideband), coderws.MessageText, []byte(`{"type":"session.close"}`))
			closeCancel()
			return context.DeadlineExceeded
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// liveWindowState is the window clock's cached view of the provisional
// record's window list (§13.2.2): the live window's seq/token/lease/opening
// instant, the session estimate E_w, the sum of the settled windows behind
// it, the PERSISTED pending amount (a crash between pending and the advance
// re-issues exactly it — never a recomputation, §13.2.2's recovery rule),
// and, after a successful re-authorization, the next window's handle until
// the advance commits (a lost advance retries the ADVANCE with the SAME
// handle — never a second arm, preserving "at most one armed hold").
type liveWindowState struct {
	seq        int
	token      string // the LIVE WINDOW's token (the settlement's AuthorizationID)
	rowToken   string // the provisional ROW's primary key (window 1's token) — the CAS address
	leaseID    string
	openedAt   time.Time
	estimate   int64
	settledSum int64
	pending    int64
	next       *LiveWindow
}

// loadLiveWindowState reads the window list by call hash (§13.2.2). A window
// with no opened_at_ms (a pre-3.7b record) anchors on the record's creation —
// the same instant class the window opened at.
func (s *OpenAIGatewayService) loadLiveWindowState(ctx context.Context, callHash string) (*liveWindowState, error) {
	if s.liveProvisional == nil {
		return nil, ErrLiveProvisionalNotFound
	}
	rec, err := s.liveProvisional.GetByCallHash(ctx, callHash)
	if err != nil {
		return nil, err
	}
	n := len(rec.Windows)
	if n == 0 || rec.Status != LiveProvisionalStatusActive {
		return nil, ErrLiveProvisionalNotFound
	}
	last := rec.Windows[n-1]
	st := &liveWindowState{
		seq:      last.WindowSeq,
		token:    last.Token,
		rowToken: rec.Token,
		leaseID:  last.LeaseID,
		estimate: rec.EstimatedUnits,
		pending:  last.PendingUnits,
		openedAt: rec.CreatedAt,
	}
	if last.OpenedAtMS > 0 {
		st.openedAt = time.UnixMilli(last.OpenedAtMS)
	}
	for _, w := range rec.Windows[:n-1] {
		st.settledSum += w.SettledUnits
	}
	return st, nil
}

// liveUsageUnits prices the session's cumulative tokens with the record's
// frozen prices exactly as finalization does (§13.2.2: A_n =
// units(total tokens at close) − Σ_{k<n} A_k; the cost block factored out of
// tryFinalizeLiveCall so both settle the same way).
func liveUsageUnits(record *LiveCallRecord) (int64, error) {
	if record == nil {
		return 0, errors.New("nil live call record")
	}
	inputTokens := record.InputTokens - record.CacheReadTokens
	if inputTokens < 0 {
		inputTokens = 0
	}
	inputCost := float64(inputTokens) * record.InputPricePerToken
	outputCost := float64(record.OutputTokens) * record.OutputPricePerToken
	cacheReadCost := float64(record.CacheReadTokens) * record.CacheReadPricePerToken
	sourceCost := inputCost + outputCost + cacheReadCost
	baseCost := sourceCost * record.ExchangeRate
	actualCost := baseCost * record.RateMultiplier
	return canonicalWalletUnitsFromCNY(actualCost)
}

// liveClockArmed: the clock runs only with a provisional store, an
// authorizer and a snapshot service (the re-authorization needs all three);
// unit fixtures without them are untouched.
func (s *OpenAIGatewayService) liveClockArmed() bool {
	return s != nil && s.liveProvisional != nil && s.authorizer != nil &&
		s.billingSnapshotSettler.snapshots != nil
}

// maybeCloseLiveWindow (§13.2.1–§13.2.4) runs on the observer's controller
// tick. It returns the (possibly reloaded) state — nil only together with a
// non-nil error (the loop's terminal exits) — and settles nothing unless a
// close condition holds. A nil st on entry loads fresh.
//
// Close conditions, evaluated per tick: (i) now ≥ opened_at + lease_ttl −
// skew (the lease horizon); (ii) now ≥ opened_at + floor AND accrued ≥ E_w
// (the held estimate — a chunk, not a bound). The floor bounds the
// settlement storm: a session far over its estimate closes at most one
// window per floor.
func (s *OpenAIGatewayService) maybeCloseLiveWindow(ctx context.Context, record *LiveCallRecord, upstream liveFrameConn, st *liveWindowState) (*liveWindowState, error) {
	if !s.liveClockArmed() || record == nil {
		return st, nil
	}
	if st == nil {
		loaded, err := s.loadLiveWindowState(ctx, record.CallHash)
		if err != nil {
			// A missing/inactive record parks the clock silently (the unit
			// fixtures); the loop's own exits handle the terminal cases.
			if os.Getenv("P37B_DEBUG") != "" {
				fmt.Fprintf(os.Stderr, "P37B-EXIT load err=%v\n", err)
			}
			return nil, nil
		}
		st = loaded
	}
	store, err := s.liveStore()
	if err != nil {
		return st, nil
	}
	// The counters are the Redis hash's — read fresh each tick.
	fresh, getErr := store.GetLiveCall(ctx, record.CallHash)
	if getErr != nil {
		return st, nil
	}
	total, unitsErr := liveUsageUnits(fresh)
	if unitsErr != nil {
		return st, nil
	}
	A := total - st.settledSum
	if A < 0 {
		A = 0
	}
	now := time.Now().UTC()
	horizon := st.openedAt.Add(s.liveLeaseTTL() - s.liveExpirySkew())
	closeByClock := !now.Before(horizon)
	closeByUsage := !now.Before(st.openedAt.Add(s.liveWindowFloor())) && st.estimate > 0 && A >= st.estimate
	if !closeByClock && !closeByUsage {
		return st, nil
	}

	mode := s.canonicalWalletMode()
	// §13.2.3: in disabled mode no settlement is attempted at all; the clock
	// advances on the horizon alone so the record shape stays uniform.
	if mode == config.CanonicalWalletModeDisabled {
		if !closeByClock {
			return st, nil
		}
		next := LiveWindow{WindowSeq: st.seq + 1, OpenedAtMS: now.UnixMilli()}
		if advErr := s.liveProvisional.AdvanceLiveWindow(ctx, st.token, st.seq, 0, next); advErr != nil {
			liveWindowMetrics.reauthRetry.Add(1)
			return st, nil
		}
		liveWindowMetrics.closed.Add(1)
		return s.loadLiveWindowState(ctx, record.CallHash)
	}

	idle := A == 0
	if !idle {
		// §13.2.2: persist FIRST (recovery re-issues this exact amount); skip
		// the write when a pending amount is already persisted.
		if st.pending == 0 {
			if casErr := s.liveProvisional.SetLiveWindowPending(ctx, st.rowToken, st.seq, A); casErr != nil {
				return s.loadLiveWindowState(ctx, record.CallHash) // CAS lost: reload, retry next tick
			}
			st.pending = A
		}
		amount := st.pending
		ok := s.canonicalWallet.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID:   liveWindowRequestID(record.CallHash, st.seq),
			PlatformUserID:     fresh.PlatformUserID,
			Currency:           fresh.BillingCurrency,
			AmountUnits:        amount,
			OccurredAt:         now,
			AuthorizationID:    st.token,
			AuthorizationToken: st.token,
		})
		if !ok {
			liveWindowMetrics.settleRetry.Add(1)
			if closeByClock {
				// §13.2.3: an unsettled window at the horizon closes the
				// session — the money resolves by conversion or the reaper.
				return st, s.hardStopLiveSession(ctx, fresh, upstream, "settle_unresolved")
			}
			return st, nil
		}
	} else {
		// §13.2.3: an idle window writes NO outbox row; its armed hold, if
		// any, is released through §11.7's zero-cost abort point.
		s.canonicalWallet.releaseHoldZeroCost(ctx, fresh.PlatformUserID, st.token)
	}

	// Re-authorize the next window through the ordinary gate at the session's
	// original estimate (§13.2.1). The advance carries the new window
	// {seq+1, lease, token, 0, 0, now} — only after a handle exists.
	if st.next == nil {
		snap, snapErr := s.billingSnapshotSettler.snapshots.Load(ctx, fresh.BillingSnapshotID)
		if snapErr != nil {
			snap = nil
		}
		handle, authErr := s.authorizer.Authorize(ctx, AuthorizeInput{
			Snapshot:           snap,
			User:               &User{PlatformUserID: fresh.PlatformUserID, BillingCurrency: fresh.BillingCurrency},
			FixedEstimateUnits: st.estimate, // the session's ORIGINAL estimate (§13.2.1)
		})
		if authErr != nil && errors.Is(authErr, ErrAuthorizationRefused) {
			// §13.2.4: the hard stop. The refused handle still names the next
			// window (its id, no lease, no hold) — shadow admits below,
			// enforce closes.
			if handle != nil {
				next := LiveWindow{WindowSeq: st.seq + 1, Token: handle.ID, OpenedAtMS: now.UnixMilli()}
				if handle.LeaseID != "" {
					next.LeaseID = handle.LeaseID
				}
				st.next = &next
			}
			if mode == config.CanonicalWalletModeEnforce {
				// The refused handle still names the next window; the advance
				// is best-effort here (a lost CAS retries nothing — the stop
				// is terminal and finalization owns the record from here).
				_ = s.advanceLiveWindowSafe(ctx, st, idle)
				return st, s.hardStopLiveSession(ctx, fresh, upstream, "reauthorization_refused")
			}
			liveWindowMetrics.refusedShadow.Add(1)
			// Shadow admits: the window advances with the refused handle's id
			// and no hold, and the session continues.
		} else if authErr != nil {
			liveWindowMetrics.reauthRetry.Add(1)
			return st, nil // transient: the window stays pending-resolved
		} else if handle != nil {
			next := LiveWindow{WindowSeq: st.seq + 1, Token: handle.ID, OpenedAtMS: now.UnixMilli()}
			if handle.LeaseID != "" {
				next.LeaseID = handle.LeaseID
			}
			st.next = &next
		}
	}
	if st.next == nil {
		return st, nil
	}
	if advErr := s.advanceLiveWindowSafe(ctx, st, idle); advErr != nil {
		return st, nil
	}
	if idle {
		liveWindowMetrics.idle.Add(1)
	} else {
		liveWindowMetrics.closed.Add(1)
	}
	return s.loadLiveWindowState(ctx, record.CallHash)
}

// advanceLiveWindowSafe settles window seq and appends st.next in ONE store
// statement; a failure keeps st.next so the next tick retries the ADVANCE
// with the SAME handle (never a second arm).
func (s *OpenAIGatewayService) advanceLiveWindowSafe(ctx context.Context, st *liveWindowState, idle bool) error {
	settled := st.pending
	if idle {
		settled = 0
	}
	next := *st.next
	if err := s.liveProvisional.AdvanceLiveWindow(ctx, st.rowToken, st.seq, settled, next); err != nil {
		liveWindowMetrics.reauthRetry.Add(1)
		return err
	}
	st.next = nil
	return nil
}

// hardStopLiveSession (§13.2.4): the hard stop. In enforce the observer
// writes session.close on its own sideband conn (non-billable — the close
// frame is sideband, least of all on the path that fires because the user is
// out of money), marks the record closed, counts the refusal, and returns
// ErrLiveCallNotFound so observeLiveCall finalizes. In shadow there is no
// stop (counted, the session continues).
func (s *OpenAIGatewayService) hardStopLiveSession(ctx context.Context, record *LiveCallRecord, upstream liveFrameConn, reason string) error {
	liveWindowMetrics.refused.Add(1)
	logger.L().Warn("openai.live_window_hard_stop",
		zap.String("call_hash", record.CallHash), zap.String("reason", reason))
	if upstream != nil {
		closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = upstream.WriteFrame(WithNonBillableUpstream(closeCtx, NonBillableLiveSideband), coderws.MessageText, []byte(`{"type":"session.close"}`))
		cancel()
	}
	if store, err := s.liveStore(); err == nil {
		markCtx, markCancel := context.WithTimeout(ctx, liveRedisOperationTimeout)
		_, _ = store.MarkLiveCallClosed(markCtx, record.CallHash, liveClosedRecordTTL)
		markCancel()
	}
	return ErrLiveCallNotFound
}

func (s *OpenAIGatewayService) waitForLiveObserverRetry(record *LiveCallRecord) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	<-timer.C
	store, err := s.liveStore()
	if err != nil {
		return false
	}
	controller, err := store.GetLiveController(context.Background(), record.CallHash)
	// 过期不在此处判定：返回 true 让调用方回到循环顶部的过期分支，由它 finalize
	// （写 usage log + 释放租约）。在这里直接返回 false 会让会话静默结束、不留记录。
	return err == nil && controller == LiveControllerObserver
}

func (s *OpenAIGatewayService) refreshLiveLease(record *LiveCallRecord) bool {
	cache, err := s.liveConcurrencyCache()
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	refreshed, err := cache.RefreshLiveLease(ctx, record.AccountID, record.UserID, record.APIKeyID, record.LeaseID)
	return err == nil && refreshed
}

func (s *OpenAIGatewayService) releaseLiveLease(accountID, userID, apiKeyID int64, leaseID string) {
	cache, err := s.liveConcurrencyCache()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	_ = cache.ReleaseLiveLease(ctx, accountID, userID, apiKeyID, leaseID)
}

func (s *OpenAIGatewayService) finalizeLiveCall(record *LiveCallRecord) {
	s.queueLiveFinalization(record)
	if !s.tryFinalizeLiveCall(record) {
		s.scheduleLiveFinalizationRetry(record)
		return
	}
	s.removeLiveFinalization(record)
}

func (s *OpenAIGatewayService) tryFinalizeLiveCall(record *LiveCallRecord) bool {
	if record == nil {
		return true
	}
	store, err := s.liveStore()
	if err != nil {
		return false
	}
	usageStore, ok := s.cache.(LiveUsageStore)
	if !ok {
		return false
	}
	if err := s.flushLiveUsageFallback(record, usageStore); err != nil {
		logger.L().Error("openai.live_usage_flush_failed", zap.String("call_hash", record.CallHash), zap.Error(err))
		return false
	}
	latest, loadErr := store.GetLiveCall(context.Background(), record.CallHash)
	if loadErr != nil {
		return errors.Is(loadErr, ErrLiveCallNotFound)
	}
	if latest.Controller == LiveControllerClosed {
		return true
	}
	record = latest
	if s.usageLogRepo == nil || s.usageBillingRepo == nil {
		return false
	}
	duration := int(time.Since(record.CreatedAt).Milliseconds())
	if duration < 0 {
		duration = 0
	}
	inboundEndpoint := record.InboundEndpoint
	upstreamEndpoint := "/backend-api/codex/realtime/calls"
	userAgent := record.UserAgent
	ipAddress := record.IPAddress
	billingType := int8(BillingTypeBalance)
	if record.SubscriptionID > 0 {
		billingType = BillingTypeSubscription
	}
	inputTokens := record.InputTokens - record.CacheReadTokens
	if inputTokens < 0 {
		inputTokens = 0
	}
	inputCost := float64(inputTokens) * record.InputPricePerToken
	outputCost := float64(record.OutputTokens) * record.OutputPricePerToken
	cacheReadCost := float64(record.CacheReadTokens) * record.CacheReadPricePerToken
	sourceCost := inputCost + outputCost + cacheReadCost
	baseCost := sourceCost * record.ExchangeRate
	actualCost := baseCost * record.RateMultiplier
	usageLog := &UsageLog{
		UserID:             record.UserID,
		APIKeyID:           record.APIKeyID,
		AccountID:          record.AccountID,
		RequestID:          record.CallHash,
		Model:              record.Model,
		RequestedModel:     record.Model,
		GroupID:            liveOptionalID(record.GroupID),
		SubscriptionID:     liveOptionalID(record.SubscriptionID),
		InputTokens:        inputTokens,
		OutputTokens:       record.OutputTokens,
		CacheReadTokens:    record.CacheReadTokens,
		InputCost:          inputCost,
		OutputCost:         outputCost,
		CacheReadCost:      cacheReadCost,
		TotalCost:          sourceCost,
		ActualCost:         actualCost,
		RateMultiplier:     record.RateMultiplier,
		BillingType:        billingType,
		RequestType:        RequestTypeLive,
		DurationMs:         &duration,
		UserAgent:          &userAgent,
		IPAddress:          &ipAddress,
		InboundEndpoint:    &inboundEndpoint,
		UpstreamEndpoint:   &upstreamEndpoint,
		SourceCurrency:     CurrencyUSD,
		SettlementCurrency: record.BillingCurrency,
		ExchangeRate:       record.ExchangeRate,
		ExchangeRateSource: record.ExchangeRateSource,
		ExchangeRateAsOf:   &record.ExchangeRateAsOf,
		SourceCost:         sourceCost,
		BaseCost:           baseCost,
		CreatedAt:          record.CreatedAt,
	}
	liveTargetPlatform := PlatformOpenAI
	usageLog.GovernanceTargetPlatform = &liveTargetPlatform
	if record.BillingSnapshotID != "" {
		snapID := record.BillingSnapshotID
		usageLog.BillingSnapshotID = &snapID
	}
	apiKey := &APIKey{ID: record.APIKeyID, UserID: record.UserID, GroupID: liveOptionalID(record.GroupID), Quota: record.APIKeyQuota, RateLimit5h: record.RateLimit5h, RateLimit1d: record.RateLimit1d, RateLimit7d: record.RateLimit7d}
	user := &User{ID: record.UserID, PlatformUserID: record.PlatformUserID, BillingCurrency: record.BillingCurrency}
	account := &Account{ID: record.AccountID}
	var subscription *UserSubscription
	if record.SubscriptionID > 0 {
		subscription = &UserSubscription{ID: record.SubscriptionID, UserID: record.UserID, GroupID: record.GroupID}
	}
	cost := &CostBreakdown{InputCost: inputCost, OutputCost: outputCost, CacheReadCost: cacheReadCost, TotalCost: sourceCost, ActualCost: actualCost, BillingMode: string(BillingModeToken)}

	applied, _, billingErr := applyUsageBillingDetailed(context.Background(), record.CallHash, usageLog, &postUsageBillingParams{ // billingResult was only consumed by the pre-3.7b per-session observer call; the last-window event carries the remainder directly (§13.2.3)
		Cost: cost,
		User: user, APIKey: apiKey, Account: account, Subscription: subscription,
		IsSubscriptionBill:    record.SubscriptionBilling,
		AccountRateMultiplier: record.AccountRateMultiplier,
		APIKeyService:         liveAPIKeyQuotaUpdater{},
		Platform:              PlatformOpenAI,
	}, s.billingDeps(), s.usageBillingRepo)
	if billingErr != nil {
		logger.L().Error("openai.live_billing_failed", zap.String("call_hash", record.CallHash), zap.Error(billingErr))
		return false
	}

	usageCtx, usageCancel := detachedBillingContext(context.Background())
	_, usageErr := s.usageLogRepo.Create(usageCtx, usageLog)
	usageCancel()
	if usageErr != nil {
		logger.L().Error("openai.live_usage_log_failed", zap.String("call_hash", record.CallHash), zap.Error(usageErr))
		return false
	}

	// Phase 3.3c: Live reaches canonical settlement (spec §2.3, §6 item 5) — with the
	// platform identity finalization never had, the REAL billingApplied, and the attempt's
	// token. One-shot under finalization retries: the row's active → finalizing claim is
	// the guard, and the row is marked finalized only if the bridge committed the outbox
	// row. A pre-3.3c or disabled-mode record (no token) skips the observer exactly as
	// today; a nil store skips it too (constraint 9).
	if s.canonicalWalletMode() != config.CanonicalWalletModeDisabled && record.AuthorizationID != "" && s.liveProvisional != nil {
		claimed, claimErr := s.claimLiveProvisionalFinalization(record.AuthorizationID)
		if claimErr != nil {
			logger.L().Error("openai.live_provisional_finalize_failed", zap.String("call_hash", record.CallHash), zap.Error(claimErr))
			return false // retried by scheduleLiveFinalizationRetry; QueueLiveFinalization keeps the Redis record
		}
		if claimed {
			// Phase 3.7b (§13.2.3): finalization is the LAST window. The
			// provisional row names the windows; the final seq is the row's
			// window count, the amount is the remainder after the settled
			// windows, and the settlement's authorization is the LAST
			// window's token. A pre-3.7 record (one window, never advanced)
			// finalizes exactly as today under seq 1 and the bare call hash.
			seq := 1
			settledSum := int64(0)
			// Window 1's authorization is the record's own (== windows[0].Token
			// in production; the fixture models the settled token there, and
			// the pre-3.7 shape must stay byte-for-byte). Windows ≥ 2 carry
			// their own tokens.
			eventAuthorizationID := record.AuthorizationID
			eventAuthorizationToken := record.AuthorizationToken
			if provRec, provErr := s.liveProvisional.Get(context.Background(), record.AuthorizationID); provErr == nil && len(provRec.Windows) > 0 {
				seq = len(provRec.Windows)
				for _, w := range provRec.Windows[:seq-1] {
					settledSum += w.SettledUnits
				}
				if seq > 1 {
					eventAuthorizationID = provRec.Windows[seq-1].Token
					eventAuthorizationToken = provRec.Windows[seq-1].Token
				}
			}
			remainder := int64(0)
			if units, unitsErr := liveUsageUnits(record); unitsErr == nil {
				remainder = units - settledSum
				if remainder < 0 {
					remainder = 0
				}
			}
			if remainder == 0 {
				// The idle-window rule at finalization: there is nothing to
				// settle — no observer call, no not-enqueued count; the row
				// completes with settledUnits = 0.
				liveWindowMetrics.idleFinalized.Add(1)
				if err := s.completeLiveProvisionalFinalization(record.AuthorizationID, "", 0, seq); err != nil {
					logger.L().Error("openai.live_provisional_complete_failed", zap.String("call_hash", record.CallHash), zap.Error(err))
					return false
				}
			} else {
				// The last window's settlement carries the REMAINDER (§13.2.3),
				// not the session total — observeCanonicalWalletSettlement
				// derives its amount from the cost, so the event is built
				// here. The observer's zero-cost guards (subscription-billed,
				// not applied) route to §11.7's release exactly as before.
				currency := NormalizeUserBillingCurrency(record.BillingCurrency)
				requestID := liveWindowRequestID(record.CallHash, seq)
				eventID := CanonicalWalletSettlementEventID(requestID, user.PlatformUserID, currency)
				ok := false
				if !record.SubscriptionBilling && applied {
					ok = s.canonicalWallet.ObserveSettlement(CanonicalWalletSettlementEvent{
						GatewayRequestID:   requestID,
						PlatformUserID:     user.PlatformUserID,
						Currency:           user.BillingCurrency,
						AmountUnits:        remainder,
						OccurredAt:         time.Now().UTC(),
						AuthorizationToken: eventAuthorizationToken,
						AuthorizationID:    eventAuthorizationID,
					})
				} else {
					relCtx, relCancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
					s.canonicalWallet.releaseHoldZeroCost(relCtx, user.PlatformUserID, eventAuthorizationID)
					relCancel()
				}
				if eventAuthorizationToken != "" && ok {
					if err := s.completeLiveProvisionalFinalization(record.AuthorizationID, eventID, remainder, seq); err != nil {
						logger.L().Error("openai.live_provisional_complete_failed", zap.String("call_hash", record.CallHash), zap.Error(err))
						return false // the row stays finalizing: "settlement outcome unknown" to Phase 4 — the named residual
					}
				} else {
					// The observer's own guards (subscription-billed, non-CNY, zero cost, not applied)
					// legitimately drop the event, and so does a dropped outbox write — distinguishable
					// only by the bridge's counters. Release the claim so a later attempt or Phase 4 can
					// observe again; record no settlement_event_id.
					authorizationMetrics.liveProvisionalSettlementNotEnqueued.Add(1)
					s.releaseLiveProvisionalFinalizationClaim(record.AuthorizationID)
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	first, closeErr := store.MarkLiveCallClosed(ctx, record.CallHash, liveClosedRecordTTL)
	cancel()
	if closeErr != nil {
		return false
	}
	if first {
		s.releaseLiveLease(record.AccountID, record.UserID, record.APIKeyID, record.LeaseID)
	}
	return true
}

func (s *OpenAIGatewayService) scheduleLiveFinalizationRetry(record *LiveCallRecord) {
	if record == nil {
		return
	}
	if _, loaded := s.liveFinalizeRetrying.LoadOrStore(record.CallHash, struct{}{}); loaded {
		return
	}
	copy := *record
	go func() {
		defer s.liveFinalizeRetrying.Delete(copy.CallHash)
		delay := time.Second
		for {
			timer := time.NewTimer(delay)
			<-timer.C
			if s.tryFinalizeLiveCall(&copy) {
				s.removeLiveFinalization(&copy)
				return
			}
			if delay < time.Minute {
				delay *= 2
				if delay > time.Minute {
					delay = time.Minute
				}
			}
		}
	}()
}

func (s *OpenAIGatewayService) queueLiveFinalization(record *LiveCallRecord) {
	if record == nil {
		return
	}
	if store, ok := s.cache.(LiveFinalizationStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		defer cancel()
		if err := store.QueueLiveFinalization(ctx, record.CallHash); err != nil {
			logger.L().Error("openai.live_finalize_queue_failed", zap.String("call_hash", record.CallHash), zap.Error(err))
		}
	}
}

func (s *OpenAIGatewayService) removeLiveFinalization(record *LiveCallRecord) {
	if record == nil {
		return
	}
	if store, ok := s.cache.(LiveFinalizationStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		defer cancel()
		if err := store.RemoveLiveFinalization(ctx, record.CallHash); err != nil {
			logger.L().Error("openai.live_finalize_dequeue_failed", zap.String("call_hash", record.CallHash), zap.Error(err))
		}
	}
}

func (s *OpenAIGatewayService) recoverLiveFinalizations() {
	store, ok := s.cache.(LiveFinalizationStore)
	if !ok || s.usageLogRepo == nil || s.usageBillingRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	hashes, err := store.ListLiveFinalizations(ctx, 1000)
	cancel()
	if err != nil {
		logger.L().Error("openai.live_finalize_recovery_list_failed", zap.Error(err))
		return
	}
	callStore, err := s.liveStore()
	if err != nil {
		return
	}
	for _, callHash := range hashes {
		loadCtx, loadCancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		record, loadErr := callStore.GetLiveCall(loadCtx, callHash)
		loadCancel()
		if loadErr != nil {
			if errors.Is(loadErr, ErrLiveCallNotFound) {
				s.removeLiveFinalization(&LiveCallRecord{CallHash: callHash})
			}
			continue
		}
		s.scheduleLiveFinalizationRetry(record)
	}
}

func (s *OpenAIGatewayService) runLiveFinalizationRecovery() {
	if _, ok := s.cache.(LiveFinalizationStore); !ok || s.usageLogRepo == nil || s.usageBillingRepo == nil {
		return
	}
	for {
		s.recoverLiveFinalizations()
		timer := time.NewTimer(time.Minute)
		<-timer.C
	}
}

// liveAPIKeyQuotaUpdater is a capability marker for the unified billing command.
// The repository applies API-key quota and rate-limit mutations atomically with
// the wallet deduction; these methods are never called while that repository is present.
type liveAPIKeyQuotaUpdater struct{}

func (liveAPIKeyQuotaUpdater) UpdateQuotaUsed(context.Context, int64, float64) error { return nil }

func (liveAPIKeyQuotaUpdater) UpdateRateLimitUsage(context.Context, int64, float64) error { return nil }

func (s *OpenAIGatewayService) accumulateLiveUsage(record *LiveCallRecord, payload []byte) {
	if record == nil {
		return
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType != "response.done" && eventType != "response.completed" {
		return
	}
	usage := gjson.GetBytes(payload, "response.usage")
	if !usage.Exists() {
		usage = gjson.GetBytes(payload, "usage")
	}
	input := int(usage.Get("input_tokens").Int())
	if input == 0 {
		input = int(usage.Get("prompt_tokens").Int())
	}
	output := int(usage.Get("output_tokens").Int())
	if output == 0 {
		output = int(usage.Get("completion_tokens").Int())
	}
	cacheRead := int(usage.Get("input_tokens_details.cached_tokens").Int())
	if cacheRead == 0 {
		cacheRead = int(usage.Get("prompt_tokens_details.cached_tokens").Int())
	}
	if input <= 0 && output <= 0 && cacheRead <= 0 {
		return
	}
	responseID := strings.TrimSpace(gjson.GetBytes(payload, "response.id").String())
	if responseID == "" {
		responseID = fmt.Sprintf("%x", sha256.Sum256(payload))
	}
	store, ok := s.cache.(LiveUsageStore)
	if !ok {
		s.storeLiveUsageFallback(record.CallHash, responseID, liveUsageDelta{inputTokens: input, outputTokens: output, cacheReadTokens: cacheRead})
		return
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		_, err = store.AccumulateLiveUsage(ctx, record.CallHash, responseID, input, output, cacheRead)
		cancel()
		if err == nil {
			return
		}
	}
	s.storeLiveUsageFallback(record.CallHash, responseID, liveUsageDelta{inputTokens: input, outputTokens: output, cacheReadTokens: cacheRead})
	logger.L().Error("openai.live_usage_accumulate_failed", zap.String("call_hash", record.CallHash), zap.String("response_id", responseID), zap.Error(err))
}

func (s *OpenAIGatewayService) storeLiveUsageFallback(callHash, responseID string, delta liveUsageDelta) {
	value, _ := s.liveUsageFallback.LoadOrStore(callHash, &liveUsageFallbackBucket{responses: make(map[string]liveUsageDelta)})
	bucket := value.(*liveUsageFallbackBucket)
	bucket.mu.Lock()
	bucket.responses[responseID] = delta
	bucket.mu.Unlock()
}

func (s *OpenAIGatewayService) flushLiveUsageFallback(record *LiveCallRecord, store LiveUsageStore) error {
	if record == nil || store == nil {
		return nil
	}
	value, ok := s.liveUsageFallback.Load(record.CallHash)
	if !ok {
		return nil
	}
	bucket := value.(*liveUsageFallbackBucket)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	for responseID, delta := range bucket.responses {
		ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		_, err := store.AccumulateLiveUsage(ctx, record.CallHash, responseID, delta.inputTokens, delta.outputTokens, delta.cacheReadTokens)
		cancel()
		if err != nil {
			return err
		}
		delete(bucket.responses, responseID)
	}
	if len(bucket.responses) == 0 {
		s.liveUsageFallback.Delete(record.CallHash)
	}
	return nil
}

func snapshotIDOf(snap *BillingSnapshot) string {
	if snap == nil {
		return ""
	}
	return snap.ID
}

func (s *OpenAIGatewayService) saveLiveProvisional(ctx context.Context, rec *LiveProvisionalRecord, handle *AuthorizationHandle) (bool, error) {
	mode := config.CanonicalWalletModeDisabled
	if s.authorizer != nil {
		mode = s.authorizer.mode()
	} else if s.cfg != nil {
		mode = s.cfg.CanonicalWallet.Mode
	}
	if mode == config.CanonicalWalletModeDisabled || mode == "" {
		return false, nil
	}
	if s.liveProvisional == nil {
		authorizationMetrics.liveProvisionalStoreUnavailable.Add(1)
		if mode == config.CanonicalWalletModeEnforce {
			return false, &AuthorizationRefusedError{
				Reason:          AuthorizationRefusalLiveStoreUnavailable,
				AuthorizationID: handle.ID,
				Cause:           errors.New("live provisional store is nil"),
			}
		}
		return false, nil
	}
	writeTimeout := liveProvisionalWriteTimeout
	if s.authorizer != nil {
		writeTimeout = s.authorizer.requestTimeout()
	} else if s.cfg != nil && s.cfg.CanonicalWallet.RequestTimeoutMS > 0 {
		writeTimeout = time.Duration(s.cfg.CanonicalWallet.RequestTimeoutMS) * time.Millisecond
	}
	saveCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	err := s.liveProvisional.Save(saveCtx, rec)
	if err != nil {
		authorizationMetrics.liveProvisionalStoreUnavailable.Add(1)
		if mode == config.CanonicalWalletModeEnforce {
			return false, &AuthorizationRefusedError{
				Reason:          AuthorizationRefusalLiveStoreUnavailable,
				AuthorizationID: handle.ID,
				Cause:           err,
			}
		}
		logger.FromContext(ctx).Warn("live provisional store save failed in shadow mode", zap.String("authorization_id", handle.ID), zap.Error(err))
		return false, nil
	}
	authorizationMetrics.liveProvisionalWritten.Add(1)
	return true, nil
}

func (s *OpenAIGatewayService) abortLiveProvisional(authorizationID string, cause error) {
	if s.liveProvisional == nil || authorizationID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	if err := s.liveProvisional.Abort(ctx, authorizationID, time.Now().UTC()); err != nil {
		logger.L().Warn("abort live provisional record failed", zap.String("authorization_id", authorizationID), zap.Error(err))
	} else {
		authorizationMetrics.liveProvisionalAborted.Add(1)
	}
}

func (s *OpenAIGatewayService) activateLiveProvisional(authorizationID, callHash string) {
	if s.liveProvisional == nil || authorizationID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	if err := s.liveProvisional.Activate(ctx, authorizationID, callHash, time.Now().UTC()); err != nil {
		logger.L().Warn("activate live provisional record failed", zap.String("authorization_id", authorizationID), zap.String("call_hash", callHash), zap.Error(err))
	} else {
		authorizationMetrics.liveProvisionalActivated.Add(1)
	}
}

func (s *OpenAIGatewayService) claimLiveProvisionalFinalization(authorizationID string) (bool, error) {
	if s.liveProvisional == nil || authorizationID == "" {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	return s.liveProvisional.ClaimFinalization(ctx, authorizationID, time.Now().UTC())
}

func (s *OpenAIGatewayService) completeLiveProvisionalFinalization(authorizationID, eventID string, settledUnits int64, seq int) error {
	if s.liveProvisional == nil || authorizationID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	err := s.liveProvisional.CompleteFinalization(ctx, authorizationID, eventID, settledUnits, seq, time.Now().UTC())
	if err == nil {
		authorizationMetrics.liveProvisionalFinalized.Add(1)
	}
	return err
}

func (s *OpenAIGatewayService) releaseLiveProvisionalFinalizationClaim(authorizationID string) {
	if s.liveProvisional == nil || authorizationID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	defer cancel()
	if err := s.liveProvisional.ReleaseFinalizationClaim(ctx, authorizationID); err != nil {
		logger.L().Warn("release live provisional finalization claim failed", zap.String("authorization_id", authorizationID), zap.Error(err))
	}
}

func (s *OpenAIGatewayService) canonicalWalletMode() string {
	if s == nil {
		return config.CanonicalWalletModeDisabled
	}
	if s.authorizer != nil {
		return s.authorizer.mode()
	}
	if s.cfg != nil {
		return s.cfg.CanonicalWallet.Mode
	}
	return config.CanonicalWalletModeDisabled
}
