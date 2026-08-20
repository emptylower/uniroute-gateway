package handler

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// newTestGinContext builds a bare gin.Context backed by an httptest recorder.
func newTestGinContext() *gin.Context {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	return c
}

// TestRecordCyberPolicyIfMarked_NoMark verifies that when no cyber mark is set,
// the function returns immediately and does NOT set the recorded flag.
func TestRecordCyberPolicyIfMarked_NoMark(t *testing.T) {
	c := newTestGinContext()
	h := &OpenAIGatewayHandler{}

	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "", service.ChannelUsageFields{}, "")

	// Flag must NOT be set when there was no mark.
	require.False(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must remain false when no cyber mark is present")
}

// TestRecordCyberPolicyIfMarked_WithMark verifies that:
//  1. When a cyber mark is present, the recorded flag is set (guard activated).
//  2. A second call is a no-op (idempotent guard).
//  3. Nil services do not panic.
func TestRecordCyberPolicyIfMarked_WithMark(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{
		Message:        "flagged",
		Body:           `{"error":{"code":"cyber_policy"}}`,
		UpstreamStatus: 400,
	})

	h := &OpenAIGatewayHandler{} // nil services — must not panic

	// First call: should set the flag.
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "", service.ChannelUsageFields{}, "")
	})
	require.True(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must be true after first call with a mark")

	// Second call: flag already set — must be a no-op (idempotent).
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	})
	// Flag should still be true (not toggled or cleared).
	require.True(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must remain true after second call (guard)")
}

// TestRecordCyberPolicyIfMarked_ForwardSuccessSkipsUsageLog verifies the semantic:
// when forwardErrored=false the function still sets the guard flag (mark present),
// but the cyber usage row is NOT requested (only RecordCyberPolicyEvent fires).
// Since services are nil here we only verify the guard flag and no panic.
func TestRecordCyberPolicyIfMarked_ForwardSuccessSkipsUsageLog(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{
		Message:        "flagged",
		UpstreamStatus: 200,
	})

	h := &OpenAIGatewayHandler{}

	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false /* forwardErrored=false */, "", service.ChannelUsageFields{}, "")
	})
	require.True(t, c.GetBool(cyberPolicyRecordedKey))
}

// TestClearCyberPolicyTurnState verifies F1 at the handler level: after a turn
// is finalized, both the mark and the recorded guard are reset so the next WS
// turn detects/records independently.
func TestClearCyberPolicyTurnState(t *testing.T) {
	c := newTestGinContext()
	h := &OpenAIGatewayHandler{}

	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "turn1", UpstreamStatus: 200})
	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	require.True(t, c.GetBool(cyberPolicyRecordedKey))

	clearCyberPolicyTurnState(c)
	require.Nil(t, service.GetOpsCyberPolicy(c))
	require.False(t, c.GetBool(cyberPolicyRecordedKey))

	// turn2: a fresh cyber hit must be recordable again.
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "turn2", UpstreamStatus: 200})
	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	require.True(t, c.GetBool(cyberPolicyRecordedKey))
	require.Equal(t, "turn2", service.GetOpsCyberPolicy(c).Message)
}

// TestBuildCyberSessionBlockedOpsEntry verifies the locally-rejected request is
// auditable: 403 / phase=request / type=cyber_policy_session_blocked — distinct
// from upstream cyber_policy hits, and it must NOT touch moderation/violation.
func TestBuildCyberSessionBlockedOpsEntry(t *testing.T) {
	entry := buildCyberSessionBlockedOpsEntry(cyberPolicyOpsErrorMeta{
		RequestID: "req-9", Model: "gpt-5", RequestPath: "/openai/v1/responses",
	})
	require.Equal(t, 403, entry.StatusCode)
	require.Equal(t, "cyber_policy_session_blocked", entry.ErrorType)
	require.Equal(t, "request", entry.ErrorPhase)
	require.True(t, entry.IsBusinessLimited)
	require.Equal(t, "gateway_local", entry.ErrorSource)
	require.Equal(t, "platform", entry.ErrorOwner)
	require.Empty(t, entry.ErrorBody, "no session block key → ErrorBody must be empty")

	entryWithKey := buildCyberSessionBlockedOpsEntry(cyberPolicyOpsErrorMeta{
		RequestID: "req-9", Model: "gpt-5", RequestPath: "/openai/v1/responses",
		SessionBlockKey: "abc123",
	})
	require.Equal(t, "session_block_key=abc123", entryWithKey.ErrorBody)
}

// TestRejectIfCyberSessionBlocked_FailOpen verifies fail-open paths: nil handler
// services, no explicit session signal, and (implicitly) disabled switch all
// pass the request through.
func TestRejectIfCyberSessionBlocked_FailOpen(t *testing.T) {
	c := newTestGinContext()
	c.Request = httptest.NewRequest("POST", "/openai/v1/responses", strings.NewReader(`{}`))

	h := &OpenAIGatewayHandler{}
	require.False(t, h.rejectIfCyberSessionBlocked(c, nil, []byte(`{}`), "gpt-5", cyberBlockFormatResponses), "nil apiKey → pass")

	h2 := &OpenAIGatewayHandler{gatewayService: nil}
	key := &service.APIKey{ID: 1}
	require.False(t, h2.rejectIfCyberSessionBlocked(c, key, []byte(`{}`), "gpt-5", cyberBlockFormatResponses), "nil gateway service → pass")
}

// TestRecordCyberPolicyIfMarked_BlockKeyPlumbed verifies the 6th param is
// accepted and a non-empty key with nil gateway service does not panic
// (write-side guards live in the service layer).
func TestRecordCyberPolicyIfMarked_BlockKeyPlumbed(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "x", UpstreamStatus: 400})
	h := &OpenAIGatewayHandler{}
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "deadbeef", service.ChannelUsageFields{}, "")
	})
}

func TestCyberPolicyGovernanceTargetPlatformSnapshotsConcreteTargetBeforeDetachment(t *testing.T) {
	tests := []struct {
		name          string
		requestCtx    context.Context
		groupPlatform string
		want          string
	}{
		{
			name:          "composite target differs from antigravity account native platform",
			requestCtx:    service.WithResolvedTargetPlatform(context.Background(), service.PlatformGemini),
			groupPlatform: service.PlatformComposite,
			want:          service.PlatformGemini,
		},
		{
			name:          "forced target differs from native routing platform",
			requestCtx:    context.WithValue(context.Background(), ctxkey.ForcePlatform, service.PlatformAntigravity),
			groupPlatform: service.PlatformAnthropic,
			want:          service.PlatformAntigravity,
		},
		{
			name:          "native platform remains unchanged",
			requestCtx:    context.Background(),
			groupPlatform: service.PlatformAntigravity,
			want:          service.PlatformAntigravity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestGinContext()
			c.Request = httptest.NewRequest("POST", "/openai/v1/responses", nil).WithContext(tt.requestCtx)
			apiKey := &service.APIKey{Group: &service.Group{Platform: tt.groupPlatform}}

			require.Equal(t, tt.want, cyberPolicyGovernanceTargetPlatform(c, apiKey))
		})
	}
}

type cyberDetachedUsageRepoStub struct {
	service.UsageLogRepository
	created chan *service.UsageLog
}

func (s *cyberDetachedUsageRepoStub) Create(_ context.Context, log *service.UsageLog) (bool, error) {
	s.created <- log
	return true, nil
}

type cyberDetachedUserRepoStub struct {
	service.UserRepository
}

func (s *cyberDetachedUserRepoStub) DeductBalance(context.Context, int64, float64) error {
	return nil
}

type cyberDetachedPlatformQuotaRepoStub struct {
	service.UserPlatformQuotaRepository
	mu       sync.Mutex
	platform string
}

func (s *cyberDetachedPlatformQuotaRepoStub) IncrementUsageWithReset(_ context.Context, _ int64, platform string, _ float64, _ time.Time) error {
	s.mu.Lock()
	s.platform = platform
	s.mu.Unlock()
	return nil
}

func (s *cyberDetachedPlatformQuotaRepoStub) lastPlatform() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.platform
}

func TestRecordCyberPolicyIfMarked_DetachesConcreteEvidenceFromBaselineQuotaPlatform(t *testing.T) {
	tests := []struct {
		name                string
		requestContext      func(context.Context) context.Context
		groupPlatform       string
		account             *service.Account
		wantEvidence        string
		wantBillingPlatform string
	}{
		{
			name: "composite",
			requestContext: func(ctx context.Context) context.Context {
				return service.WithResolvedTargetPlatform(ctx, service.PlatformGemini)
			},
			groupPlatform: service.PlatformComposite,
			account: &service.Account{ID: 301, Platform: service.PlatformAntigravity,
				Extra: map[string]any{"mixed_scheduling": true}},
			wantEvidence:        service.PlatformGemini,
			wantBillingPlatform: service.PlatformComposite,
		},
		{
			name: "forced",
			requestContext: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, ctxkey.ForcePlatform, service.PlatformAntigravity)
			},
			groupPlatform:       service.PlatformAnthropic,
			account:             &service.Account{ID: 302, Platform: service.PlatformAntigravity},
			wantEvidence:        service.PlatformAntigravity,
			wantBillingPlatform: service.PlatformAnthropic,
		},
		{
			name:                "native",
			requestContext:      func(ctx context.Context) context.Context { return ctx },
			groupPlatform:       service.PlatformOpenAI,
			account:             &service.Account{ID: 303, Platform: service.PlatformOpenAI},
			wantEvidence:        service.PlatformOpenAI,
			wantBillingPlatform: service.PlatformOpenAI,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			usageRepo := &cyberDetachedUsageRepoStub{created: make(chan *service.UsageLog, 1)}
			quotaRepo := &cyberDetachedPlatformQuotaRepoStub{}
			billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, quotaRepo)
			t.Cleanup(billingCache.Stop)
			gateway := service.NewOpenAIGatewayService(
				nil, usageRepo, nil, &cyberDetachedUserRepoStub{}, nil, nil, nil, cfg,
				nil, nil, service.NewBillingService(cfg, nil), nil, billingCache, nil,
				&service.DeferredService{}, nil, nil, nil, nil, nil, nil, quotaRepo,
			)
			h := &OpenAIGatewayHandler{gatewayService: gateway}
			groupID := int64(401)
			apiKey := &service.APIKey{
				ID: 201, User: &service.User{ID: 101, BillingCurrency: service.CurrencyUSD}, GroupID: &groupID,
				Group: &service.Group{ID: groupID, Platform: tt.groupPlatform, RateMultiplier: 1},
			}
			baseCtx, cancel := context.WithCancel(context.Background())
			c := newTestGinContext()
			c.Request = httptest.NewRequest("POST", "/openai/v1/responses", nil).WithContext(tt.requestContext(baseCtx))
			service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{
				Message: "blocked", UpstreamStatus: 400, UpstreamInTok: 1200, UpstreamOutTok: 300,
			})

			h.recordCyberPolicyIfMarked(c, apiKey, tt.account, nil, "gpt-5.1", true, "", service.ChannelUsageFields{}, "")
			cancel()

			select {
			case log := <-usageRepo.created:
				require.NotNil(t, log.GovernanceTargetPlatform)
				require.Equal(t, tt.wantEvidence, *log.GovernanceTargetPlatform)
				require.Equal(t, service.RequestTypeCyberBlocked, log.RequestType)
				require.Equal(t, 1200, log.InputTokens)
				require.Equal(t, 300, log.OutputTokens)
				require.Greater(t, log.ActualCost, 0.0)
				require.Equal(t, tt.wantBillingPlatform, quotaRepo.lastPlatform())
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for detached cyber usage persistence")
			}
		})
	}
}

// TestBuildCyberPolicyOpsErrorEntry_StatusCode verifies F6: the ops error log
// records the status the codex client actually received (400 non-stream / 200 stream),
// not a hardcoded 403.
func TestBuildCyberPolicyOpsErrorEntry_StatusCode(t *testing.T) {
	for _, tc := range []struct {
		name           string
		upstreamStatus int
	}{
		{"non_stream_400", 400},
		{"stream_200", 200},
		{"zero_value", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mark := &service.CyberPolicyMark{
				Code:           "cyber_policy",
				Message:        "blocked",
				UpstreamStatus: tc.upstreamStatus,
			}
			entry := buildCyberPolicyOpsErrorEntry(cyberPolicyOpsErrorMeta{
				RequestID: "req-1", Model: "gpt-5", RequestPath: "/openai/v1/responses",
			}, mark)
			require.Equal(t, tc.upstreamStatus, entry.StatusCode)
			require.Equal(t, "cyber_policy", entry.ErrorType)
			require.Equal(t, "request", entry.ErrorPhase)
		})
	}
}
