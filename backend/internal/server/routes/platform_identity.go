package routes

import (
	"context"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RegisterPlatformIdentityRoutes exposes no route unless the bridge is explicitly enabled.
func RegisterPlatformIdentityRoutes(
	r *gin.Engine,
	h *handler.Handlers,
	identityService *service.PlatformIdentityService,
	userService *service.UserService,
	auditLog middleware.AuditLogMiddleware,
	cfg config.PlatformIdentityConfig,
	redisClient *redis.Client,
) {
	if !cfg.Enabled || h == nil || h.PlatformIdentity == nil || h.APIKey == nil || h.AvailableChannel == nil ||
		h.ModelCatalog == nil || h.Usage == nil || identityService == nil || userService == nil || auditLog == nil {
		return
	}
	internal := r.Group("/api/internal/v1/identities")
	internal.POST("", middleware.RequirePlatformAssertion(cfg, service.PlatformIdentityWriteScope, redisClient), h.PlatformIdentity.Upsert)
	internal.GET("/:platform_user_id", middleware.RequirePlatformAssertion(cfg, service.PlatformIdentityReadScope, redisClient), h.PlatformIdentity.Get)

	resolveUser := resolveDelegatedPlatformUser(identityService, userService)
	base := "/api/internal/v1/users/:platform_user_id"

	read := r.Group(base)
	read.Use(middleware.RequirePlatformAssertion(cfg, service.PlatformDataReadScope, redisClient), resolveUser)
	read.GET("/groups/available", h.APIKey.GetAvailableGroups)
	read.GET("/groups/rates", h.APIKey.GetUserGroupRates)
	read.GET("/channels/available", h.AvailableChannel.List)
	read.GET("/models/catalog", h.ModelCatalog.List)
	read.GET("/models/channel-costs", h.ModelCatalog.ListChannelCosts)
	read.GET("/channel-preferences", h.APIKey.GetDefaultChannelPreferences)
	read.GET("/group-preferences", h.APIKey.GetGroupPreferences)
	read.GET("/usage", h.Usage.List)
	read.GET("/usage/errors", h.Usage.ListErrors)
	read.GET("/usage/errors/:id", h.Usage.GetErrorDetail)
	read.GET("/usage/stats", h.Usage.Stats)
	read.GET("/usage/dashboard/stats", h.Usage.DashboardStats)
	read.GET("/usage/dashboard/trend", h.Usage.DashboardTrend)
	read.GET("/usage/dashboard/models", h.Usage.DashboardModels)
	read.GET("/usage/dashboard/snapshot-v2", h.Usage.DashboardSnapshotV2)
	read.GET("/usage/:id", h.Usage.GetByID)

	preferences := r.Group(base)
	preferences.Use(middleware.RequirePlatformAssertion(cfg, service.PlatformPreferencesWriteScope, redisClient), resolveUser, gin.HandlerFunc(auditLog))
	preferences.PUT("/channel-preferences", h.APIKey.PutDefaultChannelPreferences)
	preferences.PUT("/group-preferences", h.APIKey.PutGroupPreferences)

	if h.PlatformAPIKey != nil {
		keys := r.Group(base)
		keys.Use(
			middleware.RequirePlatformAssertion(cfg, service.PlatformKeysWriteScope, redisClient),
			resolveDelegatedPlatformUserAllowInactive(identityService, userService),
			gin.HandlerFunc(auditLog),
		)
		keys.POST("/keys", h.PlatformAPIKey.Upsert)
		keys.POST("/keys/:platform_key_id/revoke", h.PlatformAPIKey.Revoke)
	}

	registerDelegatedGatewayAdminRoutes(r, h, identityService, userService, auditLog, cfg, redisClient)
}

func registerDelegatedGatewayAdminRoutes(
	r *gin.Engine,
	h *handler.Handlers,
	identityService *service.PlatformIdentityService,
	userService delegatedUserReader,
	auditLog middleware.AuditLogMiddleware,
	cfg config.PlatformIdentityConfig,
	redisClient *redis.Client,
) {
	if h.Admin == nil || h.Admin.Account == nil || h.Admin.Group == nil || h.Admin.Channel == nil ||
		h.Admin.ChannelMonitor == nil || h.Admin.ChannelMonitorTemplate == nil || h.Admin.Proxy == nil ||
		h.Admin.Ops == nil {
		return
	}

	admin := r.Group("/api/internal/v1/gateway-admin/:platform_user_id")
	admin.Use(
		middleware.RequirePlatformAssertion(cfg, service.PlatformGatewayAdminScope, redisClient),
		resolveDelegatedPlatformUserAs(identityService, userService, service.RoleAdmin),
		gin.HandlerFunc(auditLog),
	)

	accounts := admin.Group("/accounts")
	accounts.GET("", h.Admin.Account.List)
	accounts.GET("/antigravity/default-model-mapping", h.Admin.Account.GetAntigravityDefaultModelMapping)
	accounts.POST("/models/sync-upstream-preview", h.Admin.Account.SyncUpstreamModelsPreview)
	accounts.GET("/:id", h.Admin.Account.GetByID)
	accounts.POST("", h.Admin.Account.Create)
	accounts.PUT("/:id", h.Admin.Account.Update)
	accounts.PUT("/:id/upstream-billing-probe", h.Admin.Account.SetUpstreamBillingProbeEnabled)
	accounts.DELETE("/:id", h.Admin.Account.Delete)
	accounts.POST("/:id/test", h.Admin.Account.Test)
	accounts.POST("/:id/recover-state", h.Admin.Account.RecoverState)
	accounts.POST("/:id/refresh", h.Admin.Account.Refresh)
	accounts.GET("/:id/stats", h.Admin.Account.GetStats)
	accounts.GET("/:id/usage", h.Admin.Account.GetUsage)
	accounts.GET("/:id/today-stats", h.Admin.Account.GetTodayStats)
	accounts.POST("/:id/clear-error", h.Admin.Account.ClearError)
	accounts.POST("/:id/clear-rate-limit", h.Admin.Account.ClearRateLimit)
	accounts.POST("/:id/reset-quota", h.Admin.Account.ResetQuota)
	accounts.GET("/:id/temp-unschedulable", h.Admin.Account.GetTempUnschedulable)
	accounts.DELETE("/:id/temp-unschedulable", h.Admin.Account.ClearTempUnschedulable)
	accounts.POST("/:id/schedulable", h.Admin.Account.SetSchedulable)
	accounts.GET("/:id/models", h.Admin.Account.GetAvailableModels)
	accounts.POST("/:id/models/sync-upstream", h.Admin.Account.SyncUpstreamModels)
	if h.Admin.OAuth != nil {
		accounts.POST("/generate-auth-url", h.Admin.OAuth.GenerateAuthURL)
		accounts.POST("/generate-setup-token-url", h.Admin.OAuth.GenerateSetupTokenURL)
		accounts.POST("/exchange-code", h.Admin.OAuth.ExchangeCode)
		accounts.POST("/exchange-setup-token-code", h.Admin.OAuth.ExchangeSetupTokenCode)
	}

	if h.Admin.OpenAIOAuth != nil {
		openai := admin.Group("/openai")
		openai.POST("/generate-auth-url", h.Admin.OpenAIOAuth.GenerateAuthURL)
		openai.POST("/exchange-code", h.Admin.OpenAIOAuth.ExchangeCode)
	}
	if h.Admin.GeminiOAuth != nil {
		gemini := admin.Group("/gemini/oauth")
		gemini.POST("/auth-url", h.Admin.GeminiOAuth.GenerateAuthURL)
		gemini.POST("/exchange-code", h.Admin.GeminiOAuth.ExchangeCode)
	}
	if h.Admin.AntigravityOAuth != nil {
		antigravity := admin.Group("/antigravity/oauth")
		antigravity.POST("/auth-url", h.Admin.AntigravityOAuth.GenerateAuthURL)
		antigravity.POST("/exchange-code", h.Admin.AntigravityOAuth.ExchangeCode)
	}
	if h.Admin.GrokOAuth != nil {
		grok := admin.Group("/grok/oauth")
		grok.POST("/auth-url", h.Admin.GrokOAuth.GenerateAuthURL)
		grok.POST("/exchange-code", h.Admin.GrokOAuth.ExchangeCode)
	}

	groups := admin.Group("/groups")
	groups.GET("", h.Admin.Group.List)
	groups.GET("/all", h.Admin.Group.GetAll)
	groups.GET("/usage-summary", h.Admin.Group.GetUsageSummary)
	groups.GET("/capacity-summary", h.Admin.Group.GetCapacitySummary)
	groups.GET("/live-capability", h.Admin.Group.GetLiveCapability)
	groups.PUT("/sort-order", h.Admin.Group.UpdateSortOrder)
	groups.GET("/:id/models-list-candidates", h.Admin.Group.GetModelsListCandidates)
	groups.GET("/:id/composite-routes", h.Admin.Group.ListCompositeRoutes)
	groups.POST("/:id/composite-routes", h.Admin.Group.CreateCompositeRoute)
	groups.POST("/:id/composite-routes/preview", h.Admin.Group.PreviewCompositeRoute)
	groups.PUT("/:id/composite-routes/:route_id", h.Admin.Group.UpdateCompositeRoute)
	groups.DELETE("/:id/composite-routes/:route_id", h.Admin.Group.DeleteCompositeRoute)
	groups.GET("/:id", h.Admin.Group.GetByID)
	groups.POST("", h.Admin.Group.Create)
	groups.POST("/:id/duplicate", h.Admin.Group.Duplicate)
	groups.PUT("/:id", h.Admin.Group.Update)
	groups.DELETE("/:id", h.Admin.Group.Delete)
	groups.GET("/:id/stats", h.Admin.Group.GetStats)

	channels := admin.Group("/channels")
	channels.GET("", h.Admin.Channel.List)
	channels.GET("/model-pricing", h.Admin.Channel.GetModelDefaultPricing)
	channels.GET("/pricing/sync-models", h.Admin.Channel.SyncPricingModels)
	channels.GET("/:id", h.Admin.Channel.GetByID)
	channels.POST("", h.Admin.Channel.Create)
	channels.PUT("/:id", h.Admin.Channel.Update)
	channels.DELETE("/:id", h.Admin.Channel.Delete)

	monitors := admin.Group("/channel-monitors")
	monitors.GET("", h.Admin.ChannelMonitor.List)
	monitors.POST("", h.Admin.ChannelMonitor.Create)
	monitors.GET("/:id", h.Admin.ChannelMonitor.Get)
	monitors.POST("/:id/duplicate", h.Admin.ChannelMonitor.Duplicate)
	monitors.PUT("/:id", h.Admin.ChannelMonitor.Update)
	monitors.DELETE("/:id", h.Admin.ChannelMonitor.Delete)
	monitors.POST("/:id/run", h.Admin.ChannelMonitor.Run)
	monitors.GET("/:id/history", h.Admin.ChannelMonitor.History)

	templates := admin.Group("/channel-monitor-templates")
	templates.GET("", h.Admin.ChannelMonitorTemplate.List)
	templates.POST("", h.Admin.ChannelMonitorTemplate.Create)
	templates.GET("/:id", h.Admin.ChannelMonitorTemplate.Get)
	templates.PUT("/:id", h.Admin.ChannelMonitorTemplate.Update)
	templates.DELETE("/:id", h.Admin.ChannelMonitorTemplate.Delete)
	templates.GET("/:id/monitors", h.Admin.ChannelMonitorTemplate.AssociatedMonitors)
	templates.POST("/:id/apply", h.Admin.ChannelMonitorTemplate.Apply)

	proxies := admin.Group("/proxies")
	proxies.GET("", h.Admin.Proxy.List)
	proxies.GET("/all", h.Admin.Proxy.GetAll)
	proxies.GET("/:id", h.Admin.Proxy.GetByID)
	proxies.POST("", h.Admin.Proxy.Create)
	proxies.PUT("/:id", h.Admin.Proxy.Update)
	proxies.DELETE("/:id", h.Admin.Proxy.Delete)
	proxies.POST("/:id/test", h.Admin.Proxy.Test)
	proxies.POST("/:id/quality-check", h.Admin.Proxy.CheckQuality)
	proxies.GET("/:id/stats", h.Admin.Proxy.GetStats)
	proxies.GET("/:id/accounts", h.Admin.Proxy.GetProxyAccounts)

	if h.Admin.Dashboard != nil {
		dashboard := admin.Group("/dashboard")
		dashboard.GET("/stats", h.Admin.Dashboard.GetStats)
	}

	models := admin.Group("/models")
	models.GET("/catalog", h.ModelCatalog.List)
	models.GET("/channel-costs", h.ModelCatalog.ListChannelCosts)

	if h.Admin.Usage != nil {
		usage := admin.Group("/usage")
		usage.GET("", h.Admin.Usage.List)
		usage.GET("/stats", h.Admin.Usage.Stats)
		usage.GET("/cleanup-tasks", h.Admin.Usage.ListCleanupTasks)
		usage.POST("/cleanup-tasks", h.Admin.Usage.CreateCleanupTask)
		usage.POST("/cleanup-tasks/:id/cancel", h.Admin.Usage.CancelCleanupTask)
	}

	ops := admin.Group("/ops")
	ops.GET("/concurrency", h.Admin.Ops.GetConcurrencyStats)
	ops.GET("/user-concurrency", h.Admin.Ops.GetUserConcurrencyStats)
	ops.GET("/account-availability", h.Admin.Ops.GetAccountAvailability)
	ops.GET("/realtime-traffic", h.Admin.Ops.GetRealtimeTrafficSummary)
	ops.GET("/alert-rules", h.Admin.Ops.ListAlertRules)
	ops.POST("/alert-rules", h.Admin.Ops.CreateAlertRule)
	ops.PUT("/alert-rules/:id", h.Admin.Ops.UpdateAlertRule)
	ops.DELETE("/alert-rules/:id", h.Admin.Ops.DeleteAlertRule)
	ops.GET("/alert-events", h.Admin.Ops.ListAlertEvents)
	ops.GET("/alert-events/:id", h.Admin.Ops.GetAlertEvent)
	ops.PUT("/alert-events/:id/status", h.Admin.Ops.UpdateAlertEventStatus)
	ops.POST("/alert-silences", h.Admin.Ops.CreateAlertSilence)
	ops.GET("/runtime/alert", h.Admin.Ops.GetAlertRuntimeSettings)
	ops.PUT("/runtime/alert", h.Admin.Ops.UpdateAlertRuntimeSettings)
	ops.GET("/runtime/logging", h.Admin.Ops.GetRuntimeLogConfig)
	ops.PUT("/runtime/logging", h.Admin.Ops.UpdateRuntimeLogConfig)
	ops.POST("/runtime/logging/reset", h.Admin.Ops.ResetRuntimeLogConfig)
	ops.GET("/advanced-settings", h.Admin.Ops.GetAdvancedSettings)
	ops.PUT("/advanced-settings", h.Admin.Ops.UpdateAdvancedSettings)
	ops.GET("/settings/metric-thresholds", h.Admin.Ops.GetMetricThresholds)
	ops.PUT("/settings/metric-thresholds", h.Admin.Ops.UpdateMetricThresholds)
	ops.GET("/errors", h.Admin.Ops.GetErrorLogs)
	ops.GET("/errors/:id", h.Admin.Ops.GetErrorLogByID)
	ops.PUT("/errors/:id/resolve", h.Admin.Ops.UpdateErrorResolution)
	ops.GET("/request-errors", h.Admin.Ops.ListRequestErrors)
	ops.GET("/request-errors/:id", h.Admin.Ops.GetRequestError)
	ops.GET("/request-errors/:id/upstream-errors", h.Admin.Ops.ListRequestErrorUpstreamErrors)
	ops.PUT("/request-errors/:id/resolve", h.Admin.Ops.ResolveRequestError)
	ops.GET("/ingress-rejections", h.Admin.Ops.ListIngressRejects)
	ops.GET("/ingress-rejections/health", h.Admin.Ops.GetIngressRejectHealth)
	ops.GET("/auth-cache-invalidation/health", h.Admin.Ops.GetAuthCacheInvalidationHealth)
	ops.GET("/upstream-errors", h.Admin.Ops.ListUpstreamErrors)
	ops.GET("/upstream-errors/:id", h.Admin.Ops.GetUpstreamError)
	ops.PUT("/upstream-errors/:id/resolve", h.Admin.Ops.ResolveUpstreamError)
	ops.GET("/requests", h.Admin.Ops.ListRequestDetails)
	ops.GET("/system-logs", h.Admin.Ops.ListSystemLogs)
	ops.POST("/system-logs/cleanup", h.Admin.Ops.CleanupSystemLogs)
	ops.GET("/system-logs/health", h.Admin.Ops.GetSystemLogIngestionHealth)
	ops.GET("/dashboard/snapshot-v2", h.Admin.Ops.GetDashboardSnapshotV2)
	ops.GET("/dashboard/overview", h.Admin.Ops.GetDashboardOverview)
	ops.GET("/dashboard/throughput-trend", h.Admin.Ops.GetDashboardThroughputTrend)
	ops.GET("/dashboard/latency-histogram", h.Admin.Ops.GetDashboardLatencyHistogram)
	ops.GET("/dashboard/error-trend", h.Admin.Ops.GetDashboardErrorTrend)
	ops.GET("/dashboard/error-distribution", h.Admin.Ops.GetDashboardErrorDistribution)
	ops.GET("/dashboard/openai-token-stats", h.Admin.Ops.GetDashboardOpenAITokenStats)
}

type delegatedUserReader interface {
	GetByID(ctx context.Context, id int64) (*service.User, error)
}

func resolveDelegatedPlatformUser(identityService *service.PlatformIdentityService, userService delegatedUserReader) gin.HandlerFunc {
	return resolveDelegatedPlatformUserWithOptions(identityService, userService, "", false)
}

func resolveDelegatedPlatformUserAs(identityService *service.PlatformIdentityService, userService delegatedUserReader, roleOverride string) gin.HandlerFunc {
	return resolveDelegatedPlatformUserWithOptions(identityService, userService, roleOverride, false)
}

func resolveDelegatedPlatformUserAllowInactive(identityService *service.PlatformIdentityService, userService delegatedUserReader) gin.HandlerFunc {
	return resolveDelegatedPlatformUserWithOptions(identityService, userService, "", true)
}

func resolveDelegatedPlatformUserWithOptions(identityService *service.PlatformIdentityService, userService delegatedUserReader, roleOverride string, allowInactive bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		platformUserID := strings.TrimSpace(c.Param("platform_user_id"))
		assertion, ok := middleware.PlatformAssertionFromContext(c)
		if !ok || assertion.Subject != platformUserID {
			response.Forbidden(c, "assertion subject does not match platform_user_id")
			c.Abort()
			return
		}
		identity, err := identityService.Get(c.Request.Context(), platformUserID)
		if err != nil {
			response.ErrorFrom(c, err)
			c.Abort()
			return
		}
		user, err := userService.GetByID(c.Request.Context(), identity.GatewayUserID)
		if err != nil {
			response.ErrorFrom(c, err)
			c.Abort()
			return
		}
		if !allowInactive && !user.IsActive() {
			response.Unauthorized(c, "gateway user is inactive")
			c.Abort()
			return
		}
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: user.ID, Concurrency: user.Concurrency})
		role := user.Role
		if roleOverride != "" {
			role = roleOverride
		}
		c.Set(string(middleware.ContextKeyUserRole), role)
		c.Set(middleware.ContextKeyAuthEmail, user.Email)
		requestContext := context.WithValue(c.Request.Context(), ctxkey.UserID, user.ID)
		c.Request = c.Request.WithContext(requestContext)
		c.Next()
	}
}
