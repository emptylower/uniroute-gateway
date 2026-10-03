package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RegisterPlatformChatRoutes delegates a signed platform request into the same
// gateway pipeline as an API request. No provider credential leaves the gateway.
func RegisterPlatformChatRoutes(r *gin.Engine, h *handler.Handlers, identities *service.PlatformIdentityService, users *service.UserService, apiKeys *service.APIKeyService, subscriptions *service.SubscriptionService, ops *service.OpsService, settings *service.SettingService, composite *service.CompositeRouteResolver, cfg *config.Config, redisClient *redis.Client) {
	if cfg == nil || !cfg.PlatformIdentity.Enabled || h == nil || h.PlatformInference == nil || h.Gateway == nil || h.OpenAIGateway == nil || identities == nil || users == nil || apiKeys == nil {
		return
	}
	r.GET("/api/internal/v1/users/:platform_user_id/inference/chat/catalog",
		middleware.RequirePlatformAssertion(cfg.PlatformIdentity, service.PlatformInferenceReadScope, redisClient),
		resolveDelegatedPlatformUser(identities, users),
		func(c *gin.Context) { c.Request.URL.Path = "/v1/models"; c.Next() },
		middleware.DelegatedAPIKeyAuth(h.PlatformInference.PlaygroundKey, apiKeys, subscriptions, cfg, true),
		h.Gateway.Models,
	)
	r.POST("/api/internal/v1/users/:platform_user_id/inference/chat",
		middleware.USDLedgerMaintenance(),
		middleware.RequirePlatformAssertion(cfg.PlatformIdentity, service.PlatformInferenceCreateScope, redisClient),
		resolveDelegatedPlatformUser(identities, users),
		// Native endpoint classification controls auto-channel routing and
		// snapshot/authorization selection. Never allow a browser to set it.
		func(c *gin.Context) { c.Request.URL.Path = "/v1/chat/completions"; c.Next() },
		middleware.RequestBodyLimit(cfg.Gateway.TextMaxBodySize),
		middleware.ClientRequestID(),
		handler.OpsErrorLoggerMiddleware(ops),
		handler.InboundEndpointMiddleware(),
		middleware.DelegatedAPIKeyAuth(h.PlatformInference.PlaygroundKey, apiKeys, subscriptions, cfg, false),
		middleware.CanonicalUSDWalletGate(cfg),
		compositeTargetPlatformMiddleware(composite),
		middleware.RequireGroupAssignment(settings, cfg, middleware.AnthropicErrorWriter),
		gatewayChatCompletions(h),
	)
}

func gatewayChatCompletions(h *handler.Handlers) gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := getGroupPlatform(c)
		if selected, ok := dynamicChannelRoutingPlatform(c, h.Gateway); ok {
			platform = selected
		}
		switch platform {
		case service.PlatformOpenAI, service.PlatformGrok, service.PlatformDeepseek, service.PlatformGLM, service.PlatformKimi, service.PlatformQwen, service.PlatformLongcat, service.PlatformBytedance, service.PlatformMinimax:
			h.OpenAIGateway.ChatCompletions(c)
		default:
			h.Gateway.ChatCompletions(c)
		}
	}
}
