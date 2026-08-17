package routes

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type delegatedPlatformKeyRepoStub struct {
	projection *service.PlatformAPIKeyProjection
}

func (s *delegatedPlatformKeyRepoStub) FindProjectedByPlatformKeyID(context.Context, string) (*service.PlatformAPIKeyProjection, error) {
	return s.projection, nil
}

func (s *delegatedPlatformKeyRepoStub) CreateProjected(_ context.Context, projection *service.PlatformAPIKeyProjection, _ string) error {
	copy := *projection
	copy.GatewayAPIKeyID = 88
	s.projection = &copy
	projection.GatewayAPIKeyID = copy.GatewayAPIKeyID
	return nil
}

func (s *delegatedPlatformKeyRepoStub) UpdateProjected(_ context.Context, projection *service.PlatformAPIKeyProjection, _ int64) (bool, error) {
	copy := *projection
	s.projection = &copy
	return true, nil
}

type delegatedIdentityRepoStub struct {
	identity *service.PlatformIdentity
}

func (s delegatedIdentityRepoStub) FindByPlatformUserID(context.Context, string) (*service.PlatformIdentity, error) {
	return s.identity, nil
}

func (s delegatedIdentityRepoStub) CreatePlatformUser(context.Context, string, string) (*service.PlatformIdentity, error) {
	return s.identity, nil
}

type delegatedUserReaderStub struct {
	user *service.User
}

func (s delegatedUserReaderStub) GetByID(context.Context, int64) (*service.User, error) {
	return s.user, nil
}

func delegatedTestConfig() config.PlatformIdentityConfig {
	return config.PlatformIdentityConfig{
		Enabled: true, Issuer: "shipany-test", Audience: "gateway-test",
		Secret: "0123456789abcdef0123456789abcdef", Version: "v1",
	}
}

func signDelegatedAssertion(t *testing.T, cfg config.PlatformIdentityConfig, subject, scope, jti string) string {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": cfg.Issuer, "aud": cfg.Audience, "sub": subject, "scope": scope,
		"iat": now.Unix(), "exp": now.Add(60 * time.Second).Unix(), "jti": jti,
	})
	token.Header["kid"] = cfg.Version
	signed, err := token.SignedString([]byte(cfg.Secret))
	require.NoError(t, err)
	return signed
}

func TestDelegatedPlatformUserResolutionRejectsPanelJWTAndInjectsMappedNumericUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	cfg := delegatedTestConfig()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{identity: &service.PlatformIdentity{
		GatewayUserID: 77, PlatformUserID: "shipany-user-77", Status: service.StatusActive,
	}})
	userReader := delegatedUserReaderStub{user: &service.User{
		ID: 77, Email: "projection@internal.invalid", Role: service.RoleUser,
		Status: service.StatusActive, Concurrency: 9,
	}}

	called := false
	router := gin.New()
	router.GET(
		"/api/internal/v1/users/:platform_user_id/groups/available",
		middleware.RequirePlatformAssertion(cfg, service.PlatformDataReadScope, redisClient),
		resolveDelegatedPlatformUser(identityService, userReader),
		func(c *gin.Context) {
			called = true
			subject, ok := middleware.GetAuthSubjectFromContext(c)
			require.True(t, ok)
			require.EqualValues(t, 77, subject.UserID)
			role, ok := middleware.GetUserRoleFromContext(c)
			require.True(t, ok)
			require.Equal(t, service.RoleUser, role)
			c.Status(http.StatusOK)
		},
	)

	panelToken := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "77", "exp": time.Now().Add(time.Hour).Unix()})
	panelJWT, err := panelToken.SignedString([]byte("different-panel-jwt-secret-32bytes"))
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/api/internal/v1/users/shipany-user-77/groups/available", nil)
	request.Header.Set("Authorization", "Bearer "+panelJWT)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.False(t, called)

	request = httptest.NewRequest(http.MethodGet, "/api/internal/v1/users/shipany-user-77/groups/available", nil)
	request.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformDataReadScope, "read-1"))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.True(t, called)
}

func TestPlatformKeyProjectionRouteRequiresDedicatedScopeSubjectReplayAndAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cfg := delegatedTestConfig()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{identity: &service.PlatformIdentity{
		GatewayUserID: 77, PlatformUserID: "shipany-user-77", Status: service.StatusActive,
	}})
	userReader := delegatedUserReaderStub{user: &service.User{ID: 77, Role: service.RoleUser, Status: service.StatusActive}}
	keyRepo := &delegatedPlatformKeyRepoStub{}
	keyHandler := handler.NewPlatformAPIKeyHandler(service.NewPlatformAPIKeyService(keyRepo, nil))
	auditCalls := 0
	router := gin.New()
	router.POST(
		"/users/:platform_user_id/keys",
		middleware.RequirePlatformAssertion(cfg, service.PlatformKeysWriteScope, redisClient),
		resolveDelegatedPlatformUser(identityService, userReader),
		func(c *gin.Context) { auditCalls++; c.Next() },
		keyHandler.Upsert,
	)
	body := []byte(`{"platform_key_id":"shipany-key-1","key_sha256":"` + strings.Repeat("a", 64) + `","key_prefix":"sk-live-abcd","version":1}`)

	wrongScope := httptest.NewRequest(http.MethodPost, "/users/shipany-user-77/keys", bytes.NewReader(body))
	wrongScope.Header.Set("Content-Type", "application/json")
	wrongScope.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformDataReadScope, "key-wrong-scope"))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, wrongScope)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Zero(t, auditCalls)

	wrongSubject := httptest.NewRequest(http.MethodPost, "/users/shipany-user-77/keys", bytes.NewReader(body))
	wrongSubject.Header.Set("Content-Type", "application/json")
	wrongSubject.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "different-user", service.PlatformKeysWriteScope, "key-wrong-subject"))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, wrongSubject)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Zero(t, auditCalls)

	panelToken := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "77", "exp": time.Now().Add(time.Hour).Unix()})
	panelJWT, err := panelToken.SignedString([]byte("different-panel-jwt-secret-32bytes"))
	require.NoError(t, err)
	panelRequest := httptest.NewRequest(http.MethodPost, "/users/shipany-user-77/keys", bytes.NewReader(body))
	panelRequest.Header.Set("Content-Type", "application/json")
	panelRequest.Header.Set("Authorization", "Bearer "+panelJWT)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, panelRequest)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Zero(t, auditCalls)

	replayToken := signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformKeysWriteScope, "key-one-use")
	var successBody string
	for i, expected := range []int{http.StatusCreated, http.StatusUnauthorized} {
		request := httptest.NewRequest(http.MethodPost, "/users/shipany-user-77/keys", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+replayToken)
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equalf(t, expected, response.Code, "attempt %d", i+1)
		if i == 0 {
			successBody = response.Body.String()
		}
	}
	require.Equal(t, 1, auditCalls)
	require.NotNil(t, keyRepo.projection)
	require.EqualValues(t, 77, keyRepo.projection.GatewayUserID)
	require.NotContains(t, successBody, strings.Repeat("a", 64))

	plaintextBody := []byte(`{"platform_key_id":"shipany-key-2","key_sha256":"` + strings.Repeat("b", 64) + `","key_prefix":"sk-live-efgh","version":1,"key":"plaintext-must-be-rejected"}`)
	plaintextRequest := httptest.NewRequest(http.MethodPost, "/users/shipany-user-77/keys", bytes.NewReader(plaintextBody))
	plaintextRequest.Header.Set("Content-Type", "application/json")
	plaintextRequest.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformKeysWriteScope, "key-plaintext-field"))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, plaintextRequest)
	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestDelegatedGatewayAdminScopeSetsAdminRoleAndFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cfg := delegatedTestConfig()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{identity: &service.PlatformIdentity{
		GatewayUserID: 77, PlatformUserID: "shipany-user-77", Status: service.StatusActive,
	}})
	userReader := delegatedUserReaderStub{user: &service.User{ID: 77, Role: service.RoleUser, Status: service.StatusActive}}
	router := gin.New()
	audited := false
	router.POST(
		"/gateway-admin/:platform_user_id/accounts",
		middleware.RequirePlatformAssertion(cfg, service.PlatformGatewayAdminScope, redisClient),
		resolveDelegatedPlatformUserAs(identityService, userReader, service.RoleAdmin),
		gin.HandlerFunc(func(c *gin.Context) { audited = true; c.Next() }),
		func(c *gin.Context) {
			role, ok := middleware.GetUserRoleFromContext(c)
			require.True(t, ok)
			require.Equal(t, service.RoleAdmin, role)
			c.Status(http.StatusOK)
		},
	)

	wrongScope := httptest.NewRequest(http.MethodPost, "/gateway-admin/shipany-user-77/accounts", nil)
	wrongScope.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformDataReadScope, "admin-wrong-scope"))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, wrongScope)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.False(t, audited)

	panelToken := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "77", "exp": time.Now().Add(time.Hour).Unix()})
	panelJWT, err := panelToken.SignedString([]byte("different-panel-jwt-secret-32bytes"))
	require.NoError(t, err)
	panelRequest := httptest.NewRequest(http.MethodPost, "/gateway-admin/shipany-user-77/accounts", nil)
	panelRequest.Header.Set("Authorization", "Bearer "+panelJWT)
	panelResponse := httptest.NewRecorder()
	router.ServeHTTP(panelResponse, panelRequest)
	require.Equal(t, http.StatusUnauthorized, panelResponse.Code)
	require.False(t, audited)

	wrongSubject := httptest.NewRequest(http.MethodPost, "/gateway-admin/shipany-user-77/accounts", nil)
	wrongSubject.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "different-user", service.PlatformGatewayAdminScope, "admin-wrong-subject"))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, wrongSubject)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.False(t, audited)

	replayToken := signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformGatewayAdminScope, "admin-one-use")
	for i, expected := range []int{http.StatusOK, http.StatusUnauthorized} {
		audited = false
		request := httptest.NewRequest(http.MethodPost, "/gateway-admin/shipany-user-77/accounts", nil)
		request.Header.Set("Authorization", "Bearer "+replayToken)
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equalf(t, expected, response.Code, "attempt %d", i+1)
		require.Equal(t, i == 0, audited)
	}
}

func TestDelegatedPlatformUserResolutionFailsClosedOnScopeSubjectAndReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cfg := delegatedTestConfig()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{identity: &service.PlatformIdentity{
		GatewayUserID: 77, PlatformUserID: "shipany-user-77", Status: service.StatusActive,
	}})
	userReader := delegatedUserReaderStub{user: &service.User{ID: 77, Role: service.RoleUser, Status: service.StatusActive}}
	router := gin.New()
	router.GET("/users/:platform_user_id", middleware.RequirePlatformAssertion(cfg, service.PlatformDataReadScope, redisClient), resolveDelegatedPlatformUser(identityService, userReader), func(c *gin.Context) { c.Status(http.StatusOK) })

	request := httptest.NewRequest(http.MethodGet, "/users/shipany-user-77", nil)
	request.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformPreferencesWriteScope, "wrong-scope"))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)

	request = httptest.NewRequest(http.MethodGet, "/users/shipany-user-77", nil)
	request.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "different-user", service.PlatformDataReadScope, "wrong-subject"))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)

	replayToken := signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformDataReadScope, "one-use")
	for i, expected := range []int{http.StatusOK, http.StatusUnauthorized} {
		request = httptest.NewRequest(http.MethodGet, "/users/shipany-user-77", nil)
		request.Header.Set("Authorization", "Bearer "+replayToken)
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equalf(t, expected, response.Code, "attempt %d", i+1)
	}
}

func TestInactiveDelegatedUserCanOnlyReachKeyRevocationBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	cfg := delegatedTestConfig()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{identity: &service.PlatformIdentity{
		GatewayUserID: 77, PlatformUserID: "shipany-user-77", Status: service.StatusDisabled,
	}})
	userReader := delegatedUserReaderStub{user: &service.User{ID: 77, Role: service.RoleUser, Status: service.StatusDisabled}}
	readRouter := gin.New()
	readRouter.GET(
		"/read/:platform_user_id",
		middleware.RequirePlatformAssertion(cfg, service.PlatformDataReadScope, redisClient),
		resolveDelegatedPlatformUser(identityService, userReader),
		func(c *gin.Context) { c.Status(http.StatusOK) },
	)
	readRequest := httptest.NewRequest(http.MethodGet, "/read/shipany-user-77", nil)
	readRequest.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformDataReadScope, "inactive-read"))
	readResponse := httptest.NewRecorder()
	readRouter.ServeHTTP(readResponse, readRequest)
	require.Equal(t, http.StatusUnauthorized, readResponse.Code)

	keyRouter := gin.New()
	keyRouter.POST(
		"/keys/:platform_user_id",
		middleware.RequirePlatformAssertion(cfg, service.PlatformKeysWriteScope, redisClient),
		resolveDelegatedPlatformUserAllowInactive(identityService, userReader),
		func(c *gin.Context) { c.Status(http.StatusOK) },
	)
	keyRequest := httptest.NewRequest(http.MethodPost, "/keys/shipany-user-77", nil)
	keyRequest.Header.Set("Authorization", "Bearer "+signDelegatedAssertion(t, cfg, "shipany-user-77", service.PlatformKeysWriteScope, "inactive-key-revoke"))
	keyResponse := httptest.NewRecorder()
	keyRouter.ServeHTTP(keyResponse, keyRequest)
	require.Equal(t, http.StatusOK, keyResponse.Code)
}

func TestPlatformIdentityRouteContractIsExplicitAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{})
	handlers := delegatedRouteContractHandlers(identityService, &adminhandler.UsageHandler{})
	RegisterPlatformIdentityRoutes(router, handlers, identityService, &service.UserService{}, middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }), delegatedTestConfig(), redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}))

	paths := routeContractPaths(router)
	for _, allowed := range []string{
		"GET /api/internal/v1/users/:platform_user_id/groups/available",
		"GET /api/internal/v1/users/:platform_user_id/models/catalog",
		"GET /api/internal/v1/users/:platform_user_id/channels/available",
		"GET /api/internal/v1/users/:platform_user_id/usage",
		"GET /api/internal/v1/users/:platform_user_id/channel-preferences",
		"PUT /api/internal/v1/users/:platform_user_id/channel-preferences",
		"POST /api/internal/v1/users/:platform_user_id/keys",
		"POST /api/internal/v1/users/:platform_user_id/keys/:platform_key_id/revoke",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/accounts",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/accounts",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/accounts/generate-auth-url",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/accounts/exchange-code",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/openai/generate-auth-url",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/openai/exchange-code",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/gemini/oauth/auth-url",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/gemini/oauth/exchange-code",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/antigravity/oauth/auth-url",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/antigravity/oauth/exchange-code",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/grok/oauth/auth-url",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/grok/oauth/exchange-code",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/groups",
		"PUT /api/internal/v1/gateway-admin/:platform_user_id/groups/:id",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/channels",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/channel-monitors/:id/run",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/proxies",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/dashboard/stats",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/models/catalog",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/usage",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/usage/stats",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/usage/cleanup-tasks",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/usage/cleanup-tasks",
		"POST /api/internal/v1/gateway-admin/:platform_user_id/usage/cleanup-tasks/:id/cancel",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/ops/account-availability",
		"PUT /api/internal/v1/gateway-admin/:platform_user_id/ops/alert-rules/:id",
		"PUT /api/internal/v1/gateway-admin/:platform_user_id/ops/runtime/logging",
		"PUT /api/internal/v1/gateway-admin/:platform_user_id/ops/settings/metric-thresholds",
	} {
		_, ok := paths[allowed]
		require.Truef(t, ok, "missing allowlisted route %s", allowed)
	}
	for _, forbidden := range []string{"profile", "password", "payment", "subscriptions", "redeem", "affiliate", "api-keys"} {
		for route := range paths {
			require.NotContains(t, route, "/"+forbidden)
		}
	}
	for route := range paths {
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/users")
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/payments")
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/accounts/data")
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/proxies/data")
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/keys")
	}
	for _, omitted := range []string{
		"GET /api/internal/v1/gateway-admin/:platform_user_id/usage/search-users",
		"GET /api/internal/v1/gateway-admin/:platform_user_id/usage/search-api-keys",
	} {
		_, ok := paths[omitted]
		require.Falsef(t, ok, "unexpected delegated route %s", omitted)
	}
}

func TestDelegatedGatewayAdminOptionalUsageAndDashboardRoutesAreNilSafe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	identityService := service.NewPlatformIdentityService(delegatedIdentityRepoStub{})
	handlers := delegatedRouteContractHandlers(identityService, nil)
	handlers.Admin.Dashboard = nil

	require.NotPanics(t, func() {
		RegisterPlatformIdentityRoutes(router, handlers, identityService, &service.UserService{}, middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }), delegatedTestConfig(), redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}))
	})

	paths := routeContractPaths(router)
	_, accountsRegistered := paths["GET /api/internal/v1/gateway-admin/:platform_user_id/accounts"]
	require.True(t, accountsRegistered)
	for route := range paths {
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/usage")
		require.NotContains(t, route, "/gateway-admin/:platform_user_id/dashboard")
	}
}

func delegatedRouteContractHandlers(
	identityService *service.PlatformIdentityService,
	adminUsage *adminhandler.UsageHandler,
) *handler.Handlers {
	return &handler.Handlers{
		PlatformIdentity: handler.NewPlatformIdentityHandler(identityService),
		PlatformAPIKey:   handler.NewPlatformAPIKeyHandler(service.NewPlatformAPIKeyService(&delegatedPlatformKeyRepoStub{}, nil)),
		APIKey:           &handler.APIKeyHandler{},
		AvailableChannel: &handler.AvailableChannelHandler{},
		ModelCatalog:     &handler.ModelCatalogHandler{},
		Usage:            &handler.UsageHandler{},
		Admin: &handler.AdminHandlers{
			Account:                &adminhandler.AccountHandler{},
			OAuth:                  &adminhandler.OAuthHandler{},
			OpenAIOAuth:            &adminhandler.OpenAIOAuthHandler{},
			GeminiOAuth:            &adminhandler.GeminiOAuthHandler{},
			AntigravityOAuth:       &adminhandler.AntigravityOAuthHandler{},
			GrokOAuth:              &adminhandler.GrokOAuthHandler{},
			Dashboard:              &adminhandler.DashboardHandler{},
			Group:                  &adminhandler.GroupHandler{},
			Channel:                &adminhandler.ChannelHandler{},
			ChannelMonitor:         &adminhandler.ChannelMonitorHandler{},
			ChannelMonitorTemplate: &adminhandler.ChannelMonitorRequestTemplateHandler{},
			Proxy:                  &adminhandler.ProxyHandler{},
			Ops:                    &adminhandler.OpsHandler{},
			Usage:                  adminUsage,
		},
	}
}

func routeContractPaths(router *gin.Engine) map[string]struct{} {
	paths := make(map[string]struct{})
	for _, route := range router.Routes() {
		paths[route.Method+" "+route.Path] = struct{}{}
	}
	return paths
}
