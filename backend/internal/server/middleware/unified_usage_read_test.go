//go:build unit

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func unifiedUsageKey() *service.APIKey {
	id := "platform-key"
	return &service.APIKey{ID: 7, UserID: 9, PlatformKeyID: &id, Status: service.StatusActive, RoutingMode: service.APIKeyRoutingModeAutoChannels,
		User: &service.User{ID: 9, PlatformUserID: "platform-user", Status: service.StatusActive, BillingCurrency: "USD", StoredBillingCurrency: "USD"}}
}

func TestUnifiedUsageReadExceptionIsExactAndFailClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name, method, path string
		change             func(*service.APIKey)
		allowed            bool
	}{
		{"active", "GET", "/v1/usage", func(*service.APIKey) {}, true},
		{"expired", "GET", "/v1/usage", func(k *service.APIKey) { k.Status = service.StatusAPIKeyExpired }, true},
		{"exhausted", "GET", "/v1/usage", func(k *service.APIKey) { k.Status = service.StatusAPIKeyQuotaExhausted }, true},
		{"post", "POST", "/v1/usage", func(*service.APIKey) {}, false},
		{"near-path", "GET", "/v1/usage/", func(*service.APIKey) {}, false},
		{"alias", "GET", "/usage", func(*service.APIKey) {}, false},
		{"billing", "GET", "/v1/sub2api/billing", func(*service.APIKey) {}, false},
		{"unmanaged", "GET", "/v1/usage", func(k *service.APIKey) { k.PlatformKeyID = nil }, false},
		{"empty-platform-id", "GET", "/v1/usage", func(k *service.APIKey) { empty := " "; k.PlatformKeyID = &empty }, false},
		{"legacy", "GET", "/v1/usage", func(k *service.APIKey) { k.RoutingMode = service.APIKeyRoutingModeLegacyGroup }, false},
		{"restricted", "GET", "/v1/usage", func(k *service.APIKey) { k.RoutingMode = service.APIKeyRoutingModeChannels }, false},
		{"channels", "GET", "/v1/usage", func(k *service.APIKey) { k.ChannelIDs = []int64{1} }, false},
		{"group", "GET", "/v1/usage", func(k *service.APIKey) { group := int64(1); k.GroupID = &group }, false},
		{"cny", "GET", "/v1/usage", func(k *service.APIKey) { k.User.StoredBillingCurrency = "CNY" }, false},
		{"missing-currency", "GET", "/v1/usage", func(k *service.APIKey) { k.User.StoredBillingCurrency = "" }, false},
		{"disabled", "GET", "/v1/usage", func(k *service.APIKey) { k.Status = service.StatusAPIKeyDisabled }, false},
		{"inactive-user", "GET", "/v1/usage", func(k *service.APIKey) { k.User.Status = "disabled" }, false},
		{"missing-user", "GET", "/v1/usage", func(k *service.APIKey) { k.User = nil }, false},
		{"owner-mismatch", "GET", "/v1/usage", func(k *service.APIKey) { k.User.ID++ }, false},
		{"missing-platform-user", "GET", "/v1/usage", func(k *service.APIKey) { k.User.PlatformUserID = "" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := unifiedUsageKey()
			test.change(key)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(test.method, test.path, nil)
			require.Equal(t, test.allowed, platformManagedUsageRead(ctx, key))
		})
	}
}

func TestUnifiedUsageGroupExceptionDoesNotPermitLegacyOrNearbyRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Gateway.ChannelRoutingEnabled = true
	settings := service.NewSettingService(fakeSettingRepo{values: map[string]string{service.SettingKeyAllowUngroupedKeyScheduling: "false"}}, cfg)
	for _, test := range []struct {
		method, path string
		managed      bool
		want         int
	}{
		{"GET", "/v1/usage", true, 204}, {"GET", "/v1/usage", false, 403},
		{"POST", "/v1/usage", true, 403}, {"GET", "/v1/usage/", true, 403},
		{"GET", "/v1/sub2api/billing", true, 403},
	} {
		key := unifiedUsageKey()
		if !test.managed {
			key.PlatformKeyID = nil
		}
		router := gin.New()
		router.Use(func(c *gin.Context) { c.Set(string(ContextKeyAPIKey), key); c.Next() })
		router.Use(RequireGroupAssignment(settings, cfg, AnthropicErrorWriter))
		router.Handle(test.method, test.path, func(c *gin.Context) { c.Status(http.StatusNoContent) })
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(test.method, test.path, nil))
		require.Equal(t, test.want, w.Code, test.method+test.path)
	}
}
