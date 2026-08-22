package handler

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Embedded-interface wrappers: non-nil values satisfying the interfaces that
// are only stored (never called) inside ProvideAdminHandlers.
type governanceAccountRepositoryStub struct {
	service.AccountRepository
}

type governanceUpstreamConnectionRepositoryStub struct {
	service.UpstreamConnectionRepository
}

// TestProvideAdminHandlers_WiresGovernanceHandlersForReal guards the Phase 7
// Task 0 G1 fix: the probe and connection handlers must be constructed with
// their real services, never nil. The earlier escape happened because unit
// tests construct handlers with fakes and nothing asserted the production
// wiring, so ProvideAdminHandlers silently passed nil (registered-but-dead
// endpoints returning "not configured" 500s).
func TestProvideAdminHandlers_WiresGovernanceHandlersForReal(t *testing.T) {
	adminHandlers := ProvideAdminHandlers(
		nil, // dashboardHandler
		nil, // userHandler
		nil, // groupHandler
		admin.NewAccountHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil), // accountHandler (Setters are called on it)
		nil, // announcementHandler
		nil, // dataManagementHandler
		nil, // backupHandler
		nil, // oauthHandler
		nil, // openaiOAuthHandler
		nil, // geminiOAuthHandler
		nil, // antigravityOAuthHandler
		nil, // grokOAuthHandler
		nil, // proxyHandler
		nil, // redeemHandler
		nil, // promoHandler
		nil, // settingHandler
		nil, // opsHandler
		nil, // systemHandler
		nil, // subscriptionHandler
		nil, // usageHandler
		nil, // userAttributeHandler
		nil, // errorPassthroughHandler
		nil, // tlsFingerprintProfileHandler
		nil, // apiKeyHandler
		nil, // scheduledTestHandler
		nil, // channelHandler
		nil, // channelMonitorHandler
		nil, // channelMonitorTemplateHandler
		nil, // contentModerationHandler
		nil, // promptAuditHandler
		nil, // paymentHandler
		nil, // affiliateHandler
		nil, // complianceHandler
		nil, // auditLogHandler
		nil, // modelGovernanceInventoryHandler
		nil, // modelRegistryHandler
		nil, // modelGovernanceService
		nil, // upstreamBillingProbe
		nil, // ollamaCloudUsage
		nil, // quarantineService
		nil, // activationService
		nil, // modelCatalogCandidateHandler
		service.NewAccountEndpointProbeService(nil, &http.Client{}),             // accountEndpointProbeService
		service.NewUpstreamConnectionService(nil, nil),                          // upstreamConnectionService
		service.NewAggregatorDesignationService(nil),                            // aggregatorDesignationService
		service.NewAggregatorConnectionReuseServiceWithRepo(nil, nil, nil, nil), // aggregatorConnectionReuseService
		governanceAccountRepositoryStub{},                                       // accountRepository
		governanceUpstreamConnectionRepositoryStub{},                            // upstreamConnectionRepository
	)
	require.NotNil(t, adminHandlers)

	assertNotNilField(t, adminHandlers.ModelGovernanceProbe, "probeService")
	assertNotNilField(t, adminHandlers.ModelGovernanceProbe, "accountRepo")
	assertNotNilField(t, adminHandlers.ModelGovernanceProbe, "connRepo")
	assertNotNilField(t, adminHandlers.ModelGovernanceConnection, "connService")
	assertNotNilField(t, adminHandlers.ModelGovernanceConnection, "designation")
	assertNotNilField(t, adminHandlers.ModelGovernanceConnection, "reuse")
}

func assertNotNilField(t *testing.T, target any, field string) {
	t.Helper()
	require.NotNil(t, target)
	value := reflect.ValueOf(target).Elem().FieldByName(field)
	require.True(t, value.IsValid(), "field %s does not exist", field)
	require.False(t, value.IsNil(), "field %s must not be nil", field)
}
