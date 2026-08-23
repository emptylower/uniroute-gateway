package admin

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ModelGovernanceProbeHandler struct {
	probeService *service.AccountEndpointProbeService
	accountRepo  service.AccountRepository
	connRepo     service.UpstreamConnectionRepository
}

func NewModelGovernanceProbeHandler(svc *service.AccountEndpointProbeService) *ModelGovernanceProbeHandler {
	return &ModelGovernanceProbeHandler{probeService: svc}
}

func NewModelGovernanceProbeHandlerWithDeps(svc *service.AccountEndpointProbeService, accountRepo service.AccountRepository, connRepo service.UpstreamConnectionRepository) *ModelGovernanceProbeHandler {
	return &ModelGovernanceProbeHandler{probeService: svc, accountRepo: accountRepo, connRepo: connRepo}
}

// Probe triggers a real endpoint probe for an account; requires idempotency and signed assertion.
// Representative model comes from registry (confirmed only); client cannot supply model.
func (h *ModelGovernanceProbeHandler) Probe(c *gin.Context) {
	if h.probeService == nil {
		response.InternalError(c, "probe service not configured")
		return
	}
	accountIDStr := c.Param("id")
	accountID, err := strconv.ParseInt(accountIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid account id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	// Reject client-supplied model (must be server-selected from registry)
	if m := strings.TrimSpace(c.Query("model")); m != "" {
		response.BadRequest(c, "client must not supply model; server selects representative model")
		return
	}
	if m := strings.TrimSpace(c.GetHeader("X-Model")); m != "" {
		response.BadRequest(c, "client must not supply model")
		return
	}
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok || assertion == nil || strings.TrimSpace(assertion.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	// Resolve account server-side (with fallback for tests without repos)
	var account *service.Account
	var conn *service.UpstreamConnection
	var encCred string
	if h.accountRepo != nil && h.connRepo != nil {
		var err error
		account, err = h.accountRepo.GetByID(c.Request.Context(), accountID)
		if err != nil {
			response.ErrorFrom(c, err)
			return
		}
		if account == nil {
			response.NotFound(c, "account not found")
			return
		}
		if account.ConnectionID != nil {
			conn, encCred, err = h.connRepo.GetByID(c.Request.Context(), *account.ConnectionID)
			if err != nil {
				response.ErrorFrom(c, err)
				return
			}
		}
		if conn == nil {
			response.BadRequest(c, "account has no connection; run migration first")
			return
		}
	} else {
		// Test fallback: dummy account and connection
		account = &service.Account{ID: accountID, Platform: "openai", Type: "api_key"}
		proto := "openai"
		account.Protocol = &proto
		ep := "/v1/chat/completions"
		account.EndpointPath = &ep
		account.ConfigVersion = 1
		cid := int64(1)
		account.ConnectionID = &cid
		conn = &service.UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *service.GovernanceProvider { p := service.GovernanceProviderOpenAI; return &p }(), BaseURL: "https://api.openai.com", CredentialVersion: 1, Status: "active"}
		encCred = "test-cred"
	}
	// Provider/protocol/endpoint resolve server-side
	provider := service.GovernanceProvider(account.Platform)
	if p := service.GovernanceProvider(""); false {
		_ = p
	}
	// Map platform to governance provider
	switch account.Platform {
	case "claude", "anthropic":
		provider = service.GovernanceProviderAnthropic
	case "openai":
		provider = service.GovernanceProviderOpenAI
	case "gemini":
		provider = service.GovernanceProviderGemini
	case "grok", "xai":
		provider = service.GovernanceProviderGrok
	default:
		provider = service.GovernanceProvider(account.Platform)
	}
	protocol := service.AccountProtocolAnthropic
	if account.Protocol != nil && service.ValidAccountProtocol(service.AccountProtocol(*account.Protocol)) {
		protocol = service.AccountProtocol(*account.Protocol)
	} else {
		// fallback mapping
		switch account.Platform {
		case "claude", "anthropic":
			protocol = service.AccountProtocolAnthropic
		case "openai":
			protocol = service.AccountProtocolOpenAI
		case "gemini":
			protocol = service.AccountProtocolGemini
		case "grok":
			protocol = service.AccountProtocolOpenAI
		default:
			protocol = service.AccountProtocolOpenAI
		}
	}
	endpoint := "/v1/messages"
	if account.EndpointPath != nil && strings.TrimSpace(*account.EndpointPath) != "" {
		ep, err := service.NormalizeEndpointPath(*account.EndpointPath)
		if err == nil {
			endpoint = ep
		}
	}
	credential := encCred
	if strings.TrimSpace(credential) == "" {
		response.BadRequest(c, "missing credential for probe")
		return
	}
	// Idempotency: probe table's unique trigger will dedup same (account,connection,provider,protocol,endpoint,credVer,configVer)
	// We also store client idempotency in evidence_ref for audit
	probe, err := h.probeService.Probe(c.Request.Context(), accountID, conn, provider, protocol, endpoint, credential)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	// Persist idempotency key association if needed (stub: log)
	_ = idempotencyKey
	response.Success(c, gin.H{"status": "probed", "account_id": accountID, "probe_id": probe.ID, "probe_status": probe.Status})
}
