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

type ModelGovernanceConnectionHandler struct {
	connService *service.UpstreamConnectionService
	designation *service.AggregatorDesignationService
	reuse       *service.AggregatorConnectionReuseService
	accountRepo service.AccountRepository
}

func NewModelGovernanceConnectionHandler(connSvc *service.UpstreamConnectionService, desig *service.AggregatorDesignationService, reuse *service.AggregatorConnectionReuseService, accountRepo service.AccountRepository) *ModelGovernanceConnectionHandler {
	return &ModelGovernanceConnectionHandler{connService: connSvc, designation: desig, reuse: reuse, accountRepo: accountRepo}
}

// Create registers a new upstream connection (kind/provider/base URL plus an
// at-rest encrypted credential). Replay-safe: submitting the same natural
// identity twice returns the existing active connection instead of minting a
// duplicate. The credential never appears in any response payload.
func (h *ModelGovernanceConnectionHandler) Create(c *gin.Context) {
	if h.connService == nil {
		response.InternalError(c, "connection service not configured")
		return
	}
	var req struct {
		Kind       string  `json:"kind"`
		Provider   *string `json:"provider"`
		BaseURL    string  `json:"base_url"`
		Credential string  `json:"credential"`
		ProxyID    *int64  `json:"proxy_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	var provider *service.GovernanceProvider
	if req.Provider != nil && strings.TrimSpace(*req.Provider) != "" {
		p := service.GovernanceProvider(strings.TrimSpace(*req.Provider))
		provider = &p
	}
	conn, created, err := h.connService.GetOrCreateActive(c.Request.Context(), req.Kind, provider, req.BaseURL, req.Credential, req.ProxyID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	payload := gin.H{
		"connection_id":      conn.ID,
		"kind":               conn.Kind,
		"provider":           conn.Provider,
		"base_url":           conn.BaseURL,
		"credential_version": conn.CredentialVersion,
		"status":             conn.Status,
	}
	if created {
		response.Created(c, payload)
	} else {
		response.Success(c, payload)
	}
}

// DeriveFromAccount creates an aggregator connection from an account's OWN
// saved configuration (credentials.base_url + credentials.api_key) and links
// the account to it. No secret material travels through the request or any
// response; creation is replay-safe via the natural-identity lookup, so a
// repeated click returns the existing connection instead of duplicating.
func (h *ModelGovernanceConnectionHandler) DeriveFromAccount(c *gin.Context) {
	if h.connService == nil || h.accountRepo == nil {
		response.InternalError(c, "derive dependencies not configured")
		return
	}
	if strings.TrimSpace(c.GetHeader("Idempotency-Key")) == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid account id")
		return
	}
	account, err := h.accountRepo.GetByID(c.Request.Context(), accountID)
	if err != nil || account == nil {
		response.NotFound(c, "account not found")
		return
	}
	switch service.GovernanceProvider(account.Platform) {
	case service.GovernanceProviderAnthropic, service.GovernanceProviderOpenAI,
		service.GovernanceProviderGemini, service.GovernanceProviderGrok:
	default:
		response.BadRequest(c, "derive requires a governed platform account")
		return
	}
	baseURL := strings.TrimSpace(account.GetCredential("base_url"))
	if baseURL == "" {
		response.BadRequest(c, "account has no custom base_url configured to derive a connection from")
		return
	}
	apiKey := strings.TrimSpace(account.GetCredential("api_key"))
	if apiKey == "" {
		response.BadRequest(c, "account has no api_key credential to derive a connection from")
		return
	}
	conn, created, err := h.connService.GetOrCreateActive(
		c.Request.Context(), "aggregator", nil, baseURL, apiKey, account.ProxyID,
	)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	linked := false
	if account.ConnectionID == nil || *account.ConnectionID != conn.ID {
		account.ConnectionID = &conn.ID
		if err := h.accountRepo.Update(c.Request.Context(), account); err != nil {
			response.ErrorFrom(c, err)
			return
		}
		linked = true
	} else {
		linked = true
	}
	payload := gin.H{
		"connection_id":      conn.ID,
		"kind":               conn.Kind,
		"base_url":           conn.BaseURL,
		"credential_version": conn.CredentialVersion,
		"status":             conn.Status,
		"account_id":         account.ID,
		"linked":             linked,
	}
	if created {
		response.Created(c, payload)
	} else {
		response.Success(c, payload)
	}
}

// LinkAccount attaches an existing account to an upstream connection so the
// reuse/probe evidence chain covers it. The operation is naturally idempotent
// (relinking the same connection is a no-op state), so replay safety needs no
// dedupe storage; the Idempotency-Key header stays mandatory for write ritual.
func (h *ModelGovernanceConnectionHandler) LinkAccount(c *gin.Context) {
	if h.connService == nil || h.accountRepo == nil {
		response.InternalError(c, "connection link dependencies not configured")
		return
	}
	if strings.TrimSpace(c.GetHeader("Idempotency-Key")) == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid account id")
		return
	}
	var req struct {
		ConnectionID int64 `json:"connection_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.ConnectionID <= 0 {
		response.BadRequest(c, "invalid request body")
		return
	}
	account, err := h.accountRepo.GetByID(c.Request.Context(), accountID)
	if err != nil || account == nil {
		response.NotFound(c, "account not found")
		return
	}
	conn, err := h.connService.Get(c.Request.Context(), req.ConnectionID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if conn == nil {
		response.NotFound(c, "connection not found")
		return
	}
	if err := h.connService.ValidateAccountLink(account.Platform, conn); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	account.ConnectionID = &req.ConnectionID
	if err := h.accountRepo.Update(c.Request.Context(), account); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{
		"linked":             true,
		"account_id":         account.ID,
		"connection_id":      conn.ID,
		"credential_version": conn.CredentialVersion,
	})
}

func (h *ModelGovernanceConnectionHandler) DesignateAggregator(c *gin.Context) {
	connIDStr := c.Param("id")
	connID, err := strconv.ParseInt(connIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid connection id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	ifMatch := strings.TrimSpace(c.GetHeader("If-Match"))
	if ifMatch == "" {
		response.BadRequest(c, "If-Match header is required")
		return
	}
	expectedVersion, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid If-Match")
		return
	}
	if h.designation == nil {
		response.InternalError(c, "designation service not configured")
		return
	}
	assertion, ok := middleware.PlatformAssertionFromContext(c)
	if !ok || assertion == nil || strings.TrimSpace(assertion.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	input := service.DesignateAggregatorInput{
		ConnectionID:   connID,
		ExpectedVersion: expectedVersion,
		ActorID:        assertion.Subject,
		IdempotencyKey: idempotencyKey,
		EvidenceRef:    strings.TrimSpace(c.GetHeader("X-Evidence-Ref")),
	}
	if err := h.designation.DesignateAggregator(c.Request.Context(), input); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"status": "designated"})
}

func (h *ModelGovernanceConnectionHandler) ReuseAggregatorConnection(c *gin.Context) {
	connIDStr := c.Param("id")
	connID, err := strconv.ParseInt(connIDStr, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid connection id")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.BadRequest(c, "Idempotency-Key header is required")
		return
	}
	ifMatch := strings.TrimSpace(c.GetHeader("If-Match"))
	if ifMatch == "" {
		response.BadRequest(c, "If-Match header is required")
		return
	}
	credVersion, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid If-Match")
		return
	}
	if h.reuse == nil {
		response.InternalError(c, "reuse service not configured")
		return
	}
	// Parse body for provider/protocol/endpoint
	var req struct {
		Provider string `json:"provider"`
		Protocol string `json:"protocol"`
		Endpoint string `json:"endpoint"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	normalizedEndpoint, err := service.NormalizeEndpointPath(req.Endpoint)
	if err != nil {
		response.BadRequest(c, "invalid endpoint")
		return
	}
	assertion2, ok2 := middleware.PlatformAssertionFromContext(c)
	if !ok2 || assertion2 == nil || strings.TrimSpace(assertion2.Subject) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing assertion"})
		return
	}
	input := service.ReuseAggregatorConnectionInput{
		ConnectionID:       connID,
		Provider:           service.GovernanceProvider(req.Provider),
		Protocol:           service.AccountProtocol(req.Protocol),
		NormalizedEndpoint: normalizedEndpoint,
		ClientRequestID:    idempotencyKey,
		CredentialVersion:  credVersion,
		ActorID:            assertion2.Subject,
	}
	accountID, err := h.reuse.Reuse(c.Request.Context(), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Created(c, gin.H{
		"account_id":   accountID.AccountID,
		"activated":    accountID.Activated,
		"probe_status": accountID.ProbeStatus,
		"probe_detail": accountID.ProbeDetail,
	})
}
