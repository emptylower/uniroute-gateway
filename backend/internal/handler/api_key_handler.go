// Package handler provides HTTP request handlers for the application.
package handler

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// APIKeyHandler handles API key-related requests
type APIKeyHandler struct {
	apiKeyService            *service.APIKeyService
	channelPreferenceService *service.ChannelPreferenceService
}

func (h *APIKeyHandler) SetChannelPreferenceService(s *service.ChannelPreferenceService) {
	h.channelPreferenceService = s
}

// NewAPIKeyHandler creates a new APIKeyHandler
func NewAPIKeyHandler(apiKeyService *service.APIKeyService) *APIKeyHandler {
	return &APIKeyHandler{
		apiKeyService: apiKeyService,
	}
}

// CreateAPIKeyRequest represents the create API key request payload
type CreateAPIKeyRequest struct {
	Name          string   `json:"name" binding:"required"`
	GroupID       *int64   `json:"group_id"` // nullable
	RoutingMode   string   `json:"routing_mode" binding:"omitempty,oneof=legacy_group channels auto_channels"`
	ChannelIDs    []int64  `json:"channel_ids"`
	CustomKey     *string  `json:"custom_key"`      // 可选的自定义key
	IPWhitelist   []string `json:"ip_whitelist"`    // IP 白名单
	IPBlacklist   []string `json:"ip_blacklist"`    // IP 黑名单
	Quota         *float64 `json:"quota"`           // 配额限制 (USD)
	ExpiresInDays *int     `json:"expires_in_days"` // 过期天数

	// Rate limit fields (0 = unlimited)
	RateLimit5h *float64 `json:"rate_limit_5h"`
	RateLimit1d *float64 `json:"rate_limit_1d"`
	RateLimit7d *float64 `json:"rate_limit_7d"`
}

// UpdateAPIKeyRequest represents the update API key request payload
type UpdateAPIKeyRequest struct {
	Name        string    `json:"name"`
	GroupID     *int64    `json:"group_id"`
	Status      string    `json:"status" binding:"omitempty,oneof=active inactive"`
	IPWhitelist *[]string `json:"ip_whitelist"` // IP 白名单（nil 不修改，空数组清空）
	IPBlacklist *[]string `json:"ip_blacklist"` // IP 黑名单（nil 不修改，空数组清空）
	Quota       *float64  `json:"quota"`        // 配额限制 (USD), 0=无限制
	ExpiresAt   *string   `json:"expires_at"`   // 过期时间 (ISO 8601)
	ResetQuota  *bool     `json:"reset_quota"`  // 重置已用配额

	// Rate limit fields (nil = no change, 0 = unlimited)
	RateLimit5h         *float64 `json:"rate_limit_5h"`
	RateLimit1d         *float64 `json:"rate_limit_1d"`
	RateLimit7d         *float64 `json:"rate_limit_7d"`
	ResetRateLimitUsage *bool    `json:"reset_rate_limit_usage"` // 重置限速用量
}

// List handles listing user's API keys with pagination
// GET /api/v1/api-keys
func (h *APIKeyHandler) List(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	page, pageSize := response.ParsePagination(c)
	params := pagination.PaginationParams{
		Page:      page,
		PageSize:  pageSize,
		SortBy:    c.DefaultQuery("sort_by", "created_at"),
		SortOrder: c.DefaultQuery("sort_order", "desc"),
	}

	// Parse filter parameters
	var filters service.APIKeyListFilters
	if search := strings.TrimSpace(c.Query("search")); search != "" {
		if len(search) > 100 {
			search = search[:100]
		}
		filters.Search = search
	}
	filters.Status = c.Query("status")
	if groupIDStr := c.Query("group_id"); groupIDStr != "" {
		gid, err := strconv.ParseInt(groupIDStr, 10, 64)
		if err == nil {
			filters.GroupID = &gid
		}
	}

	keys, result, err := h.apiKeyService.List(c.Request.Context(), subject.UserID, params, filters)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if h.channelPreferenceService != nil {
		if err := h.channelPreferenceService.Attach(c.Request.Context(), subject.UserID, keys); err != nil {
			response.ErrorFrom(c, err)
			return
		}
	}

	out := make([]dto.APIKey, 0, len(keys))
	for i := range keys {
		out = append(out, *dto.APIKeyFromService(&keys[i]))
	}
	response.Paginated(c, out, result.Total, page, pageSize)
}

// GetByID handles getting a single API key
// GET /api/v1/api-keys/:id
func (h *APIKeyHandler) GetByID(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid key ID")
		return
	}

	key, err := h.apiKeyService.GetByID(c.Request.Context(), keyID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}

	// 验证所有权
	if key.UserID != subject.UserID {
		response.NotFound(c, "API key not found")
		return
	}
	if h.channelPreferenceService != nil {
		if err := h.channelPreferenceService.Attach(c.Request.Context(), subject.UserID, []service.APIKey{*key}); err == nil {
			prefs, prefErr := h.channelPreferenceService.GetAPIKey(c.Request.Context(), key.ID, subject.UserID)
			if prefErr == nil {
				key.RoutingMode = prefs.RoutingMode
				key.ChannelIDs = prefs.ChannelIDs
			}
		}
	}

	response.Success(c, dto.APIKeyFromService(key))
}

// Create handles creating a new API key
// POST /api/v1/api-keys
func (h *APIKeyHandler) Create(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	var req CreateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}

	svcReq := service.CreateAPIKeyRequest{
		Name:          req.Name,
		GroupID:       req.GroupID,
		RoutingMode:   req.RoutingMode,
		ChannelIDs:    req.ChannelIDs,
		CustomKey:     req.CustomKey,
		IPWhitelist:   req.IPWhitelist,
		IPBlacklist:   req.IPBlacklist,
		ExpiresInDays: req.ExpiresInDays,
	}
	if req.Quota != nil {
		svcReq.Quota = *req.Quota
	}
	if req.RateLimit5h != nil {
		svcReq.RateLimit5h = *req.RateLimit5h
	}
	if req.RateLimit1d != nil {
		svcReq.RateLimit1d = *req.RateLimit1d
	}
	if req.RateLimit7d != nil {
		svcReq.RateLimit7d = *req.RateLimit7d
	}

	executeUserIdempotentJSON(c, "user.api_keys.create", req, service.DefaultWriteIdempotencyTTL(), func(ctx context.Context) (any, error) {
		var key *service.APIKey
		var err error
		if h.channelPreferenceService != nil {
			key, err = h.channelPreferenceService.CreateAPIKey(ctx, subject.UserID, svcReq)
		} else {
			key, err = h.apiKeyService.Create(ctx, subject.UserID, svcReq)
		}
		if err != nil {
			return nil, err
		}
		return dto.APIKeyFromService(key), nil
	})
}

type channelPreferencesRequest struct {
	ChannelIDs []int64 `json:"channel_ids" binding:"required,min=1"`
}

type groupPreferencesRequest struct {
	DisabledGroupIDs []int64 `json:"disabled_group_ids"`
}

// GetChannelPreferences returns the channels enabled for one API key.
func (h *APIKeyHandler) GetChannelPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.channelPreferenceService == nil {
		response.NotFound(c, "Channel routing is unavailable")
		return
	}
	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid key ID")
		return
	}
	prefs, err := h.channelPreferenceService.GetAPIKey(c.Request.Context(), keyID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, prefs)
}

// PutChannelPreferences replaces the channels enabled for one API key.
func (h *APIKeyHandler) PutChannelPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.channelPreferenceService == nil {
		response.NotFound(c, "Channel routing is unavailable")
		return
	}
	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid key ID")
		return
	}
	var req channelPreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	prefs, err := h.channelPreferenceService.SetAPIKey(c.Request.Context(), keyID, subject.UserID, req.ChannelIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, prefs)
}

// GetDefaultChannelPreferences returns defaults copied to new API keys.
func (h *APIKeyHandler) GetDefaultChannelPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	ids, err := h.channelPreferenceService.GetDefaults(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"channel_ids": ids})
}

// PutDefaultChannelPreferences replaces defaults copied to new API keys.
func (h *APIKeyHandler) PutDefaultChannelPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	var req channelPreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	ids, err := h.channelPreferenceService.SetDefaults(c.Request.Context(), subject.UserID, req.ChannelIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"channel_ids": ids})
}

// GetGroupPreferences returns routing groups explicitly disabled by the user.
func (h *APIKeyHandler) GetGroupPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.channelPreferenceService == nil {
		response.NotFound(c, "Group routing preferences are unavailable")
		return
	}
	ids, err := h.channelPreferenceService.GetUserDisabledGroupIDs(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"disabled_group_ids": ids})
}

// PutGroupPreferences replaces the user's disabled routing groups.
func (h *APIKeyHandler) PutGroupPreferences(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.channelPreferenceService == nil {
		response.NotFound(c, "Group routing preferences are unavailable")
		return
	}
	var req groupPreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	ids, err := h.channelPreferenceService.SetUserDisabledGroupIDs(c.Request.Context(), subject.UserID, req.DisabledGroupIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"disabled_group_ids": ids})
}

// Update handles updating an API key
// PUT /api/v1/api-keys/:id
func (h *APIKeyHandler) Update(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid key ID")
		return
	}

	var req UpdateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}

	svcReq := service.UpdateAPIKeyRequest{
		IPWhitelist:         req.IPWhitelist,
		IPBlacklist:         req.IPBlacklist,
		Quota:               req.Quota,
		ResetQuota:          req.ResetQuota,
		RateLimit5h:         req.RateLimit5h,
		RateLimit1d:         req.RateLimit1d,
		RateLimit7d:         req.RateLimit7d,
		ResetRateLimitUsage: req.ResetRateLimitUsage,
	}
	if req.Name != "" {
		svcReq.Name = &req.Name
	}
	svcReq.GroupID = req.GroupID
	if req.Status != "" {
		svcReq.Status = &req.Status
	}
	// Parse expires_at if provided
	if req.ExpiresAt != nil {
		if *req.ExpiresAt == "" {
			// Empty string means clear expiration
			svcReq.ExpiresAt = nil
			svcReq.ClearExpiration = true
		} else {
			t, err := time.Parse(time.RFC3339, *req.ExpiresAt)
			if err != nil {
				response.BadRequest(c, "Invalid expires_at format: "+err.Error())
				return
			}
			svcReq.ExpiresAt = &t
		}
	}

	key, err := h.apiKeyService.Update(c.Request.Context(), keyID, subject.UserID, svcReq)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}

	response.Success(c, dto.APIKeyFromService(key))
}

// Delete handles deleting an API key
// DELETE /api/v1/api-keys/:id
func (h *APIKeyHandler) Delete(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid key ID")
		return
	}

	err = h.apiKeyService.Delete(c.Request.Context(), keyID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}

	response.Success(c, gin.H{"message": "API key deleted successfully"})
}

// GetAvailableGroups 获取用户可以绑定的分组列表
// GET /api/v1/groups/available
func (h *APIKeyHandler) GetAvailableGroups(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	groups, err := h.apiKeyService.GetAvailableGroups(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	rates, err := h.apiKeyService.GetUserGroupRates(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}

	type availableGroup struct {
		dto.Group
		EffectiveMultiplier float64 `json:"effective_multiplier"`
	}
	out := make([]availableGroup, 0, len(groups))
	now := time.Now()
	for i := range groups {
		group := groups[i]
		rate := group.RateMultiplier
		if override, ok := rates[group.ID]; ok {
			rate = override
		}
		out = append(out, availableGroup{
			Group:               *dto.GroupFromService(&group),
			EffectiveMultiplier: rate * group.PeakMultiplierAt(now),
		})
	}
	response.Success(c, out)
}

// GetUserGroupRates 获取当前用户的专属分组倍率配置
// GET /api/v1/groups/rates
func (h *APIKeyHandler) GetUserGroupRates(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}

	rates, err := h.apiKeyService.GetUserGroupRates(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}

	response.Success(c, rates)
}
