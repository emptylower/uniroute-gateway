package handler

import (
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *ModelCatalogHandler) GetModelChannelPreferences(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.preferences == nil {
		response.NotFound(c, "Model channel preferences are unavailable")
		return
	}
	preferences, err := h.preferences.GetUserModelChannelPreferences(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"model_channel_preferences": preferences})
}

// Both signed platform requests and native user requests derive the owner
// from authentication. The payload cannot select a different user.
func (h *ModelCatalogHandler) PutModelChannelPreferences(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	if h.preferences == nil || h.catalog == nil {
		response.NotFound(c, "Model channel preferences are unavailable")
		return
	}
	var req struct {
		ModelID string `json:"model_id" binding:"required,max=200"`
		Channel string `json:"channel" binding:"required,oneof=official cloud-vendor"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid model channel preference")
		return
	}
	model, err := service.NormalizeModelChannelPreference(req.ModelID, req.Channel)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	quote, err := h.catalog.QuoteChannelCosts(c.Request.Context(), subject.UserID, time.Now(), service.CurrencyUSD)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	found, available := false, false
	for _, group := range quote.Groups {
		for _, entry := range group.Models {
			if entry.ID != model {
				continue
			}
			found = true
			if (req.Channel == service.ModelChannelOfficial && group.EffectiveMultiplier >= 1) || (req.Channel == service.ModelChannelCloudVendor && group.EffectiveMultiplier < 1) {
				available = true
			}
		}
	}
	if !found {
		response.NotFound(c, "Unknown or unavailable text model")
		return
	}
	if !available {
		response.ErrorFrom(c, infraerrors.New(422, "MODEL_CHANNEL_UNAVAILABLE", "The selected channel is unavailable for this model"))
		return
	}
	model, err = h.preferences.SetUserModelChannelPreference(c.Request.Context(), subject.UserID, model, req.Channel)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"model_id": model, "channel": req.Channel})
}
