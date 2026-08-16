package handler

import (
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ModelCatalogHandler struct {
	catalog        *service.ModelCatalogService
	settingService *service.SettingService
}

func NewModelCatalogHandler(
	catalog *service.ModelCatalogService,
	settingService *service.SettingService,
) *ModelCatalogHandler {
	return &ModelCatalogHandler{catalog: catalog, settingService: settingService}
}

// List returns a channel-neutral model catalog for the authenticated user.
// GET /api/v1/models/catalog?category=text
func (h *ModelCatalogHandler) List(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	category := strings.TrimSpace(strings.ToLower(c.DefaultQuery("category", "text")))
	if category != "text" {
		response.BadRequest(c, "Only the text model category is supported")
		return
	}
	if h.settingService == nil || !h.settingService.GetAvailableChannelsRuntime(c.Request.Context()).Enabled {
		response.Success(c, gin.H{"items": []service.TextModelCatalogItem{}})
		return
	}
	items, err := h.catalog.ListText(c.Request.Context(), subject.UserID, time.Now())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"items": items})
}

// ListChannelCosts returns per-routing-group standard token prices in the requested currency.
// GET /api/v1/models/channel-costs?category=text
func (h *ModelCatalogHandler) ListChannelCosts(c *gin.Context) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	category := strings.TrimSpace(strings.ToLower(c.DefaultQuery("category", "text")))
	if category != "text" {
		response.BadRequest(c, "Only the text model category is supported")
		return
	}
	currencyParam := strings.TrimSpace(c.Query("currency"))
	if currencyParam == "" {
		currencyParam = strings.TrimSpace(c.Query("display_currency"))
	}
	currency, err := service.NormalizeBillingCurrency(currencyParam)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if h.settingService == nil || !h.settingService.GetAvailableChannelsRuntime(c.Request.Context()).Enabled {
		quote, quoteErr := h.catalog.QuoteChannelCosts(c.Request.Context(), subject.UserID, time.Now(), currency)
		if quoteErr != nil {
			response.ErrorFrom(c, quoteErr)
			return
		}
		quote.Groups = []service.RoutingGroupModelCosts{}
		response.Success(c, quote)
		return
	}
	quote, err := h.catalog.QuoteChannelCosts(c.Request.Context(), subject.UserID, time.Now(), currency)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, quote)
}
