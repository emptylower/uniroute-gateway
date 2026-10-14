package dto

import (
	"net/url"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func usageMediaPriceContext(context *service.UsageMediaPriceContext) *UsageMediaPriceContext {
	if context == nil {
		return nil
	}
	result := &UsageMediaPriceContext{ModelID: context.ModelID, Option: context.Option, ReferenceSKUVerified: context.ReferenceSKUVerified}
	for _, model := range service.MediaTaskCatalog() {
		if model.ModelID == context.ModelID {
			result.ModelProvider = model.Provider
			if context.ReferenceSKUVerified {
				result.CurrentReferenceSKU = model.OptionReferenceSKU[context.Option]
			}
			break
		}
	}
	return result
}

// Only trusted supplier endpoints or built-in direct providers determine a channel.
// Protocol platform, route/group names and billing multipliers do not.
func usageChannelType(log *service.UsageLog) string {
	account := log.Account
	if account != nil {
		// Migration 217's reserved Kie account deliberately has no base URL:
		// the media service uses its own provider configuration. Never mistake
		// this account for OpenAI's default API when a historical task is missing.
		managedTraits := account.Platform == service.PlatformOpenAI && account.Type == service.AccountTypeAPIKey && account.Status == service.StatusDisabled && !account.Schedulable && len(account.Credentials) == 0
		media := log.MediaType != nil && (*log.MediaType == "image" || *log.MediaType == "video" || *log.MediaType == "music")
		if marker, present := account.Extra["gateway_media_provider"]; present {
			provider, valid := marker.(string)
			if !valid || provider != "kie" || !managedTraits || (log.AccountID > 0 && account.ID > 0 && log.AccountID != account.ID) {
				return ""
			}
			if log.MediaPriceContext != nil {
				return "cloud-vendor"
			}
			if media && log.AccountID > 0 && account.ID == log.AccountID {
				return "cloud-vendor"
			}
			return ""
		}
		if log.MediaPriceContext == nil && media && managedTraits {
			// Without the provider marker there is insufficient supplier evidence.
			return ""
		}
	}
	if log.MediaPriceContext != nil {
		// Owner-checked gateway_media_task rows are created by the Kie-only service.
		return "cloud-vendor"
	}
	if account == nil {
		return ""
	}
	switch account.Type {
	case service.AccountTypeAPIKey, service.AccountTypeOAuth, service.AccountTypeSetupToken, service.AccountTypeUpstream, service.AccountTypeBedrock, service.AccountTypeServiceAccount:
	default:
		return ""
	}
	baseURL := strings.TrimSpace(account.GetCredential("base_url"))
	if account.IsCustomBaseURLEnabled() {
		baseURL = strings.TrimSpace(account.GetCustomBaseURL())
		if baseURL == "" {
			return ""
		}
	}
	if baseURL != "" {
		u, err := url.Parse(baseURL)
		if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.Fragment != "" {
			return ""
		}
		host := strings.ToLower(u.Hostname())
		if host == "api.kie.ai" || host == "api.aicodewith.ai" {
			return "cloud-vendor"
		}
		officialHosts := map[string][]string{
			service.PlatformAnthropic: {"api.anthropic.com"},
			service.PlatformOpenAI:    {"api.openai.com", "chatgpt.com"},
			service.PlatformGemini:    {"generativelanguage.googleapis.com", "cloudcode-pa.googleapis.com"},
			service.PlatformGrok:      {"api.x.ai", "grok.com"},
			service.PlatformDeepseek:  {"api.deepseek.com"},
			service.PlatformGLM:       {"open.bigmodel.cn", "api.z.ai"},
			service.PlatformKimi:      {"api.moonshot.cn", "api.moonshot.ai"},
			service.PlatformQwen:      {"dashscope.aliyuncs.com", "dashscope-intl.aliyuncs.com"},
			service.PlatformLongcat:   {"api.longcat.chat"},
			service.PlatformBytedance: {"ark.cn-beijing.volces.com"},
			service.PlatformMinimax:   {"api.minimax.io", "api.minimax.chat"},
		}
		for _, official := range officialHosts[account.Platform] {
			if host == official {
				return "official"
			}
		}
		return ""
	}
	if (account.Type == service.AccountTypeBedrock && account.Platform == service.PlatformAnthropic) || (account.Type == service.AccountTypeServiceAccount && account.Platform == service.PlatformGemini) {
		return "cloud-vendor"
	}
	if account.Type == service.AccountTypeAPIKey || account.Type == service.AccountTypeOAuth || account.Type == service.AccountTypeSetupToken {
		switch account.Platform {
		case service.PlatformAnthropic, service.PlatformOpenAI, service.PlatformGemini, service.PlatformGrok:
			return "official"
		}
	}
	return ""
}
