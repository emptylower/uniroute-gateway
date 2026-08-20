package service

import "github.com/Wei-Shaw/sub2api/internal/pkg/claude"

// ResolveAnthropicFinalModel returns the model ID produced by Anthropic
// forwarding after account-type-specific mapping and normalization.
func ResolveAnthropicFinalModel(account *Account, requestedModel string) string {
	if account == nil {
		return requestedModel
	}
	if account.Type == AccountTypeAPIKey {
		return account.GetMappedModel(requestedModel)
	}
	if account.Platform != PlatformAnthropic {
		return requestedModel
	}
	switch account.Type {
	case AccountTypeServiceAccount:
		if mapped, matched := account.ResolveMappedModel(requestedModel); matched {
			return mapped
		}
		return normalizeVertexAnthropicModelID(claude.NormalizeModelID(requestedModel))
	default:
		return claude.NormalizeModelID(requestedModel)
	}
}

// ResolveAnthropicCountTokensFinalModel mirrors the count_tokens endpoint,
// which does not use Service account mapping or Vertex model IDs.
func ResolveAnthropicCountTokensFinalModel(account *Account, requestedModel string) string {
	if account == nil {
		return requestedModel
	}
	if account.Type == AccountTypeAPIKey {
		return account.GetMappedModel(requestedModel)
	}
	if account.Platform == PlatformAnthropic {
		return claude.NormalizeModelID(requestedModel)
	}
	return requestedModel
}
