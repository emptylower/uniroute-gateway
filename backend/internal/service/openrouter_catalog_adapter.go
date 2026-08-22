package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Default share of items allowed to fail required-field validation before the
// whole payload is rejected (plan: 20 percent).
const CatalogDropThresholdPercentDefault = 20

// openRouterCatalogPayload mirrors the pinned 2026-08-18 schema. Unknown fields
// are ignored; typed optionals are kept strict so malformed evidence fails.
type openRouterCatalogPayload struct {
	Data []openRouterCatalogModel `json:"data"`
}

type openRouterCatalogModel struct {
	ID                  string         `json:"id"`
	CanonicalSlug       *string        `json:"canonical_slug"`
	Name                *string        `json:"name"`
	ContextLength       *int64         `json:"context_length"`
	Pricing             map[string]any `json:"pricing"`
	SupportedParameters []string       `json:"supported_parameters"`
	Architecture        map[string]any `json:"architecture"`
}

// OpenRouterCatalogAdapter normalizes OpenRouter /api/v1/models payloads into
// candidate evidence. It is read-only: it never touches registry, publication,
// or pricing repositories, and its output grants no production authority.
type OpenRouterCatalogAdapter struct {
	maxItems             int
	dropThresholdPercent int
}

// NewOpenRouterCatalogAdapter builds the adapter. Non-positive maxItems falls
// back to CatalogMaxItemsDefault.
func NewOpenRouterCatalogAdapter(maxItems int) *OpenRouterCatalogAdapter {
	if maxItems <= 0 {
		maxItems = CatalogMaxItemsDefault
	}
	return &OpenRouterCatalogAdapter{
		maxItems:             maxItems,
		dropThresholdPercent: CatalogDropThresholdPercentDefault,
	}
}

// CatalogParseSummary reports normalization outcome for one payload.
type CatalogParseSummary struct {
	Candidates []CatalogCandidateEvidenceInput
	Total      int
	Dropped    int
}

// Parse validates and normalizes one raw payload.
func (a *OpenRouterCatalogAdapter) Parse(raw []byte) (*CatalogParseSummary, error) {
	var payload openRouterCatalogPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("invalid openrouter catalog payload: %w", err)
	}
	if payload.Data == nil {
		return nil, fmt.Errorf("invalid openrouter catalog payload: missing data array")
	}
	if len(payload.Data) > a.maxItems {
		return nil, fmt.Errorf("openrouter catalog payload has %d items, over limit of %d", len(payload.Data), a.maxItems)
	}

	summary := &CatalogParseSummary{Total: len(payload.Data)}
	for i, model := range payload.Data {
		candidate, ok := normalizeOpenRouterModel(model)
		if !ok {
			summary.Dropped++
			continue
		}
		candidate.RawRef = fmt.Sprintf("data[%d]", i)
		summary.Candidates = append(summary.Candidates, *candidate)
	}
	if summary.Total > 0 && summary.Dropped*100 > a.dropThresholdPercent*summary.Total {
		return nil, fmt.Errorf(
			"openrouter catalog payload dropped %d of %d items, over %d%% threshold",
			summary.Dropped, summary.Total, a.dropThresholdPercent,
		)
	}
	return summary, nil
}

// normalizeOpenRouterModel enforces required fields and namespace-limited
// identity. The vendor namespace before the single slash becomes the provider
// hint verbatim — an external claim that never grants ownership.
func normalizeOpenRouterModel(model openRouterCatalogModel) (*CatalogCandidateEvidenceInput, bool) {
	id := strings.TrimSpace(model.ID)
	if id == "" {
		return nil, false
	}
	slash := strings.Index(id, "/")
	if slash <= 0 || slash == len(id)-1 || strings.Contains(id[slash+1:], "/") {
		return nil, false
	}
	namespace := id[:slash]

	candidate := &CatalogCandidateEvidenceInput{
		CanonicalModelID: id,
		ProviderHint:     namespace,
	}
	if model.Name != nil {
		candidate.DisplayName = strings.TrimSpace(*model.Name)
	}
	if model.ContextLength != nil && *model.ContextLength > 0 {
		candidate.ContextWindow = *model.ContextLength
	}
	for _, p := range model.SupportedParameters {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			candidate.Capabilities = append(candidate.Capabilities, trimmed)
		}
	}
	if slug := strings.TrimSpace(derefString(model.CanonicalSlug)); slug != "" &&
		strings.HasPrefix(slug, namespace+"/") && !strings.Contains(strings.TrimPrefix(slug, namespace+"/"), "/") {
		candidate.Aliases = append(candidate.Aliases, slug)
	}
	if priceJSON, ok := normalizeOpenRouterPricing(model.Pricing); ok {
		candidate.PriceJSON = priceJSON
	}
	return candidate, true
}

// normalizeOpenRouterPricing retains only string-valued pricing entries as
// verbatim external price evidence. Typed or empty values invalidate the price.
func normalizeOpenRouterPricing(pricing map[string]any) (string, bool) {
	if len(pricing) == 0 {
		return "", false
	}
	retained := make(map[string]string, len(pricing))
	for key, value := range pricing {
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return "", false
		}
		retained[key] = text
	}
	encoded, err := json.Marshal(retained)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
