package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

// modelsdevCatalogPayload is keyed by provider id; each provider carries a
// models object keyed by model id. Unknown fields are ignored.
type modelsdevCatalogPayload map[string]modelsdevProviderEntry

type modelsdevProviderEntry struct {
	ID     string                         `json:"id"`
	Models map[string]modelsdevModelEntry `json:"models"`
}

type modelsdevModelEntry struct {
	Name       string                     `json:"name"`
	Status     string                     `json:"status"`
	ToolCall   bool                       `json:"tool_call"`
	Reasoning  bool                       `json:"reasoning"`
	Attachment bool                       `json:"attachment"`
	Modalities *modelsdevModalities       `json:"modalities"`
	Cost       map[string]json.RawMessage `json:"cost"`
	Limit      *modelsdevLimit            `json:"limit"`
}

type modelsdevModalities struct {
	Input  []string `json:"input"`
	Output []string `json:"output"`
}

type modelsdevLimit struct {
	Context *int64 `json:"context"`
}

// ModelsDevCatalogAdapter normalizes the pinned models.dev catalog into
// candidate evidence. Read-only: it never touches registry or publication
// repositories and grants no production authority.
type ModelsDevCatalogAdapter struct {
	maxItems             int
	dropThresholdPercent int
}

// NewModelsDevCatalogAdapter builds the adapter. Non-positive maxItems falls
// back to CatalogMaxItemsDefault.
func NewModelsDevCatalogAdapter(maxItems int) *ModelsDevCatalogAdapter {
	if maxItems <= 0 {
		maxItems = CatalogMaxItemsDefault
	}
	return &ModelsDevCatalogAdapter{
		maxItems:             maxItems,
		dropThresholdPercent: CatalogDropThresholdPercentDefault,
	}
}

// Parse validates and normalizes one raw models.dev payload.
func (a *ModelsDevCatalogAdapter) Parse(raw []byte) (*CatalogParseSummary, error) {
	var payload modelsdevCatalogPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("invalid modelsdev catalog payload: %w", err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("invalid modelsdev catalog payload: empty provider map")
	}

	total := 0
	for _, entry := range payload {
		total += len(entry.Models)
	}
	if total > a.maxItems {
		return nil, fmt.Errorf("modelsdev catalog payload has %d items, over limit of %d", total, a.maxItems)
	}

	summary := &CatalogParseSummary{Total: total}
	for providerKey, entry := range payload {
		for modelID, model := range entry.Models {
			candidate, ok := normalizeModelsDevModel(providerKey, modelID, model)
			if !ok {
				summary.Dropped++
				continue
			}
			summary.Candidates = append(summary.Candidates, *candidate)
		}
	}
	if summary.Total > 0 && summary.Dropped*100 > a.dropThresholdPercent*summary.Total {
		return nil, fmt.Errorf(
			"modelsdev catalog payload dropped %d of %d items, over %d%% threshold",
			summary.Dropped, summary.Total, a.dropThresholdPercent,
		)
	}
	return summary, nil
}

// normalizeModelsDevModel enforces required identity fields: a non-empty model
// id plus at least one descriptive field (name, cost block, limit block,
// modalities, or a capability flag). The provider key becomes the verbatim
// provider hint; it never grants ownership.
func normalizeModelsDevModel(providerKey, modelID string, model modelsdevModelEntry) (*CatalogCandidateEvidenceInput, bool) {
	id := strings.TrimSpace(modelID)
	if id == "" {
		return nil, false
	}
	hasName := strings.TrimSpace(model.Name) != ""
	hasDetails := len(model.Cost) > 0 || model.Limit != nil || model.Modalities != nil ||
		model.ToolCall || model.Reasoning || model.Attachment || strings.TrimSpace(model.Status) != ""
	if !hasName && !hasDetails {
		return nil, false
	}

	candidate := &CatalogCandidateEvidenceInput{
		CanonicalModelID: id,
		ProviderHint:     strings.TrimSpace(providerKey),
		DisplayName:      strings.TrimSpace(model.Name),
		RawRef:           strings.TrimSpace(providerKey) + "/" + id,
	}
	if model.Limit != nil && model.Limit.Context != nil && *model.Limit.Context > 0 {
		candidate.ContextWindow = *model.Limit.Context
	}
	if status := strings.TrimSpace(model.Status); status != "" {
		candidate.Capabilities = append(candidate.Capabilities, "status:"+strings.ToLower(status))
	}
	if model.ToolCall {
		candidate.Capabilities = append(candidate.Capabilities, "tools")
	}
	if model.Reasoning {
		candidate.Capabilities = append(candidate.Capabilities, "reasoning")
	}
	if model.Attachment {
		candidate.Capabilities = append(candidate.Capabilities, "attachments")
	}
	if model.Modalities != nil {
		for _, in := range model.Modalities.Input {
			candidate.Capabilities = append(candidate.Capabilities, "input:"+strings.ToLower(strings.TrimSpace(in)))
		}
		for _, out := range model.Modalities.Output {
			candidate.Capabilities = append(candidate.Capabilities, "output:"+strings.ToLower(strings.TrimSpace(out)))
		}
	}
	if priceJSON, ok := priceEvidenceFromRaw(model.Cost); ok {
		candidate.PriceJSON = priceJSON
	} else if len(model.Cost) > 0 {
		// A present-but-invalid cost block is malformed evidence; drop the entry.
		return nil, false
	}
	return candidate, true
}
