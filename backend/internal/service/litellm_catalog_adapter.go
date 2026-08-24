package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

// litellmCatalogPayload is a flat object keyed by model id. Entries starting
// with an underscore are metadata and skipped silently.
type litellmCatalogPayload map[string]litellmModelEntry

type litellmModelEntry struct {
	MaxTokens               *int64                     `json:"max_tokens"`
	MaxInputTokens          *int64                     `json:"max_input_tokens"`
	InputCostPerToken       json.RawMessage            `json:"input_cost_per_token"`
	OutputCostPerToken      json.RawMessage            `json:"output_cost_per_token"`
	LitellmProvider         string                     `json:"litellm_provider"`
	Mode                    string                     `json:"mode"`
	DeprecationDate         string                     `json:"deprecation_date"`
	SupportsFunctionCalling bool                       `json:"supports_function_calling"`
	SupportsVision          bool                       `json:"supports_vision"`
	SupportsReasoning       bool                       `json:"supports_reasoning"`
	Extra                   map[string]json.RawMessage `json:"-"`
}

// LiteLLMCatalogAdapter normalizes the pinned LiteLLM pricing catalog into
// candidate evidence. Read-only: it never touches registry, publication, or
// billing repositories; prices are anomaly evidence only.
type LiteLLMCatalogAdapter struct {
	maxItems             int
	dropThresholdPercent int
}

// NewLiteLLMCatalogAdapter builds the adapter. Non-positive maxItems falls
// back to CatalogMaxItemsDefault.
func NewLiteLLMCatalogAdapter(maxItems int) *LiteLLMCatalogAdapter {
	if maxItems <= 0 {
		maxItems = CatalogMaxItemsDefault
	}
	return &LiteLLMCatalogAdapter{
		maxItems:             maxItems,
		dropThresholdPercent: CatalogDropThresholdPercentDefault,
	}
}

// Parse validates and normalizes one raw LiteLLM payload.
func (a *LiteLLMCatalogAdapter) Parse(raw []byte) (*CatalogParseSummary, error) {
	// Two-phase decode: the upstream file carries documentation pseudo-entries
	// (e.g. "sample_spec" with description strings in numeric fields) whose
	// schema drift must not kill the whole catalog. Decode per entry and skip
	// the ones that do not parse.
	var rawEntries map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawEntries); err != nil {
		return nil, fmt.Errorf("invalid litellm catalog payload: %w", err)
	}
	if len(rawEntries) == 0 {
		return nil, fmt.Errorf("invalid litellm catalog payload: empty model map")
	}
	if len(rawEntries) > a.maxItems {
		return nil, fmt.Errorf("litellm catalog payload has %d items, over limit of %d", len(rawEntries), a.maxItems)
	}

	payload := make(litellmCatalogPayload, len(rawEntries))
	for key, entryRaw := range rawEntries {
		if strings.HasPrefix(key, "_") || key == "sample_spec" {
			continue
		}
		var entry litellmModelEntry
		if err := json.Unmarshal(entryRaw, &entry); err != nil {
			// Schema drift in a single entry: skip it, keep the catalog.
			continue
		}
		payload[key] = entry
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("invalid litellm catalog payload: no parseable entries")
	}

	summary := &CatalogParseSummary{Total: len(payload)}
	for modelID, model := range payload {
		if strings.HasPrefix(modelID, "_") {
			// Metadata keys are not catalog entries.
			summary.Total--
			continue
		}
		candidate, ok := normalizeLitellmModel(modelID, model)
		if !ok {
			summary.Dropped++
			continue
		}
		summary.Candidates = append(summary.Candidates, *candidate)
	}
	if summary.Total > 0 && summary.Dropped*100 > a.dropThresholdPercent*summary.Total {
		return nil, fmt.Errorf(
			"litellm catalog payload dropped %d of %d items, over %d%% threshold",
			summary.Dropped, summary.Total, a.dropThresholdPercent,
		)
	}
	return summary, nil
}

// normalizeLitellmModel enforces the required provider field. The provider hint
// is trimmed verbatim — aliases like vertex_ai/gemini stay as claimed and never
// grant governed-provider ownership. Lifecycle normalizes from deprecation_date;
// capability flags become stable tokens.
func normalizeLitellmModel(modelID string, model litellmModelEntry) (*CatalogCandidateEvidenceInput, bool) {
	id := strings.TrimSpace(modelID)
	if id == "" {
		return nil, false
	}
	hint := strings.TrimSpace(model.LitellmProvider)
	if hint == "" {
		return nil, false
	}

	candidate := &CatalogCandidateEvidenceInput{
		CanonicalModelID: id,
		ProviderHint:     hint,
		RawRef:           id,
	}
	if ctx := contextWindowFrom(model.MaxInputTokens, model.MaxTokens); ctx > 0 {
		candidate.ContextWindow = ctx
	}
	if strings.TrimSpace(model.DeprecationDate) != "" {
		candidate.Capabilities = append(candidate.Capabilities, "status:deprecated")
	}
	if model.SupportsFunctionCalling {
		candidate.Capabilities = append(candidate.Capabilities, "tools")
	}
	if model.SupportsVision {
		candidate.Capabilities = append(candidate.Capabilities, "vision")
	}
	if model.SupportsReasoning {
		candidate.Capabilities = append(candidate.Capabilities, "reasoning")
	}
	if mode := strings.TrimSpace(model.Mode); mode != "" {
		candidate.Capabilities = append(candidate.Capabilities, "mode:"+strings.ToLower(mode))
	}

	prices := make(map[string]json.RawMessage, 2)
	if len(model.InputCostPerToken) > 0 && string(model.InputCostPerToken) != "null" {
		prices["input"] = model.InputCostPerToken
	}
	if len(model.OutputCostPerToken) > 0 && string(model.OutputCostPerToken) != "null" {
		prices["output"] = model.OutputCostPerToken
	}
	if priceJSON, ok := priceEvidenceFromRaw(prices); ok {
		candidate.PriceJSON = priceJSON
	} else if len(prices) > 0 {
		// A present-but-invalid price value is malformed evidence.
		return nil, false
	}
	return candidate, true
}

func contextWindowFrom(maxInputTokens, maxTokens *int64) int64 {
	if maxInputTokens != nil && *maxInputTokens > 0 {
		return *maxInputTokens
	}
	if maxTokens != nil && *maxTokens > 0 {
		return *maxTokens
	}
	return 0
}

// priceEvidenceFromRaw retains raw JSON number tokens verbatim so external
// price evidence keeps its exact source text. Every present value must be a
// JSON number or boolean-free scalar; strings, arrays, objects fail.
func priceEvidenceFromRaw(raw map[string]json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	retained := make(map[string]json.RawMessage, len(raw))
	for key, value := range raw {
		text := strings.TrimSpace(string(value))
		if text == "" || text == "null" || !isJSONNumber(text) {
			return "", false
		}
		retained[strings.ToLower(strings.TrimSpace(key))] = value
	}
	encoded, err := json.Marshal(retained)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func isJSONNumber(text string) bool {
	var number json.Number
	return json.Unmarshal([]byte(text), &number) == nil
}
