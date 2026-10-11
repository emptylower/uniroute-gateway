package handler

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// filterDiscoveryModels only edits the known models array in upstream discovery
// responses. Unchanged payloads stay byte-for-byte intact, including extensions.
func filterDiscoveryModels(body []byte, account *service.Account) ([]byte, bool) {
	return filterDiscoveryModelsWithAllowedIDs(body, account, nil)
}

func filterDiscoveryModelsWithAllowedIDs(body []byte, account *service.Account, allowedIDs []string) ([]byte, bool) {
	var allowed *service.Account
	if allowedIDs != nil {
		mapping := make(map[string]any, len(allowedIDs))
		for _, id := range allowedIDs {
			mapping[id] = id
		}
		allowed = &service.Account{Credentials: map[string]any{"model_mapping": mapping}}
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil || envelope == nil {
		return body, false
	}
	array := bytes.TrimSpace(envelope["models"])
	if len(array) == 0 || array[0] != '[' {
		return body, false
	}
	var models []json.RawMessage
	if json.Unmarshal(array, &models) != nil {
		return body, false
	}
	available := make([]json.RawMessage, 0, len(models))
	for _, model := range models {
		var selectors struct {
			ID   string `json:"id"`
			Slug string `json:"slug"`
			Name string `json:"name"`
		}
		unavailable := false
		if json.Unmarshal(model, &selectors) == nil {
			for _, id := range []string{selectors.ID, selectors.Slug, selectors.Name} {
				bareID := strings.TrimPrefix(id, "models/")
				if id != "" && allowed != nil && (len(allowedIDs) == 0 || !allowed.IsModelSupported(bareID)) {
					unavailable = true
					break
				}
				if service.CheckModelAvailability(id) != nil || (id != "" && account != nil && service.CheckModelAvailability(account.GetMappedModel(strings.TrimPrefix(id, "models/"))) != nil) {
					unavailable = true
					break
				}
			}
		}
		if !unavailable {
			available = append(available, model)
		}
	}
	if len(available) == len(models) {
		return body, false
	}
	envelope["models"], _ = json.Marshal(available)
	filtered, err := json.Marshal(envelope)
	if err != nil {
		return body, false
	}
	return filtered, true
}
