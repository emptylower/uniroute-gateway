package service

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/tidwall/gjson"
)

const walletReaderMaxImageProofs = 64

// Counts contain selected parser results, never image bytes, URLs or prompts.
// Hashed output identities deduplicate final events against their terminal
// response. Images API data arrays retain the existing maximum-array policy.
type WalletReaderCounts struct {
	Version   int               `json:"version"`
	Images    map[string]string `json:"images,omitempty"`
	DataCount int               `json:"data_count"`
	DataSizes []string          `json:"data_sizes,omitempty"`
	Malformed bool              `json:"malformed"`
	Success   bool              `json:"success"`
	Complete  bool              `json:"complete"`
}

func (c *WalletReaderCounts) imageCount() int {
	if c == nil {
		return 0
	}
	return max(len(c.Images), c.DataCount)
}

func (c *WalletReaderCounts) imageSizes() []string {
	if c == nil {
		return nil
	}
	var sizes []string
	for _, size := range c.Images {
		if size != "" {
			sizes = append(sizes, size)
		}
	}
	if len(sizes) == 0 {
		sizes = append(sizes, c.DataSizes...)
	}
	return sizes
}

func walletReaderEvidenceTrusted(e WalletReaderEvidence) bool {
	if e.Malformed || (e.Counts != nil && e.Counts.Malformed) {
		return false
	}
	return e.Present && e.Valid || e.Counts != nil && e.Counts.Version == 1 && (e.Counts.imageCount() > 0 || e.Counts.Complete && e.Counts.Success)
}

func selectedWalletImageCounts(raw []byte, kind ...string) *WalletReaderCounts {
	if !gjson.ValidBytes(raw) {
		return nil
	}
	root := gjson.ParseBytes(raw)
	strictImageShape := len(kind) > 0 && kind[0] == "openai_images" || strings.HasPrefix(root.Get("type").String(), "image_generation.") || root.Get("type").String() == "response.output_item.done"
	counter := newOpenAIImageOutputCounter()
	counter.AddJSONResponse(raw)
	counter.AddSSEData(raw)
	counts := &WalletReaderCounts{Version: 1}
	// Validate selected shape before adopting the permissive existing parser's
	// result. Fields the parser never uses cannot choose a count or price.
	checkItem := func(item gjson.Result, data bool) {
		if !item.IsObject() {
			counts.Malformed = counts.Malformed || strictImageShape
			return
		}
		if data && !item.Get("url").Exists() && !item.Get("b64_json").Exists() {
			return
		}
		kind := item.Get("type").String()
		if !data && kind != "" && kind != "image_generation_call" && kind != "image_generation.completed" {
			return
		}
		for _, key := range []string{"id", "call_id", "size", "result", "b64_json", "url"} {
			if field := item.Get(key); field.Exists() && field.Type != gjson.String {
				counts.Malformed = true
			}
		}
	}
	for _, path := range []string{"data", "output", "response.output"} {
		array := root.Get(path)
		if !array.Exists() {
			continue
		}
		// image_generation.completed has a single output object.
		if path == "output" && root.Get("type").String() == "image_generation.completed" && array.IsObject() {
			checkItem(array, false)
			continue
		}
		if !array.IsArray() {
			counts.Malformed = counts.Malformed || strictImageShape
			continue
		}
		for _, item := range array.Array() {
			checkItem(item, path == "data")
		}
	}
	if root.Get("type").String() == "response.output_item.done" || root.Get("type").String() == "image_generation.completed" {
		if item := root.Get("item"); item.Exists() {
			checkItem(item, false)
		} else if root.Get("type").String() == "image_generation.completed" && !root.Get("output").Exists() {
			checkItem(root, false)
		}
	}
	if counter.Count() > walletReaderMaxImageProofs || len(counter.seen) > walletReaderMaxImageProofs {
		counts.Malformed = true
		return counts
	}
	if len(counter.seen) > 0 {
		counts.Images = make(map[string]string, len(counter.seen))
		for key := range counter.seen {
			tier, _ := ClassifyImageBillingTier(counter.seenSizes[key])
			counts.Images[hashOpenAIImageOutputResult(key)] = tier
		}
	}
	counts.DataCount = counter.maxDataCount
	for _, size := range counter.dataSizes {
		if tier, ok := ClassifyImageBillingTier(size); ok {
			counts.DataSizes = append(counts.DataSizes, tier)
		}
	}
	if counts.imageCount() == 0 && !counts.Malformed {
		return nil
	}
	return counts
}

func mergeWalletReaderCounts(evidence *WalletReaderEvidence, selected *WalletReaderCounts) error {
	if selected == nil {
		return nil
	}
	if selected.Version != 1 || selected.DataCount < 0 || selected.DataCount > walletReaderMaxImageProofs || len(selected.Images) > walletReaderMaxImageProofs || len(selected.DataSizes) > walletReaderMaxImageProofs {
		return errors.New("wallet selected count checkpoint is invalid")
	}
	if evidence.Counts == nil {
		evidence.Counts = &WalletReaderCounts{Version: 1}
	}
	counts := evidence.Counts
	counts.Malformed = counts.Malformed || selected.Malformed
	counts.Success = counts.Success || selected.Success
	counts.Complete = counts.Complete || selected.Complete
	if counts.Images == nil && len(selected.Images) > 0 {
		counts.Images = map[string]string{}
	}
	for key, size := range selected.Images {
		if len(key) != 64 || (size != "" && size != "1K" && size != "2K" && size != "4K") {
			return errors.New("wallet selected image identity or size is invalid")
		}
		if previous, found := counts.Images[key]; !found || previous == "" {
			counts.Images[key] = size
		}
	}
	if len(counts.Images) > walletReaderMaxImageProofs {
		counts.Malformed = true
	}
	if selected.DataCount >= counts.DataCount {
		counts.DataCount = selected.DataCount
		if len(selected.DataSizes) > 0 {
			for _, size := range selected.DataSizes {
				if size != "1K" && size != "2K" && size != "4K" {
					return errors.New("wallet selected data size is invalid")
				}
			}
			counts.DataSizes = append([]string(nil), selected.DataSizes...)
		}
	}
	evidence.Malformed = evidence.Malformed || counts.Malformed
	evidence.ObservedPositive = evidence.ObservedPositive || counts.imageCount() > 0
	return nil
}

func observeWalletReaderCounts(raw []byte, evidence *WalletReaderEvidence, facts ...*WalletReaderNormalization) error {
	kind := ""
	if len(facts) > 0 && facts[0] != nil {
		kind = facts[0].CountKind
	}
	return mergeWalletReaderCounts(evidence, selectedWalletImageCounts(raw, kind))
}

func replayWalletReaderCounts(raw []byte, evidence *WalletReaderEvidence) error {
	selected := gjson.GetBytes(raw, "_wallet_selected_counts")
	if !selected.Exists() {
		return nil
	}
	var counts WalletReaderCounts
	if err := json.Unmarshal([]byte(selected.Raw), &counts); err != nil {
		return err
	}
	return mergeWalletReaderCounts(evidence, &counts)
}

func finishWalletReaderCounts(facts *WalletReaderNormalization, evidence *WalletReaderEvidence, status int, complete bool) {
	if facts == nil || facts.CountKind == "" {
		return
	}
	if evidence.Counts == nil {
		evidence.Counts = &WalletReaderCounts{Version: 1}
	}
	evidence.Counts.Complete = complete
	evidence.Counts.Success = complete && status >= 200 && status < 300
	if evidence.Counts.Success && facts.CountKind != "openai_images" {
		evidence.ObservedPositive = true
	}
}

func selectedWalletRequestImageSize(payload []byte) (string, bool) {
	for _, path := range []string{"size", "generationConfig.imageConfig.imageSize", "generation_config.image_config.image_size"} {
		if size := gjson.GetBytes(payload, path); size.Exists() {
			if size.Type != gjson.String {
				return "", false
			}
			return ResolveImageBillingSize(size.String(), nil).BillingSize, true
		}
	}
	for _, tool := range gjson.GetBytes(payload, "tools").Array() {
		if strings.TrimSpace(tool.Get("type").String()) == "image_generation" {
			if size := tool.Get("size"); size.Exists() {
				if size.Type != gjson.String {
					return "", false
				}
				return ResolveImageBillingSize(size.String(), nil).BillingSize, true
			}
		}
	}
	return ResolveImageBillingSize("", nil).BillingSize, true
}
