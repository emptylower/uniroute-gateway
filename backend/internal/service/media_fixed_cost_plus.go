package service

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed resources/media-cost-plus-2026-10-14.json
var mediaFixedSupplierSnapshot []byte

type mediaFixedCostOption struct {
	QuotedUnits  int64  `json:"quoted_units"`
	SupplierUSD  string `json:"supplier_usd"`
	SourceSKU    string `json:"source_sku"`
	SourceUnit   string `json:"source_unit"`
	SourceModel  string `json:"source_model"`
	SourceSHA256 string `json:"source_sha256"`
	Quantity     int64  `json:"quantity"`
	Scope        string `json:"scope"`
}

var mediaFixedCostOptions = func() map[string]map[string]mediaFixedCostOption {
	var snapshot struct {
		Schema string `json:"schema"`
		Models []struct {
			ModelID string                          `json:"model_id"`
			Options map[string]mediaFixedCostOption `json:"options"`
		} `json:"models"`
	}
	if err := json.Unmarshal(mediaFixedSupplierSnapshot, &snapshot); err != nil {
		panic(err)
	}
	if snapshot.Schema != "media-fixed-cost-plus-v1" {
		panic("unsupported fixed media price snapshot")
	}
	result := make(map[string]map[string]mediaFixedCostOption, len(snapshot.Models))
	for _, model := range snapshot.Models {
		if _, ok := result[model.ModelID]; ok {
			panic("duplicate fixed media price model")
		}
		for _, option := range model.Options {
			units, err := mediaCostPlusRetailUnits(option.SupplierUSD)
			if err != nil || units != option.QuotedUnits || option.SourceSKU == "" || option.Scope == "" || len(option.SourceSHA256) != 64 {
				panic(fmt.Sprintf("invalid fixed media price evidence: %s", model.ModelID))
			}
		}
		result[model.ModelID] = model.Options
	}
	return result
}()

func applyMediaFixedCostPlusPricing(model *MediaModel) {
	options, ok := mediaFixedCostOptions[model.ModelID]
	if !ok {
		return
	} // Variable-cost models use their separate confirmed-budget policy.
	model.OptionReferenceSKU = make(map[string]string, len(options))
	for value, option := range options {
		model.OptionReferenceSKU[value] = option.SourceModel + ":" + option.SourceSKU
	}
	if !mediaCostPlusPricingEnabled() {
		return
	}
	if len(options) != len(model.Param.Options) {
		panic("media pricing/options mismatch: " + model.ModelID)
	}
	model.requestPrices = make(map[string]int64, len(options))
	model.OptionPriceUSD = make(map[string]string, len(options))
	var lowest int64
	for _, option := range model.Param.Options {
		price, exists := options[option.Value]
		if !exists {
			panic("missing verified media option: " + model.ModelID + ":" + option.Value)
		}
		model.requestPrices[option.Value] = price.QuotedUnits
		model.OptionPriceUSD[option.Value] = mediaUSD(price.QuotedUnits)
		if lowest == 0 || price.QuotedUnits < lowest {
			lowest = price.QuotedUnits
		}
	}
	model.Unit = "per_request"
	model.BillingBasis = "fixed_quote"
	model.PriceUSD = mediaUSD(lowest)
}
