package service

// Link-time rollback disables new price/policy production while preserving
// the frozen supplier-policy reader and amount recovery on existing tasks.
var mediaCostPlusPricingMode = "enabled"

func mediaCostPlusPricingEnabled() bool {
	return mediaCostPlusPricingMode == "enabled"
}

func mediaVariableSupplierModel(model string) bool {
	switch model {
	case "qwen/text-to-image", "google/gemini-3-8-flash-lite-tts", "google/gemini-3-8-flash-tts",
		"google/gemini-3-1-flash-tts", "google/gemini-2-5-pro-tts":
		return true
	default:
		return false
	}
}

func newMediaSupplierPricing(model string, budget int64) *FrozenMediaSupplierPricing {
	if !mediaCostPlusPricingEnabled() || !mediaVariableSupplierModel(model) {
		return nil
	}
	sourceVersion := "sha256:e760599549ce11379fe258b55aef4a742da4c5334e42a228cc940c19f8df556c"
	if model == "qwen/text-to-image" {
		sourceVersion = "sha256:70368c1747151297bb903365a6b586352550d667d77e9baba06425e7e56d2838"
	}
	return &FrozenMediaSupplierPricing{
		PolicyVersion: mediaCostPlusPolicyVersion,
		CreditUSD:     "0.005",
		MarkupBPS:     2500,
		BudgetUnits:   budget,
		SourceVersion: sourceVersion,
	}
}

// mediaSupplierRecommendedBudgetUnits is a recommended collection cap, not a
// supplier-enforced maximum. With at most 1000 Unicode characters, we budget
// 4000 input tokens and 2000 audio tokens. Longer audio remains possible; the
// customer's confirmed cap always limits collection regardless of usage.
// Rates are from the exact Kie catalog snapshot checked on 2026-10-14. The
// two Gemini 3.8 promotional rates are published through 2026-12-31.
func mediaSupplierRecommendedBudgetUnits(model string) (int64, bool) {
	if !mediaCostPlusPricingEnabled() {
		return 0, false
	}
	switch model {
	case "qwen/text-to-image":
		return 5000000, true // Recommended 2MP × $.02/MP × 1.25; supplier MP rounding is unpublished.
	case "google/gemini-3-8-flash-lite-tts":
		return 1225000, true // ($.35*4000 + $4.20*2000)/1M * 1.25
	case "google/gemini-3-8-flash-tts":
		return 1750000, true // ($.35*4000 + $6.30*2000)/1M * 1.25
	case "google/gemini-3-1-flash-tts", "google/gemini-2-5-pro-tts":
		return 3850000, true // ($.70*4000 + $14*2000)/1M * 1.25
	default:
		return 0, false
	}
}
