package modelpricing

import _ "embed"

//go:embed anthropic-official-2026-10-13.json
var anthropicOfficialSnapshot string

// AnthropicOfficialSnapshot returns the immutable, locally pinned official
// evidence. Runtime price lookup does not download or scrape any provider page.
func AnthropicOfficialSnapshot() string { return anthropicOfficialSnapshot }
