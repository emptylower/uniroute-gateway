package service

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiteLLMCatalogParseValidFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/litellm-valid.json")
	require.NoError(t, err)

	adapter := NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, 5, summary.Total)
	require.Equal(t, 1, summary.Dropped, "entry without litellm_provider is dropped")
	require.Len(t, summary.Candidates, 4)

	gpt := findCandidate(summary.Candidates, "gpt-test-4o")
	require.NotNil(t, gpt)
	require.Equal(t, "openai", gpt.ProviderHint)
	require.Equal(t, int64(128000), gpt.ContextWindow)
	require.Contains(t, gpt.Capabilities, "tools")
	require.Contains(t, gpt.Capabilities, "vision")
	require.Contains(t, gpt.PriceJSON, `"input":2.5e-06`)
	require.Contains(t, gpt.PriceJSON, `"output":1e-05`)
	require.Equal(t, "gpt-test-4o", gpt.RawRef)

	claude := findCandidate(summary.Candidates, "claude-test-opus")
	require.NotNil(t, claude)
	require.Equal(t, "anthropic", claude.ProviderHint)

	vertex := findCandidate(summary.Candidates, "vertex_ai/gemini-test-flash")
	require.NotNil(t, vertex)
	require.Equal(t, "vertex_ai/gemini", vertex.ProviderHint, "provider hints stay verbatim after trim")
	require.Equal(t, int64(1000000), vertex.ContextWindow)
	require.Contains(t, vertex.Capabilities, "mode:chat")
}

func TestLiteLLMCatalogLifecycleNormalization(t *testing.T) {
	raw, err := os.ReadFile("testdata/litellm-valid.json")
	require.NoError(t, err)

	adapter := NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(raw)
	require.NoError(t, err)

	deprecated := findCandidate(summary.Candidates, "old-deprecated-model")
	require.NotNil(t, deprecated)
	require.Contains(t, deprecated.Capabilities, "status:deprecated")

	active := findCandidate(summary.Candidates, "gpt-test-4o")
	require.NotNil(t, active)
	require.NotContains(t, active.Capabilities, "status:deprecated")
}

func TestLiteLLMCatalogInvalidFixtureDropsOverThreshold(t *testing.T) {
	raw, err := os.ReadFile("testdata/litellm-invalid.json")
	require.NoError(t, err)

	adapter := NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	// 3 of 4 entries invalid (75% drop): whole payload rejected.
	_, err = adapter.Parse(raw)
	require.Error(t, err)
}

func TestLiteLLMCatalogRequiredFieldRejection(t *testing.T) {
	payload := []byte(`{
		"ok-one": {"litellm_provider": "openai"},
		"ok-two": {"litellm_provider": "anthropic"},
		"missing-provider": {"max_tokens": 100},
		"empty-provider": {"litellm_provider": ""},
		"not-an-object-entry": {"litellm_provider": "openai", "mode": 1},
		"sample_spec": {"litellm_provider": "openai"}
	}`)
	adapter := NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(payload)
	// 3 of 6 dropped (50%) — over threshold.
	require.Error(t, err)

	payloadOK := []byte(`{
		"ok-one": {"litellm_provider": "openai"},
		"ok-two": {"litellm_provider": "anthropic"},
		"ok-three": {"litellm_provider": "google"},
		"missing-provider": {"max_tokens": 100},
		"ok-four": {"litellm_provider": "xai"}
	}`)
	adapter = NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	summary, err = adapter.Parse(payloadOK)
	require.NoError(t, err, "1 drop of 5 items is exactly the 20% threshold and is accepted")
	require.Equal(t, 1, summary.Dropped)
	require.Len(t, summary.Candidates, 4)
}

func TestLiteLLMCatalogCountLimit(t *testing.T) {
	raw, err := os.ReadFile("testdata/litellm-valid.json")
	require.NoError(t, err)

	adapter := NewLiteLLMCatalogAdapter(3)
	_, err = adapter.Parse(raw)
	require.Error(t, err)
}

func TestLiteLLMCatalogMalformedEnvelope(t *testing.T) {
	adapter := NewLiteLLMCatalogAdapter(CatalogMaxItemsDefault)
	_, err := adapter.Parse([]byte(`not json`))
	require.Error(t, err)
	_, err = adapter.Parse([]byte(`[]`))
	require.Error(t, err)
}

func TestLiteLLMCatalogSkipsDocumentationPseudoEntries(t *testing.T) {
	// Real payload (2026-08): the "sample_spec" key carries description strings
	// in numeric fields ("max_input_tokens": "max input tokens, if..."). One
	// bad entry must not kill the whole catalog sync.
	raw := []byte(`{
		"sample_spec": {"max_input_tokens": "max input tokens, if the provider specifies it. if not default to max_tokens", "litellm_provider": ""},
		"gpt-5.5": {"max_tokens": 4096, "max_input_tokens": 128000, "litellm_provider": "openai", "mode": "chat"},
		"claude-opus-5": {"max_tokens": 8192, "litellm_provider": "anthropic", "mode": "chat"}
	}`)
	summary, err := NewLiteLLMCatalogAdapter(0).Parse(raw)
	require.NoError(t, err)
	ids := []string{}
	for _, c := range summary.Candidates {
		ids = append(ids, c.CanonicalModelID)
	}
	require.ElementsMatch(t, []string{"gpt-5.5", "claude-opus-5"}, ids)
}
