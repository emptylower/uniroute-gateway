package service

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelsDevCatalogParseValidFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/modelsdev-valid.json")
	require.NoError(t, err)

	adapter := NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, 4, summary.Total)
	require.Equal(t, 0, summary.Dropped)
	require.Len(t, summary.Candidates, 4)

	claude := findCandidate(summary.Candidates, "claude-test-4-sonnet")
	require.NotNil(t, claude)
	require.Equal(t, "anthropic", claude.ProviderHint)
	require.Equal(t, "Claude Test 4 Sonnet", claude.DisplayName)
	require.Equal(t, int64(200000), claude.ContextWindow)
	require.Contains(t, claude.Capabilities, "tools")
	require.Contains(t, claude.Capabilities, "reasoning")
	require.Contains(t, claude.Capabilities, "input:image")
	require.Contains(t, claude.Capabilities, "output:text")
	require.Contains(t, claude.PriceJSON, `"input":0.000003`)
	require.Contains(t, claude.PriceJSON, `"output":0.000015`)
	require.Equal(t, "anthropic/claude-test-4-sonnet", claude.RawRef)

	gpt := findCandidate(summary.Candidates, "gpt-test-mini")
	require.NotNil(t, gpt)
	require.Equal(t, "openai", gpt.ProviderHint)

	mistral := findCandidate(summary.Candidates, "mistral-test-large")
	require.NotNil(t, mistral)
	require.Equal(t, "mistral", mistral.ProviderHint)
}

func TestModelsDevCatalogLifecycleNormalization(t *testing.T) {
	raw, err := os.ReadFile("testdata/modelsdev-valid.json")
	require.NoError(t, err)

	adapter := NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(raw)
	require.NoError(t, err)

	retired := findCandidate(summary.Candidates, "old-model-retired")
	require.NotNil(t, retired)
	require.Contains(t, retired.Capabilities, "status:deprecated")

	active := findCandidate(summary.Candidates, "claude-test-4-sonnet")
	require.NotNil(t, active)
	require.NotContains(t, active.Capabilities, "status:deprecated")
}

func TestModelsDevCatalogInvalidFixtureDropsOverThreshold(t *testing.T) {
	raw, err := os.ReadFile("testdata/modelsdev-invalid.json")
	require.NoError(t, err)

	adapter := NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	// 3 of 4 entries invalid (75% drop): whole payload rejected.
	_, err = adapter.Parse(raw)
	require.Error(t, err)
}

func TestModelsDevCatalogRequiredFieldRejection(t *testing.T) {
	payload := []byte(`{
		"anthropic": {
			"id": "anthropic",
			"models": {
				"": {"name": "empty key"},
				"ok-model": {"name": "OK"},
				"no-details": {}
			}
		}
	}`)
	adapter := NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(payload)
	// 2 of 3 dropped (66%) over threshold.
	require.Error(t, err)

	payloadOK := []byte(`{
		"anthropic": {
			"id": "anthropic",
			"models": {
				"ok-model": {"name": "OK"},
				"no-details": {}
			}
		},
		"openai": {
			"id": "openai",
			"models": {
				"ok-two": {"name": "Two"},
				"ok-three": {"name": "Three"},
				"ok-four": {"name": "Four"},
				"ok-five": {"name": "Five"}
			},
			"broken-provider": null
		}
	}`)
	adapter = NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	summary, err = adapter.Parse(payloadOK)
	require.NoError(t, err, "1 drop of 6 items is under threshold; provider without models object is skipped")
	require.Equal(t, 6, summary.Total)
	require.Equal(t, 1, summary.Dropped)
	require.Len(t, summary.Candidates, 5)
}

func TestModelsDevCatalogCountLimit(t *testing.T) {
	raw, err := os.ReadFile("testdata/modelsdev-valid.json")
	require.NoError(t, err)

	adapter := NewModelsDevCatalogAdapter(2)
	_, err = adapter.Parse(raw)
	require.Error(t, err)
}

func TestModelsDevCatalogMalformedEnvelope(t *testing.T) {
	adapter := NewModelsDevCatalogAdapter(CatalogMaxItemsDefault)
	_, err := adapter.Parse([]byte(`not json`))
	require.Error(t, err)
	_, err = adapter.Parse([]byte(`{"anthropic": "not-an-object"}`))
	require.Error(t, err)
}

func findCandidate(candidates []CatalogCandidateEvidenceInput, id string) *CatalogCandidateEvidenceInput {
	for i := range candidates {
		if candidates[i].CanonicalModelID == id {
			return &candidates[i]
		}
	}
	return nil
}
