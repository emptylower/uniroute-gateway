package service

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRouterCatalogSchemaFixtureParses(t *testing.T) {
	raw, err := os.ReadFile("testdata/openrouter-schema-2026-08-18.json")
	require.NoError(t, err)
	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	require.Equal(t, "object", schema["type"])
}

func TestOpenRouterCatalogParseValidFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/openrouter-valid.json")
	require.NoError(t, err)

	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse(raw)
	require.NoError(t, err)
	require.Len(t, summary.Candidates, 5)
	require.Equal(t, 5, summary.Total)
	require.Equal(t, 0, summary.Dropped)

	first := summary.Candidates[0]
	require.Equal(t, "anthropic/claude-test-4", first.CanonicalModelID)
	require.Equal(t, "anthropic", first.ProviderHint)
	require.Equal(t, "Claude Test 4", first.DisplayName)
	require.Equal(t, int64(200000), first.ContextWindow)
	require.Equal(t, []string{"anthropic/claude-test-4:thinking"}, first.Aliases)
	require.JSONEq(t, `{"prompt":"0.000003","completion":"0.000015"}`, first.PriceJSON)
	require.Contains(t, first.Capabilities, "tools")
	require.Contains(t, first.Capabilities, "reasoning")
	require.Equal(t, "data[0]", first.RawRef)

	mini := summary.Candidates[1]
	require.Equal(t, "openai/gpt-test-mini", mini.CanonicalModelID)
	require.Empty(t, mini.Aliases, "no canonical_slug means no aliases")

	grok := summary.Candidates[3]
	require.Equal(t, "x-ai/grok-test-beta", grok.CanonicalModelID)
	require.Equal(t, "x-ai", grok.ProviderHint)
	require.Empty(t, grok.PriceJSON, "missing pricing yields no price evidence")

	mistral := summary.Candidates[4]
	require.Equal(t, "mistral", mistral.ProviderHint)
}

func TestOpenRouterCatalogInvalidFixtureDropsOverThreshold(t *testing.T) {
	raw, err := os.ReadFile("testdata/openrouter-invalid.json")
	require.NoError(t, err)

	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)
	// 4 of 5 items are invalid (80% drop) — whole payload must be rejected.
	_, err = adapter.Parse(raw)
	require.Error(t, err)
}

func TestOpenRouterCatalogRequiredFieldRejection(t *testing.T) {
	payload := `{"data":[
		{"id":"anthropic/ok-model","name":"OK"},
		{"name":"missing id"},
		{"id":"","name":"empty id"}
	]}`
	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse([]byte(payload))
	// 2 of 3 dropped (66%) — over threshold.
	require.Error(t, err)

	// Below threshold the same rules keep only valid rows.
	payloadOK := `{"data":[
		{"id":"anthropic/ok-model","name":"OK"},
		{"id":"openai/ok-two","name":"Two"},
		{"id":"","name":"empty id"},
		{"id":"google/ok-three"},
		{"id":"x-ai/ok-four"}
	]}`
	summary, err = adapter.Parse([]byte(payloadOK))
	require.NoError(t, err)
	require.Equal(t, 1, summary.Dropped)
	require.Len(t, summary.Candidates, 4)
	for _, c := range summary.Candidates {
		require.NotEmpty(t, c.CanonicalModelID)
	}
}

func TestOpenRouterCatalogNamespaceLimitedAliases(t *testing.T) {
	payload := `{"data":[
		{
			"id":"anthropic/claude-alias",
			"canonical_slug":"openai/wrong-namespace-slug"
		},
		{
			"id":"anthropic/claude-alias-two",
			"canonical_slug":"anthropic/right-namespace-slug"
		},
		{
			"id":"openai/bare",
			"canonical_slug":"noslash"
		}
	]}`
	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)
	summary, err := adapter.Parse([]byte(payload))
	require.NoError(t, err)
	require.Len(t, summary.Candidates, 3)

	require.Empty(t, summary.Candidates[0].Aliases, "alias from another namespace must be dropped")
	require.Equal(t, []string{"anthropic/right-namespace-slug"}, summary.Candidates[1].Aliases)
	require.Empty(t, summary.Candidates[2].Aliases, "alias without namespace must be dropped")
}

func TestOpenRouterCatalogPayloadCountLimit(t *testing.T) {
	items := ""
	for i := 0; i < 5; i++ {
		if i > 0 {
			items += ","
		}
		items += fmt.Sprintf(`{"id":"anthropic/model-%d"}`, i)
	}
	payload := []byte(`{"data":[` + items + `]}`)

	adapter := NewOpenRouterCatalogAdapter(4)
	_, err := adapter.Parse(payload)
	require.Error(t, err, "payloads above maxItems must be rejected")

	adapter = NewOpenRouterCatalogAdapter(5)
	_, err = adapter.Parse(payload)
	require.NoError(t, err)
}

func TestOpenRouterCatalogDropThresholdBoundary(t *testing.T) {
	build := func(missing int) []byte {
		items := ""
		for i := 0; i < 10; i++ {
			if i > 0 {
				items += ","
			}
			entry := fmt.Sprintf(`{"id":"anthropic/m-%d"}`, i)
			if i < missing {
				entry = `{"name":"broken"}`
			}
			items += entry
		}
		return []byte(`{"data":[` + items + `]}`)
	}

	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)
	// Exactly 20% dropped is accepted.
	_, err := adapter.Parse(build(2))
	require.NoError(t, err)
	// 30% dropped is rejected.
	_, err = adapter.Parse(build(3))
	require.Error(t, err)
}

func TestOpenRouterCatalogMalformedEnvelope(t *testing.T) {
	adapter := NewOpenRouterCatalogAdapter(CatalogMaxItemsDefault)

	_, err := adapter.Parse([]byte(`not json`))
	require.Error(t, err)

	_, err = adapter.Parse([]byte(`{"models":[]}`))
	require.Error(t, err, "envelope without data array must be rejected")

	_, err = adapter.Parse([]byte(`{"data":"not-an-array"}`))
	require.Error(t, err)
}
