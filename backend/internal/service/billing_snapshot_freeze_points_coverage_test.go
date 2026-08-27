//go:build unit

package service

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every handler file that records usage must also freeze a snapshot, and
// Count Tokens must not. This is the cheap static form of "every family has
// a freeze point"; the exit's pricing proof is the settle tests.
func TestEveryUsageRecordingHandlerFreezesASnapshot(t *testing.T) {
	handlerDir := filepath.Join("..", "handler")
	entries, err := os.ReadDir(handlerDir)
	require.NoError(t, err)
	records := regexp.MustCompile(`\.RecordUsage(WithLongContext)?\(`)
	freezes := regexp.MustCompile(`FreezeBillingSnapshot\(`)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".go" || regexp.MustCompile(`_test\.go$`).MatchString(name) {
			continue
		}
		src, err := os.ReadFile(filepath.Join(handlerDir, name))
		require.NoError(t, err)
		if records.Match(src) {
			require.True(t, freezes.Match(src), "%s records usage but never freezes a billing snapshot", name)
		}
		if name == "gateway_count_tokens.go" || name == "openai_gateway_count_tokens.go" {
			require.False(t, freezes.Match(src), "%s must not freeze — Count Tokens is not billable", name)
		}
	}
}

// Row 3 is validation-only, so the per-file freeze grep cannot prove the WS
// family CARRIES a snapshot — the only family whose freeze site (row 4) and
// carry site (:2084) are different rows. Assert the turn-scoped carry explicitly.
func TestOpenAIWSTurnCarriesTheFrozenSnapshot(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "handler", "openai_gateway_handler.go"))
	require.NoError(t, err)
	require.Regexp(t, `billingSnapshot\s+\*service\.BillingSnapshot`, string(src), "openAIWSTurnChannelMappingSnapshot must carry the frozen snapshot")
	require.Regexp(t, `turnChannelMapping\.Store\(&openAIWSTurnChannelMappingSnapshot\{[^}]*billingSnapshot:`, string(src), "the per-turn store must carry the snapshot (row 4)")
	require.Regexp(t, `BillingSnapshot:\s+turnBillingSnapshot`, string(src), "the :2084 RecordUsage literal must pass the turn's snapshot")
}
