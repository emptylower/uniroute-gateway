package repository

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelGovernanceInventoryQueryDefensivelyAllowsOnlyConcretePersistedPlatforms(t *testing.T) {
	query := strings.ToLower(modelGovernanceInventoryQuery)
	require.Contains(t, query, "governance_target_platform in ('anthropic', 'openai', 'gemini', 'antigravity', 'grok')")
	require.NotContains(t, query, "coalesce(nullif(ul.governance_target_platform, '')")
}

func TestModelGovernanceInventoryQueryUsesOnlyServiceApprovedCurrentModels(t *testing.T) {
	query := strings.ToLower(modelGovernanceInventoryQuery)
	require.NotContains(t, query, "channel_mapping_models")
	require.NotContains(t, query, "channel_pricing_models")
	require.NotContains(t, query, "jsonb_each(")
	require.Contains(t, query, "jsonb_array_elements_text(coalesce(d.upstream_model_ids")
	require.Contains(t, query, "select account_id, platform, group_id, channel_id, upstream_model_id from account_mapping_models")
	require.Contains(t, query, "union\n    select account_id, platform, group_id, channel_id, upstream_model_id from usage_rows")
}
