package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectInventoryAccountMappingsUsesFiniteRuntimeMappings(t *testing.T) {
	tests := []struct {
		name    string
		account Account
		want    []string
	}{
		{
			name:    "empty antigravity uses synthesized defaults",
			account: Account{ID: 1, Platform: PlatformAntigravity},
			want:    inventoryMappingValues((&Account{Platform: PlatformAntigravity}).GetModelMapping()),
		},
		{
			name: "non-empty antigravity includes augmentations",
			account: Account{ID: 2, Platform: PlatformAntigravity, Credentials: map[string]any{
				"model_mapping": map[string]any{"custom": "upstream-custom"},
			}},
			want: []string{"gemini-3-flash", "gemini-3.1-pro-high", "gemini-3.1-pro-low", "upstream-custom"},
		},
		{
			name:    "empty grok uses synthesized defaults",
			account: Account{ID: 3, Platform: PlatformGrok},
			want:    inventoryMappingValues((&Account{Platform: PlatformGrok}).GetModelMapping()),
		},
		{
			name: "openai includes compact-only concrete target",
			account: Account{ID: 4, Platform: PlatformOpenAI, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-5.4": "gpt-5.4-compact-upstream"},
			}},
			want: []string{"gpt-5.4-compact-upstream"},
		},
		{
			name: "normal mapping preserves exact values and deduplicates exact matches",
			account: Account{ID: 5, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{
					"one": " exact-upstream ", "two": "exact-upstream", "three": "exact-upstream",
				},
			}},
			want: []string{" exact-upstream ", "exact-upstream"},
		},
		{
			name: "wildcard namespace is not fabricated but concrete target is enumerable",
			account: Account{ID: 6, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{"*": "*", "gpt-*": "concrete-upstream"},
			}},
			want: []string{"concrete-upstream"},
		},
		{
			name:    "empty allow-all account is non-enumerable",
			account: Account{ID: 7, Platform: PlatformOpenAI},
			want:    nil,
		},
		{
			name:    "protocol-only anthropic aliases are not mapping sources",
			account: Account{ID: 8, Platform: PlatformAnthropic, Type: AccountTypeOAuth},
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ProjectInventoryAccountMappings(&test.account)
			require.Equal(t, test.account.ID, got.AccountID)
			require.Equal(t, test.want, got.UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryAccountMappingsResolvesBedrockDefaultsForAccountRegion(t *testing.T) {
	account := &Account{
		ID: 9, Platform: PlatformAnthropic, Type: AccountTypeBedrock,
		Credentials: map[string]any{"aws_region": "eu-west-1"},
	}

	projection := ProjectInventoryAccountMappings(account)

	require.Contains(t, projection.UpstreamModelIDs, "eu.anthropic.claude-sonnet-4-5-20250929-v1:0")
	require.NotContains(t, projection.UpstreamModelIDs, "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
}

func inventoryMappingValues(mapping map[string]string) []string {
	seen := make(map[string]struct{}, len(mapping))
	values := make([]string, 0, len(mapping))
	for _, value := range mapping {
		if value == "" || value == "*" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	slicesSort(values)
	return values
}

func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
