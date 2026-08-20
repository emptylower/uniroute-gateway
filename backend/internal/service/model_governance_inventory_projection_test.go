package service

import (
	"fmt"
	"slices"
	"testing"
	"unicode"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/stretchr/testify/require"
)

func TestProjectInventoryAccountMappingsUsesFiniteRuntimeMappings(t *testing.T) {
	tests := []struct {
		name     string
		account  Account
		restrict bool
		want     []string
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
			name: "eligible openai includes trimmed compact-only concrete target",
			account: Account{ID: 4, Platform: PlatformOpenAI, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-5.4": " gpt-5.4-compact-upstream "},
			}},
			want: []string{"gpt-5.4-compact-upstream"},
		},
		{
			name: "openai api key trims and deduplicates effective targets",
			account: Account{ID: 5, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{
					"one": " exact-upstream ", "two": "exact-upstream", "three": "exact-upstream",
				},
			}},
			want: []string{"exact-upstream"},
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
		{
			name: "force-off openai excludes compact mapping",
			account: Account{ID: 9, Platform: PlatformOpenAI,
				Credentials: map[string]any{"compact_model_mapping": map[string]any{"gpt": "compact-target"}},
				Extra:       map[string]any{"openai_compact_mode": OpenAICompactModeForceOff},
			},
			want: nil,
		},
		{
			name: "unsupported openai excludes compact mapping",
			account: Account{ID: 10, Platform: PlatformOpenAI,
				Credentials: map[string]any{"compact_model_mapping": map[string]any{"gpt": "compact-target"}},
				Extra:       map[string]any{"openai_compact_supported": false},
			},
			want: nil,
		},
		{
			name: "openai oauth canonicalizes explicit configured target",
			account: Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{"model_mapping": map[string]any{"public": " openai/gpt-5.4-high "}},
			},
			want: []string{"gpt-5.4"},
		},
		{
			name: "anthropic api key preserves explicitly configured forwarded target",
			account: Account{ID: 12, Platform: PlatformAnthropic, Type: AccountTypeAPIKey,
				Credentials: map[string]any{"model_mapping": map[string]any{"public": " claude-sonnet-4-5 "}},
			},
			want: []string{" claude-sonnet-4-5 "},
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

func TestResolveAnthropicFinalModelMatchesForwardingAccountTypes(t *testing.T) {
	tests := []struct {
		name      string
		account   Account
		requested string
		want      string
	}{
		{
			name: "api key uses ordinary account mapping",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "api-upstream"},
			}},
			requested: "public",
			want:      "api-upstream",
		},
		{
			name: "service account preserves explicit identity mapping",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeServiceAccount, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-3-5-sonnet-latest": "claude-3-5-sonnet-latest"},
			}},
			requested: "claude-3-5-sonnet-latest",
			want:      "claude-3-5-sonnet-latest",
		},
		{
			name:      "service account normalizes unmapped claude alias for vertex",
			account:   Account{Platform: PlatformAnthropic, Type: AccountTypeServiceAccount},
			requested: "claude-3-5-sonnet-latest",
			want:      normalizeVertexAnthropicModelID(claude.NormalizeModelID("claude-3-5-sonnet-latest")),
		},
		{
			name: "oauth ignores ordinary mapping value",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeOAuth, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-3-5-sonnet-latest": "poison-oauth-target"},
			}},
			requested: "claude-3-5-sonnet-latest",
			want:      claude.NormalizeModelID("claude-3-5-sonnet-latest"),
		},
		{
			name: "setup token ignores ordinary mapping value",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeSetupToken, Credentials: map[string]any{
				"model_mapping": map[string]any{"claude-3-5-sonnet-latest": "poison-setup-target"},
			}},
			requested: "claude-3-5-sonnet-latest",
			want:      claude.NormalizeModelID("claude-3-5-sonnet-latest"),
		},
		{
			name: "non anthropic api key preserves ordinary mapping behavior",
			account: Account{Platform: PlatformGemini, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "mapped"},
			}},
			requested: "public",
			want:      "mapped",
		},
		{
			name:      "non anthropic service account remains unchanged",
			account:   Account{Platform: PlatformGemini, Type: AccountTypeServiceAccount},
			requested: "claude-sonnet-4-5",
			want:      "claude-sonnet-4-5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, ResolveAnthropicFinalModel(&test.account, test.requested))
		})
	}
}

func TestProjectInventoryUsesAnthropicFinalModelResolverForAccountAndChannelPaths(t *testing.T) {
	requested := "claude-3-5-sonnet-latest"
	want := claude.NormalizeModelID(requested)
	account := Account{ID: 1201, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Credentials: map[string]any{
		"model_mapping": map[string]any{requested: "poison-account-target"},
	}}

	accountOnly := ProjectInventoryAccountMappings(&account)
	require.Equal(t, []string{want}, accountOnly.UpstreamModelIDs)

	groupID, channelID := int64(1202), int64(1203)
	route := CompositeModelRoute{ID: 1204, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformAnthropic, UpstreamModel: requested, Endpoint: CompositeRouteEndpointMessages, Enabled: true}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{
		PlatformAnthropic: {requested: requested},
	}}
	grouped := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformAnthropic, CompositeRoute: &route, Channel: &channel,
		}},
	}, config.RunModeStandard)
	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformAnthropic,
		UpstreamModelIDs: []string{want},
	}}, grouped)
}

func TestResolveAnthropicCountTokensFinalModelMatchesRuntimeAccountTypes(t *testing.T) {
	requested := "claude-sonnet-4-5"
	normalized := claude.NormalizeModelID(requested)
	tests := []struct {
		name    string
		account Account
		want    string
	}{
		{
			name: "api key uses ordinary mapping",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{requested: "api-count-final"},
			}},
			want: "api-count-final",
		},
		{
			name: "service ignores mapping and vertex normalization",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeServiceAccount, Credentials: map[string]any{
				"model_mapping": map[string]any{requested: "poison-service-final"},
			}},
			want: normalized,
		},
		{
			name: "oauth uses plain claude normalization",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeOAuth, Credentials: map[string]any{
				"model_mapping": map[string]any{requested: "poison-oauth-final"},
			}},
			want: normalized,
		},
		{
			name: "setup token uses plain claude normalization",
			account: Account{Platform: PlatformAnthropic, Type: AccountTypeSetupToken, Credentials: map[string]any{
				"model_mapping": map[string]any{requested: "poison-setup-final"},
			}},
			want: normalized,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, ResolveAnthropicCountTokensFinalModel(&test.account, requested))
		})
	}
}

func TestProjectInventoryCompositeCountTokensUsesEndpointAwareAnthropicResolver(t *testing.T) {
	requested := "claude-sonnet-4-5"
	groupID, channelID := int64(1250), int64(1251)
	accountTypes := []string{AccountTypeAPIKey, AccountTypeServiceAccount, AccountTypeOAuth, AccountTypeSetupToken}

	for index, accountType := range accountTypes {
		t.Run(accountType, func(t *testing.T) {
			mapped := "poison-" + accountType
			want := claude.NormalizeModelID(requested)
			if accountType == AccountTypeAPIKey {
				mapped = "api-count-final"
				want = mapped
			}
			account := Account{ID: int64(1252 + index), Platform: PlatformAnthropic, Type: accountType, Credentials: map[string]any{
				"model_mapping": map[string]any{requested: mapped},
			}}
			route := CompositeModelRoute{ID: account.ID, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
				TargetPlatform: PlatformAnthropic, UpstreamModel: requested, Endpoint: CompositeRouteEndpointCountTokens, Enabled: true}
			channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{
				PlatformAnthropic: {requested: requested},
			}}

			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
					TargetPlatform: PlatformAnthropic, CompositeRoute: &route, Channel: &channel,
				}},
			}, config.RunModeStandard)

			require.Equal(t, []string{want}, got[0].UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryUngroupedAnthropicServiceAccountUnionsFiniteEndpointOutcomes(t *testing.T) {
	requested := "claude-sonnet-4-5"
	messagesTarget := "vertex-messages-target"
	countTokensTarget := claude.NormalizeModelID(requested)
	account := Account{ID: 1260, Platform: PlatformAnthropic, Type: AccountTypeServiceAccount, Credentials: map[string]any{
		"model_mapping": map[string]any{requested: messagesTarget},
	}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, TargetPlatform: PlatformAnthropic, Ungrouped: true,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, TargetPlatform: PlatformAnthropic,
		UpstreamModelIDs: []string{countTokensTarget, messagesTarget},
	}}, got)
	require.NotEqual(t, messagesTarget, countTokensTarget,
		"the Messages/Vertex target must not stand in for count_tokens")
}

func TestProjectInventoryUngroupedProviderGeneratedRequestsTraverseFiniteEndpoints(t *testing.T) {
	tests := []struct {
		name        string
		account     Account
		want        []string
		notContains []string
	}{
		{
			name: "openai compact-only request domain",
			account: Account{ID: 1261, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-5.4": "gpt-5.4-compact-integration"},
			}},
			want: []string{"gpt-5.4", "gpt-5.4-compact-integration"},
		},
		{
			name: "openai compact-only wildcard request domain",
			account: Account{ID: 1263, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-*": "gpt-wildcard-compact-integration"},
			}},
			want: []string{"gpt-wildcard-compact-integration"},
		},
		{
			name: "bedrock regional default request domain",
			account: Account{ID: 1262, Platform: PlatformAnthropic, Type: AccountTypeBedrock, Credentials: map[string]any{
				"aws_region": "eu-west-1",
			}},
			want:        []string{"eu.anthropic.claude-sonnet-4-5-20250929-v1:0"},
			notContains: []string{claude.NormalizeModelID("claude-sonnet-4-5")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: test.account.ID, TargetPlatform: test.account.Platform, Ungrouped: true,
				}},
			}, config.RunModeStandard)

			require.Len(t, got, 1)
			for _, model := range test.want {
				require.Contains(t, got[0].UpstreamModelIDs, model)
			}
			for _, model := range test.notContains {
				require.NotContains(t, got[0].UpstreamModelIDs, model)
			}
		})
	}
}

func TestProjectInventoryCompositeCountTokensRejectsAntigravityTarget(t *testing.T) {
	groupID := int64(1270)
	account := Account{ID: 1271, Platform: PlatformAntigravity, Credentials: map[string]any{
		"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5"},
	}}
	route := CompositeModelRoute{ID: 1272, GroupID: groupID, PublicModel: "claude-sonnet-4-5",
		MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAntigravity,
		Endpoint: CompositeRouteEndpointCountTokens, Enabled: true}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformAntigravity, CompositeRoute: &route,
		}},
	}, config.RunModeStandard)

	require.Len(t, got, 1)
	require.Nil(t, got[0].UpstreamModelIDs)
}

func TestProjectInventoryCompositeCountTokensRejectsMixedAntigravityAccountForAnthropicTarget(t *testing.T) {
	groupID := int64(1273)
	account := Account{ID: 1274, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
		"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5"},
	}}
	route := CompositeModelRoute{ID: 1275, GroupID: groupID, PublicModel: "claude-sonnet-4-5",
		MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAnthropic,
		Endpoint: CompositeRouteEndpointCountTokens, Enabled: true}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformAnthropic, CompositeRoute: &route,
		}},
	}, config.RunModeStandard)

	require.Len(t, got, 1)
	require.Nil(t, got[0].UpstreamModelIDs)
}

func TestProjectInventoryUngroupedAntigravityThinkingDiffersByTargetPlatform(t *testing.T) {
	account := Account{ID: 1280, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"claude-sonnet-4-5":          "claude-sonnet-4-5",
			"claude-sonnet-4-5-thinking": "claude-sonnet-4-5-thinking",
		},
	}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: account.ID, TargetPlatform: PlatformAntigravity, Ungrouped: true},
			{AccountID: account.ID, TargetPlatform: PlatformAnthropic, Ungrouped: true},
			{AccountID: account.ID, TargetPlatform: PlatformGemini, Ungrouped: true},
		},
	}, config.RunModeStandard)

	byTarget := make(map[string][]string, len(got))
	for _, projection := range got {
		byTarget[projection.TargetPlatform] = projection.UpstreamModelIDs
	}
	require.Contains(t, byTarget[PlatformAntigravity], "claude-sonnet-4-5-thinking")
	require.Contains(t, byTarget[PlatformAnthropic], "claude-sonnet-4-5-thinking")
	require.NotContains(t, byTarget[PlatformGemini], "claude-sonnet-4-5-thinking")
	require.Contains(t, byTarget[PlatformGemini], "claude-sonnet-4-5")
}

func TestStableRuntimePlatformEligibilityMatchesSchedulers(t *testing.T) {
	tests := []struct {
		name         string
		account      Account
		target       string
		force        bool
		want         bool
		gatewayMixed bool
		geminiMixed  bool
	}{
		{name: "native ordinary", account: Account{Platform: PlatformAnthropic}, target: PlatformAnthropic, want: true, gatewayMixed: true},
		{name: "native forced", account: Account{Platform: PlatformAntigravity}, target: PlatformAntigravity, force: true, want: true},
		{name: "mixed anthropic", account: Account{Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}}, target: PlatformAnthropic, want: true, gatewayMixed: true},
		{name: "mixed gemini", account: Account{Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}}, target: PlatformGemini, want: true, gatewayMixed: true, geminiMixed: true},
		{name: "mixed disabled", account: Account{Platform: PlatformAntigravity}, target: PlatformAnthropic, gatewayMixed: true},
		{name: "mixed unsupported target", account: Account{Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}}, target: PlatformOpenAI},
		{name: "forced mismatch", account: Account{Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}}, target: PlatformAnthropic, force: true},
		{name: "native mismatch", account: Account{Platform: PlatformOpenAI}, target: PlatformAnthropic, gatewayMixed: true},
	}

	gateway := &GatewayService{}
	gemini := &GeminiMessagesCompatService{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsStableRuntimePlatformEligible(&test.account, test.target, test.force))
			require.Equal(t, test.want, gateway.isAccountAllowedForPlatform(&test.account, test.target, test.gatewayMixed))
			if test.target == PlatformGemini || test.account.Platform == PlatformGemini {
				require.Equal(t, test.want, gemini.isAccountValidForPlatform(&test.account, test.target, test.geminiMixed))
			}
		})
	}
}

func TestProjectInventoryRuntimeDimensionsApprovesOnlyStableRuntimePaths(t *testing.T) {
	groupAnthropic, groupComposite, channel := int64(10), int64(20), int64(30)
	input := InventoryProjectionInput{
		Accounts: []Account{
			{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"p": "native"}}},
			{ID: 2, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{"model_mapping": map[string]any{"p": "mixed"}}},
			{ID: 3, Platform: PlatformAntigravity, Credentials: map[string]any{"model_mapping": map[string]any{"p": "mixed-off"}}},
			{ID: 4, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"p": "ungrouped"}}},
		},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: 1, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAnthropic},
			{AccountID: 2, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAnthropic},
			{AccountID: 2, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAntigravity, ForcePlatform: true},
			{AccountID: 3, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAnthropic},
			{AccountID: 1, GroupID: &groupComposite, ChannelID: &channel, TargetPlatform: PlatformAnthropic},
			{AccountID: 2, GroupID: &groupComposite, ChannelID: &channel, TargetPlatform: PlatformGemini},
			{AccountID: 2, GroupID: &groupComposite, ChannelID: &channel, TargetPlatform: PlatformOpenAI},
			{AccountID: 2, TargetPlatform: PlatformAntigravity},
			{AccountID: 2, TargetPlatform: PlatformAnthropic},
			{AccountID: 2, TargetPlatform: PlatformGemini},
			{AccountID: 3, TargetPlatform: PlatformAntigravity},
			{AccountID: 3, TargetPlatform: PlatformAnthropic},
			{AccountID: 3, TargetPlatform: PlatformGemini},
			{AccountID: 4, TargetPlatform: PlatformOpenAI},
			{AccountID: 4, TargetPlatform: PlatformAnthropic},
			{AccountID: 4, TargetPlatform: PlatformGemini},
		},
	}

	got := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)

	require.ElementsMatch(t, []InventoryRuntimeDimensionProjection{
		{AccountID: 1, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: []string{"native"}},
		{AccountID: 2, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 2, GroupID: &groupAnthropic, ChannelID: &channel, TargetPlatform: PlatformAntigravity, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 1, GroupID: &groupComposite, ChannelID: &channel, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: []string{"native"}},
		{AccountID: 2, GroupID: &groupComposite, ChannelID: &channel, TargetPlatform: PlatformGemini, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 2, TargetPlatform: PlatformAntigravity, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 2, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 2, TargetPlatform: PlatformGemini, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[1]).UpstreamModelIDs},
		{AccountID: 3, TargetPlatform: PlatformAntigravity, UpstreamModelIDs: ProjectInventoryAccountMappings(&input.Accounts[2]).UpstreamModelIDs},
		{AccountID: 4, TargetPlatform: PlatformOpenAI, UpstreamModelIDs: []string{"ungrouped"}},
	}, got)
}

func TestProjectInventoryRuntimeDimensionsUsesConfiguredRunModeForUngroupedAccounts(t *testing.T) {
	account := Account{ID: 40, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
		"model_mapping": map[string]any{"public": "simple-ungrouped-model"},
	}}
	input := InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: account.ID, TargetPlatform: PlatformAntigravity, Ungrouped: true, AccountHasGroupBindings: true},
			{AccountID: account.ID, TargetPlatform: PlatformAnthropic, Ungrouped: true, AccountHasGroupBindings: true},
			{AccountID: account.ID, TargetPlatform: PlatformGemini, Ungrouped: true, AccountHasGroupBindings: true},
		},
	}

	standard := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)
	simple := ProjectInventoryRuntimeDimensions(input, config.RunModeSimple)

	require.Empty(t, standard, "standard mode must not fabricate ungrouped dimensions for a bound account")
	require.ElementsMatch(t, []InventoryRuntimeDimensionProjection{
		{AccountID: account.ID, TargetPlatform: PlatformAntigravity, UpstreamModelIDs: ProjectInventoryAccountMappings(&account).UpstreamModelIDs},
		{AccountID: account.ID, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: ProjectInventoryAccountMappings(&account).UpstreamModelIDs},
		{AccountID: account.ID, TargetPlatform: PlatformGemini, UpstreamModelIDs: ProjectInventoryAccountMappings(&account).UpstreamModelIDs},
	}, simple)

	input.Candidates[0].AccountHasGroupBindings = false
	input.Candidates = input.Candidates[:1]
	require.Len(t, ProjectInventoryRuntimeDimensions(input, config.RunModeStandard), 1,
		"a truly ungrouped account remains inventoried in standard mode")
}

func TestProjectInventoryRuntimeDimensionsRestrictsCompositeMappingsToRouteReachability(t *testing.T) {
	groupID, channelID := int64(50), int64(60)
	account := Account{ID: 41, Platform: PlatformOpenAI, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"x-exact":   "x-exact-upstream",
			"x-prefix*": "x-prefix-upstream",
			"y-only":    "y-unreachable-upstream",
		},
	}}
	routes := []CompositeModelRoute{
		{ID: 1, GroupID: groupID, PublicModel: "x-exact", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 2, GroupID: groupID, PublicModel: "x-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointAny, Enabled: true},
	}
	candidates := make([]InventoryRuntimeDimensionCandidate, 0, len(routes))
	for _, route := range routes {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route,
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{Accounts: []Account{account}, Candidates: candidates}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI,
		UpstreamModelIDs: []string{"x-exact-upstream", "x-prefix-upstream"},
	}}, got)
	require.NotContains(t, got[0].UpstreamModelIDs, "y-unreachable-upstream")
}

func TestProjectInventoryRuntimeDimensionsCompositeRoutePrecedenceAndMixedEligibility(t *testing.T) {
	groupID := int64(51)
	account := Account{ID: 42, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
		"model_mapping": map[string]any{"shared-exact": "native-target", "shared-prefix-model": "mixed-target", "shadowed-model": "shadowed-target"},
	}}
	routes := []CompositeModelRoute{
		{ID: 10, GroupID: groupID, PublicModel: "shared-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformAnthropic, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 11, GroupID: groupID, PublicModel: "shared-exact", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAntigravity, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 12, GroupID: groupID, PublicModel: "shadowed-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformAnthropic, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 13, GroupID: groupID, PublicModel: "shadowed-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformGemini, Endpoint: CompositeRouteEndpointAny, Priority: -1, Enabled: true},
	}
	candidates := make([]InventoryRuntimeDimensionCandidate, 0, len(routes))
	for i := range routes {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: account.ID, GroupID: &groupID, TargetPlatform: routes[i].TargetPlatform, CompositeRoute: &routes[i],
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{Accounts: []Account{account}, Candidates: candidates}, config.RunModeStandard)
	byTarget := make(map[string][]string)
	for _, projection := range got {
		byTarget[projection.TargetPlatform] = projection.UpstreamModelIDs
	}
	require.Contains(t, byTarget[PlatformAntigravity], "native-target")
	require.Contains(t, byTarget[PlatformAnthropic], "mixed-target")
	require.NotContains(t, byTarget[PlatformAnthropic], "native-target", "exact native route shadows the mixed prefix")
	require.NotContains(t, byTarget[PlatformAnthropic], "shadowed-target", "lower-priority duplicate prefix never wins")
	require.Contains(t, byTarget[PlatformGemini], "shadowed-target")
}

func TestProjectInventoryRuntimeDimensionsDoesNotInventDynamicPrefixModelForAllowAllAccount(t *testing.T) {
	groupID := int64(52)
	route := CompositeModelRoute{
		ID: 20, GroupID: groupID, PublicModel: "dynamic-prefix-", MatchType: CompositeRouteMatchPrefix,
		TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointAny, Enabled: true,
	}
	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{{ID: 43, Platform: PlatformOpenAI}},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: 43, GroupID: &groupID, TargetPlatform: PlatformOpenAI, CompositeRoute: &route,
		}},
	}, config.RunModeStandard)

	require.Len(t, got, 1)
	require.Nil(t, got[0].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsFindsPrefixWitnessBeyondExactRouteBlockers(t *testing.T) {
	groupID := int64(53)
	account := Account{ID: 44, Platform: PlatformOpenAI, Credentials: map[string]any{
		"model_mapping": map[string]any{"x-*": "reachable-after-exact-blocker"},
	}}
	routes := []CompositeModelRoute{
		{ID: 30, GroupID: groupID, PublicModel: "x-", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAnthropic, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 31, GroupID: groupID, PublicModel: "x-!", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformGemini, Endpoint: CompositeRouteEndpointAny, Enabled: true},
		{ID: 32, GroupID: groupID, PublicModel: "x-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointAny, Enabled: true},
	}
	candidates := make([]InventoryRuntimeDimensionCandidate, 0, len(routes))
	for i := range routes {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: account.ID, GroupID: &groupID, TargetPlatform: routes[i].TargetPlatform, CompositeRoute: &routes[i],
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{Accounts: []Account{account}, Candidates: candidates}, config.RunModeStandard)
	for _, projection := range got {
		if projection.TargetPlatform == PlatformOpenAI {
			require.Contains(t, projection.UpstreamModelIDs, "reachable-after-exact-blocker")
			return
		}
	}
	t.Fatal("missing reachable OpenAI prefix dimension")
}

func TestProjectInventoryRuntimeDimensionsComposesCompositeRouteChannelAndAccountMappings(t *testing.T) {
	groupID, channelID := int64(54), int64(64)
	account := Account{ID: 45, Platform: PlatformOpenAI, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"channel-fixed": "actual-fixed",
			"channel-pass":  "actual-pass",
			"route-fixed":   "wrong-direct-route-target",
			"public-pass":   "wrong-direct-pass-target",
			"unrelated":     "unrelated-account-target",
		},
	}}
	routes := []CompositeModelRoute{
		{ID: 40, GroupID: groupID, PublicModel: "public-fixed", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, UpstreamModel: "route-fixed", Endpoint: CompositeRouteEndpointResponses, Enabled: true},
		{ID: 41, GroupID: groupID, PublicModel: "public-pass", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true},
	}
	channel := InventoryChannelCandidate{
		ID: channelID,
		ModelMapping: map[string]map[string]string{PlatformOpenAI: {
			"route-fixed": "channel-fixed",
			"public-pass": "channel-pass",
			"unrelated":   "unrelated-channel-target",
		}},
		Pricing: []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"route-fixed", "public-pass", "unrelated-pricing-model"}}},
	}
	candidates := make([]InventoryRuntimeDimensionCandidate, 0, len(routes))
	for i := range routes {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &routes[i], Channel: &channel,
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{Accounts: []Account{account}, Candidates: candidates}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI,
		UpstreamModelIDs: []string{"actual-fixed", "actual-pass"},
	}}, got)
}

func TestProjectInventoryRuntimeDimensionsComposesResponsesRouteChannelAndOpenAICompactForwarding(t *testing.T) {
	groupID, channelID := int64(540), int64(640)
	route := CompositeModelRoute{
		ID: 400, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, UpstreamModel: "route-model", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"route-model": "account-input",
	}}}
	account := Account{ID: 450, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"route-model":   "scheduler-support-only",
			"account-input": "actual-normal",
			"account-only":  "must-not-leak",
		},
		"compact_model_mapping": map[string]any{"actual-normal": "actual-compact"},
	}, Extra: map[string]any{"openai_compact_supported": true}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI,
		UpstreamModelIDs: []string{"actual-compact", "actual-normal"},
	}}, got)
	require.Equal(t, "actual-normal", resolveOpenAIForwardModelForEndpoint(&account, "account-input", "", false).UpstreamModel)
	require.Equal(t, "actual-compact", resolveOpenAIForwardModelForEndpoint(&account, "account-input", "", true).UpstreamModel)
	require.NotContains(t, got[0].UpstreamModelIDs, "must-not-leak")
}

func TestProjectInventoryRuntimeDimensionsCompactPassthroughUsesAccountInput(t *testing.T) {
	groupID, channelID := int64(542), int64(642)
	route := CompositeModelRoute{
		ID: 403, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, UpstreamModel: "route-model", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"route-model": "account-input",
	}}}
	account := Account{ID: 453, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping":         map[string]any{"account-input": "wrong-normal-remap"},
		"compact_model_mapping": map[string]any{"account-input": "actual-compact", "wrong-normal-remap": "wrong-compact"},
	}, Extra: map[string]any{"openai_passthrough": true, "openai_compact_supported": true}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"account-input", "actual-compact"}, got[0].UpstreamModelIDs)
	require.NotContains(t, got[0].UpstreamModelIDs, "wrong-normal-remap")
	require.NotContains(t, got[0].UpstreamModelIDs, "wrong-compact")
}

func TestProjectInventoryRuntimeDimensionsGroupedChannelEnumeratesResponsesCompact(t *testing.T) {
	groupID, channelID := int64(545), int64(645)
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"public": "account-input",
	}}}
	account := Account{ID: 457, Platform: PlatformOpenAI, Credentials: map[string]any{
		"model_mapping":         map[string]any{"public": "scheduler-support", "account-input": "actual-normal"},
		"compact_model_mapping": map[string]any{"actual-normal": "actual-compact"},
	}, Extra: map[string]any{"openai_compact_supported": true}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformOpenAI, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI,
		UpstreamModelIDs: []string{"actual-compact", "actual-normal"},
	}}, got)
	require.NotContains(t, got[0].UpstreamModelIDs, "scheduler-support")
}

func TestProjectInventoryRuntimeDimensionsAnyRouteRequiresConcreteHTTPEndpointReachability(t *testing.T) {
	groupID := int64(543)
	generic := CompositeModelRoute{
		ID: 410, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, UpstreamModel: "generic-input", Endpoint: CompositeRouteEndpointAny, Enabled: true,
	}
	targets := map[string]string{
		CompositeRouteEndpointMessages:        PlatformAnthropic,
		CompositeRouteEndpointCountTokens:     PlatformAnthropic,
		CompositeRouteEndpointResponses:       PlatformOpenAI,
		CompositeRouteEndpointChatCompletions: PlatformOpenAI,
		CompositeRouteEndpointEmbeddings:      PlatformOpenAI,
		CompositeRouteEndpointImages:          PlatformGrok,
		CompositeRouteEndpointGemini:          PlatformGemini,
	}
	routes := []CompositeModelRoute{generic}
	candidates := []InventoryRuntimeDimensionCandidate{{
		AccountID: 454, GroupID: &groupID, GroupPlatform: PlatformComposite,
		TargetPlatform: PlatformOpenAI, CompositeRoute: &routes[0],
	}}
	id := int64(411)
	for endpoint, platform := range targets {
		routes = append(routes, CompositeModelRoute{
			ID: id, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
			TargetPlatform: platform, UpstreamModel: "specific-input", Endpoint: endpoint, Enabled: true,
		})
		id++
	}
	for i := 1; i < len(routes); i++ {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: 999, GroupID: &groupID, GroupPlatform: PlatformComposite,
			TargetPlatform: routes[i].TargetPlatform, CompositeRoute: &routes[i],
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{{ID: 454, Platform: PlatformOpenAI, Credentials: map[string]any{
			"model_mapping": map[string]any{"generic-input": "generic-target"},
		}}},
		Candidates: candidates,
	}, config.RunModeStandard)

	require.Len(t, got, 1)
	require.Nil(t, got[0].UpstreamModelIDs)
	require.NotContains(t, got[0].UpstreamModelIDs, "generic-target")
}

func TestProjectInventoryRuntimeDimensionsPreservesConfiguredWitnessOnObservationCollision(t *testing.T) {
	groupID := int64(544)
	route := CompositeModelRoute{
		ID: 420, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}
	account := Account{ID: 455, Platform: PlatformOpenAI, Credentials: map[string]any{
		"model_mapping": map[string]any{"public": "actual-upstream"},
	}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route,
		}},
		Observations: []InventoryModelObservation{{AccountID: account.ID, UpstreamModelID: "public"}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"actual-upstream"}, got[0].UpstreamModelIDs)
	require.NotContains(t, got[0].UpstreamModelIDs, "public")

	observed := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{{ID: 456, Platform: PlatformOpenAI, Extra: map[string]any{"openai_passthrough": true}}},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: 456, GroupID: &groupID, GroupPlatform: PlatformComposite, TargetPlatform: PlatformOpenAI,
		}},
		Observations: []InventoryModelObservation{{AccountID: 456, UpstreamModelID: "gpt-observed-exact"}},
	}, config.RunModeStandard)
	require.Equal(t, []string{"gpt-observed-exact"}, observed[0].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsUsesPathCorrectSchedulerModel(t *testing.T) {
	groupID, channelID := int64(541), int64(641)
	account := Account{ID: 451, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"route-model": "wrong-direct", "channel-model": "actual-upstream",
	}}}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"route-model": "channel-model",
	}}}
	responsesRoute := CompositeModelRoute{ID: 401, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, UpstreamModel: "route-model", Endpoint: CompositeRouteEndpointResponses, Enabled: true}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &responsesRoute, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"actual-upstream"}, got[0].UpstreamModelIDs)

	geminiRoute := responsesRoute
	geminiRoute.ID = 402
	geminiRoute.Endpoint = CompositeRouteEndpointGemini
	geminiRoute.TargetPlatform = PlatformGemini
	geminiAccount := Account{ID: 452, Platform: PlatformGemini, Credentials: map[string]any{"model_mapping": map[string]any{
		"route-model": "route-only-upstream",
	}}}
	geminiChannel := channel
	geminiChannel.ModelMapping = map[string]map[string]string{PlatformGemini: {"route-model": "channel-model"}}
	gemini := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{geminiAccount},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: geminiAccount.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformGemini, CompositeRoute: &geminiRoute, Channel: &geminiChannel,
		}},
	}, config.RunModeStandard)
	require.Nil(t, gemini[0].UpstreamModelIDs, "Gemini scheduling checks the channel output, which this account does not support")
}

func TestProjectInventoryRuntimeDimensionsIncludesEveryOverlappingChannelWildcardOutcome(t *testing.T) {
	groupID, channelID := int64(55), int64(65)
	account := Account{ID: 46, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"gpt-5.4-mini": "scheduler-supported", "long-channel": "actual-long", "short-channel": "actual-short",
	}}}
	route := CompositeModelRoute{ID: 50, GroupID: groupID, PublicModel: "gpt-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"gpt-*": "short-channel", "gpt-5.4*": "long-channel",
	}}}
	input := InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route, Channel: &channel,
		}},
	}

	got := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)
	require.Equal(t, []string{"actual-long", "actual-short"}, got[0].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsExactChannelMappingSuppressesWildcardVariants(t *testing.T) {
	groupID, channelID := int64(551), int64(651)
	account := Account{ID: 461, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"gpt-5.4": "scheduler-supported", "exact-channel": "actual-exact", "long-channel": "actual-long", "short-channel": "actual-short",
	}}}
	route := CompositeModelRoute{ID: 501, GroupID: groupID, PublicModel: "gpt-", MatchType: CompositeRouteMatchPrefix, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
		"gpt-5.4": "exact-channel", "gpt-*": "short-channel", "gpt-5.4*": "long-channel",
	}}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"actual-exact"}, got[0].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsExplicitRouteOverridesDetectorAndEndpointCompatibility(t *testing.T) {
	groupID := int64(56)
	account := Account{ID: 47, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{
		"claude-upstream": "actual-claude", "gpt-5.4": "detector-would-be-openai",
	}}}
	routes := []CompositeModelRoute{
		{ID: 60, GroupID: groupID, PublicModel: "gpt-5.4", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-upstream", Endpoint: CompositeRouteEndpointMessages, Enabled: true},
		{ID: 61, GroupID: groupID, PublicModel: "embed-public", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-upstream", Endpoint: CompositeRouteEndpointEmbeddings, Enabled: true},
	}
	candidates := make([]InventoryRuntimeDimensionCandidate, 0, len(routes))
	for i := range routes {
		candidates = append(candidates, InventoryRuntimeDimensionCandidate{
			AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformAnthropic, CompositeRoute: &routes[i],
		})
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{Accounts: []Account{account}, Candidates: candidates}, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, TargetPlatform: PlatformAnthropic, UpstreamModelIDs: []string{"actual-claude"},
	}}, got)
}

func TestCompositeEndpointTargetCompatibilityMatchesCurrentDispatchGates(t *testing.T) {
	require.True(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointEmbeddings, PlatformOpenAI))
	require.False(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointEmbeddings, PlatformAnthropic))
	require.True(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointImages, PlatformOpenAI))
	require.True(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointImages, PlatformGrok))
	require.False(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointImages, PlatformGemini))
	require.True(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointGemini, PlatformGemini))
	require.False(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointGemini, PlatformAntigravity))
	require.False(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointCountTokens, PlatformAntigravity))
	require.True(t, IsCompositeEndpointTargetCompatible(CompositeRouteEndpointResponses, PlatformAnthropic))
}

func TestProjectInventoryRuntimeDimensionsUsesStableEndpointCapabilities(t *testing.T) {
	groupID := int64(1300)
	project := func(account Account, endpoint, model string) []string {
		route := CompositeModelRoute{ID: account.ID, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
			TargetPlatform: account.Platform, UpstreamModel: model, Endpoint: endpoint, Enabled: true}
		got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
			Accounts: []Account{account},
			Candidates: []InventoryRuntimeDimensionCandidate{{
				AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
				TargetPlatform: account.Platform, CompositeRoute: &route,
			}},
		}, config.RunModeStandard)
		require.Len(t, got, 1)
		return got[0].UpstreamModelIDs
	}

	embeddingsDisabled := Account{ID: 1301, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping":       map[string]any{"embed-model": "embed-final"},
		"openai_capabilities": []any{string(OpenAIEndpointCapabilityChatCompletions)},
	}}
	require.False(t, accountSupportsOpenAICapabilities(&embeddingsDisabled, OpenAIEndpointCapabilityEmbeddings, ""))
	require.Nil(t, project(embeddingsDisabled, CompositeRouteEndpointEmbeddings, "embed-model"))

	responsesDisabled := Account{ID: 1302, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping":       map[string]any{"gpt-text": "gpt-text-final"},
		"openai_capabilities": []any{string(OpenAIEndpointCapabilityChatCompletions)},
	}, Extra: map[string]any{"openai_responses_supported": false}}
	require.True(t, accountSupportsOpenAICapabilities(&responsesDisabled, OpenAIEndpointCapabilityChatCompletions, ""))
	require.False(t, accountSupportsOpenAICapabilities(&responsesDisabled, OpenAIEndpointCapabilityResponses, ""))
	require.Nil(t, project(responsesDisabled, CompositeRouteEndpointResponses, "gpt-text"),
		"persisted Responses-disabled accounts are excluded from the Responses inventory")
	require.Equal(t, []string{"gpt-text-final"}, project(responsesDisabled, CompositeRouteEndpointChatCompletions, "gpt-text"),
		"the same account remains reachable in the Chat Completions inventory")

	responsesUnknown := responsesDisabled
	responsesUnknown.ID = 1309
	responsesUnknown.Extra = nil
	require.True(t, accountSupportsOpenAICapabilities(&responsesUnknown, OpenAIEndpointCapabilityResponses, ""))
	require.Equal(t, []string{"gpt-text-final"}, project(responsesUnknown, CompositeRouteEndpointResponses, "gpt-text"))

	responsesEnabled := responsesDisabled
	responsesEnabled.ID = 1310
	responsesEnabled.Extra = map[string]any{"openai_responses_supported": true}
	require.True(t, accountSupportsOpenAICapabilities(&responsesEnabled, OpenAIEndpointCapabilityResponses, ""))
	require.Equal(t, []string{"gpt-text-final"}, project(responsesEnabled, CompositeRouteEndpointResponses, "gpt-text"))

	imageOnly := Account{ID: 1303, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping":       map[string]any{"gpt-image-2": "gpt-image-2"},
		"openai_capabilities": []any{string(OpenAIEndpointCapabilityChatCompletions)},
	}, Extra: map[string]any{"openai_responses_supported": true}}
	require.True(t, accountSupportsOpenAICapabilities(&imageOnly, OpenAIEndpointCapabilityResponses, ""))
	require.Equal(t, []string{"gpt-image-2"}, project(imageOnly, CompositeRouteEndpointResponses, "gpt-image-2"),
		"an image-generation model is reachable through the Responses-capability request shape")

	imageOnlyResponsesDisabled := imageOnly
	imageOnlyResponsesDisabled.ID = 1308
	imageOnlyResponsesDisabled.Extra = map[string]any{"openai_responses_supported": false}
	require.False(t, accountSupportsOpenAICapabilities(&imageOnlyResponsesDisabled, OpenAIEndpointCapabilityResponses, ""))
	require.Nil(t, project(imageOnlyResponsesDisabled, CompositeRouteEndpointResponses, "gpt-image-2"))

	images := Account{ID: 1304, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping": map[string]any{"gpt-image-2": "gpt-image-final"},
	}}
	require.True(t, accountSupportsOpenAICapabilities(&images, "", OpenAIImagesCapabilityBasic))
	require.True(t, accountSupportsOpenAICapabilities(&images, "", OpenAIImagesCapabilityNative))
	require.Equal(t, []string{"gpt-image-final"}, project(images, CompositeRouteEndpointImages, "gpt-image-2"))

	for _, test := range []struct {
		name    string
		account Account
		want    []string
	}{
		{name: "explicitly disabled", account: Account{ID: 1305, Platform: PlatformGrok, Type: AccountTypeAPIKey, Extra: map[string]any{GrokMediaEligibleExtraKey: false}}},
		{name: "oauth billing unobserved", account: Account{ID: 1306, Platform: PlatformGrok, Type: AccountTypeOAuth}, want: nil},
		{name: "positive override", account: Account{ID: 1307, Platform: PlatformGrok, Type: AccountTypeOAuth, Extra: map[string]any{GrokMediaEligibleExtraKey: true}}, want: []string{"grok-imagine-image-quality"}},
	} {
		t.Run("grok "+test.name, func(t *testing.T) {
			test.account.Credentials = map[string]any{"model_mapping": map[string]any{"grok-imagine-image-quality": "grok-imagine-image-quality"}}
			eligible, _ := test.account.GrokMediaGenerationEligibility()
			require.Equal(t, len(test.want) > 0, eligible,
				"inventory requires positive forwarding evidence even though the scheduler admits unobserved OAuth for probing")
			require.Equal(t, test.want, project(test.account, CompositeRouteEndpointImages, "grok-imagine"))
		})
	}
}

func TestProjectInventoryRuntimeDimensionsNormalizesGrokMediaBeforeMapping(t *testing.T) {
	groupID := int64(1400)
	tests := []struct {
		name     string
		endpoint string
		model    string
		mapping  map[string]any
		want     []string
	}{
		{
			name: "image generation and edit aliases normalize before mapping", endpoint: CompositeRouteEndpointImages, model: "grok-imagine",
			mapping: map[string]any{"grok-imagine-image-quality": "image-final"}, want: []string{"image-final"},
		},
		{
			name: "endpoint any enumerates text and image input video outcomes", endpoint: CompositeRouteEndpointAny, model: "grok-imagine-video-1.5",
			mapping: map[string]any{"grok-imagine-video": "video-text-final", "grok-imagine-video-1.5": "video-image-final"},
			want:    []string{"video-image-final", "video-text-final"},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := Account{ID: int64(1401 + index), Platform: PlatformGrok, Type: AccountTypeAPIKey,
				Credentials: map[string]any{"model_mapping": test.mapping}}
			route := CompositeModelRoute{ID: account.ID, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
				TargetPlatform: PlatformGrok, UpstreamModel: test.model, Endpoint: test.endpoint, Enabled: true}
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{account},
				Candidates: []InventoryRuntimeDimensionCandidate{{AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite,
					TargetPlatform: PlatformGrok, CompositeRoute: &route}},
			}, config.RunModeStandard)

			require.Equal(t, test.want, got[0].UpstreamModelIDs)
			if test.endpoint == CompositeRouteEndpointImages {
				require.Equal(t, "grok-imagine-image-quality", NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointImagesGenerations, test.model, false))
				require.Equal(t, "grok-imagine-image-quality", NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointImagesEdits, test.model, true))
			} else {
				require.Equal(t, "grok-imagine-video", NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointVideosGenerations, test.model, false))
				require.Equal(t, "grok-imagine-video-1.5", NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointVideosGenerations, test.model, true))
			}
		})
	}
}

func TestProjectInventoryRuntimeDimensionsNormalizesOrdinaryChannelBackedGrokVideo(t *testing.T) {
	groupID, channelID := int64(1450), int64(1451)
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{
		PlatformGrok: {"grok-imagine-video-1.5": "grok-imagine-video-1.5"},
	}}
	account := Account{ID: 1452, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"grok-imagine-video*":   "video-text-final",
			"grok-imagine-video-1*": "video-image-final",
		},
	}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformGrok, Channel: &channel,
		}},
	}, config.RunModeStandard)
	require.Equal(t, []string{"video-image-final", "video-text-final"}, got[0].UpstreamModelIDs)

	for _, test := range []struct {
		name    string
		account Account
	}{
		{name: "explicitly disabled", account: Account{ID: 1453, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: account.Credentials, Extra: map[string]any{GrokMediaEligibleExtraKey: false}}},
		{name: "oauth unobserved", account: Account{ID: 1454, Platform: PlatformGrok, Type: AccountTypeOAuth, Credentials: account.Credentials}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ineligible := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: test.account.ID, GroupID: &groupID, ChannelID: &channelID,
					TargetPlatform: PlatformGrok, Channel: &channel,
				}},
			}, config.RunModeStandard)
			require.Nil(t, ineligible[0].UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryRuntimeDimensionsDetectorUsesOnlyConfiguredFiniteWitnesses(t *testing.T) {
	groupID := int64(57)
	account := Account{ID: 48, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"gpt-configured": "actual-detector-target", "custom-unrecognized": "unreachable-custom",
	}}}
	input := InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, GroupPlatform: PlatformComposite, TargetPlatform: PlatformOpenAI,
		}},
	}

	got := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)

	require.Equal(t, []InventoryRuntimeDimensionProjection{{
		AccountID: account.ID, GroupID: &groupID, TargetPlatform: PlatformOpenAI, UpstreamModelIDs: []string{"actual-detector-target"},
	}}, got)
}

func TestProjectInventoryRuntimeDimensionsDoesNotLeakObservationsAcrossDimensions(t *testing.T) {
	groupA, groupB := int64(58), int64(59)
	account := Account{ID: 49, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"public-a": "observed-a", "public-b": "observed-b",
	}}}
	routeA := CompositeModelRoute{ID: 70, GroupID: groupA, PublicModel: "public-a", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true}
	routeB := CompositeModelRoute{ID: 71, GroupID: groupB, PublicModel: "public-b", MatchType: CompositeRouteMatchExact, TargetPlatform: PlatformOpenAI, Endpoint: CompositeRouteEndpointResponses, Enabled: true}
	input := InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: account.ID, GroupID: &groupA, GroupPlatform: PlatformComposite, TargetPlatform: PlatformOpenAI, CompositeRoute: &routeA},
			{AccountID: account.ID, GroupID: &groupB, GroupPlatform: PlatformComposite, TargetPlatform: PlatformOpenAI, CompositeRoute: &routeB},
		},
		Observations: []InventoryModelObservation{{AccountID: account.ID, UpstreamModelID: "observed-a"}},
	}

	got := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)
	byGroup := make(map[int64][]string)
	for _, projection := range got {
		byGroup[*projection.GroupID] = projection.UpstreamModelIDs
	}
	require.Equal(t, []string{"observed-a"}, byGroup[groupA])
	require.Equal(t, []string{"observed-b"}, byGroup[groupB])
	require.NotContains(t, byGroup[groupB], "observed-a")
}

func TestProjectInventoryRuntimeDimensionsSeedsRequestedPricingThroughFullChain(t *testing.T) {
	groupID, channelID := int64(580), int64(680)
	tests := []struct {
		name    string
		account Account
		channel InventoryChannelCandidate
		want    []string
	}{
		{
			name:    "pricing-only identity on allow-all channel and account",
			account: Account{ID: 580, Platform: PlatformOpenAI},
			channel: InventoryChannelCandidate{ID: channelID, BillingModelSource: BillingModelSourceRequested,
				Pricing: []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"requested-priced"}}}},
			want: []string{"requested-priced"},
		},
		{
			name: "channel and account rewrites emit only final upstream",
			account: Account{ID: 581, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
				"requested-priced": "scheduler-support-only", "channel-output": "final-upstream",
			}}},
			channel: InventoryChannelCandidate{ID: channelID, BillingModelSource: BillingModelSourceRequested,
				ModelMapping: map[string]map[string]string{PlatformOpenAI: {"requested-priced": "channel-output"}},
				Pricing:      []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"requested-priced"}}}},
			want: []string{"final-upstream"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: test.account.ID, GroupID: &groupID, ChannelID: &channelID,
					TargetPlatform: PlatformOpenAI, Channel: &test.channel,
				}},
			}, config.RunModeStandard)
			require.Equal(t, test.want, got[0].UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryRuntimeDimensionsRequestedPricingKeepsOriginalCoordinateAcrossExplicitRoute(t *testing.T) {
	groupID, channelID := int64(581), int64(681)
	route := CompositeModelRoute{
		ID: 581, GroupID: groupID, PublicModel: "requested-priced", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, UpstreamModel: "routed-model", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}
	channel := InventoryChannelCandidate{
		ID: channelID, BillingModelSource: BillingModelSourceRequested, RestrictModels: true,
		ModelMapping: map[string]map[string]string{PlatformOpenAI: {"routed-model": "account-input"}},
		Pricing:      []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"requested-priced"}}},
	}
	account := Account{ID: 581, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"routed-model": "scheduler-support-only", "account-input": "final-upstream",
	}}}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
			TargetPlatform: PlatformOpenAI, CompositeRoute: &route, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"final-upstream"}, got[0].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsSeedsChannelMappedPricingPreimagesAndIdentity(t *testing.T) {
	groupID, channelID := int64(582), int64(682)
	tests := []struct {
		name    string
		account Account
		mapping map[string]map[string]string
		want    []string
	}{
		{
			name: "finite channel preimage reaches final upstream",
			account: Account{ID: 582, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
				"public": "scheduler-support-only", "priced-key": "final-upstream",
			}}},
			mapping: map[string]map[string]string{PlatformOpenAI: {"public": "priced-key"}},
			want:    []string{"final-upstream"},
		},
		{
			name:    "identity is permitted without channel mapping",
			account: Account{ID: 583, Platform: PlatformOpenAI},
			want:    []string{"priced-key"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := InventoryChannelCandidate{ID: channelID, BillingModelSource: "", RestrictModels: false,
				ModelMapping: test.mapping,
				Pricing:      []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"priced-key"}}}}
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: test.account.ID, GroupID: &groupID, ChannelID: &channelID,
					TargetPlatform: PlatformOpenAI, Channel: &channel,
				}},
			}, config.RunModeStandard)
			require.Equal(t, test.want, got[0].UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryRuntimeDimensionsUpstreamPricingRequiresProducibleFinalModel(t *testing.T) {
	groupID, channelID := int64(584), int64(684)
	tests := []struct {
		name     string
		account  Account
		restrict bool
		want     []string
	}{
		{
			name: "account preimage resolves to priced upstream",
			account: Account{ID: 584, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "priced-upstream"},
			}},
			restrict: true,
			want:     []string{"priced-upstream"},
		},
		{
			name: "unproducible concrete pricing key is excluded",
			account: Account{ID: 585, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "different-upstream"},
			}},
			restrict: true,
			want:     nil,
		},
		{
			name: "unrestricted pricing remains evidence but cannot relabel a different wildcard output",
			account: Account{ID: 586, Platform: PlatformOpenAI, Credentials: map[string]any{
				"model_mapping": map[string]any{"*": "different-upstream"},
			}},
			restrict: false,
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := InventoryChannelCandidate{ID: channelID, BillingModelSource: BillingModelSourceUpstream, RestrictModels: test.restrict,
				Pricing: []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"priced-upstream"}}}}
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account},
				Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: test.account.ID, GroupID: &groupID, ChannelID: &channelID,
					TargetPlatform: PlatformOpenAI, Channel: &channel,
				}},
			}, config.RunModeStandard)
			require.Equal(t, test.want, got[0].UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryRuntimeDimensionsWildcardPricingOnlyFiltersFiniteWitnesses(t *testing.T) {
	groupID, channelID := int64(586), int64(686)
	channel := InventoryChannelCandidate{ID: channelID, BillingModelSource: BillingModelSourceUpstream, RestrictModels: true,
		Pricing: []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"priced-*"}}}}
	input := InventoryProjectionInput{
		Accounts: []Account{
			{ID: 586, Platform: PlatformOpenAI},
			{ID: 587, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
				"public": "priced-final", "other": "not-priced",
			}}},
		},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: 586, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI, Channel: &channel},
			{AccountID: 587, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformOpenAI, Channel: &channel},
		},
	}

	got := ProjectInventoryRuntimeDimensions(input, config.RunModeStandard)
	require.Nil(t, got[0].UpstreamModelIDs, "wildcard pricing must not fabricate an ID or suffix")
	require.Equal(t, []string{"priced-final"}, got[1].UpstreamModelIDs)
}

func TestProjectInventoryRuntimeDimensionsUsesPricingForResolvedMixedAndForcedTargets(t *testing.T) {
	groupID, channelID := int64(588), int64(688)
	pricing := []InventoryChannelPricingCandidate{
		{Platform: PlatformAnthropic, Models: []string{"anthropic-priced"}},
		{Platform: PlatformGemini, Models: []string{"gemini-priced"}},
		{Platform: PlatformAntigravity, Models: []string{"native-priced"}},
	}
	channel := InventoryChannelCandidate{ID: channelID, BillingModelSource: BillingModelSourceRequested, RestrictModels: true, Pricing: pricing}
	accounts := []Account{
		{ID: 588, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
			"model_mapping": map[string]any{
				"anthropic-priced": "anthropic-priced",
				"gemini-priced":    "gemini-priced",
				"native-priced":    "native-priced",
			},
		}},
		{ID: 589, Platform: PlatformAntigravity},
	}
	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: accounts,
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: 588, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformAnthropic, Channel: &channel},
			{AccountID: 588, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformGemini, Channel: &channel},
			{AccountID: 588, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformAntigravity, ForcePlatform: true, Channel: &channel},
			{AccountID: 589, GroupID: &groupID, ChannelID: &channelID, TargetPlatform: PlatformAnthropic, Channel: &channel},
		},
	}, config.RunModeStandard)

	byTarget := make(map[string][]string)
	for _, projection := range got {
		byTarget[projection.TargetPlatform] = projection.UpstreamModelIDs
	}
	require.Equal(t, []string{"anthropic-priced"}, byTarget[PlatformAnthropic])
	require.Equal(t, []string{"gemini-priced"}, byTarget[PlatformGemini])
	require.Equal(t, []string{"native-priced"}, byTarget[PlatformAntigravity])
	require.Len(t, got, 3, "mixed-disabled account must not gain a target dimension from pricing")
}

func TestProjectInventoryRuntimeDimensionsInvalidBillingSourceUsesMappedRestrictionDefault(t *testing.T) {
	groupID, channelID := int64(590), int64(690)
	account := Account{ID: 590, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"mapped-public": "scheduler-support-only", "priced-key": "mapped-final", "upstream-public": "priced-key",
	}}}
	channel := InventoryChannelCandidate{
		ID: channelID, BillingModelSource: "legacy-invalid", RestrictModels: true,
		ModelMapping: map[string]map[string]string{PlatformOpenAI: {"mapped-public": "priced-key"}},
		Pricing:      []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"priced-key"}}},
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformOpenAI, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"mapped-final"}, got[0].UpstreamModelIDs)
	require.NotContains(t, got[0].UpstreamModelIDs, "priced-key")
	require.Equal(t, "legacy-invalid", channel.BillingModelSource, "projection must not normalize or mutate persisted legacy state")
}

func TestProjectInventoryRuntimeDimensionsInvalidBillingSourceUnrestrictedKeepsReachableFinals(t *testing.T) {
	groupID, channelID := int64(591), int64(691)
	account := Account{ID: 591, Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{
		"mapped-public": "scheduler-support-only", "priced-key": "mapped-final", "upstream-public": "priced-key",
	}}}
	channel := InventoryChannelCandidate{
		ID: channelID, BillingModelSource: "legacy-invalid", RestrictModels: false,
		ModelMapping: map[string]map[string]string{PlatformOpenAI: {"mapped-public": "priced-key"}},
		Pricing:      []InventoryChannelPricingCandidate{{Platform: PlatformOpenAI, Models: []string{"priced-key"}}},
	}

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformOpenAI, Channel: &channel,
		}},
	}, config.RunModeStandard)

	require.Equal(t, []string{"mapped-final", "priced-key"}, got[0].UpstreamModelIDs)
	require.Equal(t, "legacy-invalid", channel.BillingModelSource)
}

func TestProjectInventoryAccountMappingsMatchesAntigravityThinkingReachability(t *testing.T) {
	tests := []struct {
		name       string
		mapping    map[string]any
		requestKey string
	}{
		{
			name:       "base target alone cannot reach thinking target",
			mapping:    map[string]any{"custom": "claude-sonnet-4-5"},
			requestKey: "custom",
		},
		{
			name: "exact support key permits thinking target",
			mapping: map[string]any{
				"claude-sonnet-4-5":          "claude-sonnet-4-5",
				"claude-sonnet-4-5-thinking": "claude-sonnet-4-5",
			},
			requestKey: "claude-sonnet-4-5",
		},
		{
			name: "wildcard support key permits thinking target",
			mapping: map[string]any{
				"claude-sonnet-4-5": "claude-sonnet-4-5",
				"claude-*":          "claude-sonnet-4-5",
			},
			requestKey: "claude-sonnet-4-5",
		},
		{
			name:       "non-transform target remains deduplicated",
			mapping:    map[string]any{"custom": "upstream-custom"},
			requestKey: "custom",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{
				ID:          13,
				Platform:    PlatformAntigravity,
				Credentials: map[string]any{"model_mapping": test.mapping},
			}
			mapped := mapAntigravityModel(account, test.requestKey)
			expected := []string{mapped}
			transformed := applyThinkingModelSuffix(mapped, true)
			if transformed != mapped && account.IsModelSupported(transformed) {
				expected = append(expected, transformed)
			}

			projection := ProjectInventoryAccountMappings(account)

			for _, model := range expected {
				require.Contains(t, projection.UpstreamModelIDs, model)
			}
			if transformed != mapped && !account.IsModelSupported(transformed) {
				require.NotContains(t, projection.UpstreamModelIDs, transformed)
			}
			require.Equal(t, 1, countInventoryModel(projection.UpstreamModelIDs, mapped))
		})
	}
}

func TestProjectInventoryRuntimeDimensionsEmitsAntigravityThinkingOnlyForClaudeCompatiblePaths(t *testing.T) {
	groupID, channelID := int64(1500), int64(1501)
	mapping := map[string]any{
		"claude-sonnet-4-5":          "claude-sonnet-4-5",
		"claude-sonnet-4-5-thinking": "claude-sonnet-4-5",
	}
	channel := InventoryChannelCandidate{ID: channelID, ModelMapping: map[string]map[string]string{
		PlatformAntigravity: {"claude-sonnet-4-5": "claude-sonnet-4-5"},
		PlatformAnthropic:   {"claude-sonnet-4-5": "claude-sonnet-4-5"},
		PlatformGemini:      {"claude-sonnet-4-5": "claude-sonnet-4-5"},
	}}
	baseAccount := Account{Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true},
		Credentials: map[string]any{"model_mapping": mapping}}
	wantThinking := applyThinkingModelSuffix("claude-sonnet-4-5", true)
	require.True(t, baseAccount.IsModelSupported(wantThinking))

	tests := []struct {
		name         string
		account      Account
		candidate    InventoryRuntimeDimensionCandidate
		wantThinking bool
	}{
		{
			name: "channel backed native", account: baseAccount,
			candidate: InventoryRuntimeDimensionCandidate{GroupID: &groupID, ChannelID: &channelID,
				TargetPlatform: PlatformAntigravity, Channel: &channel}, wantThinking: true,
		},
		{
			name: "channel backed mixed anthropic", account: baseAccount,
			candidate: InventoryRuntimeDimensionCandidate{GroupID: &groupID, ChannelID: &channelID,
				TargetPlatform: PlatformAnthropic, Channel: &channel}, wantThinking: true,
		},
		{
			name: "channel backed forced native", account: baseAccount,
			candidate: InventoryRuntimeDimensionCandidate{GroupID: &groupID, ChannelID: &channelID,
				TargetPlatform: PlatformAntigravity, ForcePlatform: true, Channel: &channel}, wantThinking: true,
		},
		{
			name: "gemini native protocol excluded", account: baseAccount,
			candidate: InventoryRuntimeDimensionCandidate{GroupID: &groupID, ChannelID: &channelID,
				TargetPlatform: PlatformGemini, Channel: &channel},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.account.ID = int64(1502 + index)
			test.candidate.AccountID = test.account.ID
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{test.account}, Candidates: []InventoryRuntimeDimensionCandidate{test.candidate},
			}, config.RunModeStandard)
			require.Len(t, got, 1)
			require.Contains(t, got[0].UpstreamModelIDs, "claude-sonnet-4-5")
			require.Equal(t, test.wantThinking, slices.Contains(got[0].UpstreamModelIDs, wantThinking))
		})
	}

	for _, endpoint := range []string{CompositeRouteEndpointResponses, CompositeRouteEndpointAny} {
		t.Run("composite "+endpoint, func(t *testing.T) {
			account := baseAccount
			account.ID = 1510
			route := CompositeModelRoute{ID: 1511, GroupID: groupID, PublicModel: "public", MatchType: CompositeRouteMatchExact,
				TargetPlatform: PlatformAntigravity, UpstreamModel: "claude-sonnet-4-5", Endpoint: endpoint, Enabled: true}
			got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
				Accounts: []Account{account}, Candidates: []InventoryRuntimeDimensionCandidate{{
					AccountID: account.ID, GroupID: &groupID, ChannelID: &channelID, GroupPlatform: PlatformComposite,
					TargetPlatform: PlatformAntigravity, CompositeRoute: &route, Channel: &channel,
				}},
			}, config.RunModeStandard)
			require.Equal(t, []string{"claude-sonnet-4-5", wantThinking}, got[0].UpstreamModelIDs)
		})
	}

	unsupported := baseAccount
	unsupported.ID = 1512
	unsupported.Credentials = map[string]any{"model_mapping": map[string]any{"claude-sonnet-4-5": "claude-sonnet-4-5"}}
	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{unsupported}, Candidates: []InventoryRuntimeDimensionCandidate{{
			AccountID: unsupported.ID, GroupID: &groupID, ChannelID: &channelID,
			TargetPlatform: PlatformAntigravity, Channel: &channel,
		}},
	}, config.RunModeStandard)
	require.Contains(t, got[0].UpstreamModelIDs, "claude-sonnet-4-5")
	require.NotContains(t, got[0].UpstreamModelIDs, wantThinking)
	require.Equal(t,
		slices.Contains(ProjectInventoryAccountMappings(&unsupported).UpstreamModelIDs, wantThinking),
		slices.Contains(got[0].UpstreamModelIDs, wantThinking),
		"account-only and complete-chain thinking support gates remain aligned")
}

func TestProjectInventoryRuntimeDimensionsDoesNotUseAccountOnlyShortcutForGroupedAntigravityTargets(t *testing.T) {
	groupID := int64(1550)
	account := Account{ID: 1551, Platform: PlatformAntigravity, Extra: map[string]any{"mixed_scheduling": true}, Credentials: map[string]any{
		"model_mapping": map[string]any{
			"claude-sonnet-4-5":          "claude-sonnet-4-5",
			"claude-sonnet-4-5-thinking": "claude-sonnet-4-5",
		},
	}}
	thinking := applyThinkingModelSuffix("claude-sonnet-4-5", true)

	got := ProjectInventoryRuntimeDimensions(InventoryProjectionInput{
		Accounts: []Account{account},
		Candidates: []InventoryRuntimeDimensionCandidate{
			{AccountID: account.ID, GroupID: &groupID, TargetPlatform: PlatformGemini},
			{AccountID: account.ID, GroupID: &groupID, TargetPlatform: PlatformAnthropic},
			{AccountID: account.ID, GroupID: &groupID, TargetPlatform: PlatformAntigravity, ForcePlatform: true},
			{AccountID: account.ID, TargetPlatform: PlatformAntigravity},
		},
	}, config.RunModeStandard)

	byTarget := make(map[string][]string)
	var ungrouped []string
	for _, projection := range got {
		if projection.GroupID == nil {
			ungrouped = projection.UpstreamModelIDs
			continue
		}
		byTarget[projection.TargetPlatform] = projection.UpstreamModelIDs
	}
	require.NotContains(t, byTarget[PlatformGemini], thinking)
	require.Contains(t, byTarget[PlatformAnthropic], thinking)
	require.Contains(t, byTarget[PlatformAntigravity], thinking)
	require.Contains(t, ungrouped, thinking, "ungrouped finite endpoint union retains Claude-compatible thinking")
}

func TestProjectInventoryAccountMappingsResolvesBedrockDefaultsForAccountRegion(t *testing.T) {
	account := &Account{
		ID: 13, Platform: PlatformAnthropic, Type: AccountTypeBedrock,
		Credentials: map[string]any{"aws_region": "eu-west-1"},
	}

	projection := ProjectInventoryAccountMappings(account)

	require.Contains(t, projection.UpstreamModelIDs, "eu.anthropic.claude-sonnet-4-5-20250929-v1:0")
	require.NotContains(t, projection.UpstreamModelIDs, "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
}

func TestProjectInventoryAccountMappingsMatchesOAuthCompactForwardModel(t *testing.T) {
	account := &Account{
		ID:       14,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"compact_model_mapping": map[string]any{"public": " openai/gpt-5.4-high "},
		},
		Extra: map[string]any{"openai_compact_supported": true},
	}

	effectiveModel := resolveOpenAICompactForwardModel(account, "public")
	projection := ProjectInventoryAccountMappings(account)

	require.Equal(t, "openai/gpt-5.4-high", effectiveModel)
	require.Equal(t, []string{effectiveModel}, projection.UpstreamModelIDs)
	require.NotContains(t, projection.UpstreamModelIDs, "gpt-5.4")
}

func TestProjectInventoryAccountMappingsMatchesSequentialOpenAICompactReachability(t *testing.T) {
	tests := []struct {
		name       string
		account    Account
		witnesses  map[string]string
		want       []string
		notPresent []string
	}{
		{
			name: "normal mapping shadows same requested compact key",
			account: Account{ID: 15, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "normal-upstream"},
				"compact_model_mapping": map[string]any{"public": "compact-only"},
			}},
			witnesses:  map[string]string{"public": "normal-upstream"},
			want:       []string{"normal-upstream"},
			notPresent: []string{"compact-only"},
		},
		{
			name: "normal target reaches compact mapping key",
			account: Account{ID: 16, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "compact-key"},
				"compact_model_mapping": map[string]any{"compact-key": "compact-target"},
			}},
			witnesses: map[string]string{"public": "compact-target"},
			want:      []string{"compact-key", "compact-target"},
		},
		{
			name: "compact lookup trims actual normal mapping output",
			account: Account{ID: 17, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": " compact-key "},
				"compact_model_mapping": map[string]any{"compact-key": " compact-target "},
			}},
			witnesses: map[string]string{"public": "compact-target"},
			want:      []string{"compact-key", "compact-target"},
		},
		{
			name: "normal target matches wildcard compact rule",
			account: Account{ID: 18, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "gpt-5.4-mini"},
				"compact_model_mapping": map[string]any{"gpt-*": "wildcard-target"},
			}},
			witnesses: map[string]string{"public": "wildcard-target"},
			want:      []string{"gpt-5.4-mini", "wildcard-target"},
		},
		{
			name: "normal target does not match wildcard compact rule",
			account: Account{ID: 19, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "other-model"},
				"compact_model_mapping": map[string]any{"gpt-*": "unreachable-target"},
			}},
			witnesses:  map[string]string{"public": "other-model"},
			want:       []string{"other-model"},
			notPresent: []string{"unreachable-target"},
		},
		{
			name: "exact compact rule wins over wildcard",
			account: Account{ID: 20, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "gpt-5.4-mini"},
				"compact_model_mapping": map[string]any{
					"gpt-*":        "wildcard-loser",
					"gpt-5.4-mini": "exact-winner",
				},
			}},
			witnesses:  map[string]string{"public": "exact-winner"},
			want:       []string{"exact-winner", "gpt-5.4-mini"},
			notPresent: []string{"wildcard-loser"},
		},
		{
			name: "longest compact wildcard wins",
			account: Account{ID: 21, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"public": "gpt-5.4-mini"},
				"compact_model_mapping": map[string]any{
					"gpt-*":    "short-loser",
					"gpt-5.4*": "long-winner",
				},
			}},
			witnesses:  map[string]string{"public": "long-winner"},
			want:       []string{"gpt-5.4-mini", "long-winner"},
			notPresent: []string{"short-loser"},
		},
		{
			name: "compact-only account reaches compact target",
			account: Account{ID: 22, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"public": "compact-target"},
			}},
			witnesses: map[string]string{"public": "compact-target"},
			want:      []string{"compact-target"},
		},
		{
			name: "compact-only wildcard key has reachable concrete target",
			account: Account{ID: 23, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-*": " wildcard-only-target "},
			}},
			witnesses: map[string]string{"gpt-anything": "wildcard-only-target"},
			want:      []string{"wildcard-only-target"},
		},
		{
			name: "oauth compact fallback normalizes normal mapping target",
			account: Account{ID: 24, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "openai/gpt-5.4-high"},
				"compact_model_mapping": map[string]any{"public": "compact-only"},
			}},
			witnesses:  map[string]string{"public": "gpt-5.4"},
			want:       []string{"gpt-5.4"},
			notPresent: []string{"compact-only", "openai/gpt-5.4-high"},
		},
		{
			name: "oauth compact match preserves explicit compact target",
			account: Account{ID: 25, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"public": "openai/gpt-5.4-high"},
			}},
			witnesses: map[string]string{"public": "openai/gpt-5.4-high"},
			want:      []string{"openai/gpt-5.4-high"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for requested, want := range test.witnesses {
				normalMapped, _ := test.account.ResolveMappedModel(requested)
				compactMapped := resolveOpenAICompactForwardModel(&test.account, normalMapped)
				if compactMapped == normalMapped {
					compactMapped = normalizeOpenAIModelForUpstream(&test.account, normalMapped)
				}
				require.Equal(t, want, compactMapped, "runtime witness %q", requested)
			}

			projection := ProjectInventoryAccountMappings(&test.account)

			require.Equal(t, test.want, projection.UpstreamModelIDs)
			for _, unreachable := range test.notPresent {
				require.NotContains(t, projection.UpstreamModelIDs, unreachable)
			}
		})
	}
}

func TestProjectInventoryAccountMappingsMatchesOpenAIPassthroughReachability(t *testing.T) {
	tests := []struct {
		name       string
		account    Account
		witnesses  map[string][2]string
		want       []string
		notPresent []string
	}{
		{
			name: "api key new flag resolves normal and compact from original request",
			account: Account{ID: 26, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "normal-upstream"},
				"compact_model_mapping": map[string]any{"public": "compact-upstream"},
			}, Extra: map[string]any{"openai_passthrough": true}},
			witnesses:  map[string][2]string{"public": {"public", "compact-upstream"}},
			want:       []string{"compact-upstream", "public"},
			notPresent: []string{"normal-upstream"},
		},
		{
			name: "oauth legacy flag preserves compact target without ordinary normalization",
			account: Account{ID: 27, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
				"model_mapping":         map[string]any{"public": "normal-upstream"},
				"compact_model_mapping": map[string]any{"public": "openai/gpt-5.4-high"},
			}, Extra: map[string]any{"openai_oauth_passthrough": true}},
			witnesses:  map[string][2]string{"public": {"public", "openai/gpt-5.4-high"}},
			want:       []string{"openai/gpt-5.4-high", "public"},
			notPresent: []string{"gpt-5.4", "normal-upstream"},
		},
		{
			name: "passthrough allow all remains finite but exposes concrete wildcard compact targets",
			account: Account{ID: 28, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
				"compact_model_mapping": map[string]any{"gpt-*": "wildcard-compact", "*": "fallback-compact"},
			}, Extra: map[string]any{"openai_passthrough": true}},
			witnesses: map[string][2]string{
				"gpt-any": {"gpt-any", "wildcard-compact"},
				"other":   {"other", "fallback-compact"},
			},
			want:       []string{"fallback-compact", "wildcard-compact"},
			notPresent: []string{"*", "gpt-*", "gpt-any", "other"},
		},
		{
			name: "finite exact request witnesses preserve compact exact precedence",
			account: Account{ID: 29, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"gpt-5.4": "normal-upstream"},
				"compact_model_mapping": map[string]any{
					"gpt-*":   "wildcard-loser",
					"gpt-5.4": "exact-winner",
				},
			}, Extra: map[string]any{"openai_passthrough": true}},
			witnesses:  map[string][2]string{"gpt-5.4": {"gpt-5.4", "exact-winner"}},
			want:       []string{"exact-winner", "gpt-5.4"},
			notPresent: []string{"normal-upstream", "wildcard-loser"},
		},
		{
			name: "wildcard request domain exposes longest and fallback compact targets",
			account: Account{ID: 30, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"model_mapping": map[string]any{"gpt-*": "normal-upstream"},
				"compact_model_mapping": map[string]any{
					"gpt-*":    "short-target",
					"gpt-5.4*": "long-target",
				},
			}, Extra: map[string]any{"openai_passthrough": true}},
			witnesses: map[string][2]string{
				"gpt-other": {"gpt-other", "short-target"},
				"gpt-5.4x":  {"gpt-5.4x", "long-target"},
			},
			want:       []string{"long-target", "short-target"},
			notPresent: []string{"gpt-*", "normal-upstream"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for requested, want := range test.witnesses {
				normal := resolveOpenAIForwardModelForEndpoint(&test.account, requested, "", false)
				compact := resolveOpenAIForwardModelForEndpoint(&test.account, requested, "", true)
				require.Equal(t, want[0], normal.UpstreamModel, "normal runtime witness %q", requested)
				require.Equal(t, want[1], compact.UpstreamModel, "compact runtime witness %q", requested)
			}

			projection := ProjectInventoryAccountMappings(&test.account)

			require.Equal(t, test.want, projection.UpstreamModelIDs)
			for _, unreachable := range test.notPresent {
				require.NotContains(t, projection.UpstreamModelIDs, unreachable)
			}
		})
	}
}

func TestProjectInventoryAccountMappingsPassthroughRespectsNormalMappingSupportDomain(t *testing.T) {
	account := &Account{
		ID:       37,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping": map[string]any{"public": "normal-upstream"},
			"compact_model_mapping": map[string]any{
				"other": "compact-other",
				"gpt-*": "compact-gpt",
			},
		},
		Extra: map[string]any{"openai_passthrough": true},
	}

	projection := ProjectInventoryAccountMappings(account)

	require.Equal(t, []string{"public"}, projection.UpstreamModelIDs)
	require.NotContains(t, projection.UpstreamModelIDs, "compact-gpt")
	require.NotContains(t, projection.UpstreamModelIDs, "compact-other")
	require.NotContains(t, projection.UpstreamModelIDs, "other")
	require.NotContains(t, projection.UpstreamModelIDs, "normal-upstream")
}

func TestProjectInventoryAccountMappingsPassthroughCompactEligibilityMatrix(t *testing.T) {
	tests := []struct {
		name  string
		extra map[string]any
		want  []string
	}{
		{name: "unknown compact support cannot expand requested domain", extra: map[string]any{"openai_passthrough": true}, want: []string{"public"}},
		{name: "known compact support cannot expand requested domain", extra: map[string]any{"openai_passthrough": true, "openai_compact_supported": true}, want: []string{"public"}},
		{name: "unsupported compact remains in normal domain", extra: map[string]any{"openai_passthrough": true, "openai_compact_supported": false}, want: []string{"public"}},
		{name: "force on cannot expand requested domain", extra: map[string]any{"openai_passthrough": true, "openai_compact_mode": OpenAICompactModeForceOn, "openai_compact_supported": false}, want: []string{"public"}},
		{name: "force off remains in normal domain", extra: map[string]any{"openai_passthrough": true, "openai_compact_mode": OpenAICompactModeForceOff, "openai_compact_supported": true}, want: []string{"public"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{
				ID:       38,
				Platform: PlatformOpenAI,
				Type:     AccountTypeAPIKey,
				Credentials: map[string]any{
					"model_mapping":         map[string]any{"public": "normal-upstream"},
					"compact_model_mapping": map[string]any{"other": "compact-other"},
				},
				Extra: test.extra,
			}

			require.Equal(t, test.want, ProjectInventoryAccountMappings(account).UpstreamModelIDs)
		})
	}
}

func TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet(t *testing.T) {
	compactMapping := map[string]any{
		"gpt-*": "reachable-short-target",
		"gpt-a": "exact-base-target",
	}
	for value := 0; value < 300; value++ {
		compactMapping[fmt.Sprintf("gpt-a%03d*", value)] = fmt.Sprintf("longer-target-%03d", value)
	}
	account := &Account{
		ID:       31,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping":         map[string]any{"gpt-a*": "normal-upstream"},
			"compact_model_mapping": compactMapping,
		},
		Extra: map[string]any{"openai_passthrough": true},
	}

	witnesses, comparisons := openAIPassthroughCompactWitnessesWithConstructionComparisons(account)
	projection := ProjectInventoryAccountMappings(account)

	require.LessOrEqual(t, len(witnesses), len(compactMapping))
	ruleCount := len(account.GetModelMapping()) + len(account.GetCompactModelMapping())
	require.LessOrEqual(t, comparisons, 4*ruleCount*ruleCount, "witness construction comparisons must remain quadratic")
	for _, witness := range witnesses {
		require.NotEmpty(t, witness)
		for _, r := range witness {
			require.False(t, unicode.IsControl(r), "witness %q contains control rune", witness)
			require.True(t, unicode.IsPrint(r), "witness %q contains non-printable rune", witness)
		}
	}
	require.Contains(t, projection.UpstreamModelIDs, "reachable-short-target")
	require.Contains(t, projection.UpstreamModelIDs, "exact-base-target")
	require.Contains(t, projection.UpstreamModelIDs, "longer-target-000")
	require.Contains(t, projection.UpstreamModelIDs, "longer-target-299")
	require.NotContains(t, projection.UpstreamModelIDs, "normal-upstream")
}

func TestOpenAIPassthroughCompactWitnessesRejectMalformedRules(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping": map[string]any{"valid-*": "normal", "bad\t*": "normal", "middle*star": "normal"},
			"compact_model_mapping": map[string]any{
				"valid-*":     "compact-valid",
				"bad\t*":      "compact-control",
				"middle*star": "compact-middle",
				"empty":       "   ",
				"wild-target": "bad-*",
				"non-string":  42,
			},
		},
		Extra: map[string]any{"openai_passthrough": true},
	}

	witnesses := openAIPassthroughCompactWitnesses(account)
	projection := ProjectInventoryAccountMappings(account)

	require.Equal(t, []string{"valid-"}, witnesses)
	require.Equal(t, []string{"compact-valid"}, projection.UpstreamModelIDs)
}

func TestOpenAIProjectionPrintableRejectsInvalidUTF8(t *testing.T) {
	invalid := string([]byte{'g', 'p', 't', '-', 0xff})
	require.False(t, openAIProjectionPrintable(invalid))

	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping":         map[string]any{invalid: "normal"},
			"compact_model_mapping": map[string]any{invalid: "compact", "valid": invalid},
		},
		Extra: map[string]any{"openai_passthrough": true},
	}
	require.Nil(t, ProjectInventoryAccountMappings(account).UpstreamModelIDs)
}

func TestProjectInventoryAccountMappingsPassthroughRejectsPaddedConfiguredKeys(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping": map[string]any{
				" normal-padded ": "normal-target",
				"normal-valid":    "ignored-target",
				"compact-valid":   "ignored-compact-target",
				"valid-*":         "ignored-wildcard-target",
			},
			"compact_model_mapping": map[string]any{
				" compact-padded ": "padded-exact-target",
				" padded-*":        "padded-wildcard-target",
				"compact-valid":    "valid-exact-target",
				"valid-*":          "valid-wildcard-target",
			},
		},
		Extra: map[string]any{"openai_passthrough": true},
	}

	projection := ProjectInventoryAccountMappings(account)

	require.Equal(t, []string{
		"compact-valid",
		"normal-valid",
		"valid-exact-target",
		"valid-wildcard-target",
	}, projection.UpstreamModelIDs)
	for _, unreachable := range []string{
		" normal-padded ",
		"normal-padded",
		" compact-padded ",
		"compact-padded",
		"padded-exact-target",
		" padded-*",
		"padded-*",
		"padded-",
		"padded-wildcard-target",
	} {
		require.NotContains(t, projection.UpstreamModelIDs, unreachable)
	}
}

func TestOpenAIPassthroughCompactWitnessesExactBlockerDoesNotReserveBranch(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"compact_model_mapping": map[string]any{
				"gpt-":  "exact-base-target",
				"gpt-!": "exact-target",
				"gpt-*": "wildcard-target",
			},
		},
		Extra: map[string]any{"openai_passthrough": true},
	}

	require.Equal(t, []string{"gpt-", "gpt-!", "gpt-!!"}, openAIPassthroughCompactWitnesses(account))
}

func TestProjectInventoryAccountMappingsPassthroughCompactDisabled(t *testing.T) {
	account := &Account{
		ID:       32,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"model_mapping":         map[string]any{"public": "normal-upstream"},
			"compact_model_mapping": map[string]any{"public": "compact-upstream"},
		},
		Extra: map[string]any{
			"openai_passthrough":  true,
			"openai_compact_mode": OpenAICompactModeForceOff,
		},
	}

	require.Equal(t, []string{"public"}, ProjectInventoryAccountMappings(account).UpstreamModelIDs)
	require.Nil(t, openAIPassthroughCompactWitnesses(account))
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

func countInventoryModel(models []string, target string) int {
	count := 0
	for _, model := range models {
		if model == target {
			count++
		}
	}
	return count
}
