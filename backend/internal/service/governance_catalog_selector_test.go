package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Governance filtering tests for Task 3: every candidate and catalog path enforces one eligibility projection.

func TestModelCatalog_GovernanceFiltersInEnforceMode(t *testing.T) {
	cfg := &config.Config{}
	cfg.ModelGovernance.AuthorizationMode = "enforce"
	fakeStore := &fakePublicationStore{
		channelEligibleFn: func(ctx context.Context, channelID int64, canonical string) (bool, error) {
			// Only claude models eligible; gpt/gemini blocked.
			if canonical == "claude-opus-4-6" {
				return true, nil
			}
			return false, nil
		},
	}
	svc := NewModelCatalogService(nil, nil, nil, nil, nil)
	svc.SetConfig(cfg)
	svc.SetPublicationStore(fakeStore)

	require.True(t, svc.isModelEligibleForChannel(context.Background(), 1, "claude-opus-4-6"))
	require.False(t, svc.isModelEligibleForChannel(context.Background(), 1, "gpt-4o"))
	require.False(t, svc.isModelEligibleForChannel(context.Background(), 1, "gemini-2.5-pro"))
	require.False(t, svc.isModelEligibleForChannel(context.Background(), 1, "unknown-model-xyz"))
	// Wildcard and non-text should also be blocked (they never have eligible rows)
	require.False(t, svc.isModelEligibleForChannel(context.Background(), 1, "claude-*"))
	require.False(t, svc.isModelEligibleForChannel(context.Background(), 1, "dall-e-3"))
}

func TestModelCatalog_GovernanceAllowsWhenNotEnforce(t *testing.T) {
	cfg := &config.Config{}
	cfg.ModelGovernance.AuthorizationMode = "shadow"
	fakeStore := &fakePublicationStore{
		channelEligibleFn: func(ctx context.Context, channelID int64, canonical string) (bool, error) {
			return false, nil
		},
	}
	svc := NewModelCatalogService(nil, nil, nil, nil, nil)
	svc.SetConfig(cfg)
	svc.SetPublicationStore(fakeStore)
	// shadow mode must not filter
	require.True(t, svc.isModelEligibleForChannel(context.Background(), 1, "gpt-4o"))
	// off mode also not filter
	cfg.ModelGovernance.AuthorizationMode = "off"
	require.True(t, svc.isModelEligibleForChannel(context.Background(), 1, "gpt-4o"))
	// nil store also allows (fail open before wiring)
	svc2 := NewModelCatalogService(nil, nil, nil, nil, nil)
	svc2.SetConfig(cfg)
	require.True(t, svc2.isModelEligibleForChannel(context.Background(), 1, "gpt-4o"))
}

func TestChannelRoutingSelector_GovernanceFiltersCandidates(t *testing.T) {
	cfg := &config.Config{}
	cfg.ModelGovernance.AuthorizationMode = "enforce"
	cfg.Gateway.ChannelRoutingEnabled = true
	fakeStore := &fakePublicationStore{
		channelEligibleFn: func(ctx context.Context, channelID int64, canonical string) (bool, error) {
			// Only anthropic channel 10 allows claude, not gpt
			if channelID == 10 && canonical == "claude-opus-4-6" {
				return true, nil
			}
			if channelID == 20 && canonical == "gpt-4o" {
				return true, nil
			}
			return false, nil
		},
	}
	selector := NewChannelRoutingSelector(nil, nil, cfg)
	selector.SetPublicationStore(fakeStore)
	require.True(t, selector.isModelEligibleForChannel(context.Background(), 10, "claude-opus-4-6"))
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 10, "gpt-4o"))
	require.True(t, selector.isModelEligibleForChannel(context.Background(), 20, "gpt-4o"))
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 20, "claude-opus-4-6"))

	// When not enforce, all models pass
	cfg.ModelGovernance.AuthorizationMode = "shadow"
	require.True(t, selector.isModelEligibleForChannel(context.Background(), 10, "gpt-4o"))

	// Unknown and cross-provider models fail closed in enforce mode
	cfg.ModelGovernance.AuthorizationMode = "enforce"
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 10, "unknown-model-xyz"))
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 10, "wildcard-*"))
}

func TestChannelRoutingSelector_GovernanceHandlesAnthropicObservingGPT(t *testing.T) {
	// Anthropic account discovering GPT/Gemini should be blocked: GPT not eligible for anthropic channel.
	cfg := &config.Config{}
	cfg.ModelGovernance.AuthorizationMode = "enforce"
	cfg.Gateway.ChannelRoutingEnabled = true
	fakeStore := &fakePublicationStore{
		channelEligibleFn: func(ctx context.Context, channelID int64, canonical string) (bool, error) {
			// Channel 1 is anthropic, only claude eligible
			if channelID == 1 {
				if canonical == "claude-opus-4-6" {
					return true, nil
				}
				return false, nil
			}
			return false, nil
		},
	}
	selector := NewChannelRoutingSelector(nil, nil, cfg)
	selector.SetPublicationStore(fakeStore)
	// Simulate anthropic channel candidate for gpt
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 1, "gpt-4o"))
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 1, "gemini-2.5-pro"))
	require.True(t, selector.isModelEligibleForChannel(context.Background(), 1, "claude-opus-4-6"))

	// Also test missing price / ambiguous mapping scenario: if no eligible row, blocked.
	require.False(t, selector.isModelEligibleForChannel(context.Background(), 1, "claude-opus-4-6-missing-price"))
}
