package service

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// FreezeBillingSnapshot is the post-selection freeze point (Phase 3.2).
//
// gated=true: this site REPLACED an EnsureModelPricing that refused today —
// it keeps refusing exactly as that did, same error, in every mode.
// gated=false: no refusal existed at this site today — in off/record mode
// NOTHING here may refuse (global constraint 4: every error response is
// byte-for-byte what it is today); a failed freeze is counted and yields
// (nil, nil). Only settle mode refuses on a freeze failure, because a
// snapshot is mandatory to settle.
func (s *GatewayService) FreezeBillingSnapshot(ctx context.Context, apiKey *APIKey, account *Account, subscription *UserSubscription, requestedModel, billingModel string, gated bool, longContextThreshold int, longContextMultiplier float64) (*BillingSnapshot, error) {
	if gated {
		if err := s.EnsureModelPricing(ctx, apiKey, billingModel); err != nil {
			return nil, err
		}
	}
	if s.snapshots == nil || s.snapshots.Mode() == BillingSnapshotModeOff {
		return nil, nil
	}
	var user *User
	if apiKey != nil {
		user = apiKey.User
	}
	override := cacheTTLOverride{}
	if target, ok := s.resolveCacheTTLUsageOverrideTarget(ctx, account); ok {
		override = cacheTTLOverride{Enabled: true, Target: target}
	}
	snap, err := s.snapshots.Freeze(ctx, FreezeInput{
		APIKey: apiKey, User: user, Account: account, Subscription: subscription,
		RequestedModel: requestedModel, BillingModel: billingModel, Family: BillingFamilyGeneric,
		ResolveUserGroupRate: s.ResolveUserGroupRateMultiplier, CacheTTLOverride: override,
		LongContextThreshold: longContextThreshold, LongContextMultiplier: longContextMultiplier,
	})
	return s.snapshots.freezeOutcome(snap, err, billingModel)
}

func (s *OpenAIGatewayService) FreezeBillingSnapshot(ctx context.Context, apiKey *APIKey, account *Account, subscription *UserSubscription, requestedModel, billingModel string, gated bool) (*BillingSnapshot, error) {
	if gated {
		if err := s.EnsureModelPricing(ctx, apiKey, billingModel); err != nil {
			return nil, err
		}
	}
	if s.snapshots == nil || s.snapshots.Mode() == BillingSnapshotModeOff {
		return nil, nil
	}
	var user *User
	if apiKey != nil {
		user = apiKey.User
	}
	// RecordUsage reads the long-context flag from the credential account behind
	// a shadow (openai_gateway_usage.go:205-212); freeze from the same account.
	billingAccount := account
	if account != nil && account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			return s.snapshots.freezeOutcome(nil, err, billingModel)
		}
		billingAccount = resolved
	}
	snap, err := s.snapshots.Freeze(ctx, FreezeInput{
		APIKey: apiKey, User: user, Account: account, BillingAccount: billingAccount, Subscription: subscription,
		RequestedModel: requestedModel, BillingModel: billingModel, Family: BillingFamilyOpenAI,
		ResolveUserGroupRate: s.ResolveUserGroupRateMultiplier, RefreshGroupMediaPricing: s.apiKeyWithFreshGroupMediaPricing,
	})
	return s.snapshots.freezeOutcome(snap, err, billingModel)
}

// freezeOutcome applies the mode rule to a Freeze result.
func (s *BillingSnapshotService) freezeOutcome(snap *BillingSnapshot, err error, billingModel string) (*BillingSnapshot, error) {
	if err == nil {
		return snap, nil
	}
	if s.Mode() == BillingSnapshotModeSettle {
		return nil, err
	}
	billingSnapshotMetrics.freezeError.Add(1)
	logger.LegacyPrintf("service.billing_snapshot", "freeze failed in %s mode for model %q (settling live): %v", s.Mode(), billingModel, err)
	return nil, nil
}
