//go:build media_integration

package service

import (
	"context"
	"errors"
)

// These probes expose the existing recovery operations to the external isolated
// fixture. They are excluded from release and rollback binaries.
func CleanupFundingTerminalForIntegration(ctx context.Context, bridge *CanonicalWalletBridge, user, lease string) error {
	return bridge.cleanupFundingTerminal(ctx, user, lease)
}
func RecoverFundingTerminalForIntegration(ctx context.Context, bridge *CanonicalWalletBridge, user, lease string) error {
	return bridge.recoverFundingTerminal(ctx, user, lease)
}
func ReplayFundingReturnForIntegration(ctx context.Context, bridge *CanonicalWalletBridge, user, lease string) error {
	return bridge.replayPendingFundingReturn(ctx, user, lease)
}

func PrepareFundingReturnForIntegration(ctx context.Context, bridge *CanonicalWalletBridge, user, lease string) error {
	store, ok := bridge.store.(CanonicalWalletFundingStore)
	if !ok {
		return errors.New("funding test store unavailable")
	}
	return bridge.prepareFundingReturn(ctx, user, lease, "partial", store)
}
func LockFundingReturnForIntegration(ctx context.Context, bridge *CanonicalWalletBridge, user, lease string) (func() error, error) {
	_, release, err := bridge.lockFundingReturnDispatch(ctx, user, lease)
	return release, err
}
