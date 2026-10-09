package repository

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// These are Redis primitive tests. The service layer verifies the D1 signature
// before invoking Apply; the external funding suite proves that real boundary.
func fundingRedisFixture(t *testing.T) (*redis.Client, service.CanonicalWalletLeaseStore, service.CanonicalWalletFundingStore, service.CanonicalWalletLease) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewGatewayCache(client)
	wallet, ok := cache.(service.CanonicalWalletLeaseStore)
	require.True(t, ok)
	lease := service.CanonicalWalletLease{LeaseID: "funding-lease", PlatformUserID: "funding-user", Currency: "USD", BudgetUnits: 1000, FundedUnits: 1000, FundingScope: "legacy", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), lease))
	funding, ok := cache.(service.CanonicalWalletFundingStore)
	require.True(t, ok)
	return client, wallet, funding, lease
}

func fundingRedisACK(lease service.CanonicalWalletLease, after, revision int64, mode string) service.WalletFundingReturnReceipt {
	return service.WalletFundingReturnReceipt{PlatformUserID: lease.PlatformUserID, LeaseID: lease.LeaseID, FundedUnits: strconv.FormatInt(lease.FundingPrincipalUnits(), 10), ReturnedBeforeUnits: strconv.FormatInt(lease.ReturnedUnits, 10), ReturnedAfterUnits: strconv.FormatInt(after, 10), ReturnRevision: revision, Mode: mode, Signature: fmt.Sprintf("trusted-service-ack-unit-fixture-%d", revision)}
}

func TestCanonicalWalletFundingPreservesHoldsAndExactConversion(t *testing.T) {
	client, wallet, funding, lease := fundingRedisFixture(t)
	ctx := context.Background()
	_, _, _, err := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "old-held", 100, 60000, time.Now())
	require.NoError(t, err)
	basis, err := funding.FreezeCanonicalWalletFunding(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(100), basis.Lease.ConsumedUnits)
	require.Zero(t, basis.Lease.ReleasedUnits)
	require.Len(t, basis.Holds, 1)
	require.Equal(t, "old-held", basis.Holds[0].AuthorizationID)
	require.Equal(t, int64(100), basis.Holds[0].HeldUnits)
	require.Equal(t, int64(-1), client.TTL(ctx, canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID)).Val().Nanoseconds(), "frozen obligations persist")
	_, err = wallet.GetCanonicalWalletLease(ctx, lease.PlatformUserID)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing)
	ack := fundingRedisACK(lease, 900, 1, "partial")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, ack))
	snapshot, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(100), snapshot.BudgetUnits)
	require.Equal(t, int64(100), snapshot.ConsumedUnits)
	require.Zero(t, snapshot.ReleasedUnits)
	require.True(t, snapshot.FundingFrozen)
	_, _, _, err = wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "new-hold", 1, 0, time.Now())
	require.Error(t, err)
	_, err = wallet.ReserveCanonicalWalletLease(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "new-event", 1, time.Now())
	require.Error(t, err)
	_, _, duplicate, err := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "old-held", 100, 60000, time.Now())
	require.NoError(t, err)
	require.True(t, duplicate)
	conversion, err := wallet.ConvertCanonicalWalletHold(ctx, lease.PlatformUserID, "old-held", "captured-event", 80, time.Now())
	require.NoError(t, err)
	require.Equal(t, 0, conversion.Code)
	snapshot, err = wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(100), snapshot.ConsumedUnits)
	require.Equal(t, int64(20), snapshot.ReleasedUnits)
	closeACK := fundingRedisACK(*snapshot, 920, 2, "close")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, closeACK))
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, closeACK), "exact replay does not return twice")
	closed, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(80), closed.BudgetUnits)
	require.Equal(t, int64(100), closed.ConsumedUnits)
	require.Equal(t, int64(20), closed.ReleasedUnits)
	require.True(t, closed.Sealed)
	require.Error(t, wallet.InstallCanonicalWalletLease(ctx, lease))
	closeACK.Signature = "different-receipt"
	require.Error(t, funding.ApplyCanonicalWalletFundingReturn(ctx, closeACK))
}

func TestCanonicalWalletFundingTombstoneSurvivesLeaseLossAndACKReplay(t *testing.T) {
	client, wallet, funding, lease := fundingRedisFixture(t)
	ctx := context.Background()
	_, _, _, err := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "old-held", 100, 0, time.Now())
	require.NoError(t, err)
	_, err = funding.FreezeCanonicalWalletFunding(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	ack := fundingRedisACK(lease, 900, 1, "partial")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, ack))
	snapshot, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.NoError(t, client.Del(ctx, canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID)).Err())
	require.Error(t, wallet.InstallCanonicalWalletLease(ctx, lease), "retained tombstone rejects stale snapshot")
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, *snapshot))
	require.NoError(t, client.Del(ctx, canonicalWalletFundingTombstoneKey(lease.PlatformUserID, lease.LeaseID)).Err())
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, *snapshot), "confirmed frozen metadata may rebuild a tombstone before its signature")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, ack), "verified same-revision ACK attaches its original signature without increasing backing")
	_, err = wallet.ReleaseCanonicalWalletHold(ctx, lease.PlatformUserID, "old-held", "released", "abandoned")
	require.NoError(t, err)
	snapshot, err = wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	latest := fundingRedisACK(*snapshot, 1000, 2, "close")
	require.NoError(t, client.Del(ctx, canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), canonicalWalletFundingTombstoneKey(lease.PlatformUserID, lease.LeaseID)).Err())
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, latest), "a verified latest primary ACK rebuilds a lost tombstone")
	_, err = wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.ErrorIs(t, err, service.ErrCanonicalWalletLeaseMissing, "receipt replay does not fabricate a lease or free balance")
	require.Error(t, wallet.InstallCanonicalWalletLease(ctx, lease))
	require.Error(t, funding.ApplyCanonicalWalletFundingReturn(ctx, ack), "old signed revision cannot roll the tombstone back")
}

func TestCanonicalWalletFundingFreezeRacesArmAtomically(t *testing.T) {
	for i := 0; i < 24; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, wallet, funding, lease := fundingRedisFixture(t)
			ctx := context.Background()
			_, _, _, err := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "old-held", 100, 0, time.Now())
			require.NoError(t, err)
			start := make(chan struct{})
			armed := make(chan error, 1)
			go func() {
				<-start
				_, _, _, e := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "racing-held", 200, 0, time.Now())
				armed <- e
			}()
			close(start)
			basis, err := funding.FreezeCanonicalWalletFunding(ctx, lease.PlatformUserID, lease.LeaseID)
			require.NoError(t, err)
			armErr := <-armed
			held := int64(100)
			if armErr == nil {
				held = 300
				require.Len(t, basis.Holds, 2)
			} else {
				require.Len(t, basis.Holds, 1)
			}
			require.Equal(t, held, basis.Lease.ConsumedUnits)
			require.Zero(t, basis.Lease.ReleasedUnits)
			ack := fundingRedisACK(basis.Lease, 1000-held, 1, "partial")
			require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, ack))
			after, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
			require.NoError(t, err)
			require.Equal(t, held, after.BudgetUnits)
			require.Equal(t, held, after.ConsumedUnits)
			require.Zero(t, after.ReleasedUnits)
			_, _, _, err = wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "post-freeze-held", 1, 0, time.Now())
			require.Error(t, err)
		})
	}
}

func TestCanonicalWalletFundingIdentityCannotChangeOrBecomeCurrent(t *testing.T) {
	_, wallet, _, lease := fundingRedisFixture(t)
	ctx := context.Background()
	media := lease
	media.LeaseID = "task-owned-lease"
	media.FundingScope, media.FundingOwnerID, media.FundingIssuanceKey = "media", "task-a", "task-a.funding.0"
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, media))
	current, err := wallet.GetCanonicalWalletLease(ctx, lease.PlatformUserID)
	require.NoError(t, err)
	require.Equal(t, lease.LeaseID, current.LeaseID)
	media.FundingOwnerID = "task-b"
	require.Error(t, wallet.InstallCanonicalWalletLease(ctx, media))
	media.FundingOwnerID, media.FundingScope = "task-a", "llm"
	require.Error(t, wallet.InstallCanonicalWalletLease(ctx, media))
}

func TestCanonicalWalletFundingOlderTopupSnapshotKeepsPrincipalAndRevision(t *testing.T) {
	_, wallet, funding, original := fundingRedisFixture(t)
	ctx := context.Background()
	topped := original
	topped.BudgetUnits, topped.FundedUnits, topped.BudgetRevision = 1500, 1500, 1
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, topped))
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, original), "an older control response may arrive after the top-up")
	current, err := wallet.GetCanonicalWalletLeaseByID(ctx, original.PlatformUserID, original.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(1500), current.BudgetUnits)
	require.Equal(t, int64(1500), current.FundedUnits)
	require.Equal(t, int64(1), current.BudgetRevision, "the newer principal must retain its corresponding signed D1 version")
	basis, err := funding.FreezeCanonicalWalletFunding(ctx, original.PlatformUserID, original.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(1500), basis.Lease.FundingPrincipalUnits())
	require.Equal(t, int64(1), basis.Lease.BudgetRevision)
}

// A second worker can install the lease from a D1 view that already carries the
// next revision after its primary-PG ACK but before the first worker applied
// that revision to Redis. The tombstone then moves forward without the new
// receipt; the previous revision's signature must not stay behind, or the
// receipt of this very revision is rejected as a conflicting signature on every
// replay and the lease can never return or close again.
func TestCanonicalWalletFundingInstallBetweenPGACKAndApplyDoesNotWedgeTheReceipt(t *testing.T) {
	_, wallet, funding, lease := fundingRedisFixture(t)
	ctx := context.Background()
	_, _, _, err := wallet.ArmCanonicalWalletHold(ctx, lease.PlatformUserID, lease.LeaseID, "USD", "old-held", 100, 0, time.Now())
	require.NoError(t, err)
	_, err = funding.FreezeCanonicalWalletFunding(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	first := fundingRedisACK(lease, 900, 1, "partial")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, first), "revision 1 stores its signature on the tombstone")
	conversion, err := wallet.ConvertCanonicalWalletHold(ctx, lease.PlatformUserID, "old-held", "captured-event", 80, time.Now())
	require.NoError(t, err)
	require.Equal(t, 0, conversion.Code)
	before, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	second := fundingRedisACK(*before, 920, 2, "partial")
	view := *before
	view.BudgetUnits, view.ReturnedUnits, view.ReturnRevision = 80, 920, 2
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, view), "the other worker installs the revision-2 view first")
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, second), "the verified revision-2 receipt must still apply")
	after, err := wallet.GetCanonicalWalletLeaseByID(ctx, lease.PlatformUserID, lease.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(80), after.BudgetUnits)
	require.Equal(t, int64(920), after.ReturnedUnits)
	require.Equal(t, int64(2), after.ReturnRevision)
	require.NoError(t, funding.ApplyCanonicalWalletFundingReturn(ctx, second), "exact replay stays idempotent")
	other := second
	other.Signature = "a-different-receipt-for-the-same-revision"
	require.Error(t, funding.ApplyCanonicalWalletFundingReturn(ctx, other), "a different receipt for an applied revision still conflicts")
	older := first
	require.Error(t, funding.ApplyCanonicalWalletFundingReturn(ctx, older), "an older revision cannot roll the tombstone back")
}
