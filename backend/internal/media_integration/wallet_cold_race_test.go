//go:build media_integration

package media_integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type installRaceStore struct {
	service.CanonicalWalletLeaseStore
	service.MediaWalletStateStore
	pool          service.CanonicalWalletPoolStore
	once          atomic.Bool
	installed     func()
	beforeInstall func()
	beforeOnce    atomic.Bool
}

func (s *installRaceStore) InstallCanonicalWalletLease(ctx context.Context, l service.CanonicalWalletLease) error {
	if !l.RequireCachedLease && s.beforeOnce.CompareAndSwap(false, true) && s.beforeInstall != nil {
		s.beforeInstall()
	}
	if err := s.CanonicalWalletLeaseStore.InstallCanonicalWalletLease(ctx, l); err != nil {
		return err
	}
	if !l.RequireCachedLease && s.once.CompareAndSwap(false, true) && s.installed != nil {
		s.installed()
	}
	return nil
}
func (s *installRaceStore) ArmCanonicalWalletPool(ctx context.Context, user string, segments []service.AuthorizationSegment, grace int64, now time.Time) error {
	return s.pool.ArmCanonicalWalletPool(ctx, user, segments, grace, now)
}

func TestExternalColdMainAndTitleRefreshAfterConcurrentConsumption(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	task := f.create(t, "cold-race-snapshot", "google/nano-banana", "1:1")
	var snapID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&snapID))
	snap, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapID)
	require.NoError(t, err)
	snap.Family = service.BillingFamilyOpenAI
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	f.control.mu.Lock()
	f.control.issued = false
	f.control.pool = nil
	f.control.budget = 16825620
	f.control.unleased = 13717095
	f.control.mu.Unlock()
	base := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	race := &installRaceStore{CanonicalWalletLeaseStore: base, MediaWalletStateStore: base.(service.MediaWalletStateStore), pool: base.(service.CanonicalWalletPoolStore)}
	f.bridge.Close()
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, race, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	auth := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots)
	var title *service.AuthorizationHandle
	race.installed = func() {
		// A separate logical request wins after the main ensure was signed, but
		// before the main authorizer refreshes Redis. The real Redis install must
		// retain this title's consumption when the main retries funding.
		title, err = auth.Authorize(context.Background(), service.AuthorizeInput{User: user, Snapshot: snap, FixedEstimateUnits: 13717095})
		require.NoError(t, err)
	}
	main, err := auth.Authorize(context.Background(), service.AuthorizeInput{User: user, Snapshot: snap, FixedEstimateUnits: 16825620})
	require.NoError(t, err)
	require.NotNil(t, title)
	require.NotEqual(t, main.ID, title.ID)
	require.Equal(t, int64(16825620), main.HeldUnits)
	require.Equal(t, int64(13717095), title.HeldUnits)
	lease, err := base.GetCanonicalWalletLeaseByID(context.Background(), "media-user", main.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(30542715), lease.ConsumedUnits)
	require.Equal(t, int64(30542715), lease.BudgetUnits)
	require.Zero(t, lease.RemainingUnits())
	var total int64
	require.NoError(t, f.db.QueryRow(`SELECT sum(held_units) FROM wallet_authorization_segment WHERE parent_authorization_id IN ($1,$2)`, main.ID, title.ID).Scan(&total))
	require.Equal(t, int64(30542715), total)
}

func TestExternalColdD1CommitWaitsForRedisCreatorInstallation(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	task := f.create(t, "install-window", "google/nano-banana", "1:1")
	var snapID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&snapID))
	snap, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapID)
	require.NoError(t, err)
	snap.Family = service.BillingFamilyOpenAI
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	f.control.mu.Lock()
	f.control.issued = false
	f.control.pool = nil
	f.control.budget = 2000000
	f.control.mu.Unlock()
	base := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	race := &installRaceStore{CanonicalWalletLeaseStore: base, MediaWalletStateStore: base.(service.MediaWalletStateStore), pool: base.(service.CanonicalWalletPoolStore)}
	committed, resume := make(chan struct{}), make(chan struct{})
	race.beforeInstall = func() { close(committed); <-resume }
	f.bridge.Close()
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, race, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	auth := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots)
	type result struct {
		handle *service.AuthorizationHandle
		err    error
	}
	mainDone, titleDone := make(chan result, 1), make(chan result, 1)
	go func() {
		h, e := auth.Authorize(context.Background(), service.AuthorizeInput{User: user, Snapshot: snap, FixedEstimateUnits: 1000000})
		mainDone <- result{h, e}
	}()
	select {
	case <-committed:
	case <-time.After(3 * time.Second):
		t.Fatal("creator did not reach installation window")
	}
	go func() {
		h, e := auth.Authorize(context.Background(), service.AuthorizeInput{User: user, Snapshot: snap, FixedEstimateUnits: 1000000})
		titleDone <- result{h, e}
	}()
	select {
	case got := <-titleDone:
		close(resume)
		t.Fatalf("title must wait for Redis basis, got %v", got.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(resume)
	var main, title result
	select {
	case main = <-mainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not finish")
	}
	select {
	case title = <-titleDone:
	case <-time.After(3 * time.Second):
		t.Fatal("title did not finish")
	}
	require.NoError(t, main.err)
	require.NoError(t, title.err)
	require.NotEqual(t, main.handle.ID, title.handle.ID)
	lease, err := base.GetCanonicalWalletLeaseByID(context.Background(), "media-user", main.handle.LeaseID)
	require.NoError(t, err)
	require.Equal(t, int64(2000000), lease.ConsumedUnits)
	require.Zero(t, lease.RemainingUnits())
}
