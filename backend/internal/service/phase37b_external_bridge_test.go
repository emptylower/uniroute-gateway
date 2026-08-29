//go:build integration

// Phase 3.7b (Task 4): the external bridge. Package service cannot import
// repository (repository imports service — a real import cycle for in-package
// test files), so this file registers the real constructors at init time,
// before any test runs, through the seam in openai_live_export_test.go.
package service_test

import (
	"database/sql"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	redisclient "github.com/redis/go-redis/v9"
)

func init() {
	service.RegisterGatewayCacheCtorForTest(func(rdb *redisclient.Client) service.GatewayCache {
		return repository.NewGatewayCache(rdb)
	})
	service.RegisterBillingSnapshotStoreCtorForTest(func(db *sql.DB) service.BillingSnapshotStore {
		return repository.NewBillingSnapshotStore(db)
	})
}
