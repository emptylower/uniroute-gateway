package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type inventoryRepositoryStub struct {
	cutoff7d  time.Time
	cutoff30d time.Time
	windowEnd time.Time
	items     []InventoryItem
	err       error
}

func (s *inventoryRepositoryStub) List(_ context.Context, cutoff7d, cutoff30d, windowEnd time.Time) ([]InventoryItem, error) {
	s.cutoff7d = cutoff7d
	s.cutoff30d = cutoff30d
	s.windowEnd = windowEnd
	return s.items, s.err
}

func TestModelGovernanceInventoryUsesOneUTCWindowPerRequest(t *testing.T) {
	repo := &inventoryRepositoryStub{items: []InventoryItem{{AccountID: 1, UpstreamModelID: "model", BillingCurrency: "USD"}}}
	svc := NewModelGovernanceInventoryService(repo)
	now := time.Date(2026, time.August, 19, 7, 8, 9, 123, time.FixedZone("offset", 8*60*60))
	clockCalls := 0
	svc.now = func() time.Time {
		clockCalls++
		return now
	}

	items, err := svc.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, repo.items, items)
	require.Equal(t, 1, clockCalls)
	require.Equal(t, now.UTC().Add(-7*24*time.Hour), repo.cutoff7d)
	require.Equal(t, now.UTC().Add(-30*24*time.Hour), repo.cutoff30d)
	require.Equal(t, now.UTC(), repo.windowEnd)
	require.Equal(t, time.UTC, repo.cutoff7d.Location())
	require.Equal(t, time.UTC, repo.cutoff30d.Location())
	require.Equal(t, time.UTC, repo.windowEnd.Location())
	require.Equal(t, 23*24*time.Hour, repo.cutoff7d.Sub(repo.cutoff30d))
}
