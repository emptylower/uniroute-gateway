package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeMigrationRepo struct {
	accounts []*Account
	created map[int64]int64
}

func (f *fakeMigrationRepo) ListAccountsForMigration(ctx context.Context) ([]*Account, error) {
	return f.accounts, nil
}
func (f *fakeMigrationRepo) CreateFirstPartyConnectionForAccount(ctx context.Context, account *Account, baseURL string, enc string) (int64, error) {
	if f.created == nil {
		f.created = map[int64]int64{}
	}
	if _, ok := f.created[account.ID]; ok {
		return f.created[account.ID], nil
	}
	id := int64(1000 + account.ID)
	f.created[account.ID] = id
	cid := id
	account.ConnectionID = &cid
	return id, nil
}

func TestUpstreamConnectionMigrationDryRunOnePerAccount(t *testing.T) {
	repo := &fakeMigrationRepo{
		accounts: []*Account{
			{ID: 1, Platform: "claude", Type: "api_key", Credentials: map[string]any{"api_key": "sk-1"}, Extra: map[string]any{}},
			{ID: 2, Platform: "openai", Type: "api_key", Credentials: map[string]any{"api_key": "sk-2"}, Extra: map[string]any{}},
			{ID: 3, Platform: "gemini", Type: "api_key", Credentials: map[string]any{"api_key": "sk-3"}, Extra: map[string]any{}},
			{ID: 4, Platform: "unsupported", Type: "api_key", Credentials: map[string]any{}, Extra: map[string]any{}},
		},
	}
	svc := NewUpstreamConnectionMigrationService(repo)
	report, err := svc.DryRun(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, report.TotalAccounts)
	require.Equal(t, 3, report.FirstPartyToCreate)
	require.Len(t, report.UnsupportedPlatforms, 1)
	require.Contains(t, report.UnsupportedPlatforms[0], "unsupported")
	require.True(t, report.Redacted)
	require.NotEmpty(t, report.InventoryHash)
	require.Len(t, report.Details, 3)
	// Deterministic hash: same input same hash
	report2, _ := svc.DryRun(context.Background())
	require.Equal(t, report.InventoryHash, report2.InventoryHash)
	// No aggregator inference
	for _, d := range report.Details {
		require.Equal(t, "first_party", d.Kind)
		require.NotEmpty(t, d.Provider)
	}
}

func TestUpstreamConnectionMigrationSecretRedacted(t *testing.T) {
	repo := &fakeMigrationRepo{
		accounts: []*Account{{ID: 1, Platform: "claude", Credentials: map[string]any{"api_key": "sk-secret"}, Extra: map[string]any{}}},
	}
	svc := NewUpstreamConnectionMigrationService(repo)
	report, _ := svc.DryRun(context.Background())
	require.True(t, report.Redacted)
	for _, d := range report.Details {
		require.NotContains(t, d.BaseURL, "sk-secret")
		require.NotContains(t, d.Note, "sk-secret")
	}
	// Report should not contain raw credential
	require.NotContains(t, report.InventoryHash, "sk-secret")
}

func TestUpstreamConnectionMigrationReplay(t *testing.T) {
	repo := &fakeMigrationRepo{
		accounts: []*Account{{ID: 1, Platform: "claude", Credentials: map[string]any{}, Extra: map[string]any{}}},
	}
	svc := NewUpstreamConnectionMigrationService(repo)
	report, _ := svc.DryRun(context.Background())
	_, err := svc.Apply(context.Background(), report.InventoryHash)
	require.NoError(t, err)
	// Replay with same hash should be idempotent (no duplicate)
	acc := repo.accounts[0]
	require.NotNil(t, acc.ConnectionID)
	firstID := *acc.ConnectionID
	report2, _ := svc.DryRun(context.Background())
	_, err = svc.Apply(context.Background(), report2.InventoryHash)
	require.NoError(t, err)
	require.Equal(t, firstID, *acc.ConnectionID)
}

func TestUpstreamConnectionMigrationUnsupportedPlatform(t *testing.T) {
	repo := &fakeMigrationRepo{
		accounts: []*Account{{ID: 99, Platform: "wechat", Credentials: map[string]any{}, Extra: map[string]any{}}},
	}
	svc := NewUpstreamConnectionMigrationService(repo)
	report, _ := svc.DryRun(context.Background())
	require.Equal(t, 0, report.FirstPartyToCreate)
	require.Contains(t, report.UnsupportedPlatforms, "wechat")
}
