package repository

import (
	"context"
	"database/sql"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type upstreamConnectionMigrationRepository struct {
	client *dbent.Client
	db     *sql.DB
}

func NewUpstreamConnectionMigrationRepository(client *dbent.Client, db *sql.DB) service.UpstreamConnectionMigrationRepository {
	return &upstreamConnectionMigrationRepository{client: client, db: db}
}

func NewUpstreamConnectionMigrationRepositoryWithClient(client *dbent.Client) service.UpstreamConnectionMigrationRepository {
	return &upstreamConnectionMigrationRepository{client: client}
}

func (r *upstreamConnectionMigrationRepository) ListAccountsForMigration(ctx context.Context) ([]*service.Account, error) {
	ents, err := r.client.Account.Query().All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*service.Account, 0, len(ents))
	for _, e := range ents {
		out = append(out, accountEntityToService(e))
	}
	return out, nil
}

func (r *upstreamConnectionMigrationRepository) CreateFirstPartyConnectionForAccount(ctx context.Context, account *service.Account, baseURL string, encryptedCredential string) (int64, error) {
	if account == nil {
		return 0, fmt.Errorf("nil account")
	}
	// Determine provider from platform
	provider := service.GovernanceProvider(account.Platform)
	// Map claude -> anthropic etc.
	switch account.Platform {
	case "claude":
		provider = service.GovernanceProviderAnthropic
	case "grok", "xai":
		provider = service.GovernanceProviderGrok
	}
	// If not governed, reject
	if !service.ValidGovernanceProvider(provider) {
		return 0, fmt.Errorf("unsupported platform %s", account.Platform)
	}
	kind := "first_party"
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	// Create connection
	connCreate := tx.UpstreamConnection.Create().
		SetKind(kind).
		SetProvider(string(provider)).
		SetBaseURL(baseURL).
		SetEncryptedCredential(encryptedCredential).
		SetCredentialVersion(1)
	created, err := connCreate.Save(ctx)
	if err != nil {
		return 0, err
	}
	// Link account to connection
	_, err = tx.Account.UpdateOneID(account.ID).SetConnectionID(created.ID).Save(ctx)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	account.ConnectionID = &created.ID
	return created.ID, nil
}

var _ service.UpstreamConnectionMigrationRepository = (*upstreamConnectionMigrationRepository)(nil)
