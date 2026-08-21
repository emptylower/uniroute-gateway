package repository

import (
	"context"
	"database/sql"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type accountEndpointProbeRepository struct {
	client *dbent.Client
	db     *sql.DB
}

func NewAccountEndpointProbeRepository(client *dbent.Client, db *sql.DB) service.AccountEndpointProbeRepository {
	return &accountEndpointProbeRepository{client: client, db: db}
}

func (r *accountEndpointProbeRepository) FindLatestValid(ctx context.Context, accountID int64, now time.Time) (*service.AccountEndpointProbe, error) {
	entProbe, err := r.client.AccountEndpointProbe.Query().
		Where().
		All(ctx)
	if err != nil {
		return nil, err
	}
	var latest *service.AccountEndpointProbe
	for _, p := range entProbe {
		if p.AccountID != accountID {
			continue
		}
		if p.ExpiresAt.Before(now) {
			continue
		}
		if p.Status != "success" {
			continue
		}
		curr := entProbeToService(p)
		if latest == nil || curr.ProbedAt.After(latest.ProbedAt) {
			latest = curr
		}
	}
	return latest, nil
}

func (r *accountEndpointProbeRepository) FindByKey(ctx context.Context, accountID int64, connectionID int64, provider service.GovernanceProvider, protocol service.AccountProtocol, endpoint string, credentialVersion, configVersion int64) (*service.AccountEndpointProbe, error) {
	// Stub scan – real would use ent query with unique index
	probes, err := r.client.AccountEndpointProbe.Query().All(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range probes {
		if p.AccountID == accountID && p.ConnectionID == connectionID && p.Provider == string(provider) && p.Protocol == string(protocol) && p.NormalizedEndpointPath == endpoint && p.CredentialVersion == credentialVersion && p.ConfigVersion == configVersion {
			return entProbeToService(p), nil
		}
	}
	return nil, nil
}

func (r *accountEndpointProbeRepository) Insert(ctx context.Context, probe *service.AccountEndpointProbe) error {
	builder := r.client.AccountEndpointProbe.Create().
		SetAccountID(probe.AccountID).
		SetConnectionID(probe.ConnectionID).
		SetProvider(string(probe.Provider)).
		SetProtocol(string(probe.Protocol)).
		SetNormalizedEndpointPath(probe.NormalizedEndpoint).
		SetCredentialVersion(probe.CredentialVersion).
		SetConfigVersion(probe.ConfigVersion).
		SetStatus(probe.Status).
		SetProbedAt(probe.ProbedAt).
		SetExpiresAt(probe.ExpiresAt).
		SetResponseSummary(probe.ResponseSummary)
	if probe.EvidenceRef != nil {
		builder.SetEvidenceRef(*probe.EvidenceRef)
	}
	if probe.ResponseSummary == nil {
		builder.SetResponseSummary(map[string]any{})
	}
	created, err := builder.Save(ctx)
	if err != nil {
		return err
	}
	probe.ID = created.ID
	return nil
}

func (r *accountEndpointProbeRepository) InvalidateOnConfigChange(ctx context.Context, accountID int64) error {
	// Invalidate by deleting expired? For append-only, we just rely on expires_at; stub no-op
	return nil
}

func entProbeToService(p *dbent.AccountEndpointProbe) *service.AccountEndpointProbe {
	return &service.AccountEndpointProbe{
		ID:                 p.ID,
		AccountID:          p.AccountID,
		ConnectionID:       p.ConnectionID,
		Provider:           service.GovernanceProvider(p.Provider),
		Protocol:           service.AccountProtocol(p.Protocol),
		NormalizedEndpoint: p.NormalizedEndpointPath,
		CredentialVersion:  p.CredentialVersion,
		ConfigVersion:      p.ConfigVersion,
		Status:             p.Status,
		ProbedAt:           p.ProbedAt,
		ExpiresAt:          p.ExpiresAt,
		EvidenceRef:        p.EvidenceRef,
		ResponseSummary:    p.ResponseSummary,
	}
}

var _ service.AccountEndpointProbeRepository = (*accountEndpointProbeRepository)(nil)

// Ensure time import used
var _ = time.Now
