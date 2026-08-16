package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/schema/mixins"
	dbuser "github.com/Wei-Shaw/sub2api/ent/user"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

const platformManagedPasswordSentinel = "!platform-managed:no-local-login!"

type platformIdentityRepository struct {
	client             *dbent.Client
	defaultConcurrency int
}

func NewPlatformIdentityRepository(client *dbent.Client, cfg *config.Config) service.PlatformIdentityRepository {
	defaultConcurrency := 5
	if cfg != nil && cfg.Default.UserConcurrency > 0 {
		defaultConcurrency = cfg.Default.UserConcurrency
	}
	return &platformIdentityRepository{client: client, defaultConcurrency: defaultConcurrency}
}

func (r *platformIdentityRepository) FindByPlatformUserID(ctx context.Context, platformUserID string) (*service.PlatformIdentity, error) {
	entity, err := r.client.User.Query().
		Where(dbuser.PlatformUserIDEQ(platformUserID)).
		Only(mixins.SkipSoftDelete(ctx))
	if dbent.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return platformIdentityFromEntity(entity), nil
}

func (r *platformIdentityRepository) CreatePlatformUser(ctx context.Context, platformUserID, username string) (*service.PlatformIdentity, error) {
	if username == "" {
		username = "platform-user"
	}
	entity, err := r.client.User.Create().
		SetEmail(platformIdentityEmail(platformUserID)).
		SetUsername(username).
		SetPasswordHash(platformManagedPasswordSentinel).
		SetPlatformUserID(platformUserID).
		SetRole(domain.RoleUser).
		SetStatus(domain.StatusActive).
		SetConcurrency(r.defaultConcurrency).
		SetSignupSource("platform").
		Save(ctx)
	if err == nil {
		return platformIdentityFromEntity(entity), nil
	}
	if !dbent.IsConstraintError(err) {
		return nil, err
	}

	// A concurrent retry may win the unique platform_user_id insert. Resolve the
	// canonical row so this endpoint remains idempotent without mutating it.
	existing, lookupErr := r.FindByPlatformUserID(ctx, platformUserID)
	if lookupErr != nil {
		return nil, errors.Join(err, lookupErr)
	}
	if existing == nil {
		return nil, fmt.Errorf("create platform identity: %w", err)
	}
	return existing, nil
}

func (r *platformIdentityRepository) UpdatePlatformUserStatus(ctx context.Context, platformUserID, status string) (*service.PlatformIdentity, error) {
	entity, err := r.client.User.Query().
		Where(dbuser.PlatformUserIDEQ(platformUserID)).
		Only(mixins.SkipSoftDelete(ctx))
	if err != nil {
		return nil, err
	}
	updated, err := r.client.User.UpdateOneID(entity.ID).SetStatus(status).Save(ctx)
	if err != nil {
		return nil, err
	}
	return platformIdentityFromEntity(updated), nil
}

func platformIdentityEmail(platformUserID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(platformUserID)))
	return "platform-" + hex.EncodeToString(sum[:]) + "@internal.invalid"
}

func platformIdentityFromEntity(entity *dbent.User) *service.PlatformIdentity {
	if entity == nil || entity.PlatformUserID == nil {
		return nil
	}
	return &service.PlatformIdentity{
		GatewayUserID:  entity.ID,
		PlatformUserID: *entity.PlatformUserID,
		Status:         entity.Status,
		CreatedAt:      entity.CreatedAt,
		DeletedAt:      entity.DeletedAt,
	}
}
