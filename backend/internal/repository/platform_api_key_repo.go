package repository

import (
	"context"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/apikey"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type platformAPIKeyRepository struct {
	client *dbent.Client
}

func NewPlatformAPIKeyRepository(client *dbent.Client) service.PlatformAPIKeyRepository {
	return &platformAPIKeyRepository{client: client}
}

func (r *platformAPIKeyRepository) FindProjectedByPlatformKeyID(ctx context.Context, platformKeyID string) (*service.PlatformAPIKeyProjection, error) {
	entity, err := r.client.APIKey.Query().
		Where(apikey.PlatformKeyIDEQ(platformKeyID), apikey.DeletedAtIsNil()).
		Only(ctx)
	if dbent.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return platformAPIKeyFromEntity(entity), nil
}

func (r *platformAPIKeyRepository) CreateProjected(ctx context.Context, projection *service.PlatformAPIKeyProjection, keyPlaceholder string) error {
	entity, err := r.client.APIKey.Create().
		SetUserID(projection.GatewayUserID).
		SetKey(keyPlaceholder).
		SetPlatformKeyID(projection.PlatformKeyID).
		SetKeySha256(projection.KeySHA256).
		SetKeyPrefix(projection.KeyPrefix).
		SetPlatformKeyVersion(projection.Version).
		SetName(projection.Name).
		SetRoutingMode(service.APIKeyRoutingModeAutoChannels).
		SetStatus(projection.Status).
		Save(ctx)
	if err != nil {
		if dbent.IsConstraintError(err) {
			return service.ErrPlatformAPIKeyConflict
		}
		return err
	}
	projection.GatewayAPIKeyID = entity.ID
	return nil
}

func (r *platformAPIKeyRepository) UpdateProjected(ctx context.Context, projection *service.PlatformAPIKeyProjection, expectedVersion int64) (bool, error) {
	affected, err := r.client.APIKey.Update().
		Where(
			apikey.IDEQ(projection.GatewayAPIKeyID),
			apikey.UserIDEQ(projection.GatewayUserID),
			apikey.PlatformKeyIDEQ(projection.PlatformKeyID),
			apikey.PlatformKeyVersionEQ(expectedVersion),
			apikey.DeletedAtIsNil(),
		).
		SetKeySha256(projection.KeySHA256).
		SetKeyPrefix(projection.KeyPrefix).
		SetPlatformKeyVersion(projection.Version).
		SetName(projection.Name).
		SetStatus(projection.Status).
		SetUpdatedAt(time.Now()).
		Save(ctx)
	if dbent.IsConstraintError(err) {
		return false, service.ErrPlatformAPIKeyConflict
	}
	return affected == 1, err
}

func platformAPIKeyFromEntity(entity *dbent.APIKey) *service.PlatformAPIKeyProjection {
	if entity == nil || entity.PlatformKeyID == nil || entity.KeySha256 == nil || entity.KeyPrefix == nil || entity.PlatformKeyVersion == nil {
		return nil
	}
	return &service.PlatformAPIKeyProjection{
		GatewayAPIKeyID: entity.ID,
		GatewayUserID:   entity.UserID,
		PlatformKeyID:   *entity.PlatformKeyID,
		KeySHA256:       *entity.KeySha256,
		KeyPrefix:       *entity.KeyPrefix,
		Status:          entity.Status,
		Version:         *entity.PlatformKeyVersion,
		Name:            entity.Name,
	}
}

var _ service.PlatformAPIKeyRepository = (*platformAPIKeyRepository)(nil)
