package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestProjectedAPIKeyAuthenticatesBySHA256WithoutStoredPlaintext(t *testing.T) {
	_, client := newUserEntRepo(t)
	ctx := context.Background()
	identityRepo := NewPlatformIdentityRepository(client, &config.Config{})
	identity, err := identityRepo.CreatePlatformUser(ctx, "shipany-user-key-test", "Key Test")
	require.NoError(t, err)

	plaintext := "test-key-this-value-never-enters-the-projection-api"
	sum := sha256.Sum256([]byte(plaintext))
	hash := hex.EncodeToString(sum[:])
	projection := &service.PlatformAPIKeyProjection{
		GatewayUserID: identity.GatewayUserID,
		PlatformKeyID: "shipany-key-auth-1",
		KeySHA256:     hash,
		KeyPrefix:     "sk-live-this",
		Status:        service.StatusAPIKeyActive,
		Version:       1,
		Name:          "Projected",
	}
	projectionRepo := NewPlatformAPIKeyRepository(client)
	require.NoError(t, projectionRepo.CreateProjected(ctx, projection, platformKeyPlaceholderForTest(projection.PlatformKeyID)))

	stored, err := client.APIKey.Get(ctx, projection.GatewayAPIKeyID)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, stored.Key)
	require.False(t, strings.Contains(stored.Key, plaintext))
	require.Equal(t, hash, *stored.KeySha256)

	apiRepo := NewAPIKeyRepository(client, nil)
	authenticated, err := apiRepo.GetByKeyForAuth(ctx, plaintext)
	require.NoError(t, err)
	require.Equal(t, projection.GatewayAPIKeyID, authenticated.ID)
	require.Equal(t, identity.GatewayUserID, authenticated.UserID)
	authService := service.NewAPIKeyService(apiRepo, nil, nil, nil, nil, nil, &config.Config{})
	throughExistingAuthPath, err := authService.GetByKey(ctx, plaintext)
	require.NoError(t, err)
	require.Equal(t, projection.GatewayAPIKeyID, throughExistingAuthPath.ID)
	require.Equal(t, plaintext, throughExistingAuthPath.Key)
	cacheIDs, err := apiRepo.ListKeysByUserID(ctx, identity.GatewayUserID)
	require.NoError(t, err)
	require.Contains(t, cacheIDs, "sha256:"+hash)
	projectionService := service.NewPlatformAPIKeyService(projectionRepo, authService)
	revoked, changed, err := projectionService.Revoke(ctx, identity.GatewayUserID, projection.PlatformKeyID, 2)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, service.StatusAPIKeyDisabled, revoked.Status)
	afterRevoke, err := authService.GetByKey(ctx, plaintext)
	require.NoError(t, err)
	require.Equal(t, service.StatusAPIKeyDisabled, afterRevoke.Status)

	_, err = apiRepo.GetByKeyForAuth(ctx, plaintext+"-wrong")
	require.ErrorIs(t, err, service.ErrAPIKeyNotFound)
}

func TestPanelRepositoryCannotUpdateOrDeleteProjectedAPIKey(t *testing.T) {
	_, client := newUserEntRepo(t)
	ctx := context.Background()
	identityRepo := NewPlatformIdentityRepository(client, &config.Config{})
	identity, err := identityRepo.CreatePlatformUser(ctx, "shipany-user-key-guard", "Guard Test")
	require.NoError(t, err)

	projection := &service.PlatformAPIKeyProjection{
		GatewayUserID: identity.GatewayUserID,
		PlatformKeyID: "shipany-key-guard-1",
		KeySHA256:     strings.Repeat("c", 64),
		KeyPrefix:     "sk-guard",
		Status:        service.StatusAPIKeyActive,
		Version:       1,
		Name:          "Projected",
	}
	require.NoError(t, NewPlatformAPIKeyRepository(client).CreateProjected(ctx, projection, platformKeyPlaceholderForTest(projection.PlatformKeyID)))

	apiRepo := NewAPIKeyRepository(client, nil)
	key, err := apiRepo.GetByID(ctx, projection.GatewayAPIKeyID)
	require.NoError(t, err)
	key.Name = "Panel mutation"
	require.ErrorIs(t, apiRepo.Update(ctx, key), service.ErrProjectedAPIKeyManagedExternally)
	require.ErrorIs(t, apiRepo.Delete(ctx, key.ID), service.ErrProjectedAPIKeyManagedExternally)
	_, _, err = apiRepo.GetKeyAndOwnerID(ctx, key.ID)
	require.ErrorIs(t, err, service.ErrProjectedAPIKeyManagedExternally)
}

func platformKeyPlaceholderForTest(platformKeyID string) string {
	sum := sha256.Sum256([]byte(platformKeyID))
	return "__projected__" + hex.EncodeToString(sum[:])
}
