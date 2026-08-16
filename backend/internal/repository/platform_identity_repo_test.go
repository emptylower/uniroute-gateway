package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/ent/authidentity"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestPlatformIdentityRepositoryCreatesNonLoginProjectionWithoutAuthIdentity(t *testing.T) {
	_, client := newUserEntRepo(t)
	repo := NewPlatformIdentityRepository(client, &config.Config{Default: config.DefaultConfig{UserConcurrency: 9}})
	ctx := context.Background()

	created, err := repo.CreatePlatformUser(ctx, "shipany-user-1", "ShipAny User")
	require.NoError(t, err)
	require.NotZero(t, created.GatewayUserID)

	entity, err := client.User.Get(ctx, created.GatewayUserID)
	require.NoError(t, err)
	require.NotNil(t, entity.PlatformUserID)
	require.Equal(t, "shipany-user-1", *entity.PlatformUserID)
	require.Equal(t, "platform", entity.SignupSource)
	require.Equal(t, 9, entity.Concurrency)
	require.Error(t, bcrypt.CompareHashAndPassword([]byte(entity.PasswordHash), []byte("any-password")))

	identityCount, err := client.AuthIdentity.Query().Where(authidentity.UserIDEQ(entity.ID)).Count(ctx)
	require.NoError(t, err)
	require.Zero(t, identityCount)

	retry, err := repo.CreatePlatformUser(ctx, "shipany-user-1", "Different Name")
	require.NoError(t, err)
	require.Equal(t, created.GatewayUserID, retry.GatewayUserID)
}
