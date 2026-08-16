package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestGatewayCacheLiveCallIdentityAndController(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	cache, ok := NewGatewayCache(client).(service.LiveCallStore)
	require.True(t, ok)
	otherInstance, ok := NewGatewayCache(client).(service.LiveCallStore)
	require.True(t, ok)
	record := &service.LiveCallRecord{
		CallID:                "call_secret",
		CallHash:              HashLiveCallID("call_secret"),
		AccountID:             11,
		APIKeyID:              22,
		UserID:                33,
		GroupID:               44,
		LeaseID:               "lease",
		Model:                 "gpt-live-test",
		AttestationCiphertext: "encrypted-attestation",
		CreatedAt:             time.Now(),
		ExpiresAt:             time.Now().Add(time.Hour),
		Controller:            service.LiveControllerPending,
	}
	require.NoError(t, cache.SaveLiveCall(context.Background(), record, time.Hour))

	loaded, err := otherInstance.GetLiveCall(context.Background(), record.CallHash)
	require.NoError(t, err)
	require.Equal(t, record.CallID, loaded.CallID)
	require.Equal(t, record.AccountID, loaded.AccountID)
	require.Equal(t, record.AttestationCiphertext, loaded.AttestationCiphertext)

	finalizations, ok := NewGatewayCache(client).(service.LiveFinalizationStore)
	require.True(t, ok)
	require.NoError(t, finalizations.QueueLiveFinalization(context.Background(), record.CallHash))
	ttl, err := client.TTL(context.Background(), liveCallKey(record.CallHash)).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)

	claimed, err := cache.ClaimLiveController(context.Background(), record.CallHash, service.LiveControllerObserver, "observer-1")
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = cache.ClaimLiveController(context.Background(), record.CallHash, service.LiveControllerProxy, "proxy-1")
	require.NoError(t, err)
	require.True(t, claimed)
	controller, err := cache.GetLiveController(context.Background(), record.CallHash)
	require.NoError(t, err)
	require.Equal(t, service.LiveControllerProxy, controller)

	released, err := cache.ReleaseLiveController(context.Background(), record.CallHash, "proxy-1")
	require.NoError(t, err)
	require.True(t, released)
	closed, err := cache.MarkLiveCallClosed(context.Background(), record.CallHash, time.Hour)
	require.NoError(t, err)
	require.True(t, closed)
	ttl, err = client.TTL(context.Background(), liveCallKey(record.CallHash)).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	closed, err = cache.MarkLiveCallClosed(context.Background(), record.CallHash, time.Hour)
	require.NoError(t, err)
	require.False(t, closed)
}

func TestGatewayCacheLiveFinalizationQueueSharedAcrossInstances(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	first, ok := NewGatewayCache(client).(service.LiveFinalizationStore)
	require.True(t, ok)
	second, ok := NewGatewayCache(client).(service.LiveFinalizationStore)
	require.True(t, ok)

	require.NoError(t, first.QueueLiveFinalization(context.Background(), "call-b"))
	require.NoError(t, first.QueueLiveFinalization(context.Background(), "call-a"))
	require.NoError(t, first.QueueLiveFinalization(context.Background(), "call-a"))

	pending, err := second.ListLiveFinalizations(context.Background(), 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"call-a", "call-b"}, pending)

	require.NoError(t, second.RemoveLiveFinalization(context.Background(), "call-a"))
	pending, err = first.ListLiveFinalizations(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, []string{"call-b"}, pending)
}
