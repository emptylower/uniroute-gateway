package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageMediaPriceContextOwnerCheckedBatchIsReadOnly(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := &usageLogRepository{sql: db}
	kind := "video"
	logs := []service.UsageLog{
		{ID: 1, UserID: 7, APIKeyID: 8, RequestID: "media-one", Model: "veo-3-1", MediaType: &kind, ActualCost: 0.375, RateMultiplier: 1},
		{ID: 2, UserID: 7, APIKeyID: 8, RequestID: "media-two", Model: "veo-3-1", MediaType: &kind, ActualCost: 0.375, RateMultiplier: 1},
	}
	mock.ExpectQuery(`(?s)SELECT ul.id,t.model,t.option,.* FROM usage_logs ul JOIN gateway_media_task t ON t.id=ul.request_id AND t.user_id=ul.user_id AND t.api_key_id=ul.api_key_id AND t.model=ul.model AND t.media_type=ul.media_type WHERE \(ul.id=\$1 AND t.user_id=\$2 AND t.api_key_id=\$3 AND t.id=\$4\) OR \(ul.id=\$5 AND t.user_id=\$6 AND t.api_key_id=\$7 AND t.id=\$8\)`).
		WithArgs(int64(1), int64(7), int64(8), "media-one", int64(2), int64(7), int64(8), "media-two").
		WillReturnRows(sqlmock.NewRows([]string{"id", "model", "option", "reference_sku_verified"}).AddRow(int64(1), "veo-3-1", "8", true))
	require.NoError(t, repo.hydrateUsageMediaPriceContext(context.Background(), logs))
	require.NotNil(t, logs[0].MediaPriceContext)
	require.Equal(t, "8", logs[0].MediaPriceContext.Option)
	require.Nil(t, logs[1].MediaPriceContext, "foreign/missing task stays unknown")
	require.Equal(t, 0.375, logs[0].ActualCost)
	require.Equal(t, 1.0, logs[0].RateMultiplier)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUsageMediaPriceContextSkipsLegacyAndChunksWithoutNPlusOne(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := &usageLogRepository{sql: db}
	require.NoError(t, repo.hydrateUsageMediaPriceContext(context.Background(), []service.UsageLog{{ID: 1, RequestID: "legacy"}}))
	kind := "video"
	logs := make([]service.UsageLog, 101)
	for i := range logs {
		logs[i] = service.UsageLog{ID: int64(i + 1), UserID: 7, APIKeyID: 8, RequestID: "media-task", MediaType: &kind}
	}
	mock.ExpectQuery(`(?s)SELECT ul.id,t.model,t.option.*ul.id=\$397.*t.id=\$400`).WillReturnRows(sqlmock.NewRows([]string{"id", "model", "option", "reference_sku_verified"}).AddRow(int64(100), "veo-3-1", "8", true))
	mock.ExpectQuery(`(?s)SELECT ul.id,t.model,t.option.*ul.id=\$1.*t.id=\$4`).WithArgs(int64(101), int64(7), int64(8), "media-task").WillReturnRows(sqlmock.NewRows([]string{"id", "model", "option", "reference_sku_verified"}).AddRow(int64(101), "veo-3-1", "4", false))
	require.NoError(t, repo.hydrateUsageMediaPriceContext(context.Background(), logs))
	require.NotNil(t, logs[99].MediaPriceContext)
	require.NotNil(t, logs[100].MediaPriceContext)
	require.False(t, logs[100].MediaPriceContext.ReferenceSKUVerified)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUsageMediaReferenceProjectionOnlyTrustsFrozenVeoEngineAndReturnsBoolean(t *testing.T) {
	require.Contains(t, usageMediaReferenceVerified, "t.request_payload->>'model'='veo3_fast'")
	require.Contains(t, usageMediaReferenceVerified, "t.request_payload->>'resolution'='720p'")
	require.Contains(t, usageMediaReferenceVerified, "t.request_payload->>'generation_type'='TEXT_2_VIDEO'")
	require.Contains(t, usageMediaReferenceVerified, "COALESCE(")
	require.Contains(t, usageMediaReferenceVerified, "AND NOT t.request_payload ?| ARRAY['generate_audio','generateAudio','audio','audio_enabled','sound']")
	for _, forbidden := range []string{"prompt", "image_url", "api_key", "provider_task_id", "supplier_pricing"} {
		require.NotContains(t, usageMediaReferenceVerified, forbidden)
	}
}
