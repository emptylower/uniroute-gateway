//go:build unit

package service

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestMediaInternalCreateUnavailableWithoutDependencies(t *testing.T) {
	for _, svc := range []*MediaTaskService{nil, {}, {cfg: &config.Config{}}} {
		require.NotPanics(t, func() {
			_, err := svc.Create(context.Background(), 1, "internal-unavailable", MediaCreateInput{Model: "nano-banana-2-lite", Prompt: "a flower"})
			require.ErrorIs(t, err, ErrMediaUnavailable)
		})
	}
}

func TestPublicMediaOwnedReplayStillPrecedesDisabledAndQuotaGates(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	// No users/provider/wallet dependencies exist and creation is disabled. A
	// matching owned replay still returns its original task without any new work.
	svc := &MediaTaskService{cfg: &config.Config{}, store: &mediaTaskStore{db: db}}
	key := &APIKey{ID: 12, UserID: 7, Status: StatusAPIKeyQuotaExhausted, Quota: 1, QuotaUsed: 1, User: &User{ID: 7, Status: StatusActive}}
	canonical := []byte(`{"model":"nano-banana-2-lite","prompt":"a flower","option":"1:1","media_type":"image"}`)
	digest := sha256.Sum256(canonical)
	requestHash := hex.EncodeToString(digest[:])
	now := time.Now().UTC()
	row := []driver.Value{"media-owned", int64(7), "platform-user", int64(12), "owned-replay", requestHash, "nano-banana-2-lite", "image", "1:1", "a flower", []byte(`{"prompt":"a flower"}`), "snapshot-original", int64(2730000), "auth-original", "", nil, int64(0), nil, "event-original", "", "", "queued", "none", []byte(`{"urls":[]}`), "", "", "", now.Add(time.Hour), now, now, nil, "held", WalletImmediateReleasePolicyVersion, nil, nil, nil, nil, "", nil, nil, "", nil, nil, nil, nil, true}
	columns := make([]string, len(row))
	for i := range columns {
		columns[i] = fmt.Sprintf("c%d", i)
	}
	for _, prompt := range []string{"a flower", "another flower"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT "+mediaTaskColumns+" FROM gateway_media_task WHERE user_id=$1 AND api_key_id=$2 AND idempotency_key=$3")).WithArgs(key.UserID, key.ID, "owned-replay").WillReturnRows(sqlmock.NewRows(columns).AddRow(row...))
		task, e := svc.CreatePublicChecked(context.Background(), key, "owned-replay", MediaCreateInput{Model: "nano-banana-2-lite", Prompt: prompt}, func(MediaCreateInput) error { t.Fatal("matching replay must not repeat prompt screening"); return nil })
		if prompt == "a flower" {
			require.NoError(t, e)
			require.Equal(t, "media-owned", task.TaskID)
		} else {
			require.ErrorIs(t, e, ErrMediaIdempotencyConflict)
		}
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
