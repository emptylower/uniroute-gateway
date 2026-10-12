package repository

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
	"time"
)

type mediaProjectionScanner struct{ media string }

func (s mediaProjectionScanner) Scan(dest ...any) error {
	for _, d := range dest {
		v := reflect.ValueOf(d).Elem()
		v.Set(reflect.Zero(v.Type()))
	}
	// The appended projection is required to correspond to the DB media_type column.
	if strings.HasSuffix(usageLogSelectColumns, ", media_type") {
		mediaType, ok := dest[len(dest)-1].(*sql.NullString)
		if !ok {
			return fmt.Errorf("media projection destination must be sql.NullString")
		}
		*mediaType = sql.NullString{String: s.media, Valid: true}
	}
	return nil
}
func TestUsageLogMediaTypeProjection(t *testing.T) {
	for _, kind := range []string{"image", "video", "music"} {
		t.Run(kind, func(t *testing.T) {
			log, err := scanUsageLog(mediaProjectionScanner{kind})
			require.NoError(t, err)
			require.NotNil(t, log.MediaType)
			require.Equal(t, kind, *log.MediaType)
		})
	}
}
func TestModelStatsMediaTypeProjection(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := &usageLogRepository{sql: db}
	start := time.Now()
	end := start.Add(time.Hour)
	mock.ExpectQuery("(?s)SELECT.*media_type.*GROUP BY.*media_type").WithArgs(start, end, int64(7)).WillReturnRows(sqlmock.NewRows([]string{"model", "requests", "input_tokens", "output_tokens", "cache_creation_tokens", "cache_read_tokens", "total_tokens", "cost", "actual_cost", "account_cost", "media_type"}).AddRow("song", 1, 0, 0, 0, 0, 0, 0.1, 0.1, 0.08, "music"))
	stats, err := repo.GetUserModelStats(context.Background(), 7, start, end)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	raw := reflect.ValueOf(stats[0]).FieldByName("MediaType")
	require.True(t, raw.IsValid())
	require.Equal(t, "music", raw.String())
	require.NoError(t, mock.ExpectationsWereMet())
}
