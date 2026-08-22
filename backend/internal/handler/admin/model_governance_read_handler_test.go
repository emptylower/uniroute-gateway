package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type fakeGovernanceReadRepository struct {
	quarantine    *service.GovernancePage[service.QuarantinePoolItem]
	events        map[string]*service.GovernancePage[service.GovernanceEventItem]
	quarantineErr error
	eventsErr     error
	lastStream    string
	lastPage      int
	lastSize      int
}

func (f *fakeGovernanceReadRepository) ListQuarantinePool(
	_ context.Context, page, pageSize int,
) (*service.GovernancePage[service.QuarantinePoolItem], error) {
	f.lastPage, f.lastSize = page, pageSize
	if f.quarantineErr != nil {
		return nil, f.quarantineErr
	}
	return f.quarantine, nil
}

func (f *fakeGovernanceReadRepository) ListGovernanceEvents(
	_ context.Context, stream string, page, pageSize int,
) (*service.GovernancePage[service.GovernanceEventItem], error) {
	f.lastStream, f.lastPage, f.lastSize = stream, page, pageSize
	if stream != service.GovernanceEventStreamPublication && stream != service.GovernanceEventStreamRegistry {
		return nil, infraerrors.BadRequest(
			"GOVERNANCE_EVENT_STREAM_INVALID",
			fmt.Sprintf("unsupported governance event stream %q", stream),
		)
	}
	if f.eventsErr != nil {
		return nil, f.eventsErr
	}
	return f.events[stream], nil
}

func newGovernanceReadTestContext(t *testing.T, query string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/?"+query, nil)
	return ctx, recorder
}

func TestModelGovernanceReadHandler_QuarantineReturnsEvidenceSafePage(t *testing.T) {
	batch := "qb-1"
	now := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	repo := &fakeGovernanceReadRepository{
		quarantine: &service.GovernancePage[service.QuarantinePoolItem]{
			Items: []service.QuarantinePoolItem{{
				AccountID: 7, AccountName: "agg-account", ChannelID: 3, ChannelName: "agg-channel",
				CanonicalModelID: "gpt-5.2", Reason: "quarantined_by_admin", RegistryVersion: 12, ChannelVersion: 4,
				QuarantineBatchID: &batch, CreatedAt: now, UpdatedAt: now,
			}},
			Total: 1, Page: 1, PageSize: 50,
		},
	}
	handler := NewModelGovernanceReadHandler(repo)
	ctx, recorder := newGovernanceReadTestContext(t, "page=2&page_size=10")
	handler.Quarantine(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 2, repo.lastPage)
	require.Equal(t, 10, repo.lastSize)

	var body struct {
		Code int                                                `json:"code"`
		Data service.GovernancePage[service.QuarantinePoolItem] `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, 0, body.Code)
	require.Len(t, body.Data.Items, 1)
	item := body.Data.Items[0]
	require.Equal(t, "agg-account", item.AccountName)
	require.Equal(t, "gpt-5.2", item.CanonicalModelID)
	require.Equal(t, "qb-1", *item.QuarantineBatchID)

	// Evidence-safety: the serialized item must not contain secret-shaped keys.
	raw, err := json.Marshal(item)
	require.NoError(t, err)
	for _, forbidden := range []string{"credential", "api_key", "secret", "authorization", "payload", "snapshot"} {
		require.NotContains(t, string(raw), forbidden)
	}
}

func TestModelGovernanceReadHandler_EventsDefaultsToPublicationStream(t *testing.T) {
	accountID, channelID, channelVersion := int64(7), int64(3), int64(4)
	batch := "qb-1"
	repo := &fakeGovernanceReadRepository{
		events: map[string]*service.GovernancePage[service.GovernanceEventItem]{
			service.GovernanceEventStreamPublication: {
				Items: []service.GovernanceEventItem{{
					ID: 9, Stream: service.GovernanceEventStreamPublication, EventType: "quarantine",
					ActorID: "admin-1", RegistryVersion: 12,
					AccountID: &accountID, CanonicalModelID: "gpt-5.2", ChannelID: &channelID,
					Eligibility: "quarantined", Reason: "quarantined_by_admin", ChannelVersion: &channelVersion,
					BatchID: "b-1", QuarantineBatchID: &batch,
				}},
				Total: 1, Page: 1, PageSize: 50,
			},
		},
	}
	handler := NewModelGovernanceReadHandler(repo)

	ctx, recorder := newGovernanceReadTestContext(t, "")
	handler.Events(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, service.GovernanceEventStreamPublication, repo.lastStream)

	ctx, recorder = newGovernanceReadTestContext(t, "stream=registry")
	handler.Events(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, service.GovernanceEventStreamRegistry, repo.lastStream)

	ctx, recorder = newGovernanceReadTestContext(t, "stream=unknown")
	handler.Events(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestModelGovernanceReadHandler_NilRepositoryIs500(t *testing.T) {
	handler := NewModelGovernanceReadHandler(nil)
	ctx, recorder := newGovernanceReadTestContext(t, "")
	handler.Quarantine(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	ctx, recorder = newGovernanceReadTestContext(t, "")
	handler.Events(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}
