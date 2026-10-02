package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type preferenceHandlerCatalog struct {
	owner int64
	quote service.ChannelCostQuote
}

func (f *preferenceHandlerCatalog) ListText(context.Context, int64, time.Time) ([]service.TextModelCatalogItem, error) {
	return nil, nil
}
func (f *preferenceHandlerCatalog) QuoteChannelCosts(_ context.Context, userID int64, _ time.Time, _ string) (service.ChannelCostQuote, error) {
	f.owner = userID
	return f.quote, nil
}

func TestModelChannelPreferenceHandlerValidatesActualRouteAndOwner(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name, body    string
		authenticated bool
		status        int
		saves         bool
	}{
		{"valid", `{"model_id":" CLAUDE-A ","channel":"official","user_id":999}`, true, 200, true},
		{"cloud unavailable", `{"model_id":"claude-a","channel":"cloud-vendor"}`, true, 422, false},
		{"unknown", `{"model_id":"claude-unknown","channel":"official"}`, true, 404, false},
		{"media", `{"model_id":"veo-3-1","channel":"official"}`, true, 404, false},
		{"wildcard", `{"model_id":"claude-*","channel":"official"}`, true, 400, false},
		{"choice", `{"model_id":"claude-a","channel":"custom"}`, true, 400, false},
		{"anonymous", `{"model_id":"claude-a","channel":"official"}`, false, 401, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			catalog := &preferenceHandlerCatalog{quote: service.ChannelCostQuote{Groups: []service.RoutingGroupModelCosts{{GroupID: 1, EffectiveMultiplier: 1.5, Models: []service.ChannelModelCostItem{{ID: "claude-a"}}}}}}
			h := &ModelCatalogHandler{catalog: catalog, preferences: service.NewChannelPreferenceService(repository.NewChannelPreferenceRepository(db), nil, nil, nil)}
			if test.saves {
				mock.ExpectExec(`(?s)INSERT INTO user_model_channel_preferences.*ON CONFLICT`).WithArgs(int64(42), "claude-a", "official").WillReturnResult(sqlmock.NewResult(1, 1))
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPut, "/api/v1/user/model-channel-preferences", strings.NewReader(test.body))
			c.Request.Header.Set("Content-Type", "application/json")
			if test.authenticated {
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 42})
			}
			h.PutModelChannelPreferences(c)
			require.Equal(t, test.status, recorder.Code, recorder.Body.String())
			if test.saves {
				require.Equal(t, int64(42), catalog.owner)
				var envelope struct {
					Data map[string]string `json:"data"`
				}
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
				require.Equal(t, map[string]string{"model_id": "claude-a", "channel": "official"}, envelope.Data)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
