package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type catalogServiceStub struct {
	statuses    []service.CatalogSourceStatusView
	candidates  []service.CatalogCandidateView
	retirements []service.CatalogRetirementSuggestionView
	anomalies   []service.CatalogPriceAnomalyView

	updatedSource     string
	updatedEnabled    bool
	updatedEnabledPtr *bool
	updatedThreshold  *int
	updateErr         error
}

func (s *catalogServiceStub) SourceStatus(ctx context.Context) ([]service.CatalogSourceStatusView, error) {
	return s.statuses, nil
}
func (s *catalogServiceStub) CandidateList(ctx context.Context) ([]service.CatalogCandidateView, error) {
	return s.candidates, nil
}
func (s *catalogServiceStub) RetirementSuggestions(ctx context.Context) ([]service.CatalogRetirementSuggestionView, error) {
	return s.retirements, nil
}
func (s *catalogServiceStub) PriceAnomalies(ctx context.Context) ([]service.CatalogPriceAnomalyView, error) {
	return s.anomalies, nil
}
func (s *catalogServiceStub) UpdateSourceSetting(ctx context.Context, actorID, source string, enabled *bool, thresholdPercent *int) error {
	s.updatedSource = source
	if enabled != nil {
		s.updatedEnabled = *enabled
	}
	s.updatedEnabledPtr = enabled
	s.updatedThreshold = thresholdPercent
	return s.updateErr
}

func newCatalogHandlerRouter(stub service.ModelCatalogCandidateViews) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewModelCatalogCandidateHandler(stub)
	router.GET("/sources", handler.ListSources)
	router.GET("/candidates", handler.ListCandidates)
	router.GET("/retirements", handler.ListRetirements)
	router.GET("/price-anomalies", handler.ListPriceAnomalies)
	router.PUT("/sources/:source/settings", handler.UpdateSourceSettings)
	return router
}

func TestModelCatalogCandidateHandlerSourceStatusContract(t *testing.T) {
	stub := &catalogServiceStub{statuses: []service.CatalogSourceStatusView{{
		Source: service.CatalogSourceOpenRouter, Enabled: true,
		CountDropThresholdPercent: 20, StaleAfterHours: 24,
		LastAcceptedAt: func() *time.Time { t := time.Now().UTC(); return &t }(),
		LastItemCount:  5, Stale: false,
	}}}
	router := newCatalogHandlerRouter(stub)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/sources", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, strings.Contains(recorder.Body.String(), `"source":"openrouter"`))
	require.True(t, strings.Contains(recorder.Body.String(), `"stale":false`))
}

func TestModelCatalogCandidateHandlerListViews(t *testing.T) {
	stub := &catalogServiceStub{
		candidates: []service.CatalogCandidateView{{
			CanonicalModelID: "anthropic/claude-x",
			Confidence:       service.CatalogConfidenceCertain,
			QueueOrder:       1,
		}},
		retirements: []service.CatalogRetirementSuggestionView{{CanonicalModelID: "old-model"}},
		anomalies:   []service.CatalogPriceAnomalyView{{CanonicalModelID: "divergent", MaxSpreadPercent: 200}},
	}
	router := newCatalogHandlerRouter(stub)

	for _, tc := range []struct {
		path     string
		contains string
	}{
		{"/candidates", `"confidence":"certain"`},
		{"/retirements", `"canonical_model_id":"old-model"`},
		{"/price-anomalies", `"max_spread_percent":200`},
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
		require.Equal(t, http.StatusOK, recorder.Code, tc.path)
		require.True(t, strings.Contains(recorder.Body.String(), tc.contains), "%s body: %s", tc.path, recorder.Body.String())
	}
}

func TestModelCatalogCandidateHandlerUpdateSettingsContract(t *testing.T) {
	stub := &catalogServiceStub{}
	router := newCatalogHandlerRouter(stub)

	body := `{"enabled": false, "count_drop_threshold_percent": 30}`
	req := httptest.NewRequest(http.MethodPut, "/sources/litellm/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "litellm", stub.updatedSource)
	require.NotNil(t, stub.updatedEnabledPtr)
	require.False(t, *stub.updatedEnabledPtr)
	require.NotNil(t, stub.updatedThreshold)
	require.Equal(t, 30, *stub.updatedThreshold)

	// Omitted fields stay nil: partial updates never clobber.
	body = `{}`
	req = httptest.NewRequest(http.MethodPut, "/sources/modelsdev/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Nil(t, stub.updatedEnabledPtr)
	require.Nil(t, stub.updatedThreshold)

	// Out-of-range threshold is rejected by the handler before the service.
	body = `{"count_drop_threshold_percent": 500}`
	req = httptest.NewRequest(http.MethodPut, "/sources/litellm/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &decoded))

	// Unknown source surfaces the service error (mapped as server error).
	stub.updateErr = context.DeadlineExceeded
	body = `{"enabled": true}`
	req = httptest.NewRequest(http.MethodPut, "/sources/huggingface/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestModelCatalogCandidateHandlerNilServiceFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewModelCatalogCandidateHandler(nil)
	router.GET("/sources", handler.ListSources)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/sources", nil))
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestModelCatalogCandidateViewJSONHasNoRawPayload(t *testing.T) {
	view := service.CatalogCandidateView{
		CanonicalModelID: "anthropic/claude-x",
		Confidence:       service.CatalogConfidenceLikely,
	}
	encoded, err := json.Marshal(view)
	require.NoError(t, err)
	for _, forbidden := range []string{"payload", "credential", "api_key", "authorization"} {
		require.NotContains(t, strings.ToLower(string(encoded)), forbidden)
	}
}
