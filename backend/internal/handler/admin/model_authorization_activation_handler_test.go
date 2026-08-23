package admin

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestModelAuthorizationActivationHandler_Activate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()
	svc := service.NewModelAuthorizationActivationService(db)
	handler := NewModelAuthorizationActivationHandler(svc)
	router := gin.New()
	router.POST("/activate", handler.Activate)

	// Mock successful activation: need to mock DB queries for Activate
	// First query: idempotency check -> no rows
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT inventory_hash FROM model_authorization_activations`).WithArgs("idem-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectQuery(`SELECT governance_version FROM channels`).WithArgs(int64(10)).WillReturnRows(sqlmock.NewRows([]string{"governance_version"}).AddRow(int64(3)))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM model_inventory_runs`).WithArgs("hash-1").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO model_authorization_activations`).WithArgs("hash-1", int64(5), sqlmock.AnyArg(), "batch-1", "tester", "idem-1", "unknown").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	body := map[string]any{
		"inventory_hash": "hash-1", "registry_version": 5, "channel_versions": map[int64]int64{10: 3},
		"projected_batch_id": "batch-1", "acknowledged_by": "tester", "idempotency_key": "idem-1",
	}
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/activate", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelAuthorizationActivationHandler_Validation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewModelAuthorizationActivationHandler(service.NewModelAuthorizationActivationService(nil))
	router := gin.New()
	router.POST("/activate", handler.Activate)
	// Missing inventory_hash should be 400/409
	body := map[string]any{
		"registry_version": 5, "channel_versions": map[int64]int64{10: 3},
		"projected_batch_id": "batch-1", "acknowledged_by": "tester", "idempotency_key": "idem-1",
	}
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/activate", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	require.NotEqual(t, http.StatusOK, w.Code)
}

func TestModelAuthorizationActivationHandler_Readiness(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()
	svc := service.NewModelAuthorizationActivationService(db)
	handler := NewModelAuthorizationActivationHandler(svc)

	t.Run("returns evaluated readiness", func(t *testing.T) {
		mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\),0\) FROM model_registry_events`).
			WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(7)))
		mock.ExpectQuery(`SELECT COALESCE\(inventory_hash, ''\) FROM model_inventory_runs`).
			WillReturnRows(sqlmock.NewRows([]string{"inventory_hash"}).AddRow("hash-1"))
		mock.ExpectQuery(`SELECT id, governance_version FROM channels`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "governance_version"}).AddRow(int64(10), int64(3)))
		mock.ExpectQuery(`SELECT batch_id FROM model_publication_events`).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \('anthropic','openai','gemini','grok'\) AND status = 'active'`).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \(.*\) AND status = 'active' AND connection_id IS NULL`).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		router := gin.New()
		router.GET("/readiness", handler.Readiness)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readiness", nil))
		require.Equal(t, http.StatusOK, w.Code)

		var envelope struct {
			Code int                          `json:"code"`
			Data service.ActivationReadiness `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
		require.Equal(t, 0, envelope.Code)
		readiness := envelope.Data
		require.True(t, readiness.Ready)
		require.Equal(t, int64(7), readiness.CurrentRegistryVersion)
		require.Equal(t, "hash-1", readiness.LatestCompletedInventoryHash)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nil service is 500", func(t *testing.T) {
		nilHandler := NewModelAuthorizationActivationHandler(nil)
		router := gin.New()
		router.GET("/readiness", nilHandler.Readiness)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readiness", nil))
		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
