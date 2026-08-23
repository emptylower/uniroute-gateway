package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubRegistryService struct {
	version int64
	entries map[string]service.ModelRegistryEntry
	snapshot *service.ModelRegistrySnapshot
	createErr error
}

func (s *stubRegistryService) GetSnapshot(ctx context.Context) (*service.ModelRegistrySnapshot, error) {
	return s.snapshot, nil
}
func (s *stubRegistryService) List(ctx context.Context) ([]service.ModelRegistryEntry, error) {
	out := make([]service.ModelRegistryEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	return out, nil
}
func (s *stubRegistryService) Get(ctx context.Context, canonicalID string) (*service.ModelRegistryEntry, error) {
	if e, ok := s.entries[canonicalID]; ok {
		return &e, nil
	}
	return nil, nil
}
func (s *stubRegistryService) CreateDecision(ctx context.Context, input service.RegistryDecisionInput) (*service.ModelRegistryEntry, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	entry := service.ModelRegistryEntry{
		CanonicalID: input.CanonicalID,
		Provider:    input.Provider,
		Modality:    input.Modality,
		Status:      input.Status,
		Version:     s.version + 1,
		Aliases:     input.Aliases,
		DecidedBy:   input.ActorID,
		EvidenceRef: input.EvidenceRef,
	}
	s.version++
	s.entries[entry.CanonicalID] = entry
	if s.snapshot != nil {
		s.snapshot.Version = s.version
		s.snapshot.Entries[entry.CanonicalID] = entry
		for _, alias := range entry.Aliases {
			s.snapshot.Aliases[alias] = entry.CanonicalID
		}
	}
	return &entry, nil
}
func (s *stubRegistryService) Rebuild(ctx context.Context) error { return nil }

func TestModelRegistryHandler_CreateDecision_RequiresHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &stubRegistryService{entries: map[string]service.ModelRegistryEntry{}, snapshot: &service.ModelRegistrySnapshot{Version: 0, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}}
	handler := NewModelRegistryHandler(svc)

	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
	}{
		{"missing Idempotency-Key", map[string]string{"If-Match": `"0"`}, http.StatusBadRequest},
		{"missing If-Match", map[string]string{"Idempotency-Key": "k1"}, http.StatusBadRequest},
		{"both missing", map[string]string{}, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			body, _ := json.Marshal(map[string]any{"canonical_id": "m1", "provider": "anthropic", "modality": "text", "status": "active"})
			c.Request = httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			for k, v := range tt.headers {
				c.Request.Header.Set(k, v)
			}
			c.Set("platform_assertion", &middleware.PlatformAssertion{Subject: "tester", Scopes: map[string]struct{}{"gateway:admin": {}}})
			handler.CreateDecision(c)
			require.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

func TestModelRegistryHandler_CreateDecision_StaleVersionConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &stubRegistryService{entries: map[string]service.ModelRegistryEntry{}, snapshot: &service.ModelRegistrySnapshot{Version: 0, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}}
	svc.createErr = fmt.Errorf("stale version conflict: expected 0, current 1")
	handler := NewModelRegistryHandler(svc)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, _ := json.Marshal(map[string]any{"canonical_id": "m1", "provider": "anthropic", "modality": "text", "status": "active"})
	c.Request = httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Idempotency-Key", "k1")
	c.Request.Header.Set("If-Match", `"0"`)
	c.Set("platform_assertion", &middleware.PlatformAssertion{Subject: "tester", Scopes: map[string]struct{}{"gateway:admin": {}}})
	handler.CreateDecision(c)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestModelRegistryHandler_CreateDecision_SuccessAndETag(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &stubRegistryService{version: 5, entries: map[string]service.ModelRegistryEntry{}, snapshot: &service.ModelRegistrySnapshot{Version: 5, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}}
	handler := NewModelRegistryHandler(svc)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, _ := json.Marshal(map[string]any{"canonical_id": "m1", "provider": "anthropic", "modality": "text", "status": "active", "aliases": []string{"a1"}})
	c.Request = httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Idempotency-Key", "k1")
	c.Request.Header.Set("If-Match", `"5"`)
	c.Set("platform_assertion", &middleware.PlatformAssertion{Subject: "real-actor", Scopes: map[string]struct{}{"gateway:admin": {}}})
	c.Request.Header.Set("X-Actor-ID", "forged")

	handler.CreateDecision(c)
	require.Equal(t, http.StatusCreated, w.Code)
	require.Equal(t, `"6"`, w.Header().Get("ETag"))
	var envelope struct {
		Code int                         `json:"code"`
		Data service.ModelRegistryEntry `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	resp := envelope.Data
	require.Equal(t, "real-actor", resp.DecidedBy)
	require.NotEqual(t, "forged", resp.DecidedBy)
}

func TestModelRegistryHandler_ActorFromAssertionNotHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &stubRegistryService{entries: map[string]service.ModelRegistryEntry{}, snapshot: &service.ModelRegistrySnapshot{Version: 0, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}}
	handler := NewModelRegistryHandler(svc)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, _ := json.Marshal(map[string]any{"canonical_id": "m1", "provider": "anthropic", "modality": "text", "status": "active"})
	c.Request = httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Idempotency-Key", "k1")
	c.Request.Header.Set("If-Match", `"0"`)
	c.Request.Header.Set("X-Actor-ID", "forged-actor")
	c.Set("platform_assertion", &middleware.PlatformAssertion{Subject: "real-actor", Scopes: map[string]struct{}{"gateway:admin": {}}})

	handler.CreateDecision(c)
	require.Equal(t, http.StatusCreated, w.Code)
	var envelope struct {
		Code int                         `json:"code"`
		Data service.ModelRegistryEntry `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	resp := envelope.Data
	require.Equal(t, "real-actor", resp.DecidedBy)
}

func TestModelRegistryHandler_MissingAssertionRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &stubRegistryService{entries: map[string]service.ModelRegistryEntry{}, snapshot: &service.ModelRegistrySnapshot{Version: 0, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}}
	handler := NewModelRegistryHandler(svc)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, _ := json.Marshal(map[string]any{"canonical_id": "m1", "provider": "anthropic", "modality": "text", "status": "active"})
	c.Request = httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Idempotency-Key", "k1")
	c.Request.Header.Set("If-Match", `"0"`)
	handler.CreateDecision(c)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}
