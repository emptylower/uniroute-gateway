//go:build unit

package service

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMediaImmediateFinancialDeadlineUsesEarliestFixedClock(t *testing.T) {
	anchor := time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	for _, kind := range []string{"image", "video", "music", "voice"} {
		t.Run(kind, func(t *testing.T) {
			runtime := anchor.Add(2 * time.Hour)
			if kind == "image" {
				runtime = anchor.Add(30 * time.Minute)
			}
			episode := anchor.Add(15 * time.Minute)
			r := &mediaTaskRecord{MediaType: kind, AcceptedAt: &anchor, FinancialRuntimeDeadline: &runtime, QueryUncertaintyDeadline: &episode}
			require.Equal(t, episode, *mediaFinancialDeadline(r))
			r.QueryUncertaintyDeadline = nil
			require.Equal(t, runtime, *mediaFinancialDeadline(r))
			later := anchor.Add(3 * time.Hour)
			r.QueryUncertaintyDeadline = &later
			require.Equal(t, runtime, *mediaFinancialDeadline(r))
		})
	}
}
func TestMediaImmediateRecognizedSameIDRequired(t *testing.T) {
	r := &mediaTaskRecord{ProviderTaskID: "original"}
	for _, status := range []string{"pending", "processing", "success", "failed"} {
		require.True(t, mediaTrustedResult(r, MediaProviderResult{ProviderTaskID: "original", Status: status}))
	}
	for _, result := range []MediaProviderResult{{ProviderTaskID: "wrong", Status: "success"}, {ProviderTaskID: "original", Status: "future_state"}, {Status: "success"}} {
		require.False(t, mediaTrustedResult(r, result))
	}
}
func TestMediaImmediateProviderUnknownStateAndQueryErrorsAreNotZero(t *testing.T) {
	for _, body := range []string{`{"code":200,"data":{"taskId":"vendor","state":"mystery"}}`, `{"code":200,"data":{"taskId":"different","state":"fail"}}`, `{"code":404}`, `{"code":422}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		cfg := &config.Config{}
		cfg.CanonicalWallet.Mode = "enforce"
		provider := &kieMediaProvider{upstream: NewAuthorizingHTTPUpstream(&mediaTestUpstream{client: server.Client()}, cfg), baseURL: server.URL}
		result, err := provider.Read(context.Background(), "vendor", "image")
		require.Error(t, err)
		require.Empty(t, result.Status)
		server.Close()
	}
}
func TestMediaImmediateReleasedFinancialViewKeepsGeneration(t *testing.T) {
	r := &mediaTaskRecord{ID: "task", Status: "processing", FinancialState: "released_unknown", QuotedUnits: 2730000, HeldUnits: 2730000}
	view := mediaTaskView(r)
	require.Equal(t, "processing", view.Status)
	require.Equal(t, "released", view.Billing.State)
	require.Equal(t, "0.0273", *view.Billing.ReleasedUSD)
	require.Nil(t, view.Billing.ChargedUSD)
}
