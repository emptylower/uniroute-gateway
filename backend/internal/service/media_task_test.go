//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestMediaCatalogExactOptionPricing(t *testing.T) {
	prices := map[string]map[string]string{
		"nano-banana-2-lite": {"1:1": "0.0273", "16:9": "0.0273", "9:16": "0.0273"}, "google/imagen4-fast": {"1:1": "0.02", "16:9": "0.02"}, "google/nano-banana": {"1:1": "0.0273"}, "nano-banana-pro": {"1:1": "0.0938"}, "bytedance/seedream": {"1024x1024": "0.0245", "1280x720": "0.0245"}, "flux-2/pro-text-to-image": {"1K:16:9": "0.03", "2K:1:1": "0.12"}, "bytedance/seedance-2-mini": {"5": "0.1075", "10": "0.215"}, "grok-imagine/text-to-video": {"6": "0.21"}, "hailuo/02-text-to-video-standard": {"6": "0.1962"}, "veo-3-1": {"4": "0.14", "6": "0.21", "8": "0.28"}, "ai-music-api/generate": {"pop, upbeat": "0.084", "lofi, chill, mellow": "0.084", "cinematic, orchestral, epic": "0.084", "electronic, synth, dance": "0.084", "acoustic, folk, warm": "0.084", "jazz, smooth, brass": "0.084"},
	}
	require.Len(t, MediaTaskCatalog(), 11+len(extendedMediaModels()))
	for model, options := range prices {
		for option, price := range options {
			t.Run(model+"/"+option, func(t *testing.T) {
				quote, err := mediaQuote(model, option)
				require.NoError(t, err)
				require.Equal(t, price, quote.QuotedUSD)
				_, _, units, input, err := normalizeMediaCreate(MediaCreateInput{Model: model, Option: option, Prompt: "  a colorful garden  "})
				require.NoError(t, err)
				require.Equal(t, "a colorful garden", input["prompt"])
				require.Equal(t, price, mediaUSD(units))
				require.Greater(t, units, int64(0))
			})
		}
	}
}
func TestMediaRejectsUntrustedInputs(t *testing.T) {
	for _, in := range []MediaCreateInput{{Model: "unknown", Prompt: "hello"}, {Model: "google/nano-banana", Prompt: "ok"}, {Model: "google/nano-banana", Prompt: strings.Repeat("x", 2001)}, {Model: "google/nano-banana", Prompt: "hello", Option: "4:3"}, {Model: "google/nano-banana", Prompt: "hello", MediaType: "video"}} {
		_, _, _, _, err := normalizeMediaCreate(in)
		require.Error(t, err)
	}
	_, _, _, input, err := normalizeMediaCreate(MediaCreateInput{Model: "flux-2/pro-text-to-image", Prompt: "hello", Option: "2K:1:1"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"prompt": "hello", "resolution": "2K", "aspect_ratio": "1:1"}, input)
	_, _, _, input, err = normalizeMediaCreate(MediaCreateInput{Model: "veo-3-1", Prompt: "hello", Option: "8"})
	require.NoError(t, err)
	require.Equal(t, 8, input["duration"])
}

type mediaTestUpstream struct {
	HTTPUpstream
	client *http.Client
	err    error
}

func (p *mediaTestUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.client.Do(req)
}
func TestMediaProviderNeverTreatsUnknownCreateAsRefundable(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		body     string
		rejected bool
	}{{"accepted", 200, `{"code":200,"data":{"taskId":"vendor-1"}}`, false}, {"malformed", 200, `{"code":200,"data":{}}`, false}, {"serverfailure", 500, `{"code":500}`, false}, {"applicationfailure", 200, `{"code":500}`, false}, {"timeout", 408, `{"code":408}`, false}, {"validation", 422, `{"code":422}`, true}, {"explicitapplicationreject", 200, `{"code":422}`, true}} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v1/jobs/createTask", r.URL.Path)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.CanonicalWallet.Mode = "enforce"
			provider := &kieMediaProvider{upstream: NewAuthorizingHTTPUpstream(&mediaTestUpstream{client: server.Client()}, cfg), baseURL: server.URL, apiKey: "test"}
			handle, _ := newAuthorizationHandle("enforce")
			id, rejected, err := provider.Create(context.Background(), "model", map[string]any{"prompt": "test"}, handle)
			require.Equal(t, test.rejected, rejected)
			if test.name == "accepted" {
				require.NoError(t, err)
				require.Equal(t, "vendor-1", id)
			} else {
				require.Error(t, err)
			}
			require.Len(t, handle.Writes(), 1)
		})
	}
	provider := &kieMediaProvider{upstream: &mediaTestUpstream{err: context.DeadlineExceeded}, baseURL: "https://api.kie.ai"}
	_, rejected, err := provider.Create(context.Background(), "model", nil, &AuthorizationHandle{})
	require.Error(t, err)
	require.False(t, rejected)
}
func TestMediaProviderTerminalMusicAndDeliveryParsing(t *testing.T) {
	for _, test := range []struct {
		kind, body, status string
		urls               int
	}{{"image", `{"taskId":"vendor","state":"success","resultJson":"{\"resultUrls\":[\"https://example.com/image.png\",\"javascript:evil\"]}"}`, "success", 1}, {"video", `{"taskId":"vendor","state":"success","resultJson":"badjson"}`, "success", 0}, {"music", `{"taskId":"vendor","state":"success","response":{"data":[{"audio_url":"https://example.com/a.mp3","title":"Song"},{"stream_audio_url":"https://example.com/b.mp3"}]}}`, "success", 2}, {"image", `{"taskId":"vendor","state":"fail","failCode":"ERROR","failMsg":"failed"}`, "failed", 0}} {
		t.Run(test.kind+test.status+test.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "vendor", r.URL.Query().Get("taskId"))
				_, _ = w.Write([]byte(`{"code":200,"data":` + test.body + `}`))
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.CanonicalWallet.Mode = "enforce"
			provider := &kieMediaProvider{upstream: NewAuthorizingHTTPUpstream(&mediaTestUpstream{client: server.Client()}, cfg), baseURL: server.URL}
			result, err := provider.Read(context.Background(), "vendor", test.kind)
			require.NoError(t, err)
			require.Equal(t, test.status, result.Status)
			require.Len(t, result.URLs, test.urls)
		})
	}
}
func TestMediaTaskViewNeverClaimsRefundBeforeReleaseACK(t *testing.T) {
	actual := int64(19656000)
	r := &mediaTaskRecord{ID: "media_test", QuotedUnits: actual, HeldUnits: actual, ActualUnits: &actual, Status: "settling", CreatedAt: time.Now()}
	v := mediaTaskView(r)
	require.Equal(t, "success", v.Status)
	require.Equal(t, "settling", v.Billing.State)
	require.Nil(t, v.Billing.ChargedUSD)
	require.Nil(t, v.Billing.ReleasedUSD)
	r.Status = "indeterminate"
	v = mediaTaskView(r)
	require.Equal(t, "indeterminate", v.Billing.State)
	require.Nil(t, v.Billing.ReleasedUSD)
	r.Status = "completed"
	v = mediaTaskView(r)
	require.Equal(t, "0.19656", *v.Billing.ChargedUSD)
	r.Status = "releasing"
	v = mediaTaskView(r)
	require.Equal(t, "settling", v.Billing.State)
	require.Nil(t, v.Billing.ReleasedUSD)
	r.Status = "failed"
	v = mediaTaskView(r)
	require.Equal(t, "0.19656", *v.Billing.ReleasedUSD)
}
func TestMediaWalletUnitsStrictStrings(t *testing.T) {
	for _, raw := range []string{"", "-1", "1.5", " 1", "01", "9223372036854775808"} {
		_, err := strictAvailabilityUnits(raw)
		require.Error(t, err)
	}
	value, err := strictAvailabilityUnits("9007199254740993")
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), value)
	raw, _ := json.Marshal(WalletAvailabilityLease{FreeUnits: "9007199254740993"})
	require.Contains(t, string(raw), `"free_units":"9007199254740993"`)
}
func TestMediaCanonicalRemainingUsesRawReleasedAndSealing(t *testing.T) {
	l := CanonicalWalletLease{BudgetUnits: 100, ConsumedUnits: 150, ReleasedUnits: 75}
	require.Equal(t, int64(25), l.RemainingUnits())
	l.Sealed = true
	require.Zero(t, l.RemainingUnits())
}
