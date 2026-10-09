//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A successful reader hands selected facts to PG/WAL, then its owning process
// is stopped before RecordUsage or queue dispatch. Recovery runs with the
// feature flag off and must reproduce the frozen fee through real Apply,
// Redis conversion, outbox and Worker D1 settlement exactly once.
func TestExternalWalletReaderCountMatrixRecoversFrozenOriginalFeeAfterRestart(t *testing.T) {
	for _, tc := range []struct {
		name, payload, response string
		family                  service.BillingFamily
		mode                    service.BillingMode
		estimate                service.EstimateInput
		want                    service.SnapshotSettlementInput
	}{
		{"channel-request", `{}`, `{"ok":true}`, service.BillingFamilyOpenAI, service.BillingModePerRequest, service.EstimateInput{}, service.SnapshotSettlementInput{}},
		{"channel-image-count", `{"size":"1K"}`, `{"data":[{"url":"private-output-1"},{"url":"private-output-2"}]}`, service.BillingFamilyOpenAI, service.BillingModePerRequest, service.EstimateInput{}, service.SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K"}},
		{"images-output-size", `{"size":"1K"}`, `{"data":[{"b64_json":"private-bitmap","size":"3840x2160"}]}`, service.BillingFamilyOpenAI, service.BillingModeImage, service.EstimateInput{ImageCount: 1}, service.SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"responses-image-dedup", `{"tools":[{"type":"image_generation","size":"4K"}]}`, "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"image_generation_call\",\"id\":\"same-image\",\"result\":\"private-bitmap\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"response-id\",\"output\":[{\"type\":\"image_generation_call\",\"id\":\"same-image\",\"result\":\"private-bitmap\"}]}}\n\n", service.BillingFamilyOpenAI, service.BillingModeImage, service.EstimateInput{}, service.SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"gemini-one-request", `{"generationConfig":{"imageConfig":{"imageSize":"4K"}}}`, `{"candidates":[{"content":{"parts":[{"inlineData":{"data":"private-bitmap"}}]}}]}`, service.BillingFamilyGeneric, service.BillingModeImage, service.EstimateInput{ImageCount: 1}, service.SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"alpha-no-usage", `{}`, `{"results":[{"title":"private-search"}]}`, service.BillingFamilyOpenAI, service.BillingModeToken, service.EstimateInput{WebSearchCalls: 1}, service.SnapshotSettlementInput{WebSearchCalls: 1}},
		{"alpha-fallback-no-usage", `{"tools":[{"type":"web_search"}]}`, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fallback\",\"output\":[]}}\n\ndata: [DONE]\n\n", service.BillingFamilyOpenAI, service.BillingModeToken, service.EstimateInput{WebSearchCalls: 1}, service.SnapshotSettlementInput{WebSearchCalls: 1}},
		{"grok-submit-duration", `{"resolution":"hd","duration":30}`, `{"data":{"request_id":"video-request"}}`, service.BillingFamilyOpenAI, service.BillingModeVideo, service.EstimateInput{VideoCount: 1, GrokVideo: true}, service.SnapshotSettlementInput{VideoCount: 1, VideoResolution: "720p", VideoDurationSeconds: 15, GrokVideo: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := mediaV5WireFixture(t, "off")
			_, original := f.authorize(t, 1000000, tc.family)
			snapshot := *original
			snapshot.ID = "snapshot-count-v5-" + uuid.NewString()
			snapshot.Family = tc.family
			snapshot.Flags.WalletImmediateReleasePolicyVersion = service.WalletImmediateReleasePolicyVersion
			snapshot.Pricing.Mode = tc.mode
			snapshot.Pricing.Source = service.PricingSourceLiteLLM
			snapshot.Pricing.DefaultPerRequestPrice = .013
			if tc.mode == service.BillingModePerRequest {
				snapshot.Pricing.Source = service.PricingSourceChannel
			}
			image1, image2, image4, video480, video720, video1080, search := .005, .007, .011, .002, .003, .004, .001
			snapshot.Media.ImagePrice = &service.ImagePriceConfig{Price1K: &image1, Price2K: &image2, Price4K: &image4}
			snapshot.Media.VideoPrice = &service.VideoPriceConfig{Price480P: &video480, Price720P: &video720, Price1080P: &video1080}
			snapshot.Media.WebSearchPricePerCall = &search
			snapshot.Multipliers = service.BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}
			require.NoError(t, f.snapshots.Persist(context.Background(), &snapshot))
			f.bridge.Close()
			f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled"
			wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
			f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
			t.Cleanup(f.bridge.Close)
			user, err := f.users.GetByID(context.Background(), f.userID)
			require.NoError(t, err)
			h, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: &snapshot, User: user, FixedEstimateUnits: 100000000, Estimate: tc.estimate})
			require.NoError(t, err)
			require.Len(t, h.Segments, 1)
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.response) }))
			defer provider.Close()
			request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", provider.URL+"/actual-selected-provider", strings.NewReader(tc.payload))
			require.NoError(t, err)
			response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, f.cfg).Do(request, "", 0, 1)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			var normalization, evidence []byte
			require.NoError(t, f.db.QueryRow(`SELECT reader_fee_normalization,reader_evidence FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&normalization, &evidence))
			require.NotContains(t, string(normalization)+string(evidence), "private-")
			var pending int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			require.Zero(t, pending, "the crash boundary precedes the ordinary handler and billing queue")
			f.bridge.Close()
			f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "off"
			f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
			f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, f.db))
			t.Cleanup(f.bridge.Close)
			f.svc = f.newService(t)
			expected, err := (&service.BillingService{}).CalculateCostFromSnapshot(&snapshot, tc.want)
			require.NoError(t, err)
			feeUnits := int64(math.Round(expected.ActualCost * 100000000))
			require.Greater(t, feeUnits, int64(0))
			var applied, canonical, finished bool
			var actualUnits int64
			require.Eventually(t, func() bool {
				queryErr := f.db.QueryRow(`SELECT p.apply_ack_at IS NOT NULL,p.canonical_ack_at IS NOT NULL,a.actual_units,a.state='finished' FROM wallet_billing_pending p JOIN wallet_authorization_segment a ON a.parent_authorization_id=p.parent_authorization_id AND a.ordinal=0 WHERE p.parent_authorization_id=$1`, h.ID).Scan(&applied, &canonical, &actualUnits, &finished)
				return queryErr == nil && applied && canonical && finished && actualUnits == feeUnits
			}, 8*time.Second, 20*time.Millisecond)
			var receipts, counters int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&receipts))
			require.Equal(t, 1, receipts)
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&counters))
			require.Zero(t, counters, "selected positive counts cannot become an unknown zero release")
			base, secret := immediateV5WireConfig(t)
			code, body := immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": f.platformUserID}, true)
			require.Less(t, code, 300, string(body))
			var wire struct {
				Events []struct {
					ID     string `json:"id"`
					Amount string `json:"amountUnits"`
				} `json:"events"`
			}
			require.NoError(t, json.Unmarshal(body, &wire))
			eventID := h.Segments[0].EventID
			matches := 0
			for _, event := range wire.Events {
				if event.ID == eventID {
					matches++
					require.Equal(t, strconv.FormatInt(feeUnits, 10), event.Amount)
				}
			}
			require.Equal(t, 1, matches, "Worker D1 must contain exactly the original frozen charge")
		})
	}
}
