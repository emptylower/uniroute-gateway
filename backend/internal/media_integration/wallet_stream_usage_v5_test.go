//go:build media_integration

package media_integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// The events a Responses API stream sends before its terminal one. Every one
// carries usage:null; only response.completed reports numbers.
const walletStreamPrelude = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_stream_v5\",\"status\":\"in_progress\",\"usage\":null}}\n\n" +
	"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_stream_v5\",\"status\":\"in_progress\",\"usage\":null}}\n\n"

const walletStreamCompleted = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
	"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_v5\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n"

func (x fundingV7Fixture) streamRequest(t *testing.T, h *service.AuthorizationHandle, provider *httptest.Server) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1","stream":true}`))
	require.NoError(t, err)
	response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, x.f.cfg).Do(request, "", 0, 1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	return response
}

// A complete SSE response whose early events say usage:null and whose terminal
// event carries strict numbers is a trusted positive fee: it is charged exactly
// once and leaves no fee/evidence barrier behind. Before the parser treated
// usage:null as "not reported", that null latched Malformed, the reader evidence
// was never trusted, and the hold stayed armed with the user never charged.
func TestExternalWalletCompletedSSEWithNullUsageEventsIsChargedOnceAndReleased(t *testing.T) {
	x := newFundingV7Fixture(t)
	billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	h := x.authorize(t, 200000000)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, walletStreamPrelude)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, walletStreamCompleted)
	}))
	defer provider.Close()
	response := x.streamRequest(t, h, provider)
	_, err := io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	var usageErr error
	dispatch, err := h.PrepareUsageTask(context.Background(), func(ctx context.Context) {
		usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "stream-null-usage-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	require.NoError(t, usageErr, "a strictly valid terminal usage must not trip the positive-fee evidence barrier")
	require.NoError(t, err)
	require.NotNil(t, dispatch)
	dispatch(context.Background())
	var fee int64
	var applied bool
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT fee_units,apply_ack_at IS NOT NULL FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&fee, &applied) == nil && fee == 80000000 && applied
	}, 5*time.Second, 20*time.Millisecond, "the trusted terminal fee is staged and applied")
	var feePending, evidencePending bool
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT fee_pending,evidence_pending FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&feePending, &evidencePending) == nil && !feePending && !evidencePending
	}, 5*time.Second, 20*time.Millisecond, "no barrier may outlive a trusted fee")
	var charges, unknown int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units=80000000`, h.ID).Scan(&charges))
	require.Equal(t, 1, charges)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&unknown))
	require.Zero(t, unknown, "a charged request is not an unknown-cost release")
	require.Eventually(t, func() bool {
		hold, holdErr := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, h.ID)
		return holdErr == nil && hold.State != "armed"
	}, 8*time.Second, 20*time.Millisecond, "the hold is settled, not left armed")
}

// The client leaves after the first events, before any numbers exist. Only
// usage:null was seen, so nothing positive was observed and the hold must be
// released as an unknown cost right away (platform bears it), not frozen.
func TestExternalWalletAbortedSSEWithNullUsageEventsReleasesAsUnknown(t *testing.T) {
	x := newFundingV7Fixture(t)
	h := x.authorize(t, 200000000)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, walletStreamPrelude)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer provider.Close()
	response := x.streamRequest(t, h, provider)
	buffer := make([]byte, 64)
	_, err := io.ReadAtLeast(response.Body, buffer, 1)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	// The recovery lane of a freshly started service owns reader handoffs that no
	// request goroutine will finish.
	x.f.svc = x.f.newService(t)
	var feePending bool
	var unknown int
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT fee_pending FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&feePending) == nil &&
			x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&unknown) == nil && unknown == 1
	}, 20*time.Second, 50*time.Millisecond, "the unfinished stream is released as one unknown-cost event")
	require.False(t, feePending, "no usage was observed, so there is no fee barrier")
	hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, h.ID)
	require.NoError(t, err)
	require.NotEqual(t, "armed", hold.State)
	var charges int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&charges))
	require.Zero(t, charges, "an unknown cost is not charged")
}
