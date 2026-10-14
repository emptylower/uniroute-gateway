//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMediaSupplierUsagePreservesExactDecimalAndMissingZeroDistinction(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want *string
	}{
		{`0`, stringPointer("0")}, {`0.000001`, stringPointer("0.000001")},
		{`123456.123456789012345678`, stringPointer("123456.123456789012345678")},
		{`null`, nil}, {`"0"`, nil}, {`-1`, nil}, {`1e3`, nil}, {`{}`, nil},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"code":200,"data":{"taskId":"original","model":"original-model","state":"success","creditsConsumed":` + tc.raw + `}}`))
			}))
			defer server.Close()
			provider := &kieMediaProvider{upstream: &mediaTestUpstream{client: server.Client()}, baseURL: server.URL}
			result, err := provider.Read(context.Background(), "original", "music")
			require.NoError(t, err)
			require.Equal(t, "original-model", result.ProviderModel)
			require.Equal(t, tc.want, result.CreditsConsumed)
			first := normalizedMediaSuccess(result)
			require.Equal(t, tc.want, first.CreditsConsumed)
			if result.CreditsConsumed != nil {
				*result.CreditsConsumed = "changed"
				require.Equal(t, tc.want, first.CreditsConsumed, "journal normalization must copy the immutable usage")
			}
		})
	}
}

type mediaSupplierUnavailableSnapshots struct{ BillingSnapshotStore }

func (mediaSupplierUnavailableSnapshots) GetBillingSnapshot(context.Context, string) (*BillingSnapshot, error) {
	return nil, errors.New("fixture snapshot unavailable after provider read")
}

func TestMediaSupplierFirstUsageFsyncedBeforePostReadSnapshotOutage(t *testing.T) {
	dir := t.TempDir()
	volume, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "frozen", QuotedUnits: 10000000, ProviderTaskID: "original", Model: "google/gemini-3-8-flash-lite-tts", supplierPricingLoaded: true}
	r.supplierPricing = &FrozenMediaSupplierPricing{PolicyVersion: mediaCostPlusPolicyVersion, CreditUSD: "0.005", MarkupBPS: 2500, BudgetUnits: r.QuotedUnits, SourceVersion: "frozen-source"}
	j, err := openMediaJournal(&walletReaderJournalOwner{dir: dir, host: volume, id: "owner"}, r, false)
	require.NoError(t, err)
	defer j.close()
	r.journal = j
	s := &MediaTaskService{snapshots: &BillingSnapshotService{store: mediaSupplierUnavailableSnapshots{}}}
	credits := "4"
	err = s.completeMediaQuery(context.Background(), r, MediaProviderResult{ProviderTaskID: r.ProviderTaskID, ProviderModel: r.Model, Status: "success", CreditsConsumed: &credits, URLs: []MediaURL{{Kind: "audio", URL: "https://example.test/frozen.mp3"}}})
	require.ErrorContains(t, err, "snapshot unavailable after provider read")
	raw, err := os.ReadFile(j.path)
	require.NoError(t, err)
	var record mediaDurableJournalRecord
	require.NoError(t, json.Unmarshal(raw, &record))
	require.True(t, record.Joined)
	require.Equal(t, "4", *record.SupplierCredits)
	require.Equal(t, "https://example.test/frozen.mp3", record.Success.URLs[0].URL)
}

func TestMediaSupplierFrozenReaderRemainsEnabledAfterProducerRollback(t *testing.T) {
	old := mediaCostPlusPricingMode
	defer func() { mediaCostPlusPricingMode = old }()
	mediaCostPlusPricingMode = "enabled"
	policy := newMediaSupplierPricing("google/gemini-3-8-flash-lite-tts", 10000000)
	raw, err := json.Marshal(BillingSnapshot{RequestedModel: "google/gemini-3-8-flash-lite-tts", BillingModel: "google/gemini-3-8-flash-lite-tts", Pricing: BillingSnapshotPricing{MediaSupplierPricing: policy}})
	require.NoError(t, err)
	var restored BillingSnapshot
	require.NoError(t, json.Unmarshal(raw, &restored))
	mediaCostPlusPricingMode = "disabled"
	require.Nil(t, newMediaSupplierPricing("google/gemini-3-8-flash-lite-tts", 10000000))
	s := &MediaTaskService{snapshots: &BillingSnapshotService{store: &mediaRefusalSnapshots{snapshot: &restored}}}
	credits := "4"
	r := &mediaTaskRecord{Model: "google/gemini-3-8-flash-lite-tts", ProviderTaskID: "original", QuotedUnits: 10000000}
	result := MediaProviderResult{ProviderTaskID: "original", ProviderModel: r.Model, Status: "success", CreditsConsumed: &credits}
	actual, loss, policy, err := s.mediaSuccessAmount(context.Background(), r, result)
	require.NoError(t, err)
	require.EqualValues(t, 2500000, actual)
	require.Zero(t, loss)
	require.NotNil(t, policy)
	result.ProviderModel = "other-model"
	_, _, _, err = s.mediaSuccessAmount(context.Background(), r, result)
	require.ErrorContains(t, err, "model identity")
	result.ProviderModel = r.Model
	result.ProviderTaskID = "other-task"
	_, _, _, err = s.mediaSuccessAmount(context.Background(), r, result)
	require.ErrorContains(t, err, "model identity")
	result.ProviderTaskID = r.ProviderTaskID
	result.CreditsConsumed = nil
	_, _, _, err = s.mediaSuccessAmount(context.Background(), r, result)
	require.ErrorContains(t, err, "usage pending")
	restored.Pricing.MediaSupplierPricing = nil
	actual, _, _, err = s.mediaSuccessAmount(context.Background(), r, result)
	require.NoError(t, err)
	require.Equal(t, r.QuotedUnits, actual)
}

func TestMediaSupplierZeroShareDoesNotRequireNonexistentChargingOutbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	r := &mediaTaskRecord{SnapshotID: "snapshot", Segments: []AuthorizationSegment{
		{AuthorizationID: "positive", EventID: "positive-event", LeaseID: "positive-lease", ActualUnits: 2500000},
		{AuthorizationID: "zero", EventID: "zero-event", LeaseID: "zero-lease", ActualUnits: 0},
	}}
	mock.ExpectQuery("SELECT status,amount_units").WithArgs("positive-event").WillReturnRows(sqlmock.NewRows([]string{"status", "amount_units", "lease_id", "authorization_id", "snapshot_id"}).AddRow("delivered", 2500000, "positive-lease", "positive", "snapshot"))
	s := &MediaTaskService{db: db}
	complete, dead, err := s.mediaSettled(context.Background(), r)
	require.NoError(t, err)
	require.True(t, complete)
	require.False(t, dead)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMediaSupplierCompletedFreeSuccessIsAChargedZeroTerminal(t *testing.T) {
	zero := int64(0)
	r := &mediaTaskRecord{PublicAPI: true, Status: "completed", FinancialState: "released_zero", ActualUnits: &zero, QuotedUnits: 10000000, HeldUnits: 10000000}
	view := mediaTaskView(r)
	require.Equal(t, "success", view.Status)
	require.Equal(t, "charged", view.Billing.State)
	require.Equal(t, "0", *view.Billing.ChargedUSD)
	require.Equal(t, "0.1", *view.Billing.ReleasedUSD)
	require.Equal(t, "0", view.Billing.HeldUSD)
	r.Status, r.FinancialState, r.ActualUnits = "processing", "released_unknown", nil
	view = mediaTaskView(r)
	require.Equal(t, "processing", view.Status)
	require.Equal(t, "released_unknown", view.Billing.State)
	require.Nil(t, view.Billing.ChargedUSD)
}
