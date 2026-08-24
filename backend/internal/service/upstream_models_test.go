package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/stretchr/testify/require"
)

type codexModelsManifestFetcherStub struct {
	manifest *CodexModelsManifest
	err      error
	account  *Account
}

type accountModelDiscoveryStoreStub struct {
	accountID int64
	mapping   map[string]any
	discovery map[string]any
	err       error
}

type modelObservationRepositoryStub struct {
	inputs    []DiscoveryBatchInput
	err       error
	batchID   string
	callOrder *[]string
}

func (s *modelObservationRepositoryStub) RecordDiscovery(_ context.Context, input DiscoveryBatchInput) (string, error) {
	s.inputs = append(s.inputs, input)
	if s.callOrder != nil {
		*s.callOrder = append(*s.callOrder, "observation")
	}
	return s.batchID, s.err
}

func (s *modelObservationRepositoryStub) DeleteBatch(_ context.Context, batchID string) error {
	return nil
}

type orderedAccountModelDiscoveryStoreStub struct {
	accountModelDiscoveryStoreStub
	callOrder *[]string
}

func (s *orderedAccountModelDiscoveryStoreStub) UpdateModelDiscovery(ctx context.Context, accountID int64, mapping map[string]any, discovery map[string]any) error {
	if s.callOrder != nil {
		*s.callOrder = append(*s.callOrder, "credentials")
	}
	return s.accountModelDiscoveryStoreStub.UpdateModelDiscovery(ctx, accountID, mapping, discovery)
}

func (s *accountModelDiscoveryStoreStub) UpdateModelDiscovery(_ context.Context, accountID int64, mapping map[string]any, discovery map[string]any) error {
	s.accountID = accountID
	s.mapping = mapping
	s.discovery = discovery
	return s.err
}

func (s *codexModelsManifestFetcherStub) FetchCodexModelsManifest(_ context.Context, account *Account, _, _ string) (*CodexModelsManifest, error) {
	s.account = account
	return s.manifest, s.err
}

func upstreamModelSyncTestConfig() *config.Config {
	return &config.Config{
		Security: config.SecurityConfig{
			URLAllowlist: config.URLAllowlistConfig{Enabled: false},
		},
	}
}

func grokOAuthModelSyncTestAccount(baseURL string) *Account {
	credentials := map[string]any{
		"access_token":  "oauth-access-token",
		"refresh_token": "oauth-refresh-token",
		"expires_at":    time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		"sub":           "grok-user-id",
		"email":         "grok-user@example.com",
	}
	if strings.TrimSpace(baseURL) != "" {
		credentials["base_url"] = baseURL
	}
	return &Account{
		ID:          10,
		Platform:    PlatformGrok,
		Type:        AccountTypeOAuth,
		Credentials: credentials,
	}
}

func TestBuildV1ModelsURL(t *testing.T) {
	t.Parallel()

	require.Equal(t, "https://api.anthropic.com/v1/models", buildV1ModelsURL("https://api.anthropic.com"))
	require.Equal(t, "https://api.anthropic.com/v1/models", buildV1ModelsURL("https://api.anthropic.com/v1"))
	require.Equal(t, "https://api.anthropic.com/v1/models", buildV1ModelsURL("https://api.anthropic.com/v1/models"))
	require.Equal(t, "https://gateway.example.com/antigravity/v1/models", buildV1ModelsURL("https://gateway.example.com/antigravity/"))
}

func TestBuildOpenAIModelsURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base string
		want string
	}{
		{
			name: "zhipu v4 coding base url",
			base: "https://open.bigmodel.cn/api/coding/paas/v4",
			want: "https://open.bigmodel.cn/api/coding/paas/v4/models",
		},
		{
			name: "openai v1 base url",
			base: "https://api.openai.com/v1",
			want: "https://api.openai.com/v1/models",
		},
		{
			name: "models url unchanged",
			base: "https://api.openai.com/v1/models",
			want: "https://api.openai.com/v1/models",
		},
		{
			name: "host fallback uses v1",
			base: "https://api.openai.com",
			want: "https://api.openai.com/v1/models",
		},
		{
			name: "trailing slash on v4",
			base: "https://open.bigmodel.cn/api/coding/paas/v4/",
			want: "https://open.bigmodel.cn/api/coding/paas/v4/models",
		},
		{
			name: "v2 base url",
			base: "https://gateway.example.com/openai/v2",
			want: "https://gateway.example.com/openai/v2/models",
		},
		{
			name: "v3 base url",
			base: "https://gateway.example.com/openai/v3",
			want: "https://gateway.example.com/openai/v3/models",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, buildOpenAIModelsURL(tt.base))
		})
	}
}

func TestBuildGeminiModelsURL(t *testing.T) {
	t.Parallel()

	require.Equal(t, "https://generativelanguage.googleapis.com/v1beta/models", buildGeminiModelsURL("https://generativelanguage.googleapis.com"))
	require.Equal(t, "https://generativelanguage.googleapis.com/v1beta/models", buildGeminiModelsURL("https://generativelanguage.googleapis.com/v1beta"))
	require.Equal(t, "https://generativelanguage.googleapis.com/v1beta/models", buildGeminiModelsURL("https://generativelanguage.googleapis.com/v1beta/models"))
}

func TestExtractUpstreamModelIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "codex manifest slugs",
			body: `{"models":[{"slug":"gpt-5.6-sol"},{"slug":"gpt-5.6-terra"},{"slug":"gpt-5.6-sol"}]}`,
			want: []string{"gpt-5.6-sol", "gpt-5.6-terra"},
		},
		{
			name: "openai and anthropic data array",
			body: `{"data":[{"id":"claude-sonnet-4-5"},{"id":"gpt-5"},{"id":"gpt-5"},{"id":""}]}`,
			want: []string{"claude-sonnet-4-5", "gpt-5"},
		},
		{
			name: "gemini models array strips prefix",
			body: `{"models":[{"name":"models/gemini-2.5-pro"},{"name":"gemini-2.5-flash"}]}`,
			want: []string{"gemini-2.5-flash", "gemini-2.5-pro"},
		},
		{
			name: "top level array",
			body: `[{"id":"z-model"},{"name":"models/a-model"}]`,
			want: []string{"a-model", "z-model"},
		},
		{
			name: "standard id wins over provider-specific model field",
			body: `{"data":[{"id":"canonical-id","model":"display-model"}]}`,
			want: []string{"canonical-id"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := extractUpstreamModelIDs([]byte(tt.body))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFetchUpstreamSupportedModelsUsesCodexManifestForOpenAIOAuth(t *testing.T) {
	t.Parallel()

	fetcher := &codexModelsManifestFetcherStub{manifest: &CodexModelsManifest{
		Body: []byte(`{"models":[{"slug":"gpt-5.6-terra"},{"slug":"gpt-5.6-sol"}]}`),
	}}
	svc := &AccountTestService{codexModelsFetcher: fetcher}
	account := &Account{
		ID:       17,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":       "oauth-token",
			"chatgpt_account_id": "chatgpt-account",
		},
	}

	models, err := svc.FetchUpstreamSupportedModels(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-5.6-sol", "gpt-5.6-terra"}, models)
	require.Same(t, account, fetcher.account)
}

func TestFetchUpstreamSupportedModelsOpenAIOAuthDoesNotFallBackOnManifestFailure(t *testing.T) {
	t.Parallel()

	fetcher := &codexModelsManifestFetcherStub{err: errors.New("upstream unavailable")}
	svc := &AccountTestService{codexModelsFetcher: fetcher}
	_, err := svc.FetchUpstreamSupportedModels(context.Background(), &Account{
		ID:       18,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
	})
	require.Error(t, err)

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUpstream, syncErr.Kind)
}

func TestFetchUpstreamSupportedModelsRejectsCredentialShadow(t *testing.T) {
	t.Parallel()

	parentID := int64(17)
	fetcher := &codexModelsManifestFetcherStub{manifest: &CodexModelsManifest{
		Body: []byte(`{"models":[{"slug":"gpt-5.6-sol"}]}`),
	}}
	svc := &AccountTestService{codexModelsFetcher: fetcher}
	_, err := svc.FetchUpstreamSupportedModels(context.Background(), &Account{
		ID:              18,
		Platform:        PlatformOpenAI,
		Type:            AccountTypeOAuth,
		ParentAccountID: &parentID,
		QuotaDimension:  QuotaDimensionSpark,
	})
	require.Error(t, err)

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUnsupported, syncErr.Kind)
	require.Nil(t, fetcher.account, "shadow discovery must not fetch the parent manifest")
}

func TestPersistDiscoveredModelsReplacesAutomaticSnapshotAndPreservesAliases(t *testing.T) {
	t.Parallel()

	store := &accountModelDiscoveryStoreStub{}
	svc := &AccountTestService{
		modelDiscoveryStore:        store,
		modelObservationRepository: &modelObservationRepositoryStub{batchID: "batch-existing-behavior"},
	}
	account := &Account{
		ID: 19,
		Credentials: map[string]any{
			"model_mapping": map[string]any{
				"old-auto": "old-auto",
				"alias":    "upstream-alias-target",
				"gpt-*":    "gpt-5.6",
			},
		},
	}
	syncedAt := time.Date(2026, time.August, 17, 12, 30, 0, 0, time.UTC)

	err := svc.PersistDiscoveredModels(context.Background(), account, []string{"gpt-5.6-terra", "gpt-5.6-sol"}, syncedAt)
	require.NoError(t, err)
	require.Equal(t, int64(19), store.accountID)
	require.Equal(t, map[string]any{
		"alias":         "upstream-alias-target",
		"gpt-*":         "gpt-5.6",
		"gpt-5.6-sol":   "gpt-5.6-sol",
		"gpt-5.6-terra": "gpt-5.6-terra",
	}, store.mapping)
	require.Equal(t, map[string]any{
		"source":             "upstream",
		"models":             []string{"gpt-5.6-sol", "gpt-5.6-terra"},
		"schedulable_models": []string{"gpt-5.6-sol", "gpt-5.6-terra"},
		"synced_at":          "2026-08-17T12:30:00Z",
	}, store.discovery)
	require.Contains(t, account.GetModelMapping(), "old-auto", "persistence must not mutate the loaded account")
}

func TestPersistDiscoveredModelsRecordsAcceptedObservationBeforeExistingCredentialSync(t *testing.T) {
	t.Parallel()

	callOrder := []string{}
	store := &orderedAccountModelDiscoveryStoreStub{callOrder: &callOrder}
	observations := &modelObservationRepositoryStub{batchID: "batch-accepted", callOrder: &callOrder}
	svc := &AccountTestService{
		modelDiscoveryStore:        store,
		modelObservationRepository: observations,
	}
	syncedAt := time.Date(2026, time.August, 18, 9, 10, 11, 0, time.FixedZone("offset", 2*60*60))
	account := &Account{ID: 41, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	err := svc.PersistDiscoveredModels(context.Background(), account, []string{"Vendor/Model:Latest", "gpt-5.6-sol"}, syncedAt)
	require.NoError(t, err)
	require.Equal(t, []string{"observation", "credentials"}, callOrder)
	require.Len(t, observations.inputs, 1)

	input := observations.inputs[0]
	require.Equal(t, int64(41), input.AccountID)
	require.Nil(t, input.ConnectionID)
	require.NotNil(t, input.AccountProvider)
	require.Equal(t, GovernanceProvider("openai"), *input.AccountProvider)
	require.Equal(t, PlatformOpenAI, input.RoutingPlatform)
	require.Equal(t, []string{"Vendor/Model:Latest", "gpt-5.6-sol"}, input.ModelIDs)
	require.Equal(t, syncedAt.UTC(), input.ObservedAt)
	require.NotEmpty(t, input.IdempotencyKey)

	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(input.RawSnapshot, &snapshot))
	payload, ok := snapshot["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, []any{"Vendor/Model:Latest", "gpt-5.6-sol"}, payload["models"])
	responseMetadata, ok := snapshot["response"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "legacy_call", responseMetadata["source"])
}

func TestPersistDiscoveredModelsPreservesExactNonEmptyUpstreamModelIDs(t *testing.T) {
	t.Parallel()

	store := &accountModelDiscoveryStoreStub{}
	observations := &modelObservationRepositoryStub{batchID: "batch-exact-ids"}
	svc := &AccountTestService{
		modelDiscoveryStore:        store,
		modelObservationRepository: observations,
	}

	rawSnapshot := []byte(`{"payload":{"data":[{"id":" Vendor/Model:Latest "},{"id":"Vendor/Model:Latest"},{"id":" Vendor/Model:Latest "}]},"response":{"source":"http","status_code":200}}`)
	err := svc.PersistUpstreamModelDiscovery(context.Background(), &Account{
		ID:       43,
		Platform: PlatformOpenAI,
	}, UpstreamModelDiscovery{
		Models:           []string{"Vendor/Model:Latest"},
		EvidenceModelIDs: []string{" Vendor/Model:Latest ", "Vendor/Model:Latest", " Vendor/Model:Latest "},
		RawSnapshot:      rawSnapshot,
	}, time.Date(2026, time.August, 18, 9, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, []string{" Vendor/Model:Latest ", "Vendor/Model:Latest"}, observations.inputs[0].ModelIDs)
	require.Equal(t, rawSnapshot, observations.inputs[0].RawSnapshot)
	require.Equal(t, map[string]any{"Vendor/Model:Latest": "Vendor/Model:Latest"}, store.mapping,
		"evidence exactness must not change legacy mapping normalization")
}

func TestPersistDiscoveredModelsPreservesLongExactIDThroughGovernanceAndLegacyUpdate(t *testing.T) {
	t.Parallel()

	callOrder := []string{}
	store := &orderedAccountModelDiscoveryStoreStub{callOrder: &callOrder}
	observations := &modelObservationRepositoryStub{batchID: "batch-long-id", callOrder: &callOrder}
	svc := &AccountTestService{
		modelDiscoveryStore:        store,
		modelObservationRepository: observations,
	}
	modelID := "vendor/" + strings.Repeat("exact-model-segment-", 16)

	err := svc.PersistDiscoveredModels(context.Background(), &Account{
		ID: 46, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
	}, []string{modelID}, time.Date(2026, time.August, 19, 14, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Greater(t, len(modelID), 255)
	require.Equal(t, []string{"observation", "credentials"}, callOrder)
	require.Equal(t, []string{modelID}, observations.inputs[0].ModelIDs)
	require.Equal(t, map[string]any{modelID: modelID}, store.mapping)
	require.Equal(t, []string{modelID}, store.discovery["models"])
}

func TestPersistUpstreamModelDiscoveryCanonicalizesTimeBeforeIdempotencyAndRepositoryInput(t *testing.T) {
	t.Parallel()

	observations := &modelObservationRepositoryStub{batchID: "batch-canonical-time"}
	svc := &AccountTestService{
		modelDiscoveryStore:        &accountModelDiscoveryStoreStub{},
		modelObservationRepository: observations,
	}
	account := &Account{ID: 47, Platform: PlatformOpenAI}
	discovery := UpstreamModelDiscovery{
		Models:           []string{"model-a"},
		EvidenceModelIDs: []string{"model-a"},
		RawSnapshot:      []byte(`{"catalog":"canonical-time"}`),
	}
	base := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.FixedZone("offset", 2*60*60))

	require.NoError(t, svc.PersistUpstreamModelDiscovery(context.Background(), account, discovery, base.Add(100*time.Nanosecond)))
	require.NoError(t, svc.PersistUpstreamModelDiscovery(context.Background(), account, discovery, base.Add(200*time.Nanosecond)))
	require.Len(t, observations.inputs, 2)
	require.Equal(t, time.Date(2026, time.August, 19, 10, 0, 0, 0, time.UTC), observations.inputs[0].ObservedAt)
	require.Equal(t, observations.inputs[0].ObservedAt, observations.inputs[1].ObservedAt)
	require.Equal(t, observations.inputs[0].IdempotencyKey, observations.inputs[1].IdempotencyKey)
}

func TestFetchUpstreamModelDiscoveryPreservesHTTPPayloadMetadataAndExactIDs(t *testing.T) {
	t.Parallel()

	body := []byte(`{"object":"list","data":[{"id":" model/A "},{"id":"model/A"},{"id":" model/A "}],"metadata":{"cursor":"next"}}`)
	svc := &AccountTestService{
		httpUpstream: &httpUpstreamStub{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
				"Etag":         []string{`W/"http-etag"`},
			},
			Body: io.NopCloser(strings.NewReader(string(body))),
		}},
		cfg: upstreamModelSyncTestConfig(),
	}

	discovery, err := svc.FetchUpstreamModelDiscovery(context.Background(), &Account{
		ID:       44,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key": "key",
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"model/A"}, discovery.Models)
	require.Equal(t, []string{" model/A ", "model/A", " model/A "}, discovery.EvidenceModelIDs)

	var snapshot struct {
		PayloadBase64 string `json:"payload_base64"`
		Response      struct {
			Source      string `json:"source"`
			StatusCode  int    `json:"status_code"`
			ContentType string `json:"content_type"`
			ETag        string `json:"etag"`
		} `json:"response"`
	}
	require.NoError(t, json.Unmarshal(discovery.RawSnapshot, &snapshot))
	decodedPayload, err := base64.StdEncoding.DecodeString(snapshot.PayloadBase64)
	require.NoError(t, err)
	require.Equal(t, body, decodedPayload)
	require.Equal(t, "http", snapshot.Response.Source)
	require.Equal(t, http.StatusOK, snapshot.Response.StatusCode)
	require.Equal(t, "application/json", snapshot.Response.ContentType)
	require.Equal(t, `W/"http-etag"`, snapshot.Response.ETag)
}

func TestFetchUpstreamModelDiscoveryPreservesManifestPayloadMetadataAndExactIDs(t *testing.T) {
	t.Parallel()

	body := []byte(`{"models":[{"slug":" gpt-exact "},{"slug":"gpt-exact"},{"slug":" gpt-exact "}],"metadata":{"source":"codex"}}`)
	fetcher := &codexModelsManifestFetcherStub{manifest: &CodexModelsManifest{Body: body, ETag: `W/"manifest-etag"`}}
	svc := &AccountTestService{codexModelsFetcher: fetcher}

	discovery, err := svc.FetchUpstreamModelDiscovery(context.Background(), &Account{
		ID:       45,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-exact"}, discovery.Models)
	require.Equal(t, []string{" gpt-exact ", "gpt-exact", " gpt-exact "}, discovery.EvidenceModelIDs)

	var snapshot struct {
		PayloadBase64 string          `json:"payload_base64"`
		Response      json.RawMessage `json:"response"`
	}
	require.NoError(t, json.Unmarshal(discovery.RawSnapshot, &snapshot))
	decodedPayload, err := base64.StdEncoding.DecodeString(snapshot.PayloadBase64)
	require.NoError(t, err)
	require.Equal(t, body, decodedPayload)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(snapshot.Response, &metadata))
	require.Equal(t, "manifest", metadata["source"])
	require.Equal(t, `W/"manifest-etag"`, metadata["etag"])
}

func TestPersistUpstreamModelDiscoveryRecordsEmptyCatalogBeforeLegacyFailure(t *testing.T) {
	t.Parallel()

	callOrder := []string{}
	store := &orderedAccountModelDiscoveryStoreStub{callOrder: &callOrder}
	observations := &modelObservationRepositoryStub{batchID: "empty-batch", callOrder: &callOrder}
	svc := &AccountTestService{modelDiscoveryStore: store, modelObservationRepository: observations}
	rawSnapshot := []byte(`{"payload":{"data":[]},"response":{"source":"http","status_code":200}}`)

	err := svc.PersistUpstreamModelDiscovery(context.Background(), &Account{
		ID:       46,
		Platform: PlatformOpenAI,
	}, UpstreamModelDiscovery{RawSnapshot: rawSnapshot}, time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC))
	require.Error(t, err)
	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUpstream, syncErr.Kind)
	require.Equal(t, []string{"observation"}, callOrder)
	require.Len(t, observations.inputs, 1)
	require.Empty(t, observations.inputs[0].ModelIDs)
	require.Equal(t, rawSnapshot, observations.inputs[0].RawSnapshot)
	require.Zero(t, store.accountID, "empty legacy catalog must not modify mappings")
}

func TestPersistDiscoveredModelsRecordsRoutingOnlyPlatformsWithoutInferringProvider(t *testing.T) {
	t.Parallel()

	for _, platform := range []string{PlatformAntigravity, PlatformComposite, "future-router"} {
		platform := platform
		t.Run(platform, func(t *testing.T) {
			t.Parallel()

			store := &accountModelDiscoveryStoreStub{}
			observations := &modelObservationRepositoryStub{batchID: "batch-routing-only"}
			svc := &AccountTestService{
				modelDiscoveryStore:        store,
				modelObservationRepository: observations,
			}
			err := svc.PersistDiscoveredModels(context.Background(), &Account{
				ID:       50,
				Platform: platform,
			}, []string{"claude-through-router"}, time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC))
			require.NoError(t, err)
			require.Len(t, observations.inputs, 1)
			require.Nil(t, observations.inputs[0].AccountProvider)
			require.Equal(t, platform, observations.inputs[0].RoutingPlatform)
		})
	}
}

func TestPersistDiscoveredModelsDoesNotReportSuccessWhenObservationPersistenceFails(t *testing.T) {
	t.Parallel()

	store := &accountModelDiscoveryStoreStub{}
	observations := &modelObservationRepositoryStub{err: errors.New("evidence write failed")}
	svc := &AccountTestService{
		modelDiscoveryStore:        store,
		modelObservationRepository: observations,
	}

	err := svc.PersistDiscoveredModels(context.Background(), &Account{
		ID:       42,
		Platform: PlatformAnthropic,
	}, []string{"claude-sonnet-4-6"}, time.Now())
	require.ErrorContains(t, err, "evidence write failed")
	require.Zero(t, store.accountID, "credential sync must not run after evidence persistence fails")
}

func TestPersistDiscoveredModelsRejectsCredentialShadow(t *testing.T) {
	t.Parallel()

	store := &accountModelDiscoveryStoreStub{}
	svc := &AccountTestService{modelDiscoveryStore: store}
	parentID := int64(9)
	account := &Account{
		ID:              20,
		ParentAccountID: &parentID,
		QuotaDimension:  QuotaDimensionSpark,
		Credentials: map[string]any{
			"model_mapping": map[string]any{"old": "old"},
		},
	}

	err := svc.PersistDiscoveredModels(context.Background(), account, []string{"gpt-5.3-codex-spark"}, time.Now())
	require.Error(t, err)
	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUnsupported, syncErr.Kind)
	require.Zero(t, store.accountID)
	require.Nil(t, store.mapping)
	require.Nil(t, store.discovery)
}

func TestExtractGrokUpstreamModelIDs(t *testing.T) {
	t.Parallel()

	models, err := extractGrokUpstreamModelIDs([]byte(`{"data":[{"id":"display-id","model":"grok-4.5"},{"modelId":"grok-build-0.1"},{"model_id":"grok-composer-2.5-fast"},{"name":"Grok Meta Display Name","_meta":{"model":"grok-meta"}},{"name":"grok-name"},{"id":"grok-safe","_meta":"not-an-object"}]}`))
	require.NoError(t, err)
	require.Equal(t, []string{"grok-4.5", "grok-build-0.1", "grok-composer-2.5-fast", "grok-meta", "grok-name", "grok-safe"}, models)
}

func TestBuildUpstreamModelsRequestsForAPIKeyAccounts(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{cfg: upstreamModelSyncTestConfig()}
	ctx := context.Background()

	anthropicReq, err := svc.buildAnthropicUpstreamModelsRequest(ctx, &Account{
		Platform: PlatformAnthropic,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "anthropic-key",
			"base_url": "https://anthropic.example.com/v1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://anthropic.example.com/v1/models", anthropicReq.URL.String())
	require.Equal(t, "anthropic-key", anthropicReq.Header.Get("x-api-key"))
	require.Equal(t, "2023-06-01", anthropicReq.Header.Get("anthropic-version"))

	anthropicBearerReq, err := svc.buildAnthropicUpstreamModelsRequest(ctx, &Account{
		Platform: PlatformAnthropic,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "ollama-key",
			"base_url": "https://ollama.com",
		},
		Extra: map[string]any{
			"anthropic_apikey_auth_scheme": AnthropicAPIKeyAuthSchemeAuthorizationBearer,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://ollama.com/v1/models", anthropicBearerReq.URL.String())
	require.Equal(t, "Bearer ollama-key", anthropicBearerReq.Header.Get("Authorization"))
	require.Empty(t, anthropicBearerReq.Header.Get("x-api-key"))
	require.Equal(t, "2023-06-01", anthropicBearerReq.Header.Get("anthropic-version"))

	openAIReq, err := svc.buildOpenAIUpstreamModelsRequest(ctx, &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "openai-key",
			"base_url": "https://openai.example.com",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://openai.example.com/v1/models", openAIReq.URL.String())
	require.Equal(t, "Bearer openai-key", openAIReq.Header.Get("Authorization"))

	grokReq, err := svc.buildUpstreamModelsRequest(ctx, &Account{
		Platform: PlatformGrok,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "xai-key",
			"base_url": "https://xai.example.com/v1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://xai.example.com/v1/models", grokReq.URL.String())
	require.Equal(t, "Bearer xai-key", grokReq.Header.Get("Authorization"))

	geminiReq, err := svc.buildGeminiUpstreamModelsRequest(ctx, &Account{
		Platform: PlatformGemini,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "gemini-key",
			"base_url": "https://generativelanguage.googleapis.com/v1beta",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://generativelanguage.googleapis.com/v1beta/models", geminiReq.URL.String())
	require.Equal(t, "gemini-key", geminiReq.Header.Get("x-goog-api-key"))

	antigravityReq, err := svc.buildAntigravityAPIKeyModelsRequest(ctx, &Account{
		Platform: PlatformAntigravity,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "antigravity-key",
			"base_url": "https://gateway.example.com/antigravity",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "https://gateway.example.com/antigravity/v1/models", antigravityReq.URL.String())
	require.Equal(t, "antigravity-key", antigravityReq.Header.Get("x-api-key"))
}

func TestBuildUpstreamModelsRequestSupportsGrokOAuth(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{
		cfg:               upstreamModelSyncTestConfig(),
		grokTokenProvider: NewGrokTokenProvider(nil, nil),
	}
	req, err := svc.buildUpstreamModelsRequest(context.Background(), grokOAuthModelSyncTestAccount(""))
	require.NoError(t, err)
	require.Equal(t, "https://cli-chat-proxy.grok.com/v1/models", req.URL.String())
	require.Equal(t, "Bearer oauth-access-token", req.Header.Get("Authorization"))
	require.Equal(t, grokCLIVersion, req.Header.Get("X-Grok-Client-Version"))
	require.Equal(t, "interactive", req.Header.Get("X-Grok-Client-Mode"))
	require.Equal(t, grokUpstreamUserAgent, req.Header.Get("User-Agent"))
	require.Equal(t, "grok-user-id", req.Header.Get("X-UserID"))
	require.Equal(t, "grok-user@example.com", req.Header.Get("X-Email"))
	require.NotContains(t, req.Header.Get("Authorization"), "oauth-refresh-token")
}

func TestFetchUpstreamModelDiscoveryPreservesAntigravityOAuthPayload(t *testing.T) {
	originalBaseURLs := append([]string(nil), antigravity.BaseURLs...)
	originalAvailability := antigravity.DefaultURLAvailability
	t.Cleanup(func() {
		antigravity.BaseURLs = originalBaseURLs
		antigravity.DefaultURLAvailability = originalAvailability
	})

	payload := []byte("{\n  \"models\": {\n    \" z-model \" : {},\n    \"a-model\": {},\n    \" z-model \" : {\"duplicate\": true}\n  }\n}")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("ETag", `"antigravity-etag"`)
		w.Header().Set("X-Goog-Request-Id", "antigravity-request")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	antigravity.BaseURLs = []string{server.URL}
	antigravity.DefaultURLAvailability = antigravity.NewURLAvailability(time.Minute)

	svc := &AccountTestService{
		antigravityGatewayService: newAntigravityCompatService(config.GatewayConfig{}, nil),
	}
	discovery, err := svc.FetchUpstreamModelDiscovery(context.Background(), newAntigravityCompatAccount(AccountTypeOAuth))
	require.NoError(t, err)
	require.Equal(t, []string{"a-model", "z-model"}, discovery.Models)
	require.Equal(t, []string{" z-model ", "a-model"}, discovery.EvidenceModelIDs)

	var snapshot struct {
		PayloadBase64 string         `json:"payload_base64"`
		Response      map[string]any `json:"response"`
	}
	require.NoError(t, json.Unmarshal(discovery.RawSnapshot, &snapshot))
	decodedPayload, err := base64.StdEncoding.DecodeString(snapshot.PayloadBase64)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, decodedPayload), "accepted payload bytes must not be synthesized from model IDs")
	require.Equal(t, "antigravity_oauth", snapshot.Response["source"])
	require.Equal(t, server.URL+"/v1internal:fetchAvailableModels", snapshot.Response["endpoint"])
	require.Equal(t, float64(http.StatusOK), snapshot.Response["status_code"])
	require.Equal(t, "application/json; charset=utf-8", snapshot.Response["content_type"])
	require.Equal(t, `"antigravity-etag"`, snapshot.Response["etag"])
	require.Equal(t, "antigravity-request", snapshot.Response["request_id"])
}

func TestBuildUpstreamModelsRequestGrokOAuthRequiresTokenProvider(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{cfg: upstreamModelSyncTestConfig()}
	_, err := svc.buildUpstreamModelsRequest(context.Background(), grokOAuthModelSyncTestAccount(""))
	require.Error(t, err)

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorConfiguration, syncErr.Kind)
	require.Contains(t, syncErr.SafeMessage(), "token provider")
}

func TestBuildAntigravityAPIKeyModelsRequestRejectsOfficialCloudCodeBase(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{cfg: upstreamModelSyncTestConfig()}
	_, err := svc.buildAntigravityAPIKeyModelsRequest(context.Background(), &Account{
		Platform: PlatformAntigravity,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "antigravity-key",
			"base_url": "https://cloudcode-pa.googleapis.com",
		},
	})
	require.Error(t, err)

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUnsupported, syncErr.Kind)
	require.Contains(t, syncErr.SafeMessage(), "compatible gateway")
}

func TestBuildAnthropicUpstreamModelsRequestRejectsBedrock(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{cfg: upstreamModelSyncTestConfig()}
	_, err := svc.buildAnthropicUpstreamModelsRequest(context.Background(), &Account{
		Platform: PlatformAnthropic,
		Type:     AccountTypeBedrock,
	})
	require.Error(t, err)

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUnsupported, syncErr.Kind)
}

func TestFetchUpstreamSupportedModelsParsesOpenAIResponse(t *testing.T) {
	t.Parallel()

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"data":[{"id":"gpt-5"},{"id":"gpt-5"},{"name":"o3"}]}`)),
	}}
	svc := &AccountTestService{
		httpUpstream: upstream,
		cfg:          upstreamModelSyncTestConfig(),
	}

	models, err := svc.FetchUpstreamSupportedModels(context.Background(), &Account{
		ID:       7,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "openai-key",
			"base_url": "https://openai.example.com/v1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-5", "o3"}, models)
	require.Equal(t, "https://openai.example.com/v1/models", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer openai-key", upstream.lastReq.Header.Get("Authorization"))
}

func TestFetchUpstreamSupportedModelsParsesGrokAPIKeyResponse(t *testing.T) {
	t.Parallel()

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"data":[{"id":"grok-4.5"},{"id":"grok-4.5"},{"id":"grok-imagine"}]}`)),
	}}
	svc := &AccountTestService{
		httpUpstream: upstream,
		cfg:          upstreamModelSyncTestConfig(),
	}

	models, err := svc.FetchUpstreamSupportedModels(context.Background(), &Account{
		ID:       9,
		Platform: PlatformGrok,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "xai-key",
			"base_url": "https://xai.example.com/v1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"grok-4.5", "grok-imagine"}, models)
	require.Equal(t, "https://xai.example.com/v1/models", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer xai-key", upstream.lastReq.Header.Get("Authorization"))
}

func TestFetchUpstreamSupportedModelsParsesGrokOAuthResponse(t *testing.T) {
	t.Parallel()

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"data":[{"model":"grok-4.5"},{"model":"grok-4.5"},{"modelId":"grok-build-0.1"}]}`)),
	}}
	svc := &AccountTestService{
		httpUpstream:      upstream,
		cfg:               upstreamModelSyncTestConfig(),
		grokTokenProvider: NewGrokTokenProvider(nil, nil),
	}

	models, err := svc.FetchUpstreamSupportedModels(context.Background(), grokOAuthModelSyncTestAccount(""))
	require.NoError(t, err)
	require.Equal(t, []string{"grok-4.5", "grok-build-0.1"}, models)
	require.Equal(t, "https://cli-chat-proxy.grok.com/v1/models", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer oauth-access-token", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, grokCLIVersion, upstream.lastReq.Header.Get("X-Grok-Client-Version"))
	require.Equal(t, "interactive", upstream.lastReq.Header.Get("X-Grok-Client-Mode"))
	require.Equal(t, "grok-user-id", upstream.lastReq.Header.Get("X-UserID"))
	require.Equal(t, "grok-user@example.com", upstream.lastReq.Header.Get("X-Email"))
}

func TestBuildUpstreamModelsRequestGrokOAuthDoesNotSendIdentityToCustomBase(t *testing.T) {
	t.Parallel()

	svc := &AccountTestService{
		cfg:               upstreamModelSyncTestConfig(),
		grokTokenProvider: NewGrokTokenProvider(nil, nil),
	}
	req, err := svc.buildUpstreamModelsRequest(context.Background(), grokOAuthModelSyncTestAccount("https://relay.example/v1"))
	require.NoError(t, err)
	require.Equal(t, "https://relay.example/v1/models", req.URL.String())
	require.Empty(t, req.Header.Get("X-UserID"))
	require.Empty(t, req.Header.Get("X-Email"))
}

func TestFetchUpstreamSupportedModelsDoesNotExposeUpstreamBody(t *testing.T) {
	t.Parallel()

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusBadGateway,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":"SECRET_TOKEN should not be exposed"}`)),
	}}
	svc := &AccountTestService{
		httpUpstream: upstream,
		cfg:          upstreamModelSyncTestConfig(),
	}

	_, err := svc.FetchUpstreamSupportedModels(context.Background(), &Account{
		ID:       8,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "openai-key",
			"base_url": "https://openai.example.com/v1",
		},
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRET_TOKEN")

	var syncErr *UpstreamModelSyncError
	require.True(t, errors.As(err, &syncErr))
	require.Equal(t, UpstreamModelSyncErrorUpstream, syncErr.Kind)
	require.NotContains(t, syncErr.SafeMessage(), "SECRET_TOKEN")
	require.Contains(t, syncErr.SafeMessage(), "HTTP 502")
}

func TestPersistUpstreamModelDiscoveryFiltersForeignFamiliesFromStoredMapping(t *testing.T) {
	t.Parallel()

	// Real shape: aggregator /v1/models returns a 37-model ALL-family catalog;
	// an anthropic account must only store claude-* in its mapping while the
	// observation evidence keeps the full raw truth.
	store := &accountModelDiscoveryStoreStub{}
	observations := &modelObservationRepositoryStub{batchID: "family-filter"}
	svc := &AccountTestService{modelDiscoveryStore: store, modelObservationRepository: observations}

	err := svc.PersistUpstreamModelDiscovery(context.Background(), &Account{
		ID:       4,
		Platform: PlatformAnthropic,
	}, UpstreamModelDiscovery{
		Models:           []string{"claude-opus-5", "glm-5", "gpt-5.5", "grok-4.6", "gemini-2.5-pro", "my-custom-fine-tune"},
		EvidenceModelIDs: []string{"claude-opus-5", "glm-5", "gpt-5.5", "grok-4.6", "gemini-2.5-pro", "my-custom-fine-tune"},
		RawSnapshot:      []byte(`{"payload":{"data":[]},"response":{"source":"http","status_code":200}}`),
	}, time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(4), store.accountID)
	require.Equal(t, map[string]any{
		"claude-opus-5":        "claude-opus-5",
		"my-custom-fine-tune":  "my-custom-fine-tune", // unknown vendor: legacy passthrough
	}, store.mapping, "foreign families must not enter the schedulable mapping")
	require.Len(t, observations.inputs, 1)
	require.ElementsMatch(t,
		[]string{"claude-opus-5", "glm-5", "gpt-5.5", "grok-4.6", "gemini-2.5-pro", "my-custom-fine-tune"},
		observations.inputs[0].ModelIDs, "observation evidence keeps the full upstream truth")
}
