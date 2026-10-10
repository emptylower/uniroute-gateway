//go:build unit

package service

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestWalletReaderNormalizationFreezesEffectiveTierAndForceCache(t *testing.T) {
	h := &AuthorizationHandle{readerBillingFamily: BillingFamilyOpenAI, readerPlatform: PlatformOpenAI, readerTokenOnly: true, stageUsage: func(_ context.Context, task UsageRecordTask) (UsageRecordTask, error) { return task, nil }}
	h.captureWalletReaderNormalization(WithForceCacheBilling(context.Background()), []byte(`{"service_tier":"fast","api_key":"never-retained"}`), true)
	require.Equal(t, "priority", h.readerNormalization.ServiceTier)
	require.True(t, h.readerNormalization.ForceCacheBilling)
	h.captureWalletReaderNormalization(context.Background(), []byte(`{"service_tier":"flex"}`), true)
	require.Equal(t, "priority", h.readerNormalization.ServiceTier, "a later write cannot replace the original attempt facts")
	stripped := &AuthorizationHandle{readerBillingFamily: BillingFamilyOpenAI, readerPlatform: PlatformOpenAI, readerTokenOnly: true, stageUsage: h.stageUsage}
	stripped.captureWalletReaderNormalization(context.Background(), []byte(`{"model":"gpt-5.1"}`), true)
	require.Empty(t, stripped.readerNormalization.ServiceTier, "post-policy removal selects the default tier")
}

func TestWalletReaderNormalizationMatchesFrozenForceCacheAndTTLSettlement(t *testing.T) {
	svc, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	snapshot.Flags.CacheTTLOverrideEnabled = true
	snapshot.Flags.CacheTTLOverrideTarget = "1h"
	evidence := WalletReaderEvidence{Present: true, Valid: true, ObservedPositive: true, Tokens: UsageTokens{InputTokens: 1000, OutputTokens: 20, CacheReadTokens: 25, CacheCreationTokens: 100, CacheCreation5mTokens: 100}}
	facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyGeneric, ProviderPlatform: PlatformOpenAI, Ready: true, TokenOnly: true, ForceCacheBilling: true}
	input, err := normalizeWalletReaderFee(snapshot, facts, evidence)
	require.NoError(t, err)
	expected := snapshotSettlementInputFromClaudeUsage(ClaudeUsage{InputTokens: 0, OutputTokens: 20, CacheReadInputTokens: 1025, CacheCreationInputTokens: 100, CacheCreation5mTokens: 100}, snapshotCacheTTLOverride(snapshot), 0, "")
	require.Equal(t, expected, input)
	got, err := svc.billing.CalculateCostFromSnapshot(snapshot, input)
	require.NoError(t, err)
	original, err := svc.billing.CalculateCostFromSnapshot(snapshot, expected)
	require.NoError(t, err)
	require.Equal(t, original.ActualCost, got.ActualCost)
	unnormalized, err := svc.billing.CalculateCostFromSnapshot(snapshot, SnapshotSettlementInput{Tokens: evidence.Tokens})
	require.NoError(t, err)
	require.NotEqual(t, unnormalized.ActualCost, got.ActualCost, "the test must exercise a financially different cache policy")
}

func TestWalletReaderNormalizationPreservesExactLargeCountsAndTierPrice(t *testing.T) {
	svc, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	var evidence WalletReaderEvidence
	observeWalletWSUsage([]byte(`{"type":"response.completed","response":{"usage":{"input_tokens":9007199254740993,"output_tokens":2,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":4}}}}`), &evidence)
	require.True(t, evidence.Valid)
	require.Equal(t, int64(9007199254740993), int64(evidence.RawInputTokens))
	require.Equal(t, 4, evidence.Tokens.CacheCreationTokens)
	for _, tier := range []string{"priority", "flex", ""} {
		facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, TokenOnly: true, ServiceTier: tier}
		input, err := normalizeWalletReaderFee(snapshot, facts, evidence)
		require.NoError(t, err)
		require.Equal(t, int64(9007199254740986), int64(input.Tokens.InputTokens))
		require.Equal(t, tier, input.ServiceTier)
		got, err := svc.billing.CalculateCostFromSnapshot(snapshot, input)
		require.NoError(t, err)
		expected, err := svc.billing.CalculateCostFromSnapshot(snapshot, SnapshotSettlementInput{Tokens: openAIUsageTokens(OpenAIUsage{InputTokens: evidence.RawInputTokens, OutputTokens: 2, CacheReadInputTokens: 3, CacheCreationInputTokens: 4}), ServiceTier: tier})
		require.NoError(t, err)
		require.Equal(t, expected.ActualCost, got.ActualCost)
	}
}

func TestWalletReaderNormalizationMissingFactsAndToolsRemainUnresolved(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	evidence := WalletReaderEvidence{Present: true, Valid: true, ObservedPositive: true, RawInputTokens: 1, Tokens: UsageTokens{InputTokens: 1}}
	for _, payload := range [][]byte{nil, []byte(`{"service_tier":3}`), []byte(`{"tools":[{"type":"image_generation"}]}`)} {
		h := &AuthorizationHandle{readerBillingFamily: BillingFamilyOpenAI, readerPlatform: PlatformOpenAI, readerTokenOnly: true, stageUsage: func(_ context.Context, task UsageRecordTask) (UsageRecordTask, error) { return task, nil }}
		h.captureWalletReaderNormalization(context.Background(), payload, payload != nil)
		_, err := normalizeWalletReaderFee(snapshot, h.readerNormalization, evidence)
		require.Error(t, err, "unavailable or count-dependent normalization cannot silently select default pricing")
	}
}

func TestWalletReaderJournalActualPGNewBridgeRejectsWrongMountedVolume(t *testing.T) {
	dsn := os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL fixture is not configured")
	}
	target, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, target.Hostname())
	require.True(t, strings.Contains(target.Path, "test"))
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE wallet_reader_journal_volume(singleton boolean PRIMARY KEY,volume_id text);
        CREATE TEMP TABLE wallet_authorization_segment(reader_journal_host text,terminal_sealed_at timestamptz,evidence_pending boolean,fee_pending boolean,state text)`)
	require.NoError(t, err)
	original := t.TempDir()
	first, err := walletReaderJournalDirectory(original)
	require.NoError(t, err)
	a := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{ReaderJournalDirectory: original}}
	require.NoError(t, a.verifyWalletReaderJournalVolume(context.Background(), first))
	reopened, err := walletReaderJournalDirectory(original)
	require.NoError(t, err)
	same := &CanonicalWalletBridge{outboxDB: db}
	require.NoError(t, same.verifyWalletReaderJournalVolume(context.Background(), reopened))
	replacement := t.TempDir()
	wrong := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{ReaderJournalDirectory: replacement}}
	status, _, refused := WalletRiskRefusalDetails(wrong.ensureWalletReaderJournal(context.Background()))
	require.True(t, refused)
	require.Equal(t, 503, status)
	require.Nil(t, wrong.readerJournalOwner)
	var retained string
	require.NoError(t, db.QueryRow(`SELECT volume_id FROM wallet_reader_journal_volume`).Scan(&retained))
	require.Equal(t, first, retained)
	_, err = db.Exec(`TRUNCATE wallet_reader_journal_volume`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO wallet_authorization_segment VALUES($1,NULL,true,true,'indeterminate')`, first)
	require.NoError(t, err)
	status, _, refused = WalletRiskRefusalDetails(wrong.ensureWalletReaderJournal(context.Background()))
	require.True(t, refused)
	require.Equal(t, 503, status, "an absent registry cannot adopt another mount while historical readers are unfinished")
}
