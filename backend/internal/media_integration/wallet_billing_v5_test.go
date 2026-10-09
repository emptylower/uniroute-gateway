//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExternalWalletKnownZeroRestartOffBeforeIntentStillRequiresSignedACK(t *testing.T) {
	f, denyFinish := mediaV5WireFixture(t, "off")
	// Obtain the fixture's real API-key/user snapshot, then persist a distinct
	// LLM snapshot. Never mutate the pre-existing media snapshot's identity.
	_, original := f.authorize(t, 1000000, service.BillingFamilyOpenAI)
	snapshot := *original
	snapshot.ID = "snapshot-v5-zero-" + uuid.NewString()
	snapshot.Flags.WalletImmediateReleasePolicyVersion = service.WalletImmediateReleasePolicyVersion
	require.NoError(t, f.snapshots.Persist(context.Background(), &snapshot))
	f.bridge.Close()
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled"
	f.cfg.CanonicalWallet.ReaderJournalDirectory = f.readerJournalDir
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	h, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: &snapshot, User: user, FixedEstimateUnits: 1000000})
	require.NoError(t, err)
	require.Len(t, h.Segments, 1)
	// A real PG failure occurs after reader→PG transfer and before zero intent.
	// The crash/restart path therefore cannot rely on an existing zero_intent.
	_, err = f.db.Exec(`CREATE FUNCTION test_v5_zero_seal_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.parent_authorization_id='` + h.ID + `' AND OLD.terminal_sealed_at IS NULL AND NEW.terminal_sealed_at IS NOT NULL THEN RAISE EXCEPTION 'isolated zero handoff fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_v5_zero_seal_fault BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION test_v5_zero_seal_fault()`)
	require.NoError(t, err)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"safe validation rejection"}}`)
	}))
	defer provider.Close()
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, f.cfg)
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
	require.NoError(t, err)
	response, err := decorated.Do(request, "", 0, 1)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, service.AuthorizationOutcomeResult, h.Writes()[0].Outcome, "a safe header cannot overwrite the transport result")
	var joined, owned, intent, ack bool
	require.NoError(t, f.db.QueryRow(`SELECT reader_handoff_at IS NOT NULL,reader_owner_id IS NOT NULL,zero_intent_at IS NOT NULL,zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&joined, &owned, &intent, &ack))
	require.True(t, joined && owned)
	require.False(t, intent)
	require.False(t, ack)
	store := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	hold, err := store.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	_, err = f.db.Exec(`DROP TRIGGER test_v5_zero_seal_fault ON wallet_authorization_segment; DROP FUNCTION test_v5_zero_seal_fault()`)
	require.NoError(t, err)
	f.bridge.Close() // releases the previous PG owner session for restart recovery
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "off"
	f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "off"
	denyFinish.Store(true) // actual missing financial route must never be an ACK
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, store, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		_ = f.db.QueryRow(`SELECT zero_intent_at IS NOT NULL,zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&intent, &ack)
		return intent
	}, 8*time.Second, 20*time.Millisecond)
	require.False(t, ack)
	hold, err = store.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State, "flag-off plus missing Worker finish cannot release Redis")
	denyFinish.Store(false)
	var raw []byte
	var signedAt time.Time
	require.Eventually(t, func() bool {
		var state string
		queryErr := f.db.QueryRow(`SELECT state,zero_ack_at IS NOT NULL,zero_receipt,zero_released_at FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&state, &ack, &raw, &signedAt)
		return queryErr == nil && ack && state == "finished"
	}, 8*time.Second, 20*time.Millisecond)
	hold, err = store.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	var receipt service.WalletTaskPinReceipt
	require.NoError(t, json.Unmarshal(raw, &receipt))
	segment := h.Segments[0]
	releasedAt, err := service.VerifyWalletTaskPinReceipt(f.cfg.CanonicalWallet.Secret, service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: f.platformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "released"}, receipt)
	require.NoError(t, err)
	require.True(t, signedAt.Equal(releasedAt))
	var counters, charges int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&counters))
	require.Zero(t, counters, "known zero cannot enter the v2 unknown counter")
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
	require.Zero(t, charges)
	// Feed a changed late positive through the real RecordUsage path after the
	// actual Worker signed zero above. It belongs only to the platform anomaly.
	var balanceBefore, quotaBefore float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceBefore))
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaBefore))
	usageService := service.NewOpenAIGatewayService(nil, nil, repository.NewUsageBillingRepository(nil, f.db), f.users, nil, nil, repository.NewGatewayCache(f.rdb), f.cfg, f.db, repository.ProvideWalletOutboxStore(f.db), nil, nil, service.NewBillingService(f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	err = usageService.RecordUsage(context.Background(), &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "late-positive-after-signed-zero", Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 100, OutputTokens: 10}}, User: user, APIKey: &service.APIKey{ID: snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: &snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	require.ErrorIs(t, err, service.ErrWalletPositiveAfterZero)
	var anomalies, outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_anomaly WHERE parent_authorization_id=$1 AND authorization_token=$2 AND reason='positive_after_zero'`, h.ID, h.LastWriteToken()).Scan(&anomalies))
	require.Equal(t, 1, anomalies)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE authorization_id=$1 OR gateway_request_id='late-positive-after-signed-zero'`, h.ID).Scan(&outbox))
	require.Zero(t, outbox, "late platform evidence cannot create a settlement or receivable")
	var balanceAfter, quotaAfter float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceAfter))
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaAfter))
	require.Equal(t, balanceBefore, balanceAfter)
	require.Equal(t, quotaBefore, quotaAfter)
}

func TestExternalWalletBillingReceiptAndEffectsAreAtomicAndReplayOriginal(t *testing.T) {
	f := newMediaFixture(t)
	h, snapshot := f.authorize(t, 1000000, service.BillingFamilyOpenAI)
	token := h.MintWriteToken()
	fee := int64(1000000)
	binding := &service.WalletBillingBinding{PendingID: h.ID + ":" + token, ParentAuthorizationID: h.ID, AuthorizationToken: token, PlatformUserID: f.platformUserID, BillingSnapshotID: snapshot.ID, EventID: service.CanonicalWalletSettlementEventID(h.ID, f.platformUserID, service.CurrencyUSD), ProviderAccountID: snapshot.AccountID, Source: "llm_http_usage", PolicyVersion: service.WalletImmediateReleasePolicyVersion, FeeUnits: fee}
	cmd := &service.UsageBillingCommand{RequestID: "wallet:" + h.ID, APIKeyID: snapshot.APIKeyID, UserID: snapshot.UserID, AccountID: snapshot.AccountID, SettlementCurrency: "USD", Model: snapshot.BillingModel, ServiceTier: "priority", BillingType: service.BillingTypeBalance, InputTokens: 100, OutputTokens: 20, WalletCostUSD: .01, APIKeyQuotaCost: .01, WalletBinding: binding}
	cmd.Normalize()
	raw, err := json.Marshal(cmd)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET authorization_token=$2,first_write_at=now(),write_ended_at=now(),reader_handoff_at=now(),terminal_sealed_at=now(),terminal_evidence=jsonb_build_object('source','selected-test-billing','authorization_id',$1::text,'authorization_token',$2::text),known_fee_units=$3,fee_pending=true,evidence_pending=false WHERE parent_authorization_id=$1`, h.ID, token, fee)
	require.NoError(t, err)
	_, err = f.db.Exec(`INSERT INTO wallet_billing_pending(id,parent_authorization_id,authorization_token,platform_user_id,billing_snapshot_id,event_id,provider_account_id,evidence_source,policy_version,fee_units,command) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)`, binding.PendingID, binding.ParentAuthorizationID, binding.AuthorizationToken, binding.PlatformUserID, binding.BillingSnapshotID, binding.EventID, binding.ProviderAccountID, binding.Source, binding.PolicyVersion, binding.FeeUnits, string(raw))
	require.NoError(t, err)
	var before float64
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&before))
	_, err = f.db.Exec(`CREATE FUNCTION test_v5_charge_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'isolated charge receipt persistence fault'; END $$; CREATE TRIGGER test_v5_charge_receipt_fault BEFORE INSERT ON wallet_billing_charge_receipt FOR EACH ROW EXECUTE FUNCTION test_v5_charge_receipt_fault()`)
	require.NoError(t, err)
	repo := repository.NewUsageBillingRepository(nil, f.db)
	_, err = repo.Apply(context.Background(), cmd)
	require.ErrorContains(t, err, "charge receipt persistence fault")
	var after float64
	var dedup, receipts int
	var applied, canonical bool
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&after))
	require.Equal(t, before, after, "failed original receipt insertion must roll back the quota effect")
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID).Scan(&dedup))
	require.Zero(t, dedup, "failed receipt insertion must also roll back the dedup claim")
	require.NoError(t, f.db.QueryRow(`SELECT apply_ack_at IS NOT NULL,canonical_ack_at IS NOT NULL FROM wallet_billing_pending WHERE id=$1`, binding.PendingID).Scan(&applied, &canonical))
	require.False(t, applied || canonical)
	_, err = f.db.Exec(`DROP TRIGGER test_v5_charge_receipt_fault ON wallet_billing_charge_receipt; DROP FUNCTION test_v5_charge_receipt_fault()`)
	require.NoError(t, err)
	first, err := repo.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.True(t, first.Applied)
	require.NotNil(t, first.OriginalCharge)
	require.Equal(t, *binding, first.OriginalCharge.Binding)
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&after))
	require.InDelta(t, before+.01, after, 1e-10)
	require.NoError(t, f.db.QueryRow(`SELECT apply_ack_at IS NOT NULL,canonical_ack_at IS NOT NULL FROM wallet_billing_pending WHERE id=$1`, binding.PendingID).Scan(&applied, &canonical))
	require.True(t, applied)
	require.False(t, canonical, "Apply ACK cannot manufacture the later canonical settlement ACK")
	// A caller holding the original dedup identity may now carry different
	// mutable prices. Applied=false must return the frozen original receipt.
	replay := *cmd
	replay.WalletCostUSD, replay.APIKeyQuotaCost, replay.ServiceTier = 7, 7, "flex"
	duplicate, err := repo.Apply(context.Background(), &replay)
	require.NoError(t, err)
	require.False(t, duplicate.Applied)
	require.NotNil(t, duplicate.OriginalCharge)
	require.Equal(t, first.OriginalCharge, duplicate.OriginalCharge)
	require.Equal(t, .01, duplicate.OriginalCharge.Command.WalletCostUSD)
	require.Equal(t, "priority", duplicate.OriginalCharge.Command.ServiceTier)
	require.Equal(t, fee, duplicate.OriginalCharge.Binding.FeeUnits)
	wrong := *cmd
	wrongBinding := *binding
	wrongBinding.AuthorizationToken = "different-owner.1"
	wrong.WalletBinding = &wrongBinding
	_, err = repo.Apply(context.Background(), &wrong)
	require.ErrorIs(t, err, service.ErrUsageBillingRequestConflict)
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&after))
	require.InDelta(t, before+.01, after, 1e-10)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt WHERE pending_id=$1`, binding.PendingID).Scan(&receipts))
	require.Equal(t, 1, receipts)
	var actual int64
	require.NoError(t, f.db.QueryRow(`SELECT COALESCE(actual_units,0) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&actual))
	require.Zero(t, actual, "original Apply receipt does not bypass the canonical settlement handoff")
}
