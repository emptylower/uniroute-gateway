//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func legacyUnpriceableRestart(t *testing.T, x *fundingV7Fixture) {
	t.Helper()
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	x.f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, x.f.db))
	x.f.bridge.SetBillingEvidenceDependencies(repository.NewUsageLogRepository(nil, x.f.db), nil)
	t.Cleanup(x.f.bridge.Close)
	x.f.svc = x.f.newService(t)
}

func TestExternalWalletLegacyPositiveUnpriceableFeeReleasesUnknownAfterGrace(t *testing.T) {
	for _, tc := range []struct {
		name, platform, raw string
		malformed           bool
	}{
		{"missing-frozen-provider", "", providerGrokReasoningUsage, false},
		{"malformed-positive-safe-4xx", service.PlatformGrok, strings.Replace(providerGrokReasoningUsage, `"total_tokens":1324`, `"total_tokens":"bad"`, 1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := providerUsageFixture(t, tc.platform, service.BillingFamilyOpenAI)
			h := x.authorize(t, 100000000)
			providerUsageRead(t, x, h, 401, false, tc.raw)
			x.f.bridge.Close()
			var status, pending, outbox int
			var known bool
			var raw []byte
			require.NoError(t, x.f.db.QueryRow(`SELECT legacy_zero_candidate,known_fee_units IS NOT NULL,reader_evidence FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&status, &known, &raw))
			require.Equal(t, 401, status, "the real upstream header persisted the compatibility label")
			require.False(t, known)
			var evidence service.WalletReaderEvidence
			require.NoError(t, json.Unmarshal(raw, &evidence))
			require.True(t, evidence.ObservedPositive && evidence.Complete && evidence.Present)
			require.Equal(t, tc.malformed, evidence.Malformed)
			require.Equal(t, !tc.malformed, evidence.Valid)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox o JOIN wallet_authorization_segment a ON a.event_id=o.event_id WHERE a.parent_authorization_id=$1`, h.ID).Scan(&outbox))
			require.Zero(t, pending, "missing immutable provider facts cannot select the legacy one-token fee")
			require.Zero(t, outbox)
			b := barrierFixture{x: x}
			fee, barrier := b.flags(t, h)
			require.True(t, allTrue(fee) && allTrue(barrier))
			providerUsageAssertLedger(t, x, h, 0)

			b.ageDeadline(t, h, -1, 29)
			legacyUnpriceableRestart(t, &x)
			b.x = x
			require.Never(t, func() bool { return b.unknownCounters(h) != 0 || !b.held(t, h) }, time.Second, 20*time.Millisecond)
			x.f.svc.Stop()
			x.f.bridge.Close()
			b.ageDeadline(t, h, -1, 31)
			legacyUnpriceableRestart(t, &x)
			b.x = x
			require.Eventually(t, func() bool {
				var done int
				return x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_ack_at IS NOT NULL AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&done) == nil && done == len(h.Segments) && b.unknownCounters(h) == len(h.Segments) && !b.held(t, h)
			}, 10*time.Second, 20*time.Millisecond, "an unpriceable positive safe-4xx must use the bounded unknown exit despite its legacy label")
			fee, barrier = b.flags(t, h)
			require.True(t, allFalse(fee) && allFalse(barrier))
			var reason string
			var zero, charges int
			require.NoError(t, x.f.db.QueryRow(`SELECT terminal_evidence->>'Reason',legacy_zero_candidate,known_fee_units IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&reason, &status, &known))
			require.Equal(t, "barrier_timeout_unknown", reason)
			require.Equal(t, 401, status, "the compatibility label remains audit evidence, not proof of a fee or zero")
			require.False(t, known)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND zero_ack_at IS NOT NULL`, h.ID).Scan(&zero))
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
			require.Zero(t, zero)
			require.Zero(t, charges)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox o JOIN wallet_authorization_segment a ON a.event_id=o.event_id WHERE a.parent_authorization_id=$1`, h.ID).Scan(&outbox))
			require.Zero(t, pending)
			require.Zero(t, outbox)
			var logs, matched int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, "wallet:"+h.ID).Scan(&logs))
			require.Zero(t, logs)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter c JOIN wallet_authorization_segment a USING(authorization_id) WHERE c.parent_authorization_id=$1 AND c.authorization_token=a.authorization_token AND c.billing_snapshot_id=a.billing_snapshot_id AND c.platform_user_id=a.platform_user_id AND c.held_units=a.held_units AND c.receipt_id=a.expiry_receipt_id AND c.receipt_signature=a.expiry_receipt_signature AND c.receipt=a.expiry_receipt AND c.released_at=a.expiry_released_at`, h.ID).Scan(&matched))
			require.Equal(t, len(h.Segments), matched)
			for _, segment := range h.Segments {
				var receiptRaw []byte
				var deadline, released time.Time
				var proof, legacy string
				var version int
				require.NoError(t, x.f.db.QueryRow(`SELECT expiry_intent_version,expiry_v2_deadline,expiry_terminal_proof,expiry_legacy_mapping_proof,expiry_released_at,expiry_receipt FROM wallet_authorization_segment WHERE authorization_id=$1`, segment.AuthorizationID).Scan(&version, &deadline, &proof, &legacy, &released, &receiptRaw))
				require.Equal(t, 2, version)
				var receipt service.WalletTaskPinReceipt
				require.NoError(t, json.Unmarshal(receiptRaw, &receipt))
				expected := service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: x.f.platformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: x.snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "expired_unknown", ExpiryDeadline: deadline, ExpiryTerminalProof: proof, LegacyCompletionProof: legacy}
				at, err := service.VerifyWalletTaskPinReceipt(x.secret, expected, receipt)
				require.NoError(t, err)
				require.True(t, released.Equal(at))
				tampered := receipt
				tampered.ReceiptSignature = strings.Repeat("0", 64)
				_, err = service.VerifyWalletTaskPinReceipt(x.secret, expected, tampered)
				require.Error(t, err, "invalid HMAC cannot qualify as the unknown acknowledgement")
				hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, segment.AuthorizationID)
				require.NoError(t, err)
				require.Equal(t, "released", hold.State)
				require.Equal(t, "expiry_unknown", hold.Class)
			}
			providerUsageAssertLedger(t, x, h, 0)
			_, err := x.f.bridge.RecoverImmediateWalletExpiry(context.Background(), h.ID)
			require.NoError(t, err)
			require.Equal(t, len(h.Segments), b.unknownCounters(h), "retry cannot duplicate the signed unknown release")
		})
	}
}

func TestExternalWalletLegacyPriceableFeeFailureKeepsHoldUntilCharged(t *testing.T) {
	for _, tc := range []struct {
		name, table string
		staged      bool
	}{
		{"stage-fault", "wallet_billing_pending", false},
		{"apply-fault", "wallet_billing_charge_receipt", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := providerUsageFixture(t, service.PlatformGrok, service.BillingFamilyOpenAI)
			_, err := x.f.db.Exec(fmt.Sprintf(`CREATE SEQUENCE legacy_priceable_attempts; CREATE FUNCTION legacy_priceable_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('legacy_priceable_attempts'); RAISE EXCEPTION 'isolated priceable fee fault'; END $$; CREATE TRIGGER legacy_priceable_fault BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION legacy_priceable_fault()`, tc.table))
			require.NoError(t, err)
			h := x.authorize(t, 100000000)
			providerUsageRead(t, x, h, 401, false, providerGrokReasoningUsage)
			x.f.bridge.Close()
			var firstAttempts int64
			require.NoError(t, x.f.db.QueryRow(`SELECT last_value FROM legacy_priceable_attempts`).Scan(&firstAttempts))
			require.Positive(t, firstAttempts)
			var status, pending int
			var known bool
			require.NoError(t, x.f.db.QueryRow(`SELECT legacy_zero_candidate,known_fee_units IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&status, &known))
			require.Equal(t, 401, status)
			require.Equal(t, tc.staged, known)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			var originalCommand []byte
			if tc.staged {
				require.Equal(t, 1, pending)
				require.NoError(t, x.f.db.QueryRow(`SELECT command FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&originalCommand))
			} else {
				require.Zero(t, pending)
			}
			b := barrierFixture{x: x}
			fee, _ := b.flags(t, h)
			require.True(t, allTrue(fee))
			b.ageDeadline(t, h, -1, 90)
			legacyUnpriceableRestart(t, &x)
			b.x = x
			require.Eventually(t, func() bool {
				var attempts int64
				return x.f.db.QueryRow(`SELECT last_value FROM legacy_priceable_attempts`).Scan(&attempts) == nil && attempts > firstAttempts
			}, 5*time.Second, 20*time.Millisecond, "recovery actually retries the priceable fee and encounters the persistent database fault")
			require.Never(t, func() bool { return b.unknownCounters(h) != 0 || !b.held(t, h) }, time.Second, 20*time.Millisecond, "a frozen priceable fee failure must never take the candidate's unknown exit")
			var expiry, zero, charges int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FILTER (WHERE expiry_intent_version<>0),count(*) FILTER (WHERE zero_ack_at IS NOT NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&expiry, &zero))
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
			require.Zero(t, expiry)
			require.Zero(t, zero)
			require.Zero(t, charges)
			fee, _ = b.flags(t, h)
			require.True(t, allTrue(fee))
			providerUsageAssertLedger(t, x, h, 0)
			x.f.svc.Stop()
			x.f.bridge.Close()
			_, err = x.f.db.Exec(fmt.Sprintf(`DROP TRIGGER legacy_priceable_fault ON %s; DROP FUNCTION legacy_priceable_fault()`, tc.table))
			require.NoError(t, err)
			legacyUnpriceableRestart(t, &x)
			b.x = x
			expected, err := (&service.BillingService{}).CalculateCostFromSnapshot(x.snapshot, service.SnapshotSettlementInput{Tokens: service.UsageTokens{InputTokens: 95, OutputTokens: 77, CacheReadTokens: 1152}})
			require.NoError(t, err)
			feeUnits := int64(math.Round(expected.ActualCost * 100000000))
			var actual int64
			require.Eventually(t, func() bool {
				return x.f.db.QueryRow(`SELECT actual_units FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0 AND state='finished'`, h.ID).Scan(&actual) == nil && actual == feeUnits
			}, 10*time.Second, 20*time.Millisecond)
			var command []byte
			require.NoError(t, x.f.db.QueryRow(`SELECT command FROM wallet_billing_pending WHERE parent_authorization_id=$1 AND fee_units=$2 AND apply_ack_at IS NOT NULL AND canonical_ack_at IS NOT NULL`, h.ID, feeUnits).Scan(&command))
			if tc.staged {
				require.Equal(t, originalCommand, command, "retry uses the original immutable staged command")
			}
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
			require.Equal(t, 1, charges)
			require.Zero(t, b.unknownCounters(h))
			require.Never(t, func() bool { return b.held(t, h) }, 200*time.Millisecond, 20*time.Millisecond)
			providerUsageAssertLedger(t, x, h, feeUnits)
		})
	}
}
