//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExternalPublicMediaBudgetFundedNoWriteRecovery(t *testing.T) {
	f := newMediaFixture(t, func(cfg *config.Config) { cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled" })
	budgetFundedNoWriteRecovery(t, f, false)
}
func budgetFundedNoWriteRecovery(t *testing.T, f *mediaFixture, wire bool) {
	h, _ := f.authorize(t, 50_000_000, service.BillingFamilyOpenAI)
	f.bridge.Close() // deterministic independent recovery before restart
	store := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	hold, err := store.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	_, err = f.db.Exec(`UPDATE api_key_budget_authorization SET created_at=now()-interval '10 minutes',updated_at=now()-interval '10 minutes' WHERE authorization_id=$1`, h.ID)
	require.NoError(t, err)
	require.NoError(t, service.RecoverAPIKeyBudgets(context.Background(), f.db, 32))
	var ledger, state string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM api_key_budget_authorization WHERE authorization_id=$1`, h.ID).Scan(&ledger))
	require.Equal(t, "released", ledger)
	require.NoError(t, f.db.QueryRow(`SELECT state FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&state))
	require.Equal(t, "held", state, "allowance recovery only fences; original wallet recovery owns cleanup")
	// The original handle cannot write after independent recovery.
	calls := 0
	upstream := &budgetNeverCalledHTTP{calls: &calls}
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, "http://127.0.0.1:1/v1/responses", strings.NewReader(`{"model":"test"}`))
	require.NoError(t, err)
	_, err = service.NewAuthorizingHTTPUpstream(upstream, f.cfg).Do(req, "", 0, 1)
	require.Error(t, err)
	require.Zero(t, calls)
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET created_at=now()-interval '10 minutes' WHERE parent_authorization_id=$1`, h.ID)
	require.NoError(t, err)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, store, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close) // media workers remain stopped; bridge owns recovery
	require.Eventually(t, func() bool {
		var n int
		e := f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished' AND pin_state='finished'`, h.ID).Scan(&n)
		return e == nil && n == len(h.Segments)
	}, 10*time.Second, 50*time.Millisecond)
	hold, err = store.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	if wire {
		base, secret := immediateV5WireConfig(t)
		code, body := immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]any{"platform_user_id": f.platformUserID}, true)
		require.Less(t, code, 300, string(body))
		var result struct {
			Pins []struct {
				AuthorizationID string `json:"authorizationId"`
				Status          string `json:"state"`
			}
		}
		require.NoError(t, json.Unmarshal(body, &result))
		found := 0
		for _, pin := range result.Pins {
			if pin.AuthorizationID == h.ID {
				found++
				require.Equal(t, "released", pin.Status)
			}
		}
		require.Equal(t, 1, found)
	} else {
		f.control.mu.Lock()
		for _, segment := range h.Segments {
			require.Equal(t, "released", f.control.pinStatus[segment.AuthorizationID])
		}
		f.control.mu.Unlock()
	}
	var spent float64
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=(SELECT api_key_id FROM api_key_budget_authorization WHERE authorization_id=$1)`, h.ID).Scan(&spent))
	require.Zero(t, spent)
}

type budgetNeverCalledHTTP struct {
	service.HTTPUpstream
	calls *int
}

func (h *budgetNeverCalledHTTP) Do(*http.Request, string, int64, int) (*http.Response, error) {
	*h.calls++
	return nil, nil
}

func TestExternalPublicMediaBudgetFundedNoWriteRealD1(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "off")
	f.bridge.Close()
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled"
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	budgetFundedNoWriteRecovery(t, f, true)
}
func TestExternalPublicMediaBudgetLiveWindowsRealD1(t *testing.T) {
	f, _ := mediaV5WireFixture(t, "off")
	h, snapshot := f.authorize(t, 30_000_000, service.BillingFamilyLive)
	_, err := f.db.Exec(`UPDATE api_key_budget_authorization SET state='released' WHERE kind='media' AND authorization_id IN(SELECT authorization_id FROM gateway_media_task WHERE status='failed' AND actual_units=0)`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE api_keys SET quota=0.8,rate_limit_5h=0.8 WHERE id=$1`, snapshot.APIKeyID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	for i, n := range []int64{20_000_000, 30_000_000, 10_000_000} {
		if i > 0 {
			h, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: 30_000_000})
			require.NoError(t, err)
		}
		event := service.CanonicalWalletSettlementEvent{GatewayRequestID: "live-window-" + uuid.NewString(), PlatformUserID: f.platformUserID, Currency: "USD", AmountUnits: n, OccurredAt: time.Now().UTC(), AuthorizationID: h.ID, BillingSnapshotID: snapshot.ID}
		require.True(t, f.bridge.ObserveSettlement(event))
		require.True(t, f.bridge.ObserveSettlement(event), "final window replay must not recapture counters")
	}
	var spent, window float64
	require.NoError(t, f.db.QueryRow(`SELECT quota_used,usage_5h FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&spent, &window))
	require.InDelta(t, 0.6, spent, 1e-8)
	require.InDelta(t, 0.6, window, 1e-8)
	tx, err := f.db.Begin()
	require.NoError(t, err)
	err = service.ReserveAPIKeyBudgetTx(context.Background(), tx, service.APIKeyBudgetReservation{AuthorizationID: "auth_media_mixed", APIKeyID: snapshot.APIKeyID, UserID: f.userID, SnapshotID: snapshot.ID, ReservedUnits: 20_000_000, Kind: "media"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: 1})
	require.ErrorIs(t, err, service.ErrAPIKeyBudgetExceeded)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		err := f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE status='delivered' AND gateway_request_id LIKE 'live-window-%'`).Scan(&n)
		return err == nil && n == 3
	}, 10*time.Second, 50*time.Millisecond)
	base, secret := immediateV5WireConfig(t)
	code, body := immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]any{"platform_user_id": f.platformUserID}, true)
	require.Less(t, code, 300, string(body))
	var result struct {
		Events []json.RawMessage `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Events, 3)
}
