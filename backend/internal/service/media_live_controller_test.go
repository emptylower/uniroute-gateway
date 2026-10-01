//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type refusedWindowAdvance struct{ LiveProvisionalStore }

func (s refusedWindowAdvance) AdvanceLiveWindow(context.Context, string, int, int64, LiveWindow) error {
	return ErrLiveWindowCASLost
}

func TestMediaLiveControllerStopsAfterNextAuthorizationResolved(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeEnforce)
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	f.bridge.outboxDB = db
	f.svc.liveProvisional = refusedWindowAdvance{f.provStore}
	record := &LiveCallRecord{CallHash: "expired-next-window", PlatformUserID: f.user.PlatformUserID, BillingCurrency: "CNY", ExchangeRate: 7.2, RateMultiplier: 1}
	require.NoError(t, f.liveStore.SaveLiveCall(context.Background(), record, time.Minute))
	basis, err := json.Marshal(CanonicalWalletLease{LeaseID: "lease-next"})
	require.NoError(t, err)
	mock.ExpectQuery("SELECT authorization_id,lease_id,held_units").WithArgs("auth_next").WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "lease_id", "held_units", "lease_basis", "event_id", "actual_units", "pin_state", "kind", "state", "settlement_payload", "remainder_payload"}).AddRow("auth_next", "lease-next", 10, basis, "next-event", 0, "finished", "live", "released", nil, nil))
	state := &liveWindowState{seq: 1, rowToken: "auth_first", openedAt: time.Now().Add(-time.Hour), next: &LiveWindow{WindowSeq: 2, Token: "auth_next", LeaseID: "lease-next"}}
	_, err = f.svc.maybeCloseLiveWindow(context.Background(), record, nil, state)
	require.ErrorIs(t, err, ErrLiveCallNotFound)
	closed, err := f.liveStore.GetLiveCall(context.Background(), record.CallHash)
	require.NoError(t, err)
	require.Equal(t, LiveControllerClosed, closed.Controller)
	require.NoError(t, mock.ExpectationsWereMet())
}
