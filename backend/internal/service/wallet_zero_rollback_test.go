//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWalletKnownZeroRollbackOwnerReadFailureProtectsCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "off"}}
	mock.ExpectQuery("SELECT EXISTS").WithArgs("auth").WillReturnError(errors.New("primary unavailable"))
	err = bridge.releasePoolAttempt(context.Background(), "auth", "user", []AuthorizationSegment{{Kind: "llm", AuthorizationID: "auth"}}, "auth.1")
	require.ErrorContains(t, err, "primary unavailable")
	require.NoError(t, mock.ExpectationsWereMet(), "PG failure cannot select legacy state update or Redis cleanup")
}

func TestWalletKnownZeroRollbackReaderOwnershipRequiresSignedControl(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "off"}}
	mock.ExpectQuery("SELECT EXISTS").WithArgs("auth").WillReturnRows(sqlmock.NewRows([]string{"owned"}).AddRow(true))
	err = bridge.releasePoolAttempt(context.Background(), "auth", "user", []AuthorizationSegment{{Kind: "llm", AuthorizationID: "auth"}}, "auth.1")
	require.ErrorContains(t, err, "signed zero control unavailable")
	require.NoError(t, mock.ExpectationsWereMet(), "persisted ownership before zero_intent cannot fall back to an unsigned release")
}
