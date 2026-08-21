package service

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestModelGovernanceRealInputs_CheckersWired(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()
	sqlDB := db

	priceChecker := NewChannelPriceChecker(sqlDB)
	billingChecker := NewBillingMappingChecker(sqlDB)
	resourceChecker := NewResourceChecker(sqlDB)
	require.NotNil(t, priceChecker)
	require.NotNil(t, billingChecker)
	require.NotNil(t, resourceChecker)

	// Verify that ModelGovernanceService can be constructed with real checkers and loader can be built.
	loader := NewModelPublicationInputLoaderWithDeps(nil, nil, nil, nil, priceChecker, billingChecker, resourceChecker)
	require.NotNil(t, loader)

	svc := ProvideModelGovernanceService(
		&fakeRegistryService{snapshot: &ModelRegistrySnapshot{Version: 1}},
		&fakeObservationRepo{},
		&fakeShadowRepo{},
		NewModelClassifier(),
		NewPublicationEvaluator(),
		nil,
		priceChecker, billingChecker, resourceChecker,
	)
	require.NotNil(t, svc)
	mgs, ok := svc.(*modelGovernanceService)
	require.True(t, ok)
	require.NotNil(t, mgs.channelPriceChecker)
	require.NotNil(t, mgs.billingMappingChecker)
	require.NotNil(t, mgs.resourceChecker)
}

// Ensure sql import is used
var _ = sql.ErrNoRows
