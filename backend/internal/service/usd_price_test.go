package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUSDPriceServiceRejectsOtherCurrencies(t *testing.T) {
	s := NewUSDPriceService(nil)
	snap, err := s.Snapshot(context.Background(), "USD", "USD")
	require.NoError(t, err)
	require.Equal(t, float64(1), snap.Rate)
	for _, curr := range []string{"CNY", "EUR", ""} {
		_, err = s.Snapshot(context.Background(), "USD", curr)
		require.Error(t, err)
	}
}
