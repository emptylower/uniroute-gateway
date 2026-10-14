//go:build unit

package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMediaCostPlusRetailPrecision(t *testing.T) {
	for _, test := range []struct{ cost, retail string }{
		{"0.04", "0.05"}, {"0.025", "0.03125"}, {"0.79", "0.9875"},
		{"0.000000001", "0.00000001"},
	} {
		units, err := mediaCostPlusRetailUnits(test.cost)
		require.NoError(t, err)
		require.Equal(t, test.retail, mediaUSD(units))
	}
	for _, invalid := range []string{"", "0", "-1", "NaN", "1e-3"} {
		_, err := mediaCostPlusRetailUnits(invalid)
		require.Error(t, err)
	}
}

func TestMediaSupplierActualPreservesOldTasksAndCapsNewBilling(t *testing.T) {
	const budget int64 = 10000000 // confirmed $0.10 maximum
	policy := &FrozenMediaSupplierPricing{PolicyVersion: mediaCostPlusPolicyVersion, CreditUSD: "0.005", MarkupBPS: 2500, BudgetUnits: budget, SourceVersion: "verified-kie-20261014"}
	for _, test := range []struct {
		credits string
		actual  int64
		loss    int64
	}{
		{"8", 5000000, 0}, {"16", budget, 0}, {"24", budget, 5000000},
		{"0", 0, 0}, {"0.000001", 1, 0},
	} {
		actual, loss, err := mediaSupplierActualUnits(policy, &test.credits, budget)
		require.NoError(t, err)
		require.Equal(t, test.actual, actual)
		require.Equal(t, test.loss, loss)
		require.LessOrEqual(t, actual, budget)
	}
	_, _, err := mediaSupplierActualUnits(policy, nil, budget)
	require.ErrorContains(t, err, "usage pending")
	legacy, loss, err := mediaSupplierActualUnits(nil, nil, budget)
	require.NoError(t, err)
	require.Equal(t, budget, legacy)
	require.Zero(t, loss)
	_, _, err = mediaSupplierActualUnits(policy, nil, budget-1)
	require.ErrorContains(t, err, "invalid frozen")
}

func TestMediaSupplierPolicySurvivesFrozenJSONAndRejectsTampering(t *testing.T) {
	const budget int64 = 10000000
	policy := FrozenMediaSupplierPricing{PolicyVersion: mediaCostPlusPolicyVersion, CreditUSD: "0.005", MarkupBPS: 2500, BudgetUnits: budget, SourceVersion: "verified-kie-20261014"}
	snapshot := BillingSnapshot{Version: BillingSnapshotVersion, Pricing: BillingSnapshotPricing{MediaSupplierPricing: &policy}}
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var restored BillingSnapshot
	require.NoError(t, json.Unmarshal(raw, &restored))
	credits := "8"
	actual, _, err := mediaSupplierActualUnits(restored.Pricing.MediaSupplierPricing, &credits, budget)
	require.NoError(t, err)
	require.EqualValues(t, 5000000, actual)
	restored.Pricing.MediaSupplierPricing.CreditUSD = "0.5"
	_, _, err = mediaSupplierActualUnits(restored.Pricing.MediaSupplierPricing, &credits, budget)
	require.ErrorContains(t, err, "invalid frozen")
	require.Equal(t, "0.005", policy.CreditUSD)
}
