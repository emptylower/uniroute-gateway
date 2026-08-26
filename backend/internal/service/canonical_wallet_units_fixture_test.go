package service

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type walletLeaseFixtureCase struct {
	Name        string `json:"name"`
	AmountUnits string `json:"amount_units"`
}

type walletLeaseFixture struct {
	Cases []walletLeaseFixtureCase `json:"cases"`
}

func loadWalletLeaseFixture(t *testing.T) walletLeaseFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/wallet-lease-cny-e8-v1.json")
	require.NoError(t, err, "copy the shared fixture into testdata/ per this task's Step 5 before running this test")
	var fixture walletLeaseFixture
	require.NoError(t, json.Unmarshal(raw, &fixture))
	return fixture
}

func findFixtureCase(t *testing.T, fixture walletLeaseFixture, name string) walletLeaseFixtureCase {
	t.Helper()
	for _, c := range fixture.Cases {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("wallet lease fixture is missing case %q", name)
	return walletLeaseFixtureCase{}
}

func TestCanonicalWalletUnitsMatchesSharedFixtureRealisticCap(t *testing.T) {
	fixture := loadWalletLeaseFixture(t)
	capCase := findFixtureCase(t, fixture, "single_account_realistic_cap")
	var expectedUnits int64
	_, err := fmt.Sscan(capCase.AmountUnits, &expectedUnits)
	require.NoError(t, err)

	// 1,000,000 CNY at this fixture's scale — confirm CreditsToUnits agrees
	// with the SAME boundary value ShipAny's lease.test.ts already asserts
	// against, closing the "referenced but not consumed" gap.
	units, err := CreditsToUnits(expectedUnits / canonicalWalletUnitsPerCredit)
	require.NoError(t, err)
	require.Equal(t, expectedUnits, units)
}

func TestCanonicalWalletUnitsMatchesSharedFixtureOneCredit(t *testing.T) {
	fixture := loadWalletLeaseFixture(t)
	oneCreditCase := findFixtureCase(t, fixture, "one_credit")
	var expectedUnits int64
	_, err := fmt.Sscan(oneCreditCase.AmountUnits, &expectedUnits)
	require.NoError(t, err)

	units, err := CreditsToUnits(1)
	require.NoError(t, err)
	require.Equal(t, expectedUnits, units, "1 ShipAny credit must equal the fixture's frozen one_credit amount_units exactly")
}
