//go:build unit

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalletFundingMatchesSettleLegacyRequiresEmptyOriginalIdentity(t *testing.T) {
	for _, scope := range []string{"", "legacy"} {
		t.Run(scope, func(t *testing.T) {
			request := canonicalWalletEnsureRequest{FundingScope: "settle"}
			require.True(t, walletFundingMatches(scope, "", "", request))
			for _, identity := range [][4]string{
				{"task", "", "", ""},
				{"", "issuance", "", ""},
				{"", "", "task", ""},
				{"", "", "", "issuance"},
				{"task", "issuance", "task", "issuance"},
			} {
				request.FundingOwnerID, request.FundingIssuanceKey = identity[2], identity[3]
				require.False(t, walletFundingMatches(scope, identity[0], identity[1], request), "legacy settle cannot claim any owned or keyed funds")
			}
		})
	}
}

func TestWalletFundingMatchesPreservesStrictNativeScopeAndMediaOwner(t *testing.T) {
	for _, scope := range []string{"llm", "media", "settle"} {
		t.Run(scope, func(t *testing.T) {
			request := canonicalWalletEnsureRequest{FundingScope: scope, FundingOwnerID: "original-task", FundingIssuanceKey: "original-key"}
			require.True(t, walletFundingMatches(scope, "original-task", "original-key", request))
			require.False(t, walletFundingMatches(scope, "other-task", "original-key", request))
			require.False(t, walletFundingMatches(scope, "original-task", "other-key", request))
			for _, otherScope := range []string{"llm", "media", "settle"} {
				if otherScope != scope {
					require.False(t, walletFundingMatches(otherScope, "original-task", "original-key", request))
				}
			}
		})
	}
	for _, scope := range []string{"", "legacy", "llm", "settle"} {
		require.False(t, walletFundingMatches(scope, "", "", canonicalWalletEnsureRequest{FundingScope: "media"}), "media cannot consume unrelated or legacy funds")
	}
	require.False(t, walletFundingMatches("media", "task", "key", canonicalWalletEnsureRequest{FundingScope: "settle"}))
}
