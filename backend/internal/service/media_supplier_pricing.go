package service

import (
	"errors"
	"math/big"
	"regexp"
)

const mediaCostPlusPolicyVersion = "kie-cost-plus-25-20261014"

// FrozenMediaSupplierPricing applies only to new variable-cost tasks. Old
// snapshots without this policy retain their original fixed quoted amount.
// BudgetUnits is the confirmed collection cap, never an overrun authorization.
type FrozenMediaSupplierPricing struct {
	PolicyVersion string `json:"policy_version"`
	CreditUSD     string `json:"credit_usd"`
	MarkupBPS     int64  `json:"markup_bps"`
	BudgetUnits   int64  `json:"budget_units"`
	SourceVersion string `json:"source_version"`
}

var exactMediaDecimal = regexp.MustCompile(`^[0-9]{1,12}(\.[0-9]{1,18})?$`)

func mediaDecimal(raw string) (*big.Rat, error) {
	if !exactMediaDecimal.MatchString(raw) {
		return nil, errors.New("invalid exact media price")
	}
	value, ok := new(big.Rat).SetString(raw)
	if !ok {
		return nil, errors.New("invalid exact media price")
	}
	return value, nil
}

func mediaCeilUnits(amount *big.Rat) (int64, error) {
	if amount.Sign() < 0 {
		return 0, errors.New("negative media price")
	}
	numerator := new(big.Int).Mul(amount.Num(), big.NewInt(mediaUnitsPerUSD))
	units, remainder := new(big.Int), new(big.Int)
	units.QuoRem(numerator, amount.Denom(), remainder)
	if remainder.Sign() > 0 {
		units.Add(units, big.NewInt(1))
	}
	if !units.IsInt64() {
		return 0, errors.New("media price overflow")
	}
	return units.Int64(), nil
}

func mediaCostPlusRetailUnits(costUSD string) (int64, error) {
	cost, err := mediaDecimal(costUSD)
	if err != nil || cost.Sign() <= 0 {
		return 0, errors.New("verified positive supplier cost required")
	}
	return mediaCeilUnits(new(big.Rat).Mul(cost, big.NewRat(125, 100)))
}

func mediaSupplierActualUnits(policy *FrozenMediaSupplierPricing, credits *string, quoted int64) (int64, int64, error) {
	if policy == nil {
		return quoted, 0, nil
	}
	if policy.PolicyVersion != mediaCostPlusPolicyVersion || policy.CreditUSD != "0.005" || policy.MarkupBPS != 2500 || policy.BudgetUnits != quoted || quoted <= 0 || policy.SourceVersion == "" {
		return 0, 0, errors.New("invalid frozen media supplier policy")
	}
	if credits == nil {
		return 0, 0, errors.New("authoritative media supplier usage pending")
	}
	quantity, err := mediaDecimal(*credits)
	if err != nil {
		return 0, 0, err
	}
	cost := new(big.Rat).Mul(quantity, big.NewRat(5, 1000))
	retail, err := mediaCeilUnits(new(big.Rat).Mul(cost, big.NewRat(125, 100)))
	if err != nil {
		return 0, 0, err
	}
	// Supplier usage above the confirmed cap is an internal loss. It never
	// creates the existing overrun/remainder customer settlement event.
	if retail > quoted {
		return quoted, retail - quoted, nil
	}
	return retail, 0, nil
}
