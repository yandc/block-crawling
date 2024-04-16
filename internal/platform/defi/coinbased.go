package defi

import (
	"block-crawling/internal/data"

	"github.com/shopspring/decimal"
)

type coinBasedProfitCalc struct {
}

// String implements Profiter
func (*coinBasedProfitCalc) String() string {
	return "coinbased"
}

// ComputeProfit implements Profiter
func (*coinBasedProfitCalc) ComputeProfit(asset *data.UserDeFiAsset, histories []*data.UserDeFiAssetTxnHistory) (*decimal.Decimal, error) {
	if asset.CostUsd.IsZero() {
		return &decimal.Zero, nil
	}
	profit := asset.ValueUsd.Sub(asset.CostUsd)
	return &profit, nil
}

// IsSuitable implements Profiter
func (*coinBasedProfitCalc) IsSuitable(asset *data.UserDeFiAsset) bool {
	return true
}

func init() {
	RegisterProfiter(&coinBasedProfitCalc{})
}
