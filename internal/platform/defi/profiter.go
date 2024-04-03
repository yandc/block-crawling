package defi

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"fmt"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// Profiter defines an interface to compute profit of DeFi assets.
type Profiter interface {
	fmt.Stringer

	// ComputeProfit compute profit for this defi asset.
	ComputeProfit(asset *data.UserDeFiAsset, histories []*data.UserDeFiAssetTxnHistory) (*decimal.Decimal, error)

	// IsSuitable returns true if current object is suitable to compute profit for this asset.
	IsSuitable(asset *data.UserDeFiAsset) bool
}

var profiters []Profiter

// RegisterProfiter register a Profiter to compute profit of DeFi assets.
// We'll attempt to use each registered profiter in a reverse order.
// We'll stop immediately once a profiter returned, and use it as the result.
func RegisterProfiter(p Profiter) {
	profiters = append(profiters, p)
}

func ComputeProfit(asset *data.UserDeFiAsset, histories []*data.UserDeFiAssetTxnHistory) decimal.Decimal {
	for i := len(profiters) - 1; i >= 0; i-- {
		p := profiters[i]
		if p.IsSuitable(asset) {
			if val, err := p.ComputeProfit(asset, histories); err != nil {
				log.Info("DEFI: COMPUTE PROFIT FAILED", zap.Any("asset", asset), zap.String("err", err.Error()), zap.String("profiter", p.String()))
			} else if val != nil {
				log.Info("DEFI: COMPUTE PROFIT SUCCESSED", zap.Any("asset", asset), zap.String("val", val.String()), zap.String("profiter", p.String()))
				return *val
			}
		}
	}
	return decimal.Zero
}
