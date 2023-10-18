package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var (
	quickswapContracts = map[string]bool{
		"0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff": true,
	}
)

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("quickswap", quickswapContracts))
}
