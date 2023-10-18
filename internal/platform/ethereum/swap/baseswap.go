package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var baseSwapContracts = map[string]bool{
	"0x19cEeAd7105607Cd444F5ad10dd51356436095a1": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("baseswap", baseSwapContracts))
}
