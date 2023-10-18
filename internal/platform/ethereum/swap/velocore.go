package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var velocoreContracts = map[string]bool{
	"0xF29Eb540eEba673f8Fb6131a7C7403C8e4C3f143": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("velocore", velocoreContracts))
}
