package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var pangolinswapContracts = map[string]bool{
	"0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("pangolin", pangolinswapContracts))
}
