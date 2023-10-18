package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var kyberswapV2Contracts = map[string]bool{
	// https://bscscan.com/address/0x6131B5fae19EA4f9D964eAc0408E4408b66337b5
	// https://arbiscan.io/address/0x6131b5fae19ea4f9d964eac0408e4408b66337b5
	"0x6131B5fae19EA4f9D964eAc0408E4408b66337b5": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("kyberswap", kyberswapV2Contracts))
}
