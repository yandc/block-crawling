package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var thenaSwapContracts = map[string]bool{
	// https://bscscan.com/address/0x6352a56caadc4f1e25cd6c75970fa768a3304e64
	"0x6352a56caadC4F1E25CD6c75970Fa768A3304e64": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("thena", thenaSwapContracts))
}
