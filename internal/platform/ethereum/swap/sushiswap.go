package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var sushiswapContracts = map[string]bool{
	// https://bscscan.com/address/0x1b02da8cb0d097eb8d57a175b88c7d8b47997506
	"0x1b02da8cb0d097eb8d57a175b88c7d8b47997506": true,

	// https://bscscan.com/address/0x89b8aa89fdd0507a99d334cbe3c808fafc7d850e
	"0x89b8AA89FDd0507a99d334CBe3C808fAFC7d850E": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("sushi", sushiswapContracts))
}
