package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

var iziswapContracts = map[string]bool{
	"0xBd3bd95529e0784aD973FD14928eEDF3678cfad8": true, // BSC
	"0x943ac2310D9BC703d6AB5e5e76876e212100f894": true, // zkSync Era
}

const (
	iziswapEvent = "0xe7779a36a28ae0e49bcbd9fcf57286fb607699c0c339c202e92495640505613e"
)

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("iziswap", iziswapContracts))
}
