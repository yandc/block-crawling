package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/swap"
)

const (
	// event Swap(
	//     index_topic_1 address sender,
	//     index_topic_2 address recipient,
	//     int256 amount0,
	//     int256 amount1,
	//     uint160 sqrtPriceX96,
	//     uint128 liquidity,
	//     int24 tick,
	//     uint128 protocolFeesToken0,
	//     uint128 protocolFeesToken1
	// );
	pancakeSqrtSwapEvent = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
)

var pancakeswapContracts = map[string]bool{
	// https://bscscan.com/address/0x10ed43c718714eb63d5aa57b78b54704e256024e
	"0x10ED43C718714eb63d5aA57B78B54704E256024E": true,
	// https://bscscan.com/address/0x13f4ea83d0bd40e75c8222255bc855a974568dd4
	"0x13f4EA83D0bd40E75C8222255bc855a974568Dd4": true,
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newUniswap("pancakeswap", pancakeswapContracts))
}
