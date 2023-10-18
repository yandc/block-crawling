package swap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIziswapBSC(t *testing.T) {
	// https://bscscan.com/tx/0x6ebc3046ef096ff61942ce863765d57e1c4ae304f69d6157bc05b25e2d94fa2a#eventlog
	pairs := doExtractPairs(t, "0x6ebc3046ef096ff61942ce863765d57e1c4ae304f69d6157bc05b25e2d94fa2a", "https://rpc.ankr.com/bsc", "BSC")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0xf964529721ecd0c9386d922a37cbbd2b67ea6e93", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0xBd3bd95529e0784aD973FD14928eEDF3678cfad8", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x55d398326f99059fF775485246999027B3197955", pairs[0].Output.Address, "output token")
	assert.Equal(t, "11000000000000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "2377626531203319946", pairs[0].Output.Amount, "output amount")
}

func TestIziswapZK(t *testing.T) {
	// https://explorer.zksync.io/tx/0x4bcc5ff00542a0e88fdc86b23c876270aba6ff3fe88b0db0bd6be2c45f589dff#eventlog
	pairs := doExtractPairs(t, "0x4bcc5ff00542a0e88fdc86b23c876270aba6ff3fe88b0db0bd6be2c45f589dff", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 2, "no extracted")

	lowerCaseEqual(t, "0x2158ec5366EfFcA1216af93Ee0D77762E2EBd466", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0x943ac2310D9BC703d6AB5e5e76876e212100f894", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91", pairs[0].Output.Address, "output token")
	assert.Equal(t, "15257120", pairs[0].Input.Amount, "output amount")
	assert.Equal(t, "9293999434636621", pairs[0].Output.Amount, "input amount")

	lowerCaseEqual(t, "0x49A17bEc0bc3a8D371D16aeF46F39eFba69BD9c6", pairs[1].PairContract, "pair")
	lowerCaseEqual(t, "0x943ac2310D9BC703d6AB5e5e76876e212100f894", pairs[1].DexContract, "dex")
	lowerCaseEqual(t, "0x1382628e018010035999A1FF330447a0751aa84f", pairs[1].Input.Address, "output token")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[1].Output.Address, "output token")
	assert.Equal(t, "15407359494651015157", pairs[1].Input.Amount, "output amount")
	assert.Equal(t, "15257120", pairs[1].Output.Amount, "input amount")
}
