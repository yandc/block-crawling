package swap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVelocore(t *testing.T) {
	// https://explorer.zksync.io/tx/0x63681088ce818d94b1c55e11705e5bc161d5248175cd6903e647e24e06ac4039#eventlog
	pairs := doExtractPairs(t, "0x63681088ce818d94b1c55e11705e5bc161d5248175cd6903e647e24e06ac4039", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0xcD52cbc975fbB802F82A1F92112b1250b5a997Df", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0xF29Eb540eEba673f8Fb6131a7C7403C8e4C3f143", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Output.Address, "output token")
	assert.Equal(t, "353115000000000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "576384829", pairs[0].Output.Amount, "output amount")
}

func TestVelocoreMultipleTransfers(t *testing.T) {
	// https://explorer.zksync.io/tx/0x548bc301f7e6bc9910e8c85f04fa825aaf9bdcebc9742a5cd99824d1fdd48754#eventlog
	pairs := doExtractPairs(t, "0x548bc301f7e6bc9910e8c85f04fa825aaf9bdcebc9742a5cd99824d1fdd48754", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0xcd52cbc975fbb802f82a1f92112b1250b5a997df", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0xF29Eb540eEba673f8Fb6131a7C7403C8e4C3f143", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91", pairs[0].Input.Address, "sell weth")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Output.Address, "got usdc")
	assert.Equal(t, "506000000000000", pairs[0].Input.Amount, "weth amount")
	assert.Equal(t, "796524", pairs[0].Output.Amount, "got usdc amount")
}
