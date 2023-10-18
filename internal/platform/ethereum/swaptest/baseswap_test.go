package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseSwap(t *testing.T) {
	// https://basescan.org/tx/0x01bcfa3cfd842e5548d6ac8b781a5392b2ca05c7d92cb5fe984a5ab171d167a4#eventlog
	pairs := doExtractPairs(t, "0x01bcfa3cfd842e5548d6ac8b781a5392b2ca05c7d92cb5fe984a5ab171d167a4", "https://base-mainnet.public.blastapi.io", "Base")

	assert.Len(t, pairs, 1, "no extracted") // TODO: contains a oddo swap

	assert.Equal(t, "0x3b8000cd10625abdc7370fb47ed4d4a9c6311fd5", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0x19ceead7105607cd444f5ad10dd51356436095a1", strings.ToLower(pairs[0].DexContract), "dex")
	assert.Equal(t, "0x4200000000000000000000000000000000000006", pairs[0].Input.Address, "output token")
	assert.Equal(t, "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA", pairs[0].Output.Address, "output token")
	assert.Equal(t, "2000000000000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "3245396", pairs[0].Output.Amount, "output amount")
}
