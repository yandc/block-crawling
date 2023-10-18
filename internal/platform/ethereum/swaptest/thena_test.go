package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThenaSwap(t *testing.T) {
	// https://bscscan.com/tx/0x263aa70afd42cf22e8f363c155759dcaa45eb75bf49faeefe27cb6f8531f36ee#eventlog
	pairs := doExtractPairs(t, "0x263aa70afd42cf22e8f363c155759dcaa45eb75bf49faeefe27cb6f8531f36ee", "https://rpc.ankr.com/bsc", "BSC")
	assert.Len(t, pairs, 2, "no extracted")

	assert.Equal(t, "0x172fcd41e0913e95784454622d1c3724f546f849", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", pairs[0].Input.Address, "output token")
	assert.Equal(t, "0x55d398326f99059fF775485246999027B3197955", pairs[0].Output.Address, "input token")
	assert.Equal(t, "207005733998869388", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "45084966744228245435", pairs[0].Output.Amount, "output amount")

	assert.Equal(t, "0xc6394f580157d7e8ae16f5fa1f8640d292eeddb2", strings.ToLower(pairs[1].PairContract), "pair")
	assert.Equal(t, "0x8ea5219a16c2dbF1d6335A6aa0c6bd45c50347C5", pairs[1].Input.Address, "output token")
	assert.Equal(t, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", pairs[1].Output.Address, "input token")
	assert.Equal(t, "4372129142904800398143", pairs[1].Input.Amount, "input amount")
	assert.Equal(t, "207005733998869388", pairs[1].Output.Amount, "output amount")
}
