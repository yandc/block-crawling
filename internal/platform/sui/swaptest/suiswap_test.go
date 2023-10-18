package swaptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSuiswap(t *testing.T) {
	pairs := doExtractPairs(t, "Ci8Vuo8Jwzd8wMvFq18sA1RTq8AUDRHz4Vsn1AsBsBpA")
	assert.Equal(t, 1, len(pairs), "extract 1 pairs")
	assert.Equal(t, "0xddb2164a724d13690e347e9078d4234c9205b396633dfc6b859b09b61bbcd257", pairs[0].PairContract, "[0].pairContract")
	assert.Equal(t, "1000000000", pairs[0].Input.Amount, "[0].inputAmount")
	assert.Equal(t, "0x2::sui::SUI", pairs[0].Input.Address, "[0].inputToken")
	assert.Equal(t, "967101", pairs[0].Output.Amount, "[0].inputAmount")
	assert.Equal(t, "0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN", pairs[0].Output.Address, "[0].outputToken")
}
