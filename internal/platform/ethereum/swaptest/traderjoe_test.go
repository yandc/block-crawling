package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraderjoeSwap(t *testing.T) {
	// https://arbiscan.io/tx/0x2dc0c3ecdd244e5e153a673f69f982f5c612e1e153b377b8ccbc1f23cebb73bf#eventlog
	pairs := doExtractPairs(t, "0x2dc0c3ecdd244e5e153a673f69f982f5c612e1e153b377b8ccbc1f23cebb73bf", "https://arbitrum.blockpi.network/v1/rpc/public", "Arbitrum")
	assert.Len(t, pairs, 2, "no extracted")

	assert.Equal(t, "0xdf34e7548af638cc37b8923ef1139ea98644735a", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0x2297aEbD383787A160DD0d9F71508148769342E3", pairs[0].Input.Address, "output token")
	assert.Equal(t, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", pairs[0].Output.Address, "output token")
	assert.Equal(t, "119324", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "18801286300647658", pairs[0].Output.Amount, "output amount")

	assert.Equal(t, "0x7ec3717f70894f6d9ba0be00774610394ce006ee", strings.ToLower(pairs[1].PairContract), "pair")
	assert.Equal(t, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", pairs[1].Input.Address, "input token")
	assert.Equal(t, "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", pairs[1].Output.Address, "output token")
	assert.Equal(t, "18801286300647658", pairs[1].Input.Amount, "input amount")
	assert.Equal(t, "31134382", pairs[1].Output.Amount, "output amount")

}

func TestTraderjoeSwapUniswapStyle(t *testing.T) {
	// https://arbiscan.io/tx/0x017f1a7d3912d25893bd21f9e0575ea5706ddc132325e4047e3c9ff1ca6fd486#eventlog
	pairs := doExtractPairs(t, "0x017f1a7d3912d25893bd21f9e0575ea5706ddc132325e4047e3c9ff1ca6fd486", "https://arbitrum.blockpi.network/v1/rpc/public", "Arbitrum")
	assert.Len(t, pairs, 1, "no extracted")

	assert.Equal(t, "0x4bd82226469519cb9336303ba2de3897d7ec2a15", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0x371c7ec6D8039ff7933a2AA28EB827Ffe1F52f07", pairs[0].Input.Address, "input token")
	assert.Equal(t, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", pairs[0].Output.Address, "output token")
	assert.Equal(t, "9576332831319638218", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "1342375606057683", pairs[0].Output.Amount, "output amount")
}
