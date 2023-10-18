package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBeethovenxSwap(t *testing.T) {
	t.Skip("disabled")
	// https://optimistic.etherscan.io/tx/0xb7fdd05351171f90abc41299a7d8df51cefe098935718add5f6fb7c8a0fc341d#eventlog
	pairs := doExtractPairs(t, "0xb7fdd05351171f90abc41299a7d8df51cefe098935718add5f6fb7c8a0fc341d", "https://rpc.ankr.com/optimism", "Optimism")
	assert.Len(t, pairs, 2, "no extracted")

	assert.Equal(t, "0x39965c9dab5448482cf7e002f583c812ceb53046000100000000000000000003-0x7f5c764cbc14f9669b88837ca1490cca17c31607-0x4200000000000000000000000000000000000042", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0xba12222222228d8ba445958a75a0704d566bf2c8", strings.ToLower(pairs[0].DexContract), "dex")
	assert.Equal(t, "0x7F5c764cBc14f9669B88837ca1490cCa17c31607", pairs[0].Input.Address, "output token")
	assert.Equal(t, "0x4200000000000000000000000000000000000042", pairs[0].Output.Address, "output token")
	assert.Equal(t, "1000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "734335454784035063", pairs[0].Output.Amount, "output amount")

	assert.Equal(t, "0xd6e5824b54f64ce6f1161210bc17eebffc77e031000100000000000000000006-0x4200000000000000000000000000000000000042-0xfe8b128ba8c78aabc59d4c64cee7ff28e9379921", strings.ToLower(pairs[1].PairContract), "pair")
	assert.Equal(t, "0xba12222222228d8ba445958a75a0704d566bf2c8", strings.ToLower(pairs[1].DexContract), "dex")
	assert.Equal(t, "0x4200000000000000000000000000000000000042", pairs[1].Input.Address, "output token")
	assert.Equal(t, "0xFE8B128bA8C78aabC59d4c64cEE7fF28e9379921", pairs[1].Output.Address, "output token")
	assert.Equal(t, "734335454784035063", pairs[1].Input.Amount, "input amount")
	assert.Equal(t, "303123160870559854", pairs[1].Output.Amount, "output amount")

}
