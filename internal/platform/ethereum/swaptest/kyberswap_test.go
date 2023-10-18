package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKyberSwap(t *testing.T) {
	// https://arbiscan.io/tx/0x63f0a42984e2a01aae202bb5d1c3b17e9889731049e1adc87935353361e48079#eventlog
	pairs := doExtractPairs(t, "0x63f0a42984e2a01aae202bb5d1c3b17e9889731049e1adc87935353361e48079", "https://arbitrum.blockpi.network/v1/rpc/public", "Arbitrum")
	assert.Len(t, pairs, 2, "no extracted")

	assert.Equal(t, "0xdb9f3540955b3c5df246dba83b96727fcb3424a1", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0x912CE59144191C1204E64559FE8253a0e49E6548", pairs[0].Input.Address, "output token")
	assert.Equal(t, "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", pairs[0].Output.Address, "output token")
	assert.Equal(t, "261233646281863141099", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "248725331", pairs[0].Output.Amount, "output amount")

	assert.Equal(t, "0xc6f780497a95e246eb9449f5e4770916dcd6396a", strings.ToLower(pairs[1].PairContract), "pair")
	assert.Equal(t, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", pairs[1].Input.Address, "input token")
	assert.Equal(t, "0x912CE59144191C1204E64559FE8253a0e49E6548", pairs[1].Output.Address, "output token")
	assert.Equal(t, "150280594171536928", pairs[1].Input.Amount, "input amount")
	assert.Equal(t, "261233646281863141100", pairs[1].Output.Amount, "output amount")

	// TODO(wanghui): There is also an swap event log of DODO that didn't extract.
	// https://api.dodoex.io/dodo-contract/list
}
