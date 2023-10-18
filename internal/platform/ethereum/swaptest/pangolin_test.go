package swap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPangolin(t *testing.T) {
	// https://snowtrace.io/tx/0xc3a521f76c9831002b1b8b8c6fd634b600826c951a787c29e2f6c8c2463f128e#eventlog
	pairs := doExtractPairs(t, "0xc3a521f76c9831002b1b8b8c6fd634b600826c951a787c29e2f6c8c2463f128e", "https://rpc.ankr.com/avalanche", "Avalanche")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0x7c05d54fc5cb6e4ad87c6f5db3b807c94bb89c52", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0xe54ca86531e17ef3616d22ca28b0d458b6c89106", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", pairs[0].Output.Address, "output token")
	assert.Equal(t, "156215441375179571", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "28000000000000000000", pairs[0].Output.Amount, "output amount")
}
