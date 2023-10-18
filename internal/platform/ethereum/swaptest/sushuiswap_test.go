package swap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSushuiSwap(t *testing.T) {
	// https://bscscan.com/tx/0x045665b3b5250b99fc37d1c4fe3810136d4944b1db08c8b575a4f197fb1f729e#eventlog
	pairs := doExtractPairs(t, "0x045665b3b5250b99fc37d1c4fe3810136d4944b1db08c8b575a4f197fb1f729e", "https://rpc.ankr.com/bsc", "BSC")
	assert.Len(t, pairs, 1, "no extracted")
	assert.Equal(t, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", pairs[0].Input.Address, "input token")
	assert.Equal(t, "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82", pairs[0].Output.Address, "output token")
	assert.Equal(t, "16000000000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "2586919641442704", pairs[0].Output.Amount, "output amount")
}
