package swap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPancakeSwap(t *testing.T) {
	// https://bscscan.com/tx/0x70be3a13187da5d4933e7f9c805e87385397b61dd797e76eb7b936485e8983a4
	pairs := doExtractPairs(t, "0x70be3a13187da5d4933e7f9c805e87385397b61dd797e76eb7b936485e8983a4", "https://rpc.ankr.com/bsc", "BSC")
	assert.Len(t, pairs, 2, "no extracted")
	lowerCaseEqual(t, "0xb1f8c8d27ccbc0f7cc1d8bce5322c6434dc6e788", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", pairs[0].Input.Address, "input")
	lowerCaseEqual(t, "0x1b7cc2e9dfeadc0aa3c283d727c50df84558da59", pairs[0].Output.Address, "output")
	assert.Equal(t, "6826420875049032528997858646", pairs[0].Output.Amount, "output amount")
	assert.Equal(t, "336092042810320", pairs[0].Input.Amount, "input amount")

	lowerCaseEqual(t, "0x16b9a82891338f9ba80e2d6970fdda79d1eb0dae", pairs[1].PairContract, "pair")
	lowerCaseEqual(t, "0x55d398326f99059ff775485246999027b3197955", pairs[1].Input.Address, "input")
	lowerCaseEqual(t, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", pairs[1].Output.Address, "output")
	assert.Equal(t, "336092042810320", pairs[1].Output.Amount, "output amount")
	assert.Equal(t, "69500000000000000", pairs[1].Input.Amount, "input amount")
}
