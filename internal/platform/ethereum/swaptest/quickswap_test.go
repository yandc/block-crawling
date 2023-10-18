package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuickswap(t *testing.T) {
	// https://polygonscan.com/tx/0x1084ee551680b1711620e7fb9c07cf34c74bbfa14508dfa11d08979d1eca0c45
	pairs := doExtractPairs(t, "0x1084ee551680b1711620e7fb9c07cf34c74bbfa14508dfa11d08979d1eca0c45", "https://polygon.blockpi.network/v1/rpc/public", "Polygon")
	assert.Len(t, pairs, 1, "no extracted")

	assert.Equal(t, "0x29b6469ab9147c4bdf3a966961886733de9c8cf1", strings.ToLower(pairs[0].PairContract), "pair")
	assert.Equal(t, "0xE0339c80fFDE91F3e20494Df88d4206D86024cdF", pairs[0].Input.Address, "input token")
	assert.Equal(t, "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", pairs[0].Output.Address, "output token")
	assert.Equal(t, "595733004414085724431712256", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "148736085764417963737", pairs[0].Output.Amount, "output amount")
}
