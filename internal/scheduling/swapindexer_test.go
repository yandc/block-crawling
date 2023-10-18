package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListDexScreenerTxns(t *testing.T) {
	list, err := ListDexScreenerTxns(&topPair{
		ChainName: "ETH",
		DEX:       "uniswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/ethereum/0x2dC9050D9873F50526E467e983D435E6D8d9Afb0?q=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
	})
	assert.NoError(t, err)
	assert.Greater(t, len(list), 0, len(list))
}
