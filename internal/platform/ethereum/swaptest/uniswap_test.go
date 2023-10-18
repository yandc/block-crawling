package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum"
	_ "block-crawling/internal/platform/ethereum/swap" // register
	"block-crawling/internal/platform/swap"
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestUniswapExtractEventLog(t *testing.T) {
	// https://etherscan.io/tx/0xeedf302cc12f4b7194742694aabec075c1c229f7b9d8e57a53c44c992bc6690c#eventlog
	pairs := doExtractPairs(t, "0xeedf302cc12f4b7194742694aabec075c1c229f7b9d8e57a53c44c992bc6690c")
	assert.Len(t, pairs, 1, "no extracted")
	// to check pair contract, use:
	// https://api.dexscreener.com/latest/dex/pairs/ethereum/0x7235C4aA48B753e48c3786eC60D3BDdEF5F4b27a
	assert.Equal(t, "0x7235C4aA48B753e48c3786eC60D3BDdEF5F4b27a", pairs[0].PairContract, "pair")
	assert.Equal(t, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", pairs[0].Input.Address, "input")
	assert.Equal(t, "0xA3c31927A092BD54eb9A0B5DfE01D9DB5028BD4f", pairs[0].Output.Address, "output")
	assert.Equal(t, "70000000000000000", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "36113702113197", pairs[0].Output.Amount, "amountOut")

	// https://etherscan.io/tx/0xbc12088a86871cd6cded657d30ba5f435a3c2f3adb8dd64e5df5638a9b5c28cf#eventlog
	pairs = doExtractPairs(t, "0xbc12088a86871cd6cded657d30ba5f435a3c2f3adb8dd64e5df5638a9b5c28cf")
	assert.Len(t, pairs, 1, "no extracted")
	assert.Equal(t, "0xA04c6c14d192B961849979C3D86A659BE7aDAc13", pairs[0].PairContract, "pair")
	assert.Equal(t, "0xab39b2eb812edb1358a8e37F7196620AcF49840d", pairs[0].Input.Address, "input")
	assert.Equal(t, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", pairs[0].Output.Address, "ouptut")
	assert.Equal(t, "498749287500000000000000", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "171297568215285807", pairs[0].Output.Amount, "amountOut")

	// https://etherscan.io/tx/0xc692adc920cdfdebd1968e37a464b68bb3e03fe16c628a0739d325d7f9f6237c#eventlog
	pairs = doExtractPairs(t, "0xc692adc920cdfdebd1968e37a464b68bb3e03fe16c628a0739d325d7f9f6237c")
	assert.Len(t, pairs, 1, "no extracted")
	assert.Equal(t, "0xc4dC8C567631E92289c2Ec42943BB241E6897Ec6", pairs[0].PairContract, "pair")
	assert.Equal(t, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", pairs[0].Input.Address, "input")
	assert.Equal(t, "0x5a7e858ccC1035cc9B22a4797745b410bE1C1Ddc", pairs[0].Output.Address, "output")
	assert.Equal(t, "100000000000000000", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "147121915545984428", pairs[0].Output.Amount, "amountOut")

	// https://etherscan.io/tx/0xd90c17ecc838025a21d4bd2096cea200d246da4b56e1f0e541b07e797f3ab59f#eventlog
	pairs = doExtractPairs(t, "0xd90c17ecc838025a21d4bd2096cea200d246da4b56e1f0e541b07e797f3ab59f")
	assert.Equal(t, "0x11b815efB8f581194ae79006d24E0d814B7697F6", pairs[0].PairContract, "pair")
	assert.Equal(t, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", pairs[0].Input.Address, "input")
	assert.Equal(t, "0xdAC17F958D2ee523a2206206994597C13D831ec7", pairs[0].Output.Address, "output")
	assert.Equal(t, "5523066150635316224", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "10092862024", pairs[0].Output.Amount, "amountOut")

	// https://etherscan.io/tx/0x76986f19a860a4d1f615a389adc753b96dc956c2777b5932baeb69c0f82a351f#eventlog
	pairs = doExtractPairs(t, "0x76986f19a860a4d1f615a389adc753b96dc956c2777b5932baeb69c0f82a351f")
	assert.Equal(t, "0xB36414dcE85612f30Ce554f7b4CD3919B5d08d14", pairs[0].PairContract, "pair")
	assert.Equal(t, "0x5e1E13Ab826fD7D503726fabe56Fa3fA9C6E5F9a", pairs[0].Input.Address, "input")
	assert.Equal(t, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", pairs[0].Output.Address, "output")
	assert.Equal(t, "6317586934618392", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "74840586198497054", pairs[0].Output.Amount, "amountOut")

	// https://etherscan.io/tx/0x719df64fed3ac145abda79f91007991113d8e6c2406ec8fe91bd1e54e6472dfa#eventlog
	pairs = doExtractPairs(t, "0x719df64fed3ac145abda79f91007991113d8e6c2406ec8fe91bd1e54e6472dfa")
	assert.Equal(t, "0x1Be83c63FBa9D4ea41aAD6C2C0A66e0E742e7fD4", pairs[0].PairContract, "pair")
	assert.Equal(t, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", pairs[0].Input.Address, "input")
	assert.Equal(t, "0xB4272071eCAdd69d933AdcD19cA99fe80664fc08", pairs[0].Output.Address, "output")
	assert.Equal(t, "10142479277", pairs[0].Input.Amount, "amountIn")
	assert.Equal(t, "8889802581439192569003", pairs[0].Output.Amount, "amountOut")
}

func doExtractPairs(t *testing.T, th string, options ...string) []*biz.SwapPair {
	rpcURL := "https://rpc.ankr.com/eth"
	chain := "ETH"
	if len(options) > 1 {
		rpcURL = options[0]
		chain = options[1]
	}
	biz.SetChainNameType(chain, biz.EVM)
	client := ethereum.NewClient(rpcURL, chain)

	txHash := common.HexToHash(th)
	tx, err := client.GetTxByHash(th)
	assert.NoError(t, err, "load tx failed")

	receipt, err := client.TransactionReceipt(context.Background(), txHash)
	assert.NoError(t, err, "load receipt failed")

	pairs, err := swap.AttemptToExtractSwapPairs(chain, receipt.To, tx, receipt)

	assert.NoError(t, err, "extract pairs failed")
	return pairs
}
