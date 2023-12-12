package swaptest

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCetus(t *testing.T) {
	pairs := doExtractPairs(t, "6wthaE4QzjWM7HnAzm6Pd4ymkNtadbHnqQkTXY7Ne1L3")
	assert.Equal(t, 1, len(pairs), "extract 1 pairs")
	assert.Equal(t, "0xcf994611fd4c48e277ce3ffd4d4364c914af2c3cbb05f7bf6facd371de688630", pairs[0].PairContract, "[0].pairContract")
	assert.Equal(t, "42724144", pairs[0].Input.Amount, "[0].inputAmount")
	assert.Equal(t, "0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN", pairs[0].Input.Address, "[0].inputToken")
	assert.Equal(t, "96272716239", pairs[0].Output.Amount, "[0].inputAmount")
	assert.Equal(t, "0x2::sui::SUI", pairs[0].Output.Address, "[0].outputToken")
}

func doExtractPairs(t *testing.T, th string, options ...string) []*swap.Pair {
	rpcURL := "https://fullnode.mainnet.sui.io"
	chain := "SUI"
	if len(options) > 1 {
		rpcURL = options[0]
		chain = options[1]
	}
	biz.SetChainNameType(chain, biz.SUI)
	client := sui.NewClient(rpcURL, chain)

	tx, err := client.GetTxByHash(th)
	assert.NoError(t, err, "load tx failed")
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)
	var contract string
	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions {
		moveCall, err := tx.MoveCall()
		assert.NoError(t, err, "parse move call")
		if moveCall == nil {
			continue
		}
		contract = moveCall.Package

	}
	println("contract", contract)
	pairs, err := swap.AttemptToExtractSwapPairs(chain, contract, tx)

	assert.NoError(t, err, "extract pairs failed")
	return pairs
}
