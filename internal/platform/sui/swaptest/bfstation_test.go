package swaptest

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBFStationFailed(t *testing.T) {
	pairs := doExtractBlockPairs(t, 988_748, "https://obcrpc.openblock.vip", "BenfenTEST")
	assert.Equal(t, 1, len(pairs), "extract 1 pairs")
	assert.Equal(t, "BFC12c4d095ef555abd3bd398df224262cec56a32f82008d8f0436c7a2f1a54c4a4f107", pairs[0].PairContract, "[0].pairContract")
	assert.Equal(t, "200000000000", pairs[0].Input.Amount, "[0].inputAmount")
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000002::bfc::BFC", pairs[0].Input.Address, "[0].inputToken")
	assert.Equal(t, "199999500038", pairs[0].Output.Amount, "[0].outputAmount")
	assert.Equal(t, "00000000000000000000000000000000000000000000000000000000000000c8::busd::BUSD", pairs[0].Output.Address, "[0].outputToken")
}

func doExtractBlockPairs(t *testing.T, height int, options ...string) []*swap.Pair {
	rpcURL := "https://fullnode.mainnet.sui.io"
	chain := "SUI"
	if len(options) > 1 {
		rpcURL = options[0]
		chain = options[1]
	}
	biz.SetChainNameType(chain, biz.SUI)
	client := sui.NewClient(rpcURL, chain)

	block, err := client.GetBlock(uint64(height))
	assert.NoError(t, err, "load block failed")
	results := make([]*swap.Pair, 0, 4)
	for _, tx := range block.Transactions {
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
		pairs, err := swap.AttemptToExtractSwapPairs(chain, contract, tx)

		assert.NoError(t, err, "extract pairs failed")
		results = append(results, pairs...)
	}
	return results
}
