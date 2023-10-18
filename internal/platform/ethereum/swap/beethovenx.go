package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/swap"
	"fmt"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	beethovenxSwapEvent = "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b"
)

var beethovenxSwapContracts = map[string]bool{
	"0xBA12222222228d8Ba445958a75a0704d566BF2C8": true,
}

type beethovenxSwap struct {
	defaultIn
}

// ExtractPairs implements SwapContract
func (s *beethovenxSwap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	receipt := args[0].(*rtypes.Receipt)
	paris := make([]*swap.Pair, 0, 2)
	for _, event := range receipt.Logs {
		topic0 := strings.ToLower(event.Topics[0].Hex())
		if topic0 != beethovenxSwapEvent {
			continue
		}
		poolId := event.Topics[1].Hex()
		tokenIn := eventTopicToAddress(event.Topics[2])
		tokenOut := eventTopicToAddress(event.Topics[3])
		amounts, err := bytes2Amounts(event.Data)
		if err != nil {
			return nil, err
		}
		paris = append(paris, &swap.Pair{
			TxHash:       tx.Hash,
			DexContract:  tx.ToAddress,
			PairContract: fmt.Sprintf("%s-%s-%s", poolId, tokenIn, tokenOut),
			Input: swap.PairItem{
				Address: tokenIn,
				Amount:  amounts[0].String(),
			},
			Output: swap.PairItem{
				Address: tokenOut,
				Amount:  amounts[1].String(),
			},
		})
	}
	return paris, nil
}

// Is implements SwapContract
func (s *beethovenxSwap) Is(chainName string, tx *chain.Transaction) (bool, error) {
	return false, nil // Ignore this swap contract as its pair pair address format
}

func newBeethovenx(name string, contracts map[string]bool) *beethovenxSwap {
	return &beethovenxSwap{
		defaultIn: newDefaultIn(name, contracts),
	}

}

func init() {
	swap.RegisterSwapContract(biz.EVM, newBeethovenx("beethovenx", beethovenxSwapContracts))
}
