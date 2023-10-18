package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/swap"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var syncswapContracts = map[string]bool{
	"0x2da10A1e27bF85cEdD8FFb1AbBe97e53391C0295": true,
}

type syncswap struct {
	defaultIn
}

// ExtractPairs implements SwapContract
func (s *syncswap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	receipt := args[0].(*rtypes.Receipt)
	cursor := len(receipt.Logs) - 1
	pairs := make([]*swap.Pair, 0, 2)
	for {
		pair, err := s.extractOne(tx, receipt, &cursor)
		if err != nil {
			return nil, err
		}
		if pair == nil {
			break
		}
		pairs = append(pairs, pair)
	}
	return pairs, nil
}

func (*syncswap) extractOne(tx *chain.Transaction, receipt *rtypes.Receipt, cursor *int) (*swap.Pair, error) {
	var swapEvent *rtypes.Log
	var pairContract string

	// An input can be determined by:
	//
	//   Transfer to the pair contract
	var inputTxLog *rtypes.Log

	// An output can be determined by:
	//
	//   Transfer from the pair contract
	var outputTxLog *rtypes.Log

	for ; *cursor >= 0; *cursor-- {
		var event = receipt.Logs[*cursor]
		var topic0 = strings.ToLower(
			event.Topics[0].Hex())

		switch topic0 {
		case uniswapSwapEvent:
			swapEvent = event
			pairContract = event.Address.String()
		case transferEvent:
			if swapEvent == nil {
				outputTxLog = event
			} else {
				inputTxLog = event
			}
		}
		if swapEvent == nil {
			continue
		}
		if outputTxLog != nil && inputTxLog != nil {
			break
		}
	}
	if swapEvent == nil || inputTxLog == nil || outputTxLog == nil {
		return nil, nil
	}

	if inputTxLog != nil && outputTxLog != nil {
		amountIn, err := bytes2Amount(inputTxLog.Data)
		if err != nil {
			return nil, err
		}
		amountOut, err := bytes2Amount(outputTxLog.Data)
		if err != nil {
			return nil, err
		}
		return &swap.Pair{
			TxHash:       tx.Hash,
			DexContract:  tx.ToAddress,
			PairContract: pairContract,
			Input:        swap.PairItem{Address: inputTxLog.Address.Hex(), Amount: amountIn.String()},
			Output:       swap.PairItem{Address: outputTxLog.Address.Hex(), Amount: amountOut.String()},
		}, nil
	}
	return nil, nil
}

func newSyncswap(name string, contracts map[string]bool) *syncswap {
	return &syncswap{
		defaultIn: newDefaultIn(name, contracts),
	}
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newSyncswap("syncswap", syncswapContracts))
}
