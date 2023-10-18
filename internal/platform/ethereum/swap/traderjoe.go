package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/swap"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var traderjoeSwapContracts = map[string]bool{
	// https://arbiscan.io/address/0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30
	// https://snowtrace.io/address/0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30
	"0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30": true,

	// https://snowtrace.io/address/0xad3b67bca8935cb510c8d18bd45f0b94f54a968f
	"0xad3b67BCA8935Cb510C8D18bD45F0b94F54A968f": true,

	// https://snowtrace.io/address/0xdef171fe48cf0115b1d80b88dc8eab59176fee57
	"0x9b2cc8e6a2bbb56d6be4682891a91b0e48633c72": true,
}

const (
	traderjoeSwapEvent       = "0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70"
	traderjoeSwapEventWithId = "0xc528cda9e500228b16ce84fadae290d9a49aecb17483110004c5af0a07f6fd73"
)

type traderjoeSwap struct {
	defaultIn
}

// ExtractPairs implements SwapContract
func (s *traderjoeSwap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	receipt := args[0].(*rtypes.Receipt)
	cursor := 0
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

func (s *traderjoeSwap) extractOne(tx *chain.Transaction, receipt *rtypes.Receipt, cursor *int) (*swap.Pair, error) {
	logs := receipt.Logs
	var outputIdx *int
	var inputEvent, outputEvent, swapEvent *rtypes.Log
	for ; *cursor < len(logs); *cursor++ {
		var event = logs[*cursor]
		var topic0 = strings.ToLower(event.Topics[0].Hex())
		switch topic0 {
		case transferEvent:
			if swapEvent == nil {
				inputEvent = event
			} else {
				outputEvent = event
				outputIdx = new(int)
				*outputIdx = *cursor
			}
		case traderjoeSwapEvent, traderjoeSwapEventWithId:
			swapEvent = event
		}
		if inputEvent != nil && outputEvent != nil && swapEvent != nil {
			if outputIdx != nil {
				// current output may be the input of the next pair.
				*cursor = *outputIdx
			}
			break
		}
	}
	if inputEvent != nil && outputEvent != nil && swapEvent != nil {
		amountIn, err := bytes2Amount(inputEvent.Data)
		if err != nil {
			return nil, err
		}
		amountOut, err := bytes2Amount(outputEvent.Data)
		if err != nil {
			return nil, err
		}
		return &swap.Pair{
			TxHash:       tx.Hash,
			DexContract:  tx.ToAddress,
			PairContract: swapEvent.Address.Hex(),
			Input: swap.PairItem{
				Address: inputEvent.Address.Hex(),
				Amount:  amountIn.String(),
			},
			Output: swap.PairItem{
				Address: outputEvent.Address.Hex(),
				Amount:  amountOut.String(),
			},
		}, nil
	}
	return nil, nil
}

func newTradeJoe(name string, contracts map[string]bool) *traderjoeSwap {
	return &traderjoeSwap{
		defaultIn: newDefaultIn(name, contracts),
	}
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newTradeJoe("traderjoe", traderjoeSwapContracts), newUniswap("traderjoe", traderjoeSwapContracts))
}
