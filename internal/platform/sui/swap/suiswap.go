package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var suiswapContracts = map[string]bool{
	"0x319ecd39e76eec9dc32f51f596a37f5b5ed2a28ef55e82ec014ae654b4e3ac2a": true,
}

type suiswap struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (s *suiswap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	pairs := make([]*swap.Pair, 0, 4)
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)
	events, err := transactionInfo.Events()
	if err != nil {
		return nil, fmt.Errorf("[extract] %w", err)
	}
	var pool string
	for _, in := range transactionInfo.Transaction.Data.Transaction.Inputs {
		if in.ObjectType == "sharedObject" {
			pool = in.ObjectId
			break
		}
	}

	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions {
		if tx.MoveCall == nil {
			continue
		}
		moveCall := tx.MoveCall.(map[string]interface{})
		if s.isSwapMoveCall(moveCall) {
			input, output, err := extractInputAndOutput(moveCall)
			if err != nil {
				return nil, err
			}
			pkg := getStr(moveCall, "package")
			eventData, err := s.extractEvents(events)

			pairs = append(pairs, &swap.Pair{
				TxHash:       transactionInfo.Digest,
				Dex:          s.Name(),
				DexContract:  pkg,
				PairContract: pool,
				Input: swap.PairItem{
					Address: input,
					Amount:  eventData.AmountIn,
				},
				Output: swap.PairItem{
					Address: output,
					Amount:  eventData.AmountOut,
				},
			})
		}
	}
	return pairs, nil
}

type suiswapEventData struct {
	AmountIn  string `json:"in_amount"`
	AmountOut string `json:"out_amount"`

	// --< omit lots of other fields >--
}

func (s *suiswap) extractEvents(events []stypes.Event) (*suiswapEventData, error) {
	var data *suiswapEventData
	err := extractEvents(s.swapModule, events, &data)
	return data, err
}

func newSuiswap(name string, contracts map[string]bool) swap.SwapContract {
	return &suiswap{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   "pool",
			swapFunction: "swap_x_to_y",
		},
	}
}

func init() {
	swap.RegisterSwapContract(biz.SUI, newSuiswap("suiswap", suiswapContracts))
}
