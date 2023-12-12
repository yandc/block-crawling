package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var bluemoveContracts = map[string]bool{
	"0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9": true,
}

type bluemove struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (s *bluemove) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	pairs := make([]*swap.Pair, 0, 4)
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)
	events, err := transactionInfo.Events()
	if err != nil {
		return nil, fmt.Errorf("[extract] %w", err)
	}

	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions {
		moveCall, err := tx.MoveCall()
		if err != nil {
			return nil, err
		}

		if moveCall == nil {
			continue
		}
		if s.isSwapMoveCall(moveCall) {
			input, output, err := extractInputAndOutput(moveCall)
			if err != nil {
				return nil, err
			}
			eventData, err := s.extractEvents(events)
			var amountIn, amountOut string
			if eventData.AmountXIn == "0" {
				amountIn = eventData.AmountYIn
				amountOut = eventData.AmountXOut
			} else {
				amountIn = eventData.AmountXIn
				amountOut = eventData.AmountYOut
			}

			pairs = append(pairs, &swap.Pair{
				TxHash:       transactionInfo.Digest,
				Dex:          s.Name(),
				DexContract:  moveCall.Package,
				PairContract: eventData.Pool,
				Input: swap.PairItem{
					Address: input,
					Amount:  amountIn,
				},
				Output: swap.PairItem{
					Address: output,
					Amount:  amountOut,
				},
			})
		}
	}
	return pairs, nil
}

type bluemoveEventData struct {
	AmountXIn  string `json:"amount_x_in"`
	AmountXOut string `json:"amount_x_out"`
	AmountYIn  string `json:"amount_y_in"`
	AmountYOut string `json:"amount_y_out"`
	Pool       string `json:"pool_id"`

	// --< omit lots of other fields >--
}

func (s *bluemove) extractEvents(events []stypes.Event) (*bluemoveEventData, error) {
	var data *bluemoveEventData
	_, err := extractEvents(s.swapModule, events, &data)
	return data, err
}

func newBluemove(name string, contracts map[string]bool) swap.SwapContract {
	return &bluemove{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   "router",
			swapFunction: "swap_exact_input",
		},
	}
}

func init() {
	swap.RegisterSwapContract(biz.SUI, newBluemove("bluemove", bluemoveContracts))
}
