package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var cetusContracts = map[string]bool{
	"0x2eeaab737b37137b94bfa8f841f92e36a153641119da3456dec1926b9960d9be": true,
}

type cetus struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (c *cetus) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
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
		if c.isSwapMoveCall(moveCall) {
			input, output, err := extractInputAndOutput(moveCall)
			if err != nil {
				return nil, err
			}
			eventData, err := c.extractEvents(events)
			if eventData == nil {
				continue
			}

			pairs = append(pairs, &swap.Pair{
				TxHash:       transactionInfo.Digest,
				Dex:          c.Name(),
				DexContract:  moveCall.Package,
				PairContract: eventData.Pool,
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

type cetusEventData struct {
	AmountIn  string `json:"amount_in"`
	AmountOut string `json:"amount_out"`
	Pool      string `json:"pool"`

	// --< omit lots of other fields >--
}

func (c *cetus) extractEvents(events []stypes.Event) (*cetusEventData, error) {
	var data *cetusEventData
	_, err := extractEvents(c.swapModule, events, &data)
	return data, err
}

func newCetus(name string, contracts map[string]bool) *cetus {
	return &cetus{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   "pool_script",
			swapFunction: "swap_a2b",
		},
	}
}

func init() {
	swap.RegisterSwapContract(biz.SUI, newCetus("cetus", cetusContracts))
}
