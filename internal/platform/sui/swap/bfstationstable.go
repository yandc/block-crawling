package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"encoding/json"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var bfstationStableContracts = map[string]bool{
	"BFC00000000000000000000000000000000000000000000000000000000000000c8e30a": true,
}

type bfstationStable struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (s *bfstationStable) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
	pairs := make([]*swap.Pair, 0, 4)
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)
	events, err := transactionInfo.Events()
	if err != nil {
		return nil, fmt.Errorf("[extract] %w", err)
	}

	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions {
		if tx.MoveCall == nil {
			continue
		}
		moveCall := tx.MoveCall.(map[string]interface{})
		if s.isSwapMoveCall(moveCall) {
			pkg := getStr(moveCall, "package")
			rawEvent, eventData, err := s.extractEvents(events)
			if err != nil {
				return nil, err
			}

			pairs = append(pairs, &swap.Pair{
				TxHash:       transactionInfo.Digest,
				Dex:          s.Name(),
				DexContract:  pkg,
				PairContract: eventData.Vault,
				Input: swap.PairItem{
					Address: eventData.CoinTypeIn,
					Amount:  eventData.AmountIn,
				},
				Output: swap.PairItem{
					Address: eventData.CoinTypeOut,
					Amount:  eventData.AmountOut,
				},
				RawEvent: rawEvent,
			})
		}
	}
	return pairs, nil
}

type BFStationStableEvent struct {
	BeforeSqrtPrice string `json:"before_sqrt_price"`
	AfterSqrtPrice  string `json:"after_sqrt_price"`
	Atob            bool   `json:"atob"`
	AmountIn        string `json:"amount_in"`
	AmountOut       string `json:"amount_out"`
	CoinTypeIn      string `json:"coin_type_in"`
	CoinTypeOut     string `json:"coin_type_out"`
	Steps           string `json:"steps"`
	Vault           string `json:"vault"`
	VaultAAmount    string `json:"vault_a_amount"`
	VaultBAmount    string `json:"vault_b_amount"`
}

func (s *bfstationStable) extractEvents(events []stypes.Event) (json.RawMessage, *BFStationStableEvent, error) {
	var data *BFStationStableEvent
	raw, err := extractEvents(s.swapModule, events, &data)
	return raw, data, err
}

func newBfstationStable(name string, contracts map[string]bool, swapModule, swapFunction string) swap.SwapContract {
	return &bfstationStable{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   swapModule,
			swapFunction: swapFunction,
		},
	}
}

const (
	stableModule      = "bfc_system"
	stableB2SFunction = "swap_bfc_to_stablecoin"
	stableS2BFunction = "swap_stablecoin_to_bfc"
)

func init() {
	swap.RegisterSwapContract(biz.SUI, newBfstationStable(biz.BFStationStable, bfstationStableContracts, stableModule, stableB2SFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationStable(biz.BFStationStable, bfstationStableContracts, stableModule, stableS2BFunction))
}
