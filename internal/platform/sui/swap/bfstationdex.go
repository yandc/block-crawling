package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"encoding/json"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var bfstationDexContracts = map[string]bool{
	"BFC4efe978bd7e02ed9f1cc199cec277c765357b7b3b812f8b63393b01d3000d391a3f8": true,
}

type bfstationDexSwap struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (s *bfstationDexSwap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
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
				PairContract: eventData.Pool,
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

type BFStationDexSwapEvent struct {
	BeforeSqrtPrice string `json:"before_sqrt_price"`
	AfterSqrtPrice  string `json:"after_sqrt_price"`
	Atob            bool   `json:"a_to_b"`
	AmountIn        string `json:"amount_in"`
	AmountOut       string `json:"amount_out"`
	CoinTypeIn      string `json:"coin_type_in"`
	CoinTypeOut     string `json:"coin_type_out"`
	Steps           string `json:"steps"`
	Pool            string `json:"pool"`
	Sender          string `json:"sender"`
	FeeAmount       string `json:"fee_amount"`
	VaultAAmount    string `json:"vault_a_amount"`
	VaultBAmount    string `json:"vault_b_amount"`
}

func (s *bfstationDexSwap) extractEvents(events []stypes.Event) (json.RawMessage, *BFStationDexSwapEvent, error) {
	var data *BFStationDexSwapEvent
	raw, err := extractEvents(s.swapModule, events, &data)
	return raw, data, err
}

func newBfstationDexSwap(name string, contracts map[string]bool, swapModule, swapFunction string) swap.SwapContract {
	return &bfstationDexSwap{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   swapModule,
			swapFunction: swapFunction,
		},
	}
}

type bfstationDexLiq struct {
	defaultIn
}

// ExtractPairs implements swap.SwapContract
func (s *bfstationDexLiq) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
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
				PairContract: eventData.Pool,
				Input: swap.PairItem{
					Address: eventData.CoinTypeA,
					Amount:  eventData.AmountA,
				},
				Output: swap.PairItem{
					Address: eventData.CoinTypeB,
					Amount:  eventData.AmountB,
				},
				RawEvent: rawEvent,
			})
		}
	}
	return pairs, nil
}

type BFStationDexLiqEvent struct {
	Action         string `json:"action"`
	Liquidity      string `json:"liquidity"`
	AfterLiquidity string `json:"after_liquidity"`
	AmountA        string `json:"amount_a"`
	AmountB        string `json:"amount_b"`
	CoinTypeA      string `json:"coin_type_a"`
	CoinTypeB      string `json:"coin_type_b"`
	Pool           string `json:"pool"`
	Sender         string `json:"sender"`
	Position       string `json:"position"`
}

func (s *bfstationDexLiq) extractEvents(events []stypes.Event) (json.RawMessage, *BFStationDexLiqEvent, error) {
	var data *BFStationDexLiqEvent
	raw, err := extractEvents(s.swapModule, events, &data)
	return raw, data, err
}

func newBfstationDexLiq(name string, contracts map[string]bool, swapModule, swapFunction string) swap.SwapContract {
	return &bfstationDexLiq{
		defaultIn: defaultIn{
			name:         name,
			contracts:    contracts,
			swapModule:   swapModule,
			swapFunction: swapFunction,
		},
	}
}

const (
	dexModule            = "pool_script"
	dexA2BFunction       = "swap_a2b"
	dexB2AFunction       = "swap_b2a"
	dexAddLiqFunction    = "add_liquidity"
	dexRemoveLiqFunction = "remove_liquidity"
)

func init() {
	swap.RegisterSwapContract(biz.SUI, newBfstationDexSwap(biz.BFStationDexSwap, bfstationDexContracts, dexModule, dexA2BFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexSwap(biz.BFStationDexSwap, bfstationDexContracts, dexModule, dexB2AFunction))

	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexAddLiqFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexRemoveLiqFunction))
}
