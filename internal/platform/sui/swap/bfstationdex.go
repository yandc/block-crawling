package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"encoding/json"
	"fmt"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
)

// https://tk3en79uf0.larksuite.com/wiki/ReEpwA0ykinzNzkOzveuthZKssg
const BFStationContract = "BFCe7d93ab2b98057bcb73c4fd42dcfd91931baf73d55eb6e9ec9be1255be6edc6e3226"

var bfstationDexContracts = map[string]bool{
	// "BFC4efe978bd7e02ed9f1cc199cec277c765357b7b3b812f8b63393b01d3000d391a3f8": true,
	BFStationContract: true,

	// rpc-mainnet.benfen.org
	"BFC30a9fb40135476278f112fb0bb41a699f189bccbb8168a7aec9884141208b4f5c65a": true,

	// testrpc.benfen.org
	"BFCcc78fd233560cd4258d19ff1f90edaf235b05dda6b42cdeff1a29edddd40987734fa": true,

	// devrpc.benfen.org
	"BFCf5151f4b39365bdf767531cc0cefa7d68f99d766d08f8cbb87a0160f5288b0cd3401": true,
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

	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions() {
		moveCall, err := tx.MoveCall()
		if err != nil {
			return nil, err
		}

		if moveCall == nil {
			continue
		}
		if s.isSwapMoveCall(moveCall) {
			var pair *swap.Pair
			var err error
			if len(events) > 0 {
				pair, err = s.extractPairFromEvents(events)
			} else {
				pair, err = s.extractPairFromMoveCall(&transactionInfo.Transaction.Data.Transaction, moveCall)
			}
			if err != nil {
				return nil, err
			}
			if pair == nil {
				continue
			}

			pair.TxHash = transactionInfo.Digest
			pair.DexContract = moveCall.Package
			pairs = append(pairs, pair)
		}
	}
	return pairs, nil
}

func (s *bfstationDexSwap) extractPairFromEvents(events []stypes.Event) (*swap.Pair, error) {
	rawEvent, eventData, err := s.extractEvents(events)
	if err != nil {
		return nil, err
	}
	var input, output string
	if eventData.Atob {
		input = eventData.CoinTypeA
		output = eventData.CoinTypeB
	} else {
		input = eventData.CoinTypeB
		output = eventData.CoinTypeA
	}
	amountIn, _ := decimal.NewFromString(eventData.AmountIn)
	feeAmount, _ := decimal.NewFromString(eventData.FeeAmount)
	amountIn = amountIn.Add(feeAmount)
	return &swap.Pair{
		Dex:          s.Name(),
		PairContract: eventData.Pool,
		Input: swap.PairItem{
			Address: input,
			Amount:  amountIn.String(),
		},
		Output: swap.PairItem{
			Address: output,
			Amount:  eventData.AmountOut,
		},
		RawEvent: rawEvent,
	}, nil
}

func (s *bfstationDexSwap) extractPairFromMoveCall(txData *stypes.TransactionData, moveCall *stypes.MoveCall) (*swap.Pair, error) {
	var inputToken, outputToken string
	input, err := moveCall.GetArgInput(4, txData)
	if err != nil {
		return nil, err
	}
	amount := input.Value.(string)
	pool, err := moveCall.GetArgObjectID(1, txData)
	if err != nil {
		return nil, err
	}
	switch moveCall.Function {
	case dexA2BFunction:
		inputToken = StationizeBenfenCoinType(moveCall.TypeArguments[0].(string))
		outputToken = StationizeBenfenCoinType(moveCall.TypeArguments[1].(string))
	case dexB2AFunction:
		outputToken = StationizeBenfenCoinType(moveCall.TypeArguments[0].(string))
		inputToken = StationizeBenfenCoinType(moveCall.TypeArguments[1].(string))
	default:
		return nil, nil
	}
	return &swap.Pair{
		Dex:          s.Name(),
		PairContract: pool,
		Input: biz.SwapPairItem{
			Address: inputToken,
			Amount:  amount,
		},
		Output: biz.SwapPairItem{
			Address: outputToken,
			Amount:  amount,
		},
		RawEvent: []byte(`{"__spiderMock": true}`),
	}, nil
}

type BFStationDexSwapEvent struct {
	BeforeSqrtPrice string `json:"before_sqrt_price"`
	AfterSqrtPrice  string `json:"after_sqrt_price"`
	Atob            bool   `json:"a_to_b"`
	AmountIn        string `json:"amount_in"`
	AmountOut       string `json:"amount_out"`
	CoinTypeA       string `json:"coin_type_a"`
	CoinTypeB       string `json:"coin_type_b"`
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

	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions() {
		moveCall, err := tx.MoveCall()
		if err != nil {
			return nil, err
		}
		if moveCall == nil {
			continue
		}
		if s.isSwapMoveCall(moveCall) {
			rawEvent, eventData, err := s.extractEvents(events)
			if err != nil {
				return nil, err
			}

			pairs = append(pairs, &swap.Pair{
				TxHash:       transactionInfo.Digest,
				Dex:          s.Name(),
				DexContract:  moveCall.Package,
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
	Action                  string `json:"action"`
	DeltaLiquidity          string `json:"delta_liquidity"`
	BeforePoolLiquidity     string `json:"before_pool_liquidity"`
	AfterPoolLiquidity      string `json:"after_pool_liquidity"`
	BeforePositionLiquidity string `json:"before_position_liquidity"`
	AfterPositionLiquidity  string `json:"after_position_liquidity"`
	AmountA                 string `json:"amount_a"`
	AmountB                 string `json:"amount_b"`
	CoinTypeA               string `json:"coin_type_a"`
	CoinTypeB               string `json:"coin_type_b"`
	Pool                    string `json:"pool"`
	Sender                  string `json:"sender"`
	Position                string `json:"position"`
}

func (s *bfstationDexLiq) extractEvents(events []stypes.Event) (json.RawMessage, *BFStationDexLiqEvent, error) {
	var filterEventTypes []string
	if s.swapFunction == dexOpenLiqFunction {
		filterEventTypes = append(filterEventTypes, "event::LiquidityEvent")
	}
	var data *BFStationDexLiqEvent
	raw, err := extractEvents(s.swapModule, events, &data, filterEventTypes...)
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
	dexAddLiqFunction2   = "add_liquidity_fix_coin_with_all"
	dexRemoveLiqFunction = "remove_liquidity"
	dexOpenLiqFunction   = "open_position_with_liquidity_with_all"
)

func init() {
	swap.RegisterSwapContract(biz.SUI, newBfstationDexSwap(biz.BFStationDexSwap, bfstationDexContracts, dexModule, dexA2BFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexSwap(biz.BFStationDexSwap, bfstationDexContracts, dexModule, dexB2AFunction))

	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexAddLiqFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexAddLiqFunction2))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexRemoveLiqFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationDexLiq(biz.BFStationDexLiq, bfstationDexContracts, dexModule, dexOpenLiqFunction))
}
