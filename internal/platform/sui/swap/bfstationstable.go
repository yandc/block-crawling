package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/platform/swap"
	"block-crawling/internal/utils"
	"encoding/json"
	"fmt"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const bfcNativeCoinType = "0000000000000000000000000000000000000000000000000000000000000002::bfc::BFC"

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

func (s *bfstationStable) extractPairFromEvents(events []stypes.Event) (*swap.Pair, error) {
	rawEvent, eventData, err := s.extractEvents(events)
	if err != nil {
		return nil, err
	}

	return &swap.Pair{
		Dex:          s.Name(),
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
	}, nil
}

func (s *bfstationStable) extractPairFromMoveCall(txData *stypes.TransactionData, moveCall *stypes.MoveCall) (*swap.Pair, error) {
	var inputToken, outputToken string
	input, err := moveCall.GetArgInput(3, txData)
	if err != nil {
		return nil, err
	}
	var rawEvent []byte
	amount := input.Value.(string)
	switch moveCall.Function {
	case stableB2SFunction:
		inputToken = bfcNativeCoinType
		outputToken = normalizeBenfenCoinType(moveCall.TypeArguments[0].(string))
		// mint
		rawEvent = []byte(`{"atob": false, "__spiderMock": true}`)
	case stableS2BFunction:
		inputToken = normalizeBenfenCoinType(moveCall.TypeArguments[0].(string))
		outputToken = bfcNativeCoinType
		// redeem
		rawEvent = []byte(`{"atob": true, "__spiderMock": true}`)
	default:
		return nil, nil
	}
	return &swap.Pair{
		Dex:          s.Name(),
		PairContract: "",
		Input: biz.SwapPairItem{
			Address: inputToken,
			Amount:  amount,
		},
		Output: biz.SwapPairItem{
			Address: outputToken,
			Amount:  amount,
		},
		RawEvent: rawEvent,
	}, nil
}

func StationizeBenfenCoinType(src string) string {
	return normalizeBenfenCoinType(src)
}

func normalizeBenfenCoinType(src string) string {
	if len(src) == 0 {
		return src
	}
	if strings.HasPrefix(src, "0x") {
		src = src[2:]
	}
	parts := strings.Split(src, "::")
	addr := parts[0]
	nPrefixZero := 64 - len(addr)
	parts[0] = strings.Repeat("0", nPrefixZero) + addr
	return strings.Join(parts, "::")
}

func NormalizeBenfenCoinType(chainName, src string) string {
	if utils.IsBenfenChain(chainName) && len(src) > 0 {
		src = normalizeBenfenCoinType(src)
		parts := strings.Split(src, "::")
		parts[0] = utils.EVMAddressToBFC(chainName, parts[0])
		return strings.Join(parts, "::")
	}
	return src
}

func DenormalizeBenfenCoinType(chainName, src string) string {
	if utils.IsBenfenChain(chainName) && strings.HasPrefix(src, "BFC") {
		parts := strings.Split(src, "::")
		parts[0] = "0x" + strings.TrimLeft(parts[0][3:len(parts[0])-4], "0")
		return strings.Join(parts, "::")
	}
	return src
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
	stableModule = "bfc_system"
	// /// X treasury  swap bfc to stablecoin
	// public entry fun swap_bfc_to_stablecoin<StableCoinType>(
	//     wrapper: &mut BfcSystemState,
	//     native_coin: Coin<BFC>,
	//     clock: &Clock,
	//     amount: u64,
	//     min_amount: u64,
	//     deadline: u64,
	//     ctx: &mut TxContext,
	// )
	stableB2SFunction = "swap_bfc_to_stablecoin"
	// /// X treasury  swap stablecoin to bfc
	// public entry fun swap_stablecoin_to_bfc<StableCoinType>(
	//     wrapper: &mut BfcSystemState,
	//     stable_coin: Coin<StableCoinType>,
	//     clock: &Clock,
	//     amount: u64,
	//     min_amount: u64,
	//     deadline: u64,
	//     ctx: &mut TxContext,
	// )
	stableS2BFunction = "swap_stablecoin_to_bfc"
)

func init() {
	swap.RegisterSwapContract(biz.SUI, newBfstationStable(biz.BFStationStable, bfstationStableContracts, stableModule, stableB2SFunction))
	swap.RegisterSwapContract(biz.SUI, newBfstationStable(biz.BFStationStable, bfstationStableContracts, stableModule, stableS2BFunction))
}
