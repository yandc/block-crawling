package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/swap"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

// https://syncswap.gitbook.io/syncswap/smart-contracts/smart-contracts
const syncswapVault = "0x621425a1Ef6abE91058E9712575dcc4258F8d091"

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
		if cursor < 0 {
			break
		}
		if pair == nil {
			continue
		}
		pairs = append(pairs, pair)
	}
	return pairs, nil
}

func (s *syncswap) extractOne(tx *chain.Transaction, receipt *rtypes.Receipt, cursor *int) (*swap.Pair, error) {
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
	var gobackCursor *int

	for ; *cursor >= 0; *cursor-- {
		var event = receipt.Logs[*cursor]
		var topic0 = strings.ToLower(event.Topics[0].Hex())

		switch topic0 {
		case uniswapSwapEvent:
			to := eventTopicToAddress(event.Topics[2])
			if lowerCaseEqual(to, tx.FromAddress) {
				c := *cursor - 1
				gobackCursor = &c
				swapEvent = event
				pairContract = event.Address.String()
			}
		case transferEvent:
			src := eventTopicToAddress(event.Topics[1])
			dst := eventTopicToAddress(event.Topics[2])

			if lowerCaseEqual(src, syncswapVault) {
				outputTxLog = event
			}
			if lowerCaseEqual(dst, syncswapVault) && outputTxLog != nil {
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

		// check amount
		var swapAmountIn, swapAmountOut string
		amounts, err := bytes2Amounts(swapEvent.Data[:32*4])
		if err != nil {
			return nil, err
		}
		swapAmount0In := amounts[0].String()
		swapAmount1In := amounts[1].String()
		swapAmount0Out := amounts[2].String()
		swapAmount1Out := amounts[3].String()
		if swapAmount0In != "0" {
			swapAmountIn = swapAmount0In
		} else {
			swapAmountIn = swapAmount1In
		}
		if swapAmount0Out != "0" {
			swapAmountOut = swapAmount0Out
		} else {
			swapAmountOut = swapAmount1Out
		}
		if swapAmountIn != amountIn.String() || swapAmountOut != amountOut.String() {
			if gobackCursor != nil {
				*cursor = *gobackCursor
			}
			return nil, nil
		}

		return &swap.Pair{
			TxHash:       tx.Hash,
			DexContract:  tx.ToAddress,
			PairContract: pairContract,
			Input: swap.PairItem{
				Address: s.nativeToWETH(inputTxLog.Address.Hex()),
				Amount:  amountIn.String(),
			},
			Output: swap.PairItem{
				Address: s.nativeToWETH(outputTxLog.Address.Hex()),
				Amount:  amountOut.String(),
			},
		}, nil
	}
	return nil, nil
}

func (s *syncswap) nativeToWETH(src string) string {
	if lowerCaseEqual(src, "0x000000000000000000000000000000000000800A") {
		return "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91"
	}
	return src
}

func newSyncswap(name string, contracts map[string]bool) *syncswap {
	return &syncswap{
		defaultIn: newDefaultIn(name, contracts),
	}
}

func init() {
	swap.RegisterSwapContract(biz.EVM, newSyncswap("syncswap", syncswapContracts))
}

func lowerCaseEqual(a string, b string) bool {
	return strings.ToLower(a) == strings.ToLower(b)
}
