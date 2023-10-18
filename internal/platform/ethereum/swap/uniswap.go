package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/swap"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	// event Swap(
	//     address indexed sender,
	//     uint amount0In,
	//     uint amount1In,
	//     uint amount0Out,
	//     uint amount1Out,
	//     address indexed to
	// );
	uniswapSwapEvent = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

	// /// @notice Emitted by the pool for any swaps between token0 and token1
	// /// @param sender The address that initiated the swap call, and that received the callback
	// /// @param recipient The address that received the output of the swap
	// /// @param amount0 The delta of the token0 balance of the pool
	// /// @param amount1 The delta of the token1 balance of the pool
	// /// @param sqrtPriceX96 The sqrt(price) of the pool after the swap, as a Q64.96
	// /// @param liquidity The liquidity of the pool after the swap
	// /// @param tick The log base 1.0001 of price of the pool after the swap
	// event Swap(
	//     address indexed sender,
	//     address indexed recipient,
	//     int256 amount0,
	//     int256 amount1,
	//     uint160 sqrtPriceX96,
	//     uint128 liquidity,
	//     int24 tick
	// );
	uniswapSqrtSwapEvent   = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
	uniswapDepositEvent    = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
	uniswapWithdrawalEvent = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
)

var (
	uniswapV3Contracts = map[string]bool{
		"0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45": true,
		"0xE592427A0AEce92De3Edee1F18E0157C05861564": true,

		// https://bscscan.com/address/0x3c11f6265ddec22f4d049dde480615735f451646
		"0x3c11F6265Ddec22f4d049Dde480615735f451646": true,
		// https://bscscan.com/address/0x96794f03b3114f50b892b0dec79d753658c931ee
		"0x96794F03b3114f50b892B0deC79D753658C931eE": true,

		// https://snowtrace.io/address/0xdef171fe48cf0115b1d80b88dc8eab59176fee57
		"0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57": true,
		// https://snowtrace.io/address/0x32c16e4d4790774ac3ee96c3ebd62b7b34bf18d9
		"0x32C16e4D4790774ac3EE96C3eBD62b7B34Bf18d9": true,
		// https://snowtrace.io/address/0xd6a1941174516304bdef62400881c235ed14264d
		"0xd6a1941174516304BdeF62400881C235ED14264d": true,
		// https://snowtrace.io/address/0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae
		"0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE": true,
		// https://snowtrace.io/address/0x3a23f943181408eac424116af7b7790c94cb97a5
		"0x3a23F943181408EAC424116Af7b7790c94Cb97a5": true,
		// https://snowtrace.io/address/0xcd2e3622d483c7dc855f72e5eafadcd577ac78b4
		"0xCD2E3622d483C7Dc855F72e5eafAdCD577ac78B4": true,
		// https://snowtrace.io/address/0xebf00ad513e398a336c61b3b8a59c8a6fcf4f770
		"0xEBF00Ad513e398a336C61B3B8A59c8A6fcf4F770": true,
		// https://snowtrace.io/address/0x0bdfd2701ebf6fb32a02d118075327edc3a354c8
		"0x0bDFd2701eBF6Fb32a02d118075327eDC3A354C8": true,
		// https://snowtrace.io/address/0x88de50b233052e4fb783d4f6db78cc34fea3e9fc
		"0x88de50B233052e4Fb783d4F6db78Cc34fEa3e9FC": true,
	}

	uniswapV2Contracts = map[string]bool{
		"0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D": true,
	}

	uniswapV1Contracts = map[string]bool{
		// https://snowtrace.io/address/0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad
		"0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD": true,

		// https://metamask.io/swaps/
		"0x881D40237659C251811CEC9c364ef91dC08D300C": true,
	}
)

var _ = map[string]bool{
	// curl --url-query url=https://uniswap.org/ https://api.gopluslabs.io/api/v1/dapp_security | jq ".result.contracts_security[].contracts[].contract_address" | sed -e 's/$/: true,/g'
	"0xc0a47dfe034b400b47bdad5fecda2621de6c4d95": true,
	"0x077d52b047735976dfda76fef74d4d988ac25196": true,
	"0x2e642b8d59b45a1d8c5aef716a84ff44ea665914": true,
	"0x1c6c712b1f4a7c263b1dbd8f97fb447c945d3b9a": true,
	"0x09cabec1ead1c0ba254b09efb3ee13841712be14": true,
	"0x60a87cc7fca7e53867facb79da73181b1bb4238b": true,
	"0xe8e45431b93215566ba923a7e611b7342ea954df": true,
	"0xd883264737ed969d2696ee4b4caf529c2fc2a141": true,
	"0xb7520a5f8c832c573d6bd0df955fc5c9b72400f7": true,
	"0x49c4f9bc14884f6210f28342ced592a633801a8b": true,
	"0xf173214c720f58e03e194085b1db28b50acdeead": true,
	"0xc6581ce3a005e2801c1e0903281bbd318ec5b5c2": true,
	"0x2c4bd064b998838076fa341a83d007fc2fa50957": true,
	"0x417cb32bc991fbbdcae230c7c4771cc0d69daa6b": true,
	"0xc040d51b07aea5d94a89bc21e8078b77366fc6c7": true,
	"0x394e524b47a3ab3d3327f7ff6629dc378c1494a3": true,
	"0x1aec8f11a7e78dc22477e91ed924fab46e3a88fd": true,
	"0x069c97dba948175d10af4b2414969e0b88d44669": true,
	"0x4e395304655f0796bc3bc63709db72173b9ddf98": true,
	"0xb6cfbf322db47d39331e306005dc7e5e6549942b": true,
	"0x17e5bf07d696eaf0d14caa4b44ff8a1e17b34de3": true,
	"0xa2881a90bf33f03e7a3f803765cd2ed5c8928dfb": true,
	"0x7dc095a5cf7d6208cc680fa9866f80a53911041a": true,
}

type uniswap struct {
	defaultIn
}

// ExtractPairs implements SwapContract
func (s *uniswap) ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*swap.Pair, error) {
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

func (*uniswap) extractOne(tx *chain.Transaction, receipt *rtypes.Receipt, cursor *int) (*swap.Pair, error) {
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

	var duplicatedSwapCursor *int
	defer func() {
		if duplicatedSwapCursor != nil {
			*cursor = *duplicatedSwapCursor
		}
	}()

	for ; *cursor >= 0; *cursor-- {
		var event = receipt.Logs[*cursor]
		var topic0 = strings.ToLower(event.Topics[0].Hex())

		switch topic0 {
		case uniswapDepositEvent:
			inputTxLog = event
		case uniswapWithdrawalEvent:
			outputTxLog = event
		case uniswapSwapEvent, uniswapSqrtSwapEvent, pancakeSqrtSwapEvent, iziswapEvent:
			if swapEvent == nil {
				swapEvent = event
				pairContract = event.Address.String()
			} else if duplicatedSwapCursor == nil {
				c := *cursor // copy
				duplicatedSwapCursor = &c
			}
		case transferEvent:
			if swapEvent == nil {
				continue
			}
			tokenAddr := event.Address.Hex()
			src := eventTopicToAddress(event.Topics[1])
			dst := eventTopicToAddress(event.Topics[2])
			if src == pairContract && !lowerCaseEqual(dst, tokenAddr) {
				outputTxLog = event
			}
			if dst == pairContract {
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

func newUniswap(name string, contracts map[string]bool) *uniswap {
	return &uniswap{
		defaultIn: newDefaultIn(name, contracts),
	}
}

func init() {
	swap.RegisterSwapContract(biz.EVM,
		newUniswap("uniswap", uniswapV1Contracts),
		newUniswap("uniswap", uniswapV2Contracts),
		newUniswap("uniswap", uniswapV3Contracts),
	)
}
