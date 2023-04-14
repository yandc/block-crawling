package ptesting

import (
	"context"
	"fmt"

	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/casper"
	"block-crawling/internal/platform/cosmos"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/nervos"
	"block-crawling/internal/platform/polkadot"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/types"

	"github.com/agiledragon/gomonkey/v2"
)

// StubBackgrounds use gomonkey to patch background tasks during index block.
func StubBackgrounds() []*gomonkey.Patches {
	return []*gomonkey.Patches{
		gomonkey.ApplyFunc(aptos.HandleRecord, func(chainName string, client aptos.Client, txRecords []*data.AptTransactionRecord) {}),
		gomonkey.ApplyFunc(aptos.HandlePendingRecord, func(chainName string, client aptos.Client, txRecords []*data.AptTransactionRecord) {}),

		gomonkey.ApplyFunc(bitcoin.HandleRecord, func(chainName string, client bitcoin.Client, txRecords []*data.BtcTransactionRecord) {}),
		gomonkey.ApplyFunc(bitcoin.HandlePendingRecord, func(chainName string, client bitcoin.Client, txRecords []*data.BtcTransactionRecord) {}),

		gomonkey.ApplyFunc(casper.HandleRecord, func(chainName string, client casper.Client, txRecords []*data.CsprTransactionRecord) {}),

		gomonkey.ApplyFunc(cosmos.HandleRecord, func(chainName string, client cosmos.Client, txRecords []*data.AtomTransactionRecord) {}),
		gomonkey.ApplyFunc(cosmos.HandlePendingRecord, func(chainName string, client cosmos.Client, txRecords []*data.AtomTransactionRecord) {}),

		gomonkey.ApplyFunc(ethereum.HandleRecord, func(chainName string, client ethereum.Client, txRecords []*data.EvmTransactionRecord) {}),
		gomonkey.ApplyFunc(ethereum.HandlePendingRecord, func(chainName string, client ethereum.Client, txRecords []*data.EvmTransactionRecord) {}),

		gomonkey.ApplyFunc(nervos.HandleRecord, func(chainName string, client nervos.Client, txRecords []*data.CkbTransactionRecord) {}),
		gomonkey.ApplyFunc(nervos.HandlePendingRecord, func(chainName string, client nervos.Client, txRecords []*data.CkbTransactionRecord) {}),

		gomonkey.ApplyFunc(polkadot.HandleRecord, func(chainName string, client polkadot.Client, txRecords []*data.DotTransactionRecord) {}),
		gomonkey.ApplyFunc(polkadot.HandlePendingRecord, func(chainName string, client polkadot.Client, txRecords []*data.DotTransactionRecord) {}),

		gomonkey.ApplyFunc(solana.HandleRecord, func(chainName string, client solana.Client, txRecords []*data.SolTransactionRecord) {}),
		gomonkey.ApplyFunc(solana.HandlePendingRecord, func(chainName string, client solana.Client, txRecords []*data.SolTransactionRecord) {}),

		gomonkey.ApplyFunc(starcoin.HandleRecord, func(chainName string, client starcoin.Client, txRecords []*data.StcTransactionRecord) {}),
		gomonkey.ApplyFunc(starcoin.HandlePendingRecord, func(chainName string, client starcoin.Client, txRecords []*data.StcTransactionRecord) {}),

		gomonkey.ApplyFunc(sui.HandleRecord, func(chainName string, client sui.Client, txRecords []*data.SuiTransactionRecord) {}),
		gomonkey.ApplyFunc(sui.HandlePendingRecord, func(chainName string, client sui.Client, txRecords []*data.SuiTransactionRecord) {}),

		gomonkey.ApplyFunc(tron.HandleRecord, func(chainName string, client tron.Client, txRecords []*data.TrxTransactionRecord) {}),
		gomonkey.ApplyFunc(tron.HandlePendingRecord, func(chainName string, client tron.Client, txRecords []*data.TrxTransactionRecord) {}),
	}
}

// StubNodeProxyFuncs
func StubNodeProxyFuncs(chainName string, tokens []types.TokenInfo, nfts []*v1.GetNftReply_NftInfoResp, prices []TokenPrice) []*gomonkey.Patches {
	patches := make([]*gomonkey.Patches, 0, 4)
	if len(tokens) > 0 {
		log.Info("PATCH biz.GetTokenInfo")
		tokenInfoPatches := gomonkey.ApplyFunc(biz.GetTokenInfo, func(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
			for _, item := range tokens {
				if item.Address == tokenAddress {
					return item, nil
				}
			}
			return types.TokenInfo{}, fmt.Errorf("token has not been mocked: %s", tokenAddress)
		})
		patches = append(patches, tokenInfoPatches)
	}

	if len(nfts) > 0 {
		log.Info("PATCH biz.GetRawNftInfo")
		nftPatches := gomonkey.ApplyFunc(biz.GetRawNftInfo, func(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
			for _, item := range nfts {
				if item.TokenId == tokenId && item.TokenAddress == tokenAddress {
					return item, nil
				}
			}
			return nil, fmt.Errorf("token has not been mocked: %s - %s", tokenAddress, tokenId)
		})
		patches = append(patches, nftPatches)

		log.Info("PATCH biz.GetNftsInfo")
		nftsPatches := gomonkey.ApplyFuncReturn(biz.GetNftsInfo, nfts, nil)
		patches = append(patches, nftsPatches)
		patches = append(
			patches,
			gomonkey.ApplyFunc(biz.GetRawNftInfoDirectly, func(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
				for _, item := range nfts {
					if item.TokenId == tokenId && item.TokenAddress == tokenAddress {
						return item, nil
					}
				}
				return nil, fmt.Errorf("token has not been mocked: %s - %s", tokenAddress, tokenId)
			}),
		)
	}

	if len(prices) > 0 {
		pricesMap := make(map[string]string)

		for _, item := range prices {
			key := chainName + item.TokenAddress + item.Currency
			pricesMap[key] = item.Price
		}

		log.Info("PATCH biz.GetTokenPrice")
		tokenPricePatches := gomonkey.ApplyFunc(biz.GetTokenPrice, func(ctx context.Context, chainName string, currency string, tokenAddress string) (string, error) {
			key := chainName + tokenAddress + currency
			if v, ok := pricesMap[key]; ok {
				return v, nil
			}
			return "", fmt.Errorf("token's price has not been mocked: %s - %s", tokenAddress, currency)
		})
		patches = append(patches, tokenPricePatches)

		tokens := make(map[string]map[string]string)
		tokens[chainName] = pricesMap

		log.Info("PATCH biz.GetTokensPrice")
		tokensPricePatches := gomonkey.ApplyFuncReturn(biz.GetTokensPrice, tokens, nil)
		patches = append(patches, tokensPricePatches)
	}

	return patches
}

// StubTokenPushQueue use gomoney to hijack queue name of token pushing.
// To avoid different tests run in parallel cause conflicts.
func StubTokenPushQueue(topic string) func() {

	patches := gomonkey.ApplyMethodReturn(data.RedisQueueManager, "GetQueueName", fmt.Sprintf("%s:%s", topic, biz.TOKEN_INFO_QUEUE_PARTITION))
	return func() {
		patches.Reset()
	}
}
