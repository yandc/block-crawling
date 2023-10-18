package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"context"
	"time"

	"github.com/robfig/cron/v3"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

// TopPair is the top pair of each DEX on each chain to find the contract in the transactions.
type topPair struct {
	ChainName string
	DEX       string // the name of the dex
	URL       string // How to get this: http://recordit.co/VCLedUA7xz
}

type dexScreenerLogResponse struct {
	SchemaVersion    string           `json:"schemaVersion"`
	BaseTokenSymbol  string           `json:"baseTokenSymbol"`
	QuoteTokenSymbol string           `json:"quoteTokenSymbol"`
	Logs             []dexScreenerLog `json:"logs"`
}

type dexScreenerLog struct {
	LogType string `json:"logType"`
	TxnHash string `json:"txnHash"`
	// omit a lot fields
}

func ListDexScreenerTxns(pair *topPair) ([]string, error) {
	var response *dexScreenerLogResponse
	if err := httpclient.GetUseCloudscraper(pair.URL, &response, nil); err != nil {
		return nil, err
	}
	results := make([]string, 0, 10)
	for _, log := range response.Logs {
		if log.LogType == "swap" {
			results = append(results, log.TxnHash)
		}
	}
	return results, nil
}

type SwapIndexerTask struct {
}

// Run implements cron.Job
func (*SwapIndexerTask) Run() {
	for _, tp := range topPairs {
		log.Info("LOADING TXNs FROM DEX SCREENER", zap.Any("topPair", tp))
		txHashes, err := ListDexScreenerTxns(&tp)
		if err != nil {
			log.Warn("LOAD TXNs FROM DEX SCREENER FAILED", zap.Any("topPair", tp), zap.Error(err))
			continue
		}
		log.Info("LOADED TXNs FROM DEX SCREENER", zap.Any("topPair", tp), zap.Int("txnsNum", len(txHashes)))
		records := make([]*data.SwapContract, 0, 8)
		for _, txHash := range txHashes {
			biz.ExecuteRetry(tp.ChainName, func(client chain.Clienter) (interface{}, error) {
				tx, err := client.GetTxByHash(txHash)
				if err != nil {
					return nil, err
				}
				log.Info("FOUND SWAP CONTRACT FROM THE LOGS OF DEX SCREENER", zap.Any("topPair", tp), zap.String("txHash", txHash), zap.String("swapContract", tx.ToAddress))
				r, err := data.SwapContractRepoClient.FindOne(context.Background(), tp.ChainName, tx.ToAddress)
				if r != nil && err == nil {
					return nil, nil
				}
				records = append(records, &data.SwapContract{
					ChainName: tp.ChainName,
					Contract:  tx.ToAddress,
					DEX:       tp.DEX,
					TxHash:    txHash,
					CreatedAt: time.Now().Unix(),
				})
				return nil, nil
			})
		}
		if len(records) > 0 {
			if err := data.SwapContractRepoClient.BatchSave(context.Background(), tp.ChainName, records); err != nil {
				log.Warn("SAVE SWAP CONTRACT FROM THE LOGS OF DEX SCREENER FAILED", zap.Any("topPair", tp), zap.Error(err))
				continue
			}
			log.Info("SAVE SWAP CONTRACT FROM THE LOGS OF DEX SCREENER", zap.Any("topPair", tp), zap.Int("length", len(records)))
		}
	}
}

func NewSwapIndexerTask() cron.Job {
	return &SwapIndexerTask{}
}

var topPairs = []topPair{
	{
		ChainName: "ETH",
		DEX:       "uniswap", // uniswap_v2
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/ethereum/0x3C5F0C9e503175e3138AF564BbAbbA10217f8CEE?q=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
	},
	{
		ChainName: "ETH",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/ethereum/0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640?q=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
	},
	{
		ChainName: "ETH",
		DEX:       "pancakeswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/pcsv3/all/ethereum/0x6CA298D2983aB03Aa1dA7679389D955A4eFEE15C?q=0xdAC17F958D2ee523a2206206994597C13D831ec7",
	},
	{
		ChainName: "ETH",
		DEX:       "sushiswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/ethereum/0x06da0fd433C1A5d7a4faa01111c044910A184553?q=0xdAC17F958D2ee523a2206206994597C13D831ec7",
	},

	{
		ChainName: "BSC",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/bsc/0x6fe9E9de56356F7eDBfcBB29FAB7cd69471a4869?q=0x55d398326f99059fF775485246999027B3197955",
	},
	{
		ChainName: "BSC",
		DEX:       "pancakeswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/bsc/0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE?q=0x55d398326f99059fF775485246999027B3197955",
	},
	{
		ChainName: "BSC",
		DEX:       "thena",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/bsc/0xD405b976Ac01023c9064024880999fC450A8668b?q=0x55d398326f99059fF775485246999027B3197955",
	},

	{
		ChainName: "Arbitrum",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/arbitrum/0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443?q=0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
	},
	{
		ChainName: "Arbitrum",
		DEX:       "traderjoe",
		URL:       "https://io.dexscreener.com/dex/log/amm/joev2p1/all/arbitrum/0xdF34e7548AF638cC37b8923ef1139EA98644735a?q=0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
	},
	{
		ChainName: "Arbitrum",
		DEX:       "kyberswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/arbitrum/0xdb0f89c3aEb216d4329985442039F3737CBFc949?q=0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
	},

	{
		ChainName: "Polygon",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/polygon/0xA374094527e1673A86dE625aa59517c5dE346d32?q=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
	},
	{
		ChainName: "Polygon",
		DEX:       "quickswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/polygon/0xAE81FAc689A1b4b1e06e7ef4a2ab4CD8aC0A087D?q=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
	},

	{
		ChainName: "Optimism",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/optimism/0xD1F1baD4c9E6c44DeC1e9bF3B94902205c5Cd6C3?q=0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
	},
	{
		ChainName: "Optimism",
		DEX:       "kyberswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/optimism/0x683860E93fAB18B8e2E52f9bE310d9b36B49677a?q=0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
	},
	{
		ChainName: "Optimism",
		DEX:       "beethovenx",
		URL:       "https://io.dexscreener.com/dex/balancer/log/logs/optimism/0x7ca75bdea9dede97f8b13c6641b768650cb837820002000000000000000000d5-0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb-0x4200000000000000000000000000000000000006?q=0x4200000000000000000000000000000000000006",
	},

	{
		ChainName: "evm8453", // Base
		DEX:       "baseswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/base/0xF8c9bC71111a3cEfF8E22C4f9D8E44b72A8fD687?q=0x4200000000000000000000000000000000000006",
	},
	{
		ChainName: "evm8453",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/base/0x4C36388bE6F416A29C8d8Eee81C771cE6bE14B18?q=0x4200000000000000000000000000000000000006",
	},
	{
		ChainName: "evm8453",
		DEX:       "pancakeswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/base/0x22ca6d83aB887A535ae1C6011cc36eA9D1255C31?q=0x4200000000000000000000000000000000000006",
	},

	{
		ChainName: "zkSync",
		DEX:       "syncswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/zksync/0x80115c708E12eDd42E504c1cD52Aea96C547c05c?q=0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4",
	},
	{
		ChainName: "zkSync",
		DEX:       "iziswap",
		URL:       "https://io.dexscreener.com/dex/log/amm/iziswap/all/zksync/0x43ff8a10B6678462265b00286796e88f03C8839A?q=0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4",
	},
	{
		ChainName: "zkSync",
		DEX:       "velocore",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/zksync/0xcD52cbc975fbB802F82A1F92112b1250b5a997Df?q=0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4",
	},

	{
		ChainName: "Avalanche",
		DEX:       "traderjoe",
		URL:       "https://io.dexscreener.com/dex/log/amm/joev2p1/all/avalanche/0x9B2Cc8E6a2Bbb56d6bE4682891a91B0e48633c72?q=0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
	},
	{
		ChainName: "Avalanche",
		DEX:       "uniswap", // uniswap_v3
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/avalanche/0xfAe3f424a0a47706811521E3ee268f00cFb5c45E?q=0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
	},
	{
		ChainName: "Avalanche",
		DEX:       "pangolin",
		URL:       "https://io.dexscreener.com/dex/log/amm/uniswap/all/avalanche/0x0e0100Ab771E9288e0Aa97e11557E6654C3a9665?q=0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
	},
}
