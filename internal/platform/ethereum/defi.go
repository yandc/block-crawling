package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/defi"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/types"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	ncommon "gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
)

func HandleDeFiAsset(chainName string, records []*data.EvmTransactionRecord, receipts map[string]*rtypes.Receipt) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理DeFi 资产失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	if err := ProcessDeFiAsset(chainName, records, receipts); err != nil {
		panic(err)
	}
}

func ProcessDeFiAsset(chainName string, records []*data.EvmTransactionRecord, receipts map[string]*rtypes.Receipt) error {
	for _, tx := range records {
		if !strings.Contains(tx.TransactionHash, "#result-") {
			tableName := data.GetTableName(chainName)
			results, err := data.EvmTransactionRecordRepoClient.LoadSeries(context.Background(), tableName, tx.TransactionHash)
			if err != nil {
				panic(err)
			}
			if err := doProcessDeFiAsset(chainName, results, receipts); err != nil {
				return err
			}
		}
	}
	return nil
}

func doProcessDeFiAsset(chainName string, records []*data.EvmTransactionRecord, receipts map[string]*rtypes.Receipt) error {
	mainTxs := make(map[string]*data.EvmTransactionRecord)
	dapps := make(map[string]*biz.DappInfo)
	for _, tx := range records {
		if !strings.Contains(tx.TransactionHash, "#result-") {
			mainTxs[tx.TransactionHash] = tx
			var dapp *biz.DappInfo
			if err := json.Unmarshal([]byte(tx.DappData), &dapp); err != nil {
				log.Info("DEFI: FAILED TO PARSE DAPP", zap.String("dappData", tx.DappData), zap.String("tx", tx.TransactionHash), zap.String("chainName", chainName))
				return nil
			}
			dapps[tx.TransactionHash] = dapp
		}
	}
	byTxHashs := make(map[string]*defi.ContractAssetFlow)
	tokenAddedToFlow := make(map[string]bool)
	for _, record := range records {
		txHash := strings.Split(record.TransactionHash, "#result-")[0]
		mainRecord := mainTxs[txHash]
		if record.Status != biz.SUCCESS || mainRecord.Status != biz.SUCCESS {
			log.Info("DEFI: TX NOT SUCCESS", zap.String("tx", txHash), zap.String("chainName", chainName))
			continue
		}
		if !biz.IsDeFiTxType(mainRecord.TransactionType) {
			log.Info("DEFI: TX IS NOT DEFI OP", zap.String("tx", txHash), zap.String("chainName", chainName))
			continue
		}
		if isBlockedMethodForDeFiAsset(mainRecord.Data) {
			log.Info("DEFI: TX IS BLOCKED BY METHOD ID", zap.String("tx", txHash), zap.String("chainName", chainName))
			continue
		}
		if record.TransactionType == biz.EVENTLOG {
			// Ignore the event log that same as the main record.
			if record.Amount == mainRecord.Amount && record.TokenInfo == mainRecord.TokenInfo {
				continue
			}
		}
		receipt, ok := receipts[txHash]
		if !ok {
			raw, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
				c := client.(*Client)
				var err error
				receipt, err := c.GetTransactionReceipt(context.Background(), common.HexToHash(txHash))
				if err != nil {
					return nil, ncommon.Retry(err)
				}
				return receipt, nil
			})
			if err != nil {
				log.Warn(
					"DEFI: 扫块，从链上获取交易receipt失败",
					zap.String("chainName", chainName),
					zap.String("txHash", txHash),
					zap.Any("error", err),
				)
				continue
			}
			receipt = raw.(*rtypes.Receipt)
			if receipts == nil {
				receipts = make(map[string]*rtypes.Receipt)
			}
			receipts[txHash] = receipt
		}
		var methodId string
		if len(mainRecord.Data) >= 8 {
			methodId = mainRecord.Data[:8]
		}
		identifiedType, topicAddress := identifyAssetType(receipt.Logs)

		if identifiedType == "" && isSwapOrBridgeOrAirdrop(chainName, mainRecord.ContractAddress, methodId, receipt) {
			log.Info("DEFI: TX IS BLOCKED, SWAP OR CROSS BRIDGE OR AIRDROP", zap.String("tx", txHash), zap.String("chainName", chainName))
			continue
		}
		if record.Amount.LessThanOrEqual(decimal.Zero) {
			continue
		}
		if dapps[txHash].Origin == "" || dapps[txHash].DappName == "CrossChain" || dapps[txHash].DappName == "OpenSea" {
			log.Info("DEFI: BLOCKED DAPP", zap.String("tx", txHash), zap.String("chainName", chainName))
			continue
		}
		walletAddress := mainTxs[txHash].FromAddress
		flow, ok := byTxHashs[txHash]
		if !ok {
			plat := &data.DeFiPlatform{
				URL:       dapps[txHash].Origin,
				Icon:      dapps[txHash].Icon,
				Name:      dapps[txHash].DappName,
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			}
			if err := data.DeFiAssetRepoInst.LoadOrSavePlatform(context.Background(), plat); err != nil {
				return fmt.Errorf("[savePlat] %w", err)
			}
			flow = &defi.ContractAssetFlow{
				BlockNumber:            int64(record.BlockNumber),
				PlatformID:             plat.Id,
				PlatformEnabled:        plat.Enabled(biz.AppConfig.DefiPlatformInteractionThr),
				TxTime:                 record.TxTime,
				Address:                walletAddress,
				TransactionHash:        txHash,
				ReceivedAssets:         []defi.Asset{},
				SentAssets:             []defi.Asset{},
				IdentifiedType:         identifiedType,
				IdentifiedTopicAddress: topicAddress,
			}
			byTxHashs[txHash] = flow
		}
		var token types.TokenInfo
		if err := json.Unmarshal([]byte(record.TokenInfo), &token); err != nil {
			log.Info("DEFI: FAILED TO PARSE TOKEN", zap.String("token", record.TokenInfo), zap.String("tx", txHash), zap.String("chainName", chainName))
			return nil
		}
		// Avoid duplicated token add to the flow.
		tokenKey := fmt.Sprint(txHash, token.Address, record.Amount)
		if _, ok := tokenAddedToFlow[tokenKey]; ok {
			continue
		}
		tokenAddedToFlow[tokenKey] = true

		if record.FromAddress == walletAddress {
			flow.SentAssets = append(flow.SentAssets, defi.Asset{
				PeerAddress: record.ToAddress,
				Amount:      record.Amount,
				Token:       token,
			})
		} else if record.ToAddress == walletAddress {
			flow.ReceivedAssets = append(flow.ReceivedAssets, defi.Asset{
				PeerAddress: record.FromAddress,
				Amount:      record.Amount,
				Token:       token,
			})
		}
	}

	for _, flow := range byTxHashs {
		attemptFillSingleSidedLP(chainName, flow, receipts[flow.TransactionHash].Logs)
		log.Info("DEFI: FLOW", zap.String("chainName", chainName), zap.Any("flow", flow))
		if err := defi.ParseAndSave(chainName, data.DeFiAssetRepoInst, flow); err != nil {
			return fmt.Errorf("[parseAssets] %s %w", flow.TransactionHash, err)
		}
	}
	return nil
}

const (
	dodoDepositePairTopic = "0x18081cde2fa64894914e1080b98cca17bb6d1acf633e57f6e26ebdb945ad830b"
	dodoWithdrawPairTopic = "0xe89c586bd81ee35a18f7eac22a732b56e589a2821497cce12a0208828540a36d"
)

func attemptFillSingleSidedLP(chainName string, flow *defi.ContractAssetFlow, logs []*rtypes.Log) {
	if !(len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 1) {
		return
	}
	var dodoPiar string
	for _, log := range logs {
		if len(log.Topics) == 0 {
			continue
		}
		topic0 := log.Topics[0].String()
		switch topic0 {
		case dodoDepositePairTopic, dodoWithdrawPairTopic:
			dodoPiar = log.Address.Hex()
		}
	}
	if dodoPiar != "" {
		raw, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
			c := client.(*Client)
			var err error
			token0, token1, err := c.GetDexDodoPairTokens(dodoPiar)
			if err != nil {
				return nil, ncommon.Retry(err)
			}
			return []string{token0, token1}, nil
		})
		if err != nil {
			log.Info("DEFI: ATTEMPT FILL DODO LP FAILED", zap.String("err", err.Error()), zap.String("contract", dodoPiar), zap.String("chainName", chainName))
		} else {
			token0 := raw.([]string)[0]
			token1 := raw.([]string)[1]
			flow.ReceivedAssets = doAttemptFillAnotherSingleSideLP(chainName, token0, token1, flow.ReceivedAssets)
			flow.SentAssets = doAttemptFillAnotherSingleSideLP(chainName, token0, token1, flow.SentAssets)
		}
	} else {
		flow.ReceivedAssets = attemptFillAnotherSingleSideLP(chainName, flow.ReceivedAssets, flow.IdentifiedTopicAddress)
		flow.SentAssets = attemptFillAnotherSingleSideLP(chainName, flow.SentAssets, flow.IdentifiedTopicAddress)
	}
}

func attemptFillAnotherSingleSideLP(chainName string, assets []defi.Asset, identifiedTopicAddress string) []defi.Asset {
	pairAddress := identifiedTopicAddress
	asset := assets[0]
	if pairAddress == "" {
		pairAddress = assets[0].PeerAddress
	}

	if pairAddress == "" || pairAddress == "0x0000000000000000000000000000000000000000" {
		return []defi.Asset{asset}
	}
	raw, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c := client.(*Client)
		var err error
		token0, token1, err := c.GetDexPairTokens(pairAddress)
		if err != nil {
			return nil, ncommon.Retry(err)
		}
		return []string{token0, token1}, nil
	})
	if err != nil {
		log.Info("DEFI: ATTEMPT FILL LP FAILED", zap.String("err", err.Error()), zap.String("contract", pairAddress), zap.String("chainName", chainName))
		return []defi.Asset{asset}
	}
	token0 := raw.([]string)[0]
	token1 := raw.([]string)[1]
	return doAttemptFillAnotherSingleSideLP(chainName, token0, token1, assets)
}

func doAttemptFillAnotherSingleSideLP(chainName string, token0, token1 string, assets []defi.Asset) []defi.Asset {
	log.Info(
		"DEFI: ATTEMPT FILL SINGLE SIDE LP", zap.String("chainName", chainName),
		zap.Any("assets", assets),
		zap.String("token0", token0),
		zap.String("token1", token1),
	)
	if isTokenEqualInLP(chainName, assets[0].Token.Address, token0) {
		token, err := biz.GetTokenInfoRetryAlert(nil, chainName, token1)
		if err != nil {
			log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("error", err))
		}
		return []defi.Asset{
			assets[0],
			{
				Amount:   decimal.Zero,
				Token:    token,
				UsdPrice: decimal.Zero,
			},
		}
	} else if isTokenEqualInLP(chainName, assets[0].Token.Address, token1) {
		token, err := biz.GetTokenInfoRetryAlert(nil, chainName, token0)
		if err != nil {
			log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("error", err))
		}
		return []defi.Asset{
			{
				Amount:   decimal.Zero,
				Token:    token,
				UsdPrice: decimal.Zero,
			},
			assets[0],
		}
	}
	return assets
}

func isTokenEqualInLP(chainName, token, tokenInLp string) bool {
	if token == tokenInLp {
		return true
	}
	return token == "" && isWrappedNativeToken(chainName, tokenInLp)
}

var chainWrappedNativeTokens = map[string]string{
	"Polygon": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
	"BSC":     "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
}

func isWrappedNativeToken(chainName, token string) bool {
	if w, ok := chainWrappedNativeTokens[chainName]; ok {
		return strings.ToLower(token) == strings.ToLower(w)
	}
	return false
}

var blockedMethodIds = map[string]bool{
	"7ff36ab5": true, // swapExactETHForTokens
	"1aa3a008": true, // register
	"c6878519": true, // 跨链提取 completeTransfer
	"a44bbb15": true, // 跨链发送 outboundTransferTo
	// https://etherscan.io/tx/0x3f84614e132d20fb299f4626276404751be71c200cfb8f06f974c05274ff869a
	"eb672419": true, // zkSync 跨链发送 requestL2Transaction
	"a6cbf417": true, // Swap
}

func isBlockedMethodForDeFiAsset(data string) bool {
	if len(data) < 8 {
		return false
	}
	if _, ok := blockedMethodIds[data[:8]]; ok {
		return true
	}
	return false
}

var swapTopics = map[string]bool{
	"0x45f377f845e1cc76ae2c08f990e15d58bcb732db46f92a4852b956580c3a162f": true, // swap
	"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": true, // swap
	"0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": true, // swap
	"0xe7779a36a28ae0e49bcbd9fcf57286fb607699c0c339c202e92495640505613e": true, // swap
	"0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062": true, // swap
	"0xffebebfb273923089a3ed6bac0fd4686ac740307859becadeb82f998e30db614": true, // swap
}

var bridgeTopics = map[string]bool{
	"0x97116cf6cd4f6412bb47914d6db18da9e16ab2142f543b86e207c24fbd16b23a": true, // LogAnySwapOut
	"0x65c63bce967237d9b3fd7a8cd7f6a75e6f9d4196256f87fc332a42a71081e05c": true, // AirCash OrderAdd
	"0x05251647965762819e1d0f33e4a75125c449e74510aac2398650547d63799d0b": true, // GasStaion
	"0x02a469fab03dc47fa1dd71af4955b95209f0be432298c96518f2712d8f3dd7ea": true, // CrossChain BSC
	"0x5faac1ba95d8a22d60a2290d300369a4a7b8334e60291f5f6c83ee09f03cab38": true, // CrossChain Polygon
	// https://etherscan.io/tx/0x3c4aa9f67c2238c0a701c677123f9820f12fa5bf20f7a8b607ee8b881d914cfb#eventlog
	"0x6670de856ec8bf5cb2b7e957c5dc24759716056f79d97ea5e7c939ca0ba5a675": true, // CrossChain Scroll
	// https://bscscan.com/tx/0x53f1f0080eeb8fa5a43e6a5222d2cb0208637b4db4c184fedbc864e6dac763e4#eventlog
	"0x617b1a2bf0533cea0c5dd46104e1d48f8b290fc7d7360dba0182dbf07a00e46a": true,
}

var defiTopics = map[string]bool{
	"0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951": true, // Deposit

	// https://bscscan.com/tx/0x9419fd6f5bfa69a6aefb61f3698b610f620e92101aade5a40ab20e60e3294c91#eventlog
	"0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026": true, // Claimed
}

func isSwapOrBridgeOrAirdrop(chainName, contractAddress, methodId string, receipt *rtypes.Receipt) bool {
	var swaped, operatedDeFi bool
	for _, log := range receipt.Logs {
		var topic0 = strings.ToLower(log.Topics[0].Hex())
		if _, ok := bridgeTopics[topic0]; ok {
			return true
		}
		if _, ok := swapTopics[topic0]; ok {
			swaped = true
		}
		if _, ok := defiTopics[topic0]; ok {
			operatedDeFi = true
		}
		if isTopic0Bridge(chainName, contractAddress, methodId, topic0) {
			return true
		}
	}
	// 一些操作先进行 Swap 然后再进行 DeFi 资产操作，如:
	// https://polygonscan.com/tx/0x28919a0f5d589a4b6bbbd5b713767ba62ec40e7a2d2838a48e73a0712f6c16fd
	// https://polygonscan.com/tx/0xf4af41e9ac65afb270827a85f1b217926014ab8be8e1d8168a5fef5e240e6fec
	if swaped && !operatedDeFi {
		return true
	}
	return isBridgeMethod(chainName, contractAddress, methodId)
}

var stakedTopics = map[string]bool{
	// https://polygonscan.com/tx/0x59db783aff6f097f8584f30a64a1f0b7b3e2cf1bc2860b72f6259600c750ada4#eventlog
	"0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d": true, // Hop Staked
	"0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5": true, // Hop Withdrawn
}

var lpTopics = map[string]bool{
	// 0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde
	"0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": true, // Pakeswap Mint
	// https://polygonscan.com/tx/0x2bb4c4d9ed1f6641cf8ecc29653fa3f2da291279e52f5c77fd0ac31a01578005#eventlog
	"0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": true, // APESwap Mint
}

func identifyAssetType(logs []*rtypes.Log) (string, string) {
	for _, log := range logs {
		if len(log.Topics) == 0 {
			continue
		}
		topic0 := log.Topics[0].String()
		if _, ok := stakedTopics[topic0]; ok {
			return data.DeFiAssetTypeStake, log.Address.Hex()
		}
		if _, ok := lpTopics[topic0]; ok {
			return data.DeFiAssetTypeLP, log.Address.Hex()
		}
	}
	return "", ""
}
