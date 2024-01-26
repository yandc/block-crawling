package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type FixNftInfoTask struct {
}

func NewFixNftInfoTask() *FixNftInfoTask {
	fixNftInfoTask := &FixNftInfoTask{}
	return fixNftInfoTask
}

func (task *FixNftInfoTask) Run() {
	FixNftInfo()
}

func FixNftInfo() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("FixNftInfo error", e)
			} else {
				log.Errore("FixNftInfo panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：定时更新缺失的NFT信息失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum", "Avalanche", "Optimism", "Klaytn", "ArbitrumNova", "Conflux", "Aptos", "SUI", "SUITEST", "Solana", "ScrollL2TEST", "zkSync", "Ronin", "Sei", "SeiTEST", "Fantom", "xDai", "Cronos", "ETC", "evm534351", "evm8453", "Scroll"}
	for _, chainName := range chainNames {
		transactionRequest := &data.TransactionRequest{
			StatusNotInList:          []string{biz.PENDING},
			TransactionTypeNotInList: []string{biz.CONTRACT},
		}
		incompleteNfts, err := biz.TransactionRecordRepoClient.ListIncompleteNft(nil, chainName, transactionRequest)
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("chainName", chainName), zap.Any("error", err))
			return
		}
		txHashMap := make(map[string]string)
		for _, incompleteNft := range incompleteNfts {
			tokenInfoJson := incompleteNft.TokenInfo
			var tokenInfo *types.TokenInfo
			json.Unmarshal([]byte(tokenInfoJson), &tokenInfo)
			tokenAddress := tokenInfo.Address
			tokenId := tokenInfo.TokenId

			txHash := incompleteNft.TransactionHash
			txHash = strings.Split(txHash, "#")[0]
			if _, ok := txHashMap[txHash]; !ok {
				var token types.TokenInfo
				if tokenId != "" {
					token, err = biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
				} else {
					token, err = biz.GetCollectionInfoDirectlyRetryAlert(nil, chainName, tokenAddress)
				}
				if err != nil {
					log.Error("定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("chainName", chainName), zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					continue
				}
				if token.Address == "" {
					continue
				}
				txHashMap[txHash] = ""
			}
		}

		var txRecords []*biz.TransactionRecord
		for txHash, _ := range txHashMap {
			txRecords = append(txRecords, &biz.TransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
		}
		if len(txRecords) > 0 {
			_, err = biz.TransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, chainName, txRecords, biz.PAGE_SIZE)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("定时修复NFT信息，将数据更新到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
				return
			}
		}
	}
}
