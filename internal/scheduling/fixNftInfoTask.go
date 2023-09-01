package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
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

	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum", "Avalanche", "Optimism", "Klaytn", "ArbitrumNova", "Conflux", "Aptos", "SUI", "SUITEST", "Solana", "ScrollL2TEST", "zkSync", "Ronin", "Sei", "SeiTEST", "Fantom", "xDai", "Cronos", "ETC", "evm534351"}
	for _, chainName := range chainNames {
		tableName := biz.GetTableName(chainName)
		chainType, _ := biz.GetChainNameType(chainName)
		transactionRequest := &data.TransactionRequest{
			StatusNotInList:          []string{biz.PENDING},
			TransactionTypeNotInList: []string{biz.CONTRACT},
		}
		switch chainType {
		case biz.EVM:
			incompleteNfts, err := data.EvmTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
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
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.EvmTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.EvmTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.APTOS:
			incompleteNfts, err := data.AptTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.AptTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.AptTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.AptTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.SUI:
			incompleteNfts, err := data.SuiTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.SuiTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.SuiTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.SuiTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.SOLANA:
			incompleteNfts, err := data.SolTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.SolTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.SolTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.SolTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.COSMOS:
			incompleteNfts, err := data.AtomTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.AtomTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.AtomTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.AtomTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		}
	}
}
