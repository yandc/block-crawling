package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"time"
)

func HandleRecord(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	go func() {
		HandleTokenPush(chainName, client, txRecords)
		HandleUserAsset(chainName, client, txRecords)
	}()
	go HandleUserStatistic(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandlePendingRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandlePendingRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	go func() {
		HandleTokenPush(chainName, client, txRecords)
		HandleUserAsset(chainName, client, txRecords)
	}()
}

func HandleUserAsset(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	now := time.Now().Unix()
	var userAssets []*data.UserAsset
	userAssetMap := make(map[string]*data.UserAsset)
	var mainDecimals int32
	var mainSymbol string
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		var tokenAddress = record.ContractAddress

		if record.TransactionType != biz.CONTRACT && record.TransactionType != biz.REGISTERTOKEN {
			decimals, symbol, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"解析parseData失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber),
					zap.Any("txHash", record.TransactionHash), zap.Any("parseData", record.ParseData), zap.Any("error", err))
				continue
			}

			if record.Data == "" {
				continue
			}
			changesPayload := make(map[string]interface{})
			err = json.Unmarshal([]byte(record.Data), &changesPayload)
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链解析data失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"解析data失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber),
					zap.Any("txHash", record.TransactionHash), zap.Any("data", record.Data), zap.Any("error", err))
				continue
			}

			accountKeyStr, ok := changesPayload["accountKey"]
			if !ok || accountKeyStr == nil {
				continue
			}
			tokenBalanceStr, ok := changesPayload["tokenBalance"]
			if !ok || tokenBalanceStr == nil {
				continue
			}
			accountKeyMap := accountKeyStr.(map[string]interface{})
			tokenBalanceMap := tokenBalanceStr.(map[string]interface{})
			if tokenAddress == "" {
				if record.FromAddress != "" && record.FromUid != "" {
					if changeMapi, ok := accountKeyMap[record.FromAddress]; ok {
						changeMap := changeMapi.(map[string]interface{})
						var amount string
						if value, ok := changeMap["amount"].(float64); ok {
							amount = fmt.Sprintf("%.f", value)
						} else {
							amount = fmt.Sprintf("%v", changeMap["amount"])
						}
						balance := utils.StringDecimals(amount, int(decimals))
						var userAsset = &data.UserAsset{
							ChainName:    chainName,
							Uid:          record.FromUid,
							Address:      record.FromAddress,
							TokenAddress: tokenAddress,
							Balance:      balance,
							Decimals:     decimals,
							Symbol:       symbol,
							CreatedAt:    now,
							UpdatedAt:    now,
						}
						userAssetKey := chainName + record.FromAddress + tokenAddress
						userAssetMap[userAssetKey] = userAsset
					}
				}

				if record.ToAddress != "" && record.ToUid != "" {
					if changeMapi, ok := accountKeyMap[record.ToAddress]; ok {
						changeMap := changeMapi.(map[string]interface{})
						var amount string
						if value, ok := changeMap["amount"].(float64); ok {
							amount = fmt.Sprintf("%.f", value)
						} else {
							amount = fmt.Sprintf("%v", changeMap["amount"])
						}
						balance := utils.StringDecimals(amount, int(decimals))
						var userAsset = &data.UserAsset{
							ChainName:    chainName,
							Uid:          record.ToUid,
							Address:      record.ToAddress,
							TokenAddress: tokenAddress,
							Balance:      balance,
							Decimals:     decimals,
							Symbol:       symbol,
							CreatedAt:    now,
							UpdatedAt:    now,
						}
						userAssetKey := chainName + record.ToAddress + tokenAddress
						userAssetMap[userAssetKey] = userAsset
					}
				}
			} else {
				if record.FromAddress != "" && record.FromUid != "" {
					if changeMapi, ok := tokenBalanceMap[record.FromAddress]; ok {
						changeMap := changeMapi.(map[string]interface{})
						mint := changeMap["mint"].(string)
						if tokenAddress == mint {
							uiTokenAmount := changeMap["uiTokenAmount"].(map[string]interface{})
							balance := uiTokenAmount["uiAmountString"].(string)
							var userAsset = &data.UserAsset{
								ChainName:    chainName,
								Uid:          record.FromUid,
								Address:      record.FromAddress,
								TokenAddress: tokenAddress,
								Balance:      balance,
								Decimals:     decimals,
								Symbol:       symbol,
								CreatedAt:    now,
								UpdatedAt:    now,
							}
							userAssetKey := chainName + record.FromAddress + tokenAddress
							userAssetMap[userAssetKey] = userAsset
						}
					}
				}

				if record.ToAddress != "" && record.ToUid != "" {
					if changeMapi, ok := tokenBalanceMap[record.ToAddress]; ok {
						changeMap := changeMapi.(map[string]interface{})
						mint := changeMap["mint"].(string)
						if tokenAddress == mint {
							uiTokenAmount := changeMap["uiTokenAmount"].(map[string]interface{})
							balance := uiTokenAmount["uiAmountString"].(string)
							var userAsset = &data.UserAsset{
								ChainName:    chainName,
								Uid:          record.ToUid,
								Address:      record.ToAddress,
								TokenAddress: tokenAddress,
								Balance:      balance,
								Decimals:     decimals,
								Symbol:       symbol,
								CreatedAt:    now,
								UpdatedAt:    now,
							}
							userAssetKey := chainName + record.ToAddress + tokenAddress
							userAssetMap[userAssetKey] = userAsset
						}
					}
				}
			}

			fromUserAssetKey := chainName + record.FromAddress + tokenAddress
			if userAsset, ok := userAssetMap[fromUserAssetKey]; !ok {
				userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, tokenAddress, decimals, symbol, now)
				for i := 0; i < 10 && err != nil; i++ {
					time.Sleep(time.Duration(i*5) * time.Second)
					userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, tokenAddress, decimals, symbol, now)
				}
				if err != nil {
					// 更新用户资产出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s查询用户资产失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"查询用户资产失败", zap.Any("fromAddress", record.FromAddress), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
					return
				}
				if userAsset != nil {
					userAssetMap[fromUserAssetKey] = userAsset
				}
			}

			toUserAssetKey := chainName + record.ToAddress + tokenAddress
			if userAsset, ok := userAssetMap[toUserAssetKey]; !ok {
				userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.ToUid, record.ToAddress, tokenAddress, decimals, symbol, now)
				for i := 0; i < 10 && err != nil; i++ {
					time.Sleep(time.Duration(i*5) * time.Second)
					userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.ToUid, record.ToAddress, tokenAddress, decimals, symbol, now)
				}
				if err != nil {
					// 更新用户资产出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s查询用户资产失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"查询用户资产失败", zap.Any("toAddress", record.ToAddress), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
					return
				}
				if userAsset != nil {
					userAssetMap[toUserAssetKey] = userAsset
				}
			}
		}

		fromUserAssetKey := chainName + record.FromAddress
		if _, ok := userAssetMap[fromUserAssetKey]; !ok {
			if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
				mainDecimals = platInfo.Decimal
				mainSymbol = platInfo.NativeCurrency
			} else {
				continue
			}
			fromUserAsset, err := doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", mainDecimals, mainSymbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				fromUserAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", mainDecimals, mainSymbol, now)
			}
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s更新用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"更新用户资产失败", zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
				return
			}
			if fromUserAsset != nil {
				userAssetMap[fromUserAssetKey] = fromUserAsset
			}
		}
	}

	if len(userAssetMap) == 0 {
		return
	}
	for _, userAsset := range userAssetMap {
		userAssets = append(userAssets, userAsset)
	}
	_, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"更新用户资产，将数据插入到数据库中失败", zap.Any("slotNumber", txRecords[0].SlotNumber), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserAsset(chainName string, client Client, transactionType string, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		if transactionType == biz.NATIVE || tokenAddress == "" {
			return client.GetBalance(address)
		} else {
			return client.GetTokenBalance(address, tokenAddress, int(decimals))
		}
	})
	if err != nil {
		log.Error("query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}
	balance := result.(string)

	var userAsset = &data.UserAsset{
		ChainName:    chainName,
		Uid:          uid,
		Address:      address,
		TokenAddress: tokenAddress,
		Balance:      balance,
		Decimals:     decimals,
		Symbol:       symbol,
		CreatedAt:    nowTime,
		UpdatedAt:    nowTime,
	}
	return userAsset, nil
}

func HandleUserStatistic(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserStatistic error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserStatistic panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.REGISTERTOKEN {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		decimals, _, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"交易记录统计，解析parseData失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber),
				zap.Any("txHash", record.TransactionHash), zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}

		var userAssetStatistic = biz.UserAssetStatistic{
			ChainName:    chainName,
			FromUid:      record.FromUid,
			ToUid:        record.ToUid,
			Amount:       record.Amount,
			TokenAddress: record.ContractAddress,
			Decimals:     decimals,
		}
		userAssetStatisticList = append(userAssetStatisticList, userAssetStatistic)
	}
	biz.HandleUserAssetStatistic(chainName, userAssetStatisticList)
}

func HandleTokenPush(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleTokenPush error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleTokenPush panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var userAssetList []biz.UserTokenPush
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.REGISTERTOKEN {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		decimals, symbol, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber),
				zap.Any("txHash", record.TransactionHash), zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}

		tokenAddress := record.ContractAddress
		address := record.ToAddress
		uid := record.ToUid
		if tokenAddress != "" && address != "" && uid != "" {
			var userAsset = biz.UserTokenPush{
				ChainName:    chainName,
				Uid:          uid,
				Address:      address,
				TokenAddress: tokenAddress,
				Decimals:     decimals,
				Symbol:       symbol,
			}
			userAssetList = append(userAssetList, userAsset)
		}
	}
	biz.HandleTokenPush(chainName, userAssetList)
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}
