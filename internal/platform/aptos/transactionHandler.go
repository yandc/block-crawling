package aptos

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
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
		HandleUserAsset(false, chainName, client, txRecords)
	}()
	go HandleUserStatistic(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
	HandleUserNonce(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
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
		HandleUserAsset(true, chainName, client, txRecords)
	}()
	go HandleTransactionCount(chainName, client, txRecords)
	HandleUserNonce(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(chainName, client, txRecords)
}

func HandleNftRecord(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleNftHistory error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleNftHistory panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	tokenIdMap := make(map[string]string)
	var nftRecords []*data.NftRecordHistory
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT {
			continue
		}
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("添加NFT流转记录，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenId := tokenInfo.TokenId
		//tokenId := "06349a78dbb538cdcef68557e17a67a0017df3454c0d802654241e88e0336c6c"
		tokenAddress := tokenInfo.Address
		if tokenId == "" || tokenAddress == "" {
			log.Info("不添加NFT流转记录", zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
			continue
		}
		log.Info("添加NFT流转记录", zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
		_, ok := tokenIdMap[tokenId]
		if ok {
			continue
		} else {
			tokenIdMap[tokenId] = tokenId
		}
		result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
			return client.GetEventTransfer(tokenId, 0, 100, chainName)
		})
		tar := result.(TokenActivitiesResponse)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，获取流转记录失败，tokenAddress：%s，tokenId：%s", chainName, tokenAddress, tokenId)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("添加NFT流转记录，获取流转记录失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", record.TransactionVersion), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			continue
		}
		if tar.Errors != nil {
			log.Error(tokenId, zap.Any("chainName", chainName), zap.Any("获取交易记录失败!", tar.Errors))
			continue
		}
		events := tar.Data.TokenActivities
		userNftMap := make(map[string]string)
		if len(events) > 0 {
			now := time.Now().Unix()
			for _, e := range events {
				result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
					return client.GetTransactionByVersion(e.TransactionVersion)
				})
				tx := result.(*TransactionInfo)
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，根据txVersion获取交易数据失败，transactionVersion:%v", chainName, e.TransactionVersion)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error("添加NFT流转记录，根据txVersion获取交易数据失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", e.TransactionVersion), zap.Any("txHash", record.TransactionHash),
						zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					continue
				}
				var fromAddress, toAddress, fromUid, toUid string
				fromAddress = e.FromAddress
				toAddress = e.ToAddress
				userMeta, err := pCommon.MatchUser(fromAddress, toAddress, chainName)
				if err == nil {
					fromUid = userMeta.FromUid
					toUid = userMeta.ToUid
				}
				txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
				txTime = txTime / 1000

				sk := chainName + fromAddress + toAddress + tokenAddress + tokenId
				_, ok1 := userNftMap[sk]
				if ok1 {
					continue
				} else {
					userNftMap[sk] = sk
				}
				nrh := &data.NftRecordHistory{
					ChainName:       chainName,
					TransactionHash: tx.Hash,
					TxTime:          strconv.Itoa(int(txTime)),
					FromAddress:     fromAddress,
					ToAddress:       toAddress,
					FromUid:         fromUid,
					ToUid:           toUid,
					Quantity:        fmt.Sprint(e.TokenAmount),
					ContractAddress: tokenAddress,
					TokenId:         tokenId,
					EventType:       e.TransferType,
					CreatedAt:       now,
					UpdatedAt:       now,
				}
				nftRecords = append(nftRecords, nrh)
			}
		}
	}

	if len(nftRecords) > 0 {
		_, err := data.NftRecordHistoryRepoClient.SaveOrUpdate(nil, nftRecords)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链获取nft流转记录，将数据插入到数据库中失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("获取nft流转记录，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("tokenIdMap", tokenIdMap), zap.Any("error", err))
		}
	}
}

func HandleUserNonce(chainName string, txRecords []*data.AptTransactionRecord) {
	doneNonce := make(map[string]int)
	doneNonceTotal := make(map[string]int)

	for _, record := range txRecords {
		if record.Status == biz.DROPPED {
			pNonce := biz.ADDRESS_PENDING_NONCE + chainName + ":" + record.FromAddress + ":" + strconv.Itoa(int(record.Nonce))
			data.RedisClient.Del(pNonce)
			continue
		}
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}
		if record.TransactionType == biz.EVENTLOG {
			continue
		}
		nonceKey := biz.ADDRESS_DONE_NONCE + chainName + ":" + record.FromAddress
		dnt := doneNonceTotal[nonceKey]
		doneNonceTotal[nonceKey] = dnt + 1
		bh := doneNonce[nonceKey]
		if bh == 0 {
			doneNonce[nonceKey] = int(record.Nonce)
		} else {
			if bh < int(record.Nonce) {
				doneNonce[nonceKey] = int(record.Nonce)
			}
		}
	}

	for k, v := range doneNonce {
		total := doneNonceTotal[k]
		nonceStr, err := data.RedisClient.Get(k).Result()
		if v == 0 || fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
			data.RedisClient.Set(k, strconv.Itoa(v), 0)
		} else {
			nonce, _ := strconv.Atoi(nonceStr)
			if v > nonce && v-total == nonce {
				data.RedisClient.Set(k, strconv.Itoa(v), 0)
			} else {
				log.Info(k, zap.Any("chainName", chainName), zap.Any("无需修改nonce,交易记录的nonce值", v), zap.Any("本地记录nonce", nonce))
			}
		}
	}
}

func HandleUserAsset(isPending bool, chainName string, client Client, txRecords []*data.AptTransactionRecord) {
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
		if tokenAddress == APT_CODE {
			tokenAddress = ""
		}

		if record.TransactionType != biz.CONTRACT && record.TransactionType != biz.SWAP && record.TransactionType != biz.MINT &&
			record.TransactionType != biz.CREATEACCOUNT && record.TransactionType != biz.REGISTERTOKEN &&
			record.TransactionType != biz.DIRECTTRANSFERNFTSWITCH && record.TransactionType != biz.APPROVENFT {
			tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，解析tokenInfo失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
					zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
				continue
			}
			tokenType := tokenInfo.TokenType
			decimals := tokenInfo.Decimals
			symbol := tokenInfo.Symbol

			if tokenType != biz.APTOSNFT {
				changesPayload := make(map[string]interface{})
				if record.Data != "" {
					err = json.Unmarshal([]byte(record.Data), &changesPayload)
					if err != nil {
						// 更新用户资产出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，解析data失败", chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error("更新用户资产，解析data失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", record.TransactionVersion), zap.Any("txHash", record.TransactionHash),
							zap.Any("data", record.Data), zap.Any("error", err))
					}
				}

				changesStr, ok := changesPayload["changes"]
				if !isPending && ok {
					changes := changesStr.(map[string]interface{})
					changeTokenAddress := tokenAddress
					if changeTokenAddress == "" {
						changeTokenAddress = APT_CODE
					}
					if record.FromAddress != "" && record.FromUid != "" {
						if changeMapi, ok := changes[record.FromAddress]; ok {
							changeMap := changeMapi.(map[string]interface{})
							if balancei, ok := changeMap[changeTokenAddress]; ok {
								balance := balancei.(string)
								balance = utils.StringDecimals(balance, int(decimals))
								var userAsset = &data.UserAsset{
									ChainName:     chainName,
									Uid:           record.FromUid,
									Address:       record.FromAddress,
									TokenAddress:  tokenAddress,
									Balance:       balance,
									Decimals:      int32(decimals),
									Symbol:        symbol,
									IsSyncToChain: true,
									SyncToChainTs: now,
									CreatedAt:     now,
									UpdatedAt:     now,
								}
								userAssetKey := chainName + record.FromAddress + tokenAddress
								userAssetMap[userAssetKey] = userAsset
							}
						}
					}

					if record.ToAddress != "" && record.ToUid != "" {
						if changeMapi, ok := changes[record.ToAddress]; ok {
							changeMap := changeMapi.(map[string]interface{})
							if balancei, ok := changeMap[changeTokenAddress]; ok {
								balance := balancei.(string)
								balance = utils.StringDecimals(balance, int(decimals))
								var userAsset = &data.UserAsset{
									ChainName:     chainName,
									Uid:           record.ToUid,
									Address:       record.ToAddress,
									TokenAddress:  tokenAddress,
									Balance:       balance,
									Decimals:      int32(decimals),
									Symbol:        symbol,
									IsSyncToChain: true,
									SyncToChainTs: now,
									CreatedAt:     now,
									UpdatedAt:     now,
								}
								userAssetKey := chainName + record.ToAddress + tokenAddress
								userAssetMap[userAssetKey] = userAsset
							}
						}
					}
				}

				fromUserAssetKey := chainName + record.FromAddress + tokenAddress
				if userAsset, ok := userAssetMap[fromUserAssetKey]; !ok {
					userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, tokenAddress, tokenInfo, now)
					for i := 0; i < 10 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address("); i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, tokenAddress, tokenInfo, now)
					}
					if err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address(") {
						// 更新用户资产出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，查询用户资产失败", chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("fromAddress", record.FromAddress), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						return
					}
					if userAsset != nil {
						userAssetMap[fromUserAssetKey] = userAsset
					}
				}

				toUserAssetKey := chainName + record.ToAddress + tokenAddress
				if userAsset, ok := userAssetMap[toUserAssetKey]; !ok {
					userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.ToUid, record.ToAddress, tokenAddress, tokenInfo, now)
					for i := 0; i < 10 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address("); i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						userAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.ToUid, record.ToAddress, tokenAddress, tokenInfo, now)
					}
					if err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address(") {
						// 更新用户资产出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，查询用户资产失败", chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("toAddress", record.ToAddress), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						return
					}
					if userAsset != nil {
						userAssetMap[toUserAssetKey] = userAsset
					}
				}
			}
		}

		fromUserAssetKey := chainName + record.FromAddress
		if _, ok := userAssetMap[fromUserAssetKey]; !ok {
			if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
				mainDecimals = platInfo.Decimal
				mainSymbol = platInfo.NativeCurrency
			} else {
				continue
			}
			tokenInfo := &types.TokenInfo{
				Decimals: int64(mainDecimals),
				Symbol:   mainSymbol,
			}
			fromUserAsset, err := doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", tokenInfo, now)
			for i := 0; i < 10 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address("); i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				fromUserAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", tokenInfo, now)
			}
			if err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Resource not found by Address(") {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，更新用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，更新用户资产失败", zap.Any("chainName", chainName), zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
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
		uidType, _ := biz.GetUidTypeCode(userAsset.Address)
		userAsset.UidType = uidType
		userAssets = append(userAssets, userAsset)
	}

	//更新用户资产成本价
	_ = biz.UpdateAssetCostPrice(nil, userAssets)

	_, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserAsset(chainName string, client Client, transactionType string, uid string, address string,
	tokenAddress string, tokenInfo *types.TokenInfo, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		if transactionType == biz.NATIVE || tokenAddress == APT_CODE || tokenAddress == "" {
			return client.GetBalance(address)
		} else {
			return client.GetTokenBalance(address, tokenAddress, int(tokenInfo.Decimals))
		}
	})
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}
	balance := result.(string)

	var userAsset = &data.UserAsset{
		ChainName:     chainName,
		Uid:           uid,
		Address:       address,
		TokenAddress:  tokenAddress,
		TokenUri:      tokenInfo.TokenUri,
		Balance:       balance,
		Decimals:      int32(tokenInfo.Decimals),
		Symbol:        tokenInfo.Symbol,
		IsSyncToChain: true,
		SyncToChainTs: nowTime,
		CreatedAt:     nowTime,
		UpdatedAt:     nowTime,
	}
	return userAsset, nil
}

func HandleUserStatistic(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserStatistic error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserStatistic panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.CREATEACCOUNT || record.TransactionType == biz.REGISTERTOKEN ||
			record.TransactionType == biz.DIRECTTRANSFERNFTSWITCH || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		var tokenAddress = record.ContractAddress
		if record.ContractAddress == APT_CODE {
			tokenAddress = ""
		}

		decimals, _, err := biz.GetDecimalsSymbolFromTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("统计交易金额，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", record.TransactionVersion), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}

		var userAssetStatistic = biz.UserAssetStatistic{
			ChainName:    chainName,
			FromUid:      record.FromUid,
			ToUid:        record.ToUid,
			Amount:       record.Amount,
			TokenAddress: tokenAddress,
			Decimals:     decimals,
		}
		userAssetStatisticList = append(userAssetStatisticList, userAssetStatistic)
	}
	biz.HandleUserAssetStatistic(chainName, userAssetStatisticList)
}

func HandleTransactionCount(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleTransactionCount error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleTransactionCount panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易次数失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var transactionInfoList []biz.TransactionInfo
	for _, record := range txRecords {
		if record.TransactionType != biz.NATIVE && record.TransactionType != biz.TRANSFER && record.TransactionType != biz.TRANSFERNFT &&
			record.TransactionType != biz.CONTRACT && record.TransactionType != biz.SWAP && record.TransactionType != biz.MINT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}
		if record.FromAddress == "" || record.ToAddress == "" || (record.FromUid == "" && record.ToUid == "") {
			continue
		}

		transactionType := record.TransactionType
		if transactionType == biz.CONTRACT {
			tx, err := data.AptTransactionRecordRepoClient.SelectColumnByTxHash(nil, biz.GetTableName(chainName), record.TransactionHash, []string{"transaction_type"})
			if err == nil && tx != nil && (tx.TransactionType == biz.MINT || tx.TransactionType == biz.SWAP) {
				transactionType = tx.TransactionType
			}
		}

		var transactionInfo = biz.TransactionInfo{
			ChainName:       chainName,
			FromAddress:     record.FromAddress,
			ToAddress:       record.ToAddress,
			TransactionType: transactionType,
			TransactionHash: record.TransactionHash,
		}
		transactionInfoList = append(transactionInfoList, transactionInfo)
	}
	biz.HandleTransactionCount(chainName, transactionInfoList)
}

func HandleTokenPush(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
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
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.CREATEACCOUNT || record.TransactionType == biz.REGISTERTOKEN ||
			record.TransactionType == biz.DIRECTTRANSFERNFTSWITCH || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("推送token信息，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", record.TransactionVersion), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType == biz.APTOSNFT {
			continue
		}
		decimals := tokenInfo.Decimals
		symbol := tokenInfo.Symbol

		tokenAddress := record.ContractAddress
		address := record.ToAddress
		uid := record.ToUid
		if tokenAddress != APT_CODE && tokenAddress != "" && address != "" && uid != "" {
			var userAsset = biz.UserTokenPush{
				ChainName:    chainName,
				Uid:          uid,
				Address:      address,
				TokenAddress: tokenAddress,
				Decimals:     int32(decimals),
				Symbol:       symbol,
			}
			userAssetList = append(userAssetList, userAsset)
		}
	}
	biz.HandleTokenPush(chainName, userAssetList)
}

func HandleUserNftAsset(chainName string, client Client, txRecords []*data.AptTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserNftAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserNftAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	now := time.Now().Unix()
	var userAssets []*data.UserNftAsset
	userAssetMap := make(map[string]*data.UserNftAsset)
	addressTokenMap := make(map[string]map[string]map[string]*v1.GetNftReply_NftInfoResp)
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.CREATEACCOUNT || record.TransactionType == biz.REGISTERTOKEN ||
			record.TransactionType == biz.DIRECTTRANSFERNFTSWITCH || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户NFT资产，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("transactionVersion", record.TransactionVersion), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.APTOSNFT {
			continue
		}
		if !((record.FromAddress != "" && record.FromUid != "") || (record.ToAddress != "" && record.ToUid != "")) {
			continue
		}

		tokenId := tokenInfo.TokenId
		tokenAddress := tokenInfo.Address
		if tokenAddress == "" || tokenId == "" {
			continue
		}
		nftInfo, err := biz.GetRawNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
		if err != nil {
			log.Error("更新用户NFT资产，从nodeProxy中获取NFT信息失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			//continue
		}
		if nftInfo == nil {
			log.Error("更新用户NFT资产，从nodeProxy中获取NFT信息为空", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
			//continue
			nftInfo = &v1.GetNftReply_NftInfoResp{
				TokenAddress:   tokenAddress,
				TokenId:        tokenId,
				CollectionName: tokenInfo.CollectionName,
				NftName:        tokenInfo.ItemName,
				TokenType:      tokenType,
			}
			if nftInfo.NftName == "" && (len(tokenInfo.TokenId) != 64 || strings.Contains(tokenInfo.TokenId, " ") ||
				strings.Contains(tokenInfo.TokenId, "#") || strings.Contains(tokenInfo.TokenId, ".")) {
				nftInfo.NftName = tokenInfo.TokenId
			}
		}
		if nftInfo.TokenType == "" {
			nftInfo.TokenType = tokenType
		}

		if record.FromAddress != "" && record.FromUid != "" {
			fromKey := record.FromUid + "," + record.FromAddress
			tokenAddressIdMap, ok := addressTokenMap[fromKey]
			if !ok {
				tokenAddressIdMap = make(map[string]map[string]*v1.GetNftReply_NftInfoResp)
				addressTokenMap[fromKey] = tokenAddressIdMap
			}
			tokenIdMap, ok := tokenAddressIdMap[tokenAddress]
			if !ok {
				tokenIdMap = make(map[string]*v1.GetNftReply_NftInfoResp)
				tokenAddressIdMap[tokenAddress] = tokenIdMap
			}
			tokenIdMap[tokenId] = nftInfo
		}

		if record.ToAddress != "" && record.ToUid != "" {
			toKey := record.ToUid + "," + record.ToAddress
			tokenAddressIdMap, ok := addressTokenMap[toKey]
			if !ok {
				tokenAddressIdMap = make(map[string]map[string]*v1.GetNftReply_NftInfoResp)
				addressTokenMap[toKey] = tokenAddressIdMap
			}
			tokenIdMap, ok := tokenAddressIdMap[tokenAddress]
			if !ok {
				tokenIdMap = make(map[string]*v1.GetNftReply_NftInfoResp)
				tokenAddressIdMap[tokenAddress] = tokenIdMap
			}
			tokenIdMap[tokenId] = nftInfo
		}
	}

	for key, tokenAddressIdMap := range addressTokenMap {
		uidAddress := strings.Split(key, ",")
		userAssetsList, err := doHandleUserNftAsset(chainName, client, uidAddress[0], uidAddress[1], tokenAddressIdMap, now)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			userAssetsList, err = doHandleUserNftAsset(chainName, client, uidAddress[0], uidAddress[1], tokenAddressIdMap, now)
		}
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，查询用户资产失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户NFT资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("address", uidAddress[1]), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
			return
		}
		for _, userAsset := range userAssetsList {
			if userAsset != nil {
				userAssetKey := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress + userAsset.TokenId
				userAssetMap[userAssetKey] = userAsset
			}
		}
	}

	if len(userAssetMap) == 0 {
		return
	}
	for _, userAsset := range userAssetMap {
		userAssets = append(userAssets, userAsset)
	}
	count, err := data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户NFT资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
	if count > 0 {
		var chainNameAddressTokenAddressTokenIdList []*data.NftAssetRequest
		for _, record := range userAssets {
			if len(record.TokenId) != 64 || strings.Contains(record.TokenId, " ") ||
				strings.Contains(record.TokenId, "#") || strings.Contains(record.TokenId, ".") {
				continue
			}
			chainNameAddressTokenAddressTokenIdList = append(chainNameAddressTokenAddressTokenIdList, &data.NftAssetRequest{
				ChainName:    record.ChainName,
				Address:      record.Address,
				TokenAddress: record.TokenAddress,
				TokenId:      record.ItemName,
			})
		}

		if len(chainNameAddressTokenAddressTokenIdList) == 0 {
			return
		}
		var assetRequest = &data.NftAssetRequest{
			ChainNameAddressTokenAddressTokenIdList: chainNameAddressTokenAddressTokenIdList,
		}
		count, err = data.UserNftAssetRepoClient.Delete(nil, assetRequest)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			count, err = data.UserNftAssetRepoClient.Delete(nil, assetRequest)
		}
		log.Info("更新用户NFT资产，删除数据库中重复NFT资产信息", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("count", count))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，删除数据库中重复NFT资产信息失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户NFT资产，删除数据库中重复NFT资产信息失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
		}
	}
}

func doHandleUserNftAsset(chainName string, client Client, uid string, address string,
	tokenAddressIdMap map[string]map[string]*v1.GetNftReply_NftInfoResp, nowTime int64) ([]*data.UserNftAsset, error) {
	var userAssets []*data.UserNftAsset
	for tokenAddress, tokenIdMap := range tokenAddressIdMap {
		tokenAddressSplit := strings.Split(tokenAddress, "::")
		creatorAddress := tokenAddressSplit[0]
		collectionName := tokenAddressSplit[1]
		propertyVersion, err := strconv.Atoi(tokenAddressSplit[2])
		if err != nil {
			log.Error("get propertyVersion error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
			return nil, err
		}
		for tokenId, nftInfo := range tokenIdMap {
			result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
				return client.Erc1155BalanceByName(address, creatorAddress, collectionName, nftInfo.NftName, propertyVersion)
				//return client.Erc1155BalanceByTokenId(address, tokenId, propertyVersion)
			})
			if err != nil {
				log.Error("query nft balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
				return nil, err
			}
			balance := result.(string)

			payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
				"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
			var userAsset = &data.UserNftAsset{
				ChainName:        chainName,
				Uid:              uid,
				Address:          address,
				TokenAddress:     tokenAddress,
				TokenUri:         nftInfo.CollectionImageURL,
				TokenId:          tokenId,
				Balance:          balance,
				TokenType:        nftInfo.TokenType,
				CollectionName:   nftInfo.CollectionName,
				Symbol:           nftInfo.Symbol,
				Name:             nftInfo.Name,
				ItemName:         nftInfo.NftName,
				ItemUri:          nftInfo.ImageURL,
				ItemOriginalUri:  nftInfo.ImageOriginalURL,
				ItemAnimationUri: nftInfo.AnimationURL,
				Data:             payload,
				CreatedAt:        nowTime,
				UpdatedAt:        nowTime,
			}
			userAssets = append(userAssets, userAsset)
		}
	}
	return userAssets, nil
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}
