package cosmos

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
	go HandleTransactionCount(chainName, client, txRecords)
	HandleUserNonce(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(false, chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
	go HandleTransactionCount(chainName, client, txRecords)
	HandleUserNonce(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(true, chainName, client, txRecords)
}

func HandleNftRecord(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
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
		tokenType := tokenInfo.TokenType
		if tokenType != biz.COSMOSNFT {
			continue
		}
		tokenAddress := tokenInfo.Address
		tokenId := tokenInfo.TokenId
		if tokenAddress == "" || tokenId == "" {
			log.Info("不添加NFT流转记录", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
			continue
		}
		log.Info("添加NFT流转记录", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
		_, ok := tokenIdMap[tokenId]
		if ok {
			continue
		} else {
			tokenIdMap[tokenId] = tokenId
		}
		result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
			return client.GetNftHistories(tokenAddress, tokenId, 1, 100)
		})
		tar := result.(*NftHistories)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，获取流转记录失败，tokenAddress：%s，tokenId：%s", chainName, tokenAddress, tokenId)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("添加NFT流转记录，获取流转记录失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			continue
		}
		histories := tar.Data
		userNftMap := make(map[string]string)
		if len(histories) > 0 {
			var fromAddress, toAddress, fromUid, toUid string
			now := time.Now().Unix()
			historiesLen := len(histories)
			for i := historiesLen - 1; i >= 0; i-- {
				history := histories[i]
				attributes := history.Attributes
				fromAddress = toAddress
				fromUid = toUid
				toAddress = attributes.Address
				if toAddress != "" {
					_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
					if err != nil {
						log.Error("添加NFT流转记录，从redis中获取用户地址失败", zap.Any("chainName", chainName), zap.Any("error", err))
						return
					}
				}

				txTime := attributes.CreatedAt.Unix() * 1000

				sk := fromAddress + toAddress
				_, ok1 := userNftMap[sk]
				if ok1 {
					continue
				} else {
					userNftMap[sk] = sk
				}
				nrh := &data.NftRecordHistory{
					ChainName: chainName,
					//TransactionHash: attributes.TnxHash,
					TxTime:          strconv.Itoa(int(txTime)),
					FromAddress:     fromAddress,
					ToAddress:       toAddress,
					FromUid:         fromUid,
					ToUid:           toUid,
					Quantity:        "1",
					ContractAddress: tokenAddress,
					TokenId:         tokenId,
					//EventType:       e.TransferType,
					CreatedAt: now,
					UpdatedAt: now,
				}
				nftRecords = append(nftRecords, nrh)
			}
		}
	}

	if len(nftRecords) > 0 {
		_, err := data.NftRecordHistoryRepoClient.SaveOrUpdate(nil, nftRecords)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.NftRecordHistoryRepoClient.SaveOrUpdate(nil, nftRecords)
		}
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，将数据插入到数据库中失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("添加NFT流转记录，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("tokenIdMap", tokenIdMap), zap.Any("error", err))
		}
	}
}

func HandleUserNonce(chainName string, txRecords []*data.AtomTransactionRecord) {
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

func HandleUserAsset(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
		var denom string
		if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
			denom = strings.ToLower(platInfo.NativeCurrency)
		}
		if tokenAddress == denom {
			tokenAddress = ""
		}

		if record.TransactionType != biz.CONTRACT && record.TransactionType != biz.SWAP && record.TransactionType != biz.MINT &&
			record.TransactionType != biz.APPROVE && record.TransactionType != biz.APPROVENFT {
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

			if tokenType != biz.COSMOSNFT {
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
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，查询用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
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

	var tokenDenom string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		tokenDenom = "u" + strings.ToLower(platInfo.NativeCurrency)
	}
	realChainName := GetChainName(chainName, address)
	if chainName != realChainName && tokenAddress == "" {
		tokenAddress = tokenDenom
	}
	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		if chainName == realChainName && (transactionType == biz.NATIVE || tokenAddress == tokenDenom || tokenAddress == "") {
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
		ChainName:    realChainName,
		Uid:          uid,
		Address:      address,
		TokenAddress: tokenAddress,
		TokenUri:     tokenInfo.TokenUri,
		Balance:      balance,
		Decimals:     int32(tokenInfo.Decimals),
		Symbol:       tokenInfo.Symbol,
		CreatedAt:    nowTime,
		UpdatedAt:    nowTime,
	}
	return userAsset, nil
}

func HandleUserStatistic(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		var denom string
		if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
			denom = strings.ToLower(platInfo.NativeCurrency)
		}
		var tokenAddress = record.ContractAddress
		if record.ContractAddress == denom {
			tokenAddress = ""
		}

		decimals, _, err := biz.GetDecimalsSymbolFromTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("统计交易金额，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
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

func HandleTransactionCount(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
			tx, err := data.AtomTransactionRecordRepoClient.SelectColumnByTxHash(nil, biz.GetTableName(chainName), record.TransactionHash, []string{"transaction_type"})
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

func HandleTokenPush(chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		decimals, symbol, err := biz.GetDecimalsSymbolFromTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("推送token信息，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}

		tokenAddress := record.ContractAddress
		address := record.ToAddress
		uid := record.ToUid
		var tokenDenom string
		if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
			tokenDenom = "u" + strings.ToLower(platInfo.NativeCurrency)
		}
		realChainName := GetChainName(chainName, address)
		if chainName != realChainName && tokenAddress == "" {
			tokenAddress = tokenDenom
		}
		// ignores native token if the native token is the same when cross assets between chains.
		if chainName != realChainName && tokenAddress == tokenDenom {
			continue
		}
		if symbol == "Unknown Token" {
			continue
		}
		if (chainName != realChainName || (tokenAddress != tokenDenom && tokenAddress != "")) && address != "" && uid != "" {
			var userAsset = biz.UserTokenPush{
				ChainName:    realChainName,
				Uid:          uid,
				Address:      address,
				TokenAddress: tokenAddress,
				Decimals:     decimals,
				Symbol:       symbol,
			}

			log.Info(
				"COSMOS PUSH TOKEN",
				zap.String("chainName", chainName),
				zap.String("addressPrefix", realChainName),
				zap.String("tokenAddress", tokenAddress),
				zap.String("tokenDenom", tokenDenom),
				zap.String("address", address),
				zap.String("uid", uid),
				zap.Any("tokenInfo", record.TokenInfo),
				zap.Any("userAsset", userAsset),
			)
			userAssetList = append(userAssetList, userAsset)
		}
	}
	biz.HandleTokenPush(chainName, userAssetList)
}

func HandleUserNftAsset(isPending bool, chainName string, client Client, txRecords []*data.AtomTransactionRecord) {
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
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
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
			log.Error("更新用户NFT资产，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.COSMOSNFT {
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
		}
		if nftInfo.TokenType == "" {
			nftInfo.TokenType = tokenType
		}

		if !isPending {
			if record.FromAddress != "" && record.FromUid != "" {
				payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
					"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
				var userAsset = &data.UserNftAsset{
					ChainName:        chainName,
					Uid:              record.FromUid,
					Address:          record.FromAddress,
					TokenAddress:     tokenAddress,
					TokenUri:         nftInfo.CollectionImageURL,
					TokenId:          tokenId,
					Balance:          "0",
					TokenType:        tokenType,
					CollectionName:   nftInfo.CollectionName,
					Symbol:           nftInfo.Symbol,
					Name:             nftInfo.Name,
					ItemName:         nftInfo.NftName,
					ItemUri:          nftInfo.ImageURL,
					ItemOriginalUri:  nftInfo.ImageOriginalURL,
					ItemAnimationUri: nftInfo.AnimationURL,
					Data:             payload,
					CreatedAt:        now,
					UpdatedAt:        now,
				}
				userAssetKey := chainName + record.FromAddress + tokenAddress + tokenId
				userAssetMap[userAssetKey] = userAsset
			}

			if record.ToAddress != "" && record.ToUid != "" {
				payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
					"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
				var userAsset = &data.UserNftAsset{
					ChainName:        chainName,
					Uid:              record.ToUid,
					Address:          record.ToAddress,
					TokenAddress:     tokenAddress,
					TokenUri:         nftInfo.CollectionImageURL,
					TokenId:          tokenId,
					Balance:          "1",
					TokenType:        tokenType,
					CollectionName:   nftInfo.CollectionName,
					Symbol:           nftInfo.Symbol,
					Name:             nftInfo.Name,
					ItemName:         nftInfo.NftName,
					ItemUri:          nftInfo.ImageURL,
					ItemOriginalUri:  nftInfo.ImageOriginalURL,
					ItemAnimationUri: nftInfo.AnimationURL,
					Data:             payload,
					CreatedAt:        now,
					UpdatedAt:        now,
				}
				userAssetKey := chainName + record.ToAddress + tokenAddress + tokenId
				userAssetMap[userAssetKey] = userAsset
			}
		} else {
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
	_, err := data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户NFT资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserNftAsset(chainName string, client Client, uid string, address string,
	tokenAddressIdMap map[string]map[string]*v1.GetNftReply_NftInfoResp, nowTime int64) ([]*data.UserNftAsset, error) {
	var userAssets []*data.UserNftAsset
	for tokenAddress, tokenIdMap := range tokenAddressIdMap {
		for tokenId, nftInfo := range tokenIdMap {
			result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
				return client.Erc721BalanceByTokenId(address, tokenAddress, tokenId)
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

// GetChainName 处理to地址的链名称不对的问题：跨链交易时fromAddress和toAddress是两个不同链的地址，但是此时两个地址对应的chainName却都是from地址的链名称，to地址的链名称需要根据toAddress来重新计算。
func GetChainName(chainName string, address string) string {
	var realChainName string
	var chainNameIndex int
	for i, val := range address {
		if val == 49 {
			chainNameIndex = i
			break
		}
	}
	// stride16sumdc85dwsznkeaxv3unsdtnls4r5n7g7754c
	// neutron16sumdc85dwsznkeaxv3unsdtnls4r5n702h2mn
	// kujira16sumdc85dwsznkeaxv3unsdtnls4r5n76ausv7
	// celestia16sumdc85dwsznkeaxv3unsdtnls4r5n76l0cme
	chain := address[:chainNameIndex]
	if chain == "osmo" {
		realChainName = "Osmosis"
	} else if chain == "celestia" && biz.IsTestNet(chainName) {
		realChainName = "CelestiaMocha"
	} else {
		realChainName = strings.ToUpper(chain[:1]) + chain[1:]
	}

	if biz.IsTestNet(chainName) {
		realChainName += "TEST"
	}
	return realChainName
}
