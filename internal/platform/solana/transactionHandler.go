package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strconv"
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
		handleTokenPush(chainName, client, txRecords)
		handleUserAsset(chainName, client, txRecords)
	}()
	go handleUserStatistic(chainName, client, txRecords)
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
		handleTokenPush(chainName, client, txRecords)
		handleUserAsset(chainName, client, txRecords)
	}()
}

func handleUserAsset(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("handleUserAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("handleUserAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
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
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		var tokenAddress = record.ContractAddress
		decimals, symbol, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber), zap.Any("parseData", record.ParseData), zap.Any("error", err))
			return
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
			log.Error(chainName+"解析data失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber), zap.Any("data", record.Data), zap.Any("error", err))
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

		fromUserAssetKey = chainName + record.FromAddress
		if fromUserAsset, ok := userAssetMap[fromUserAssetKey]; !ok {
			if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
				decimals = platInfo.Decimal
				symbol = platInfo.NativeCurrency
			}
			fromUserAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", decimals, symbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				fromUserAsset, err = doHandleUserAsset(chainName, client, record.TransactionType, record.FromUid, record.FromAddress, "", decimals, symbol, now)
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

	var balance string
	var err error
	if transactionType == biz.NATIVE || tokenAddress == "" {
		balance, err = client.GetBalance(address)
	} else if tokenAddress != "" {
		balance, err = client.GetTokenBalance(address, tokenAddress, int(decimals))
	}
	if err != nil {
		log.Error("query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}

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

func handleUserStatistic(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("handleUserStatistic error, chainName:"+chainName, e)
			} else {
				log.Errore("handleUserStatistic panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	tm := time.Now()
	nowTime := tm.Unix()
	var dt = utils.GetDayTime(&tm)
	//资金流向: 1:充值, 2:提现, 3:内部转账

	//资金类型: 1:小单提现, 2:次中单提现, 3:中单提现, 4:大单提现, 5:超大单提现
	//小单：金额<1K
	//次中单：1K=<金额<1W
	//中单：1W=<金额<10W
	//大单：10W=<金额<100W
	//超大单：100W=<金额

	var transactionStatisticMap = make(map[string]*data.TransactionStatistic)
	var transactionStatisticList []*data.TransactionStatistic
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.CREATEACCOUNT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		var fundDirection int16
		var fundType int16
		var amount = record.Amount
		var tokenAddress = record.ContractAddress
		var cnyAmount decimal.Decimal
		var usdAmount decimal.Decimal

		if record.FromUid == "" && record.ToUid == "" {
			continue
		} else if record.FromUid == "" {
			fundDirection = 1
		} else if record.ToUid == "" {
			fundDirection = 2
		} else {
			fundDirection = 3
		}

		price, err := biz.GetTokenPrice(nil, chainName, biz.CNY, tokenAddress)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			price, err = biz.GetTokenPrice(nil, chainName, biz.CNY, tokenAddress)
		}
		if err != nil {
			// 调用nodeProxy出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币价格失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"交易记录统计，从nodeProxy中获取代币价格失败", zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
			return
		}

		decimals, _, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"交易记录统计，解析parseData失败", zap.Any("slotNumber", record.SlotNumber), zap.Any("blockNumber", record.BlockNumber), zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}
		prices, _ := decimal.NewFromString(price)
		balance := utils.StringDecimals(amount.String(), int(decimals))
		balances, _ := decimal.NewFromString(balance)
		cnyAmount = prices.Mul(balances)
		if cnyAmount.LessThan(decimal.NewFromInt(1000)) {
			fundType = 1
		} else if cnyAmount.LessThan(decimal.NewFromInt(10000)) {
			fundType = 2
		} else if cnyAmount.LessThan(decimal.NewFromInt(100000)) {
			fundType = 3
		} else if cnyAmount.LessThan(decimal.NewFromInt(100000)) {
			fundType = 4
		} else {
			fundType = 5
		}

		key := chainName + strconv.Itoa(int(fundDirection)) + strconv.Itoa(int(fundType))
		if statistic, ok := transactionStatisticMap[key]; ok {
			statistic.TransactionQuantity += 1
			statistic.Amount = statistic.Amount.Add(amount)
			statistic.CnyAmount = statistic.CnyAmount.Add(cnyAmount)
			statistic.UsdAmount = statistic.UsdAmount.Add(usdAmount)
		} else {
			var transactionStatistic = &data.TransactionStatistic{
				ChainName:           chainName,
				TokenAddress:        tokenAddress,
				FundDirection:       fundDirection,
				FundType:            fundType,
				TransactionQuantity: 1,
				Amount:              amount,
				CnyAmount:           cnyAmount,
				UsdAmount:           usdAmount,
				Dt:                  dt,
				CreatedAt:           nowTime,
				UpdatedAt:           nowTime,
			}

			transactionStatisticMap[key] = transactionStatistic
		}
	}

	if len(transactionStatisticMap) == 0 {
		return
	}
	for _, transactionStatistic := range transactionStatisticMap {
		transactionStatisticList = append(transactionStatisticList, transactionStatistic)
	}
	_, err := data.TransactionStatisticRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionStatisticList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.TransactionStatisticRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionStatisticList, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"交易记录统计，将数据插入到数据库中失败", zap.Any("slotNumber", txRecords[0].SlotNumber), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func handleTokenPush(chainName string, client Client, txRecords []*data.SolTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("handleTokenPush error, chainName:"+chainName, e)
			} else {
				log.Errore("handleTokenPush panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
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
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		decimals, symbol, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("parseData", record.ParseData), zap.Any("error", err))
			return
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
