package ethereum

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
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

	go biz.DappApproveFilter(chainName, txRecords)
	go biz.NftApproveFilter(chainName, txRecords)
	go func() {
		handleTokenPush(chainName, client, txRecords)
		handleUserAsset(chainName, client, txRecords)
	}()
	go handleUserStatistic(chainName, client, txRecords)
	go handleUserNonce(chainName, txRecords)
	go HandleRecordStatus(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go handleUserNftAsset(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
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
	go HandleRecordStatus(chainName, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go handleUserNftAsset(chainName, client, txRecords)
}

func HandleRecordStatus(chainName string, txRecords []*data.EvmTransactionRecord) {
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}
		if record.Type == biz.EVENTLOG {
			continue
		}
		txHashs := strings.Split(record.TransactionHash, "#")
		realAmount := record.Amount
		realDappData := record.DappData
		if (realAmount.String() == "0" || realAmount == decimal.Zero) && realDappData == "" {
			data.EvmTransactionRecordRepoClient.UpdateCancelByNonce(nil, biz.GetTalbeName(chainName), record.FromAddress, record.Nonce, txHashs[0], record.ToAddress)
		} else {
			if record.TransactionType == biz.TRANSFER || record.TransactionType == biz.SPEED_UP {
				record.Amount = decimal.Zero
				record.Data = ""
			}
			data.EvmTransactionRecordRepoClient.UpdateStatusByNonce(nil, biz.GetTalbeName(chainName), record.FromAddress, record.Nonce, txHashs[0], record.ToAddress, record.Amount, record.Data)
		}
	}
}

func handleUserNonce(chainName string, txRecords []*data.EvmTransactionRecord) {
	doneNonce := make(map[string]int)
	doneNonceTotal := make(map[string]int)
	for _, record := range txRecords {
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
	//1   2条 2 3  取出 3 -2 == 1
	for k, v := range doneNonce {
		total := doneNonceTotal[k]
		if v == 0 {
			_, err := data.RedisClient.Get(k).Result()
			if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
				data.RedisClient.Set(k, strconv.Itoa(v), 0)
			}
		} else {
			nonceStr, _ := data.RedisClient.Get(k).Result()
			nonce, _ := strconv.Atoi(nonceStr)
			if v > nonce && v-total == nonce {
				data.RedisClient.Set(k, strconv.Itoa(v), 0)
			} else {
				log.Info(k, zap.Any("无需修改nonce,交易记录的nonce值", v), zap.Any("本地记录nonce", nonce))
			}
		}
	}
}

func handleUserAsset(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
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
	addressTokenMap := make(map[string]map[string]int)
	tokenSymbolMap := make(map[string]string)
	addressUidMap := make(map[string]string)
	var mainDecimals int32
	var mainSymbol string
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		var tokenAddress = record.ContractAddress
		if strings.HasPrefix(chainName, "Polygon") && tokenAddress == POLYGON_CODE {
			tokenAddress = ""
		}
		tokenInfo, err := biz.PaseGetTokenInfo(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		decimals := tokenInfo.Decimals
		symbol := tokenInfo.Symbol

		if record.TransactionType == biz.NATIVE || tokenAddress == "" {
			if record.FromAddress != "" && record.FromUid != "" {
				_, ok := addressUidMap[record.FromAddress]
				if !ok {
					addressUidMap[record.FromAddress] = record.FromUid
				}
			}

			if record.ToAddress != "" && record.ToUid != "" {
				_, ok := addressUidMap[record.ToAddress]
				if !ok {
					addressUidMap[record.ToAddress] = record.ToUid
				}
			}

			if mainSymbol == "" {
				mainDecimals = int32(decimals)
				mainSymbol = symbol
			}
		} else if tokenAddress != "" {
			if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
				tokenSymbolMap[tokenAddress] = symbol
				if record.FromAddress != "" && record.FromUid != "" {
					fromKey := record.FromUid + "," + record.FromAddress
					tokenDecimalsMap, ok := addressTokenMap[fromKey]
					if !ok {
						tokenDecimalsMap = make(map[string]int)
						addressTokenMap[fromKey] = tokenDecimalsMap
					}
					tokenDecimalsMap[tokenAddress] = int(decimals)
				}

				if record.ToAddress != "" && record.ToUid != "" {
					toKey := record.ToUid + "," + record.ToAddress
					tokenDecimalsMap, ok := addressTokenMap[toKey]
					if !ok {
						tokenDecimalsMap = make(map[string]int)
						addressTokenMap[toKey] = tokenDecimalsMap
					}
					tokenDecimalsMap[tokenAddress] = int(decimals)
				}
			}
		}

		if record.FromAddress != "" && record.FromUid != "" {
			_, ok := addressUidMap[record.FromAddress]
			if !ok {
				if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
					mainDecimals = platInfo.Decimal
					mainSymbol = platInfo.NativeCurrency
				} else {
					continue
				}
				addressUidMap[record.FromAddress] = record.FromUid
			}
		}
	}

	for address, uid := range addressUidMap {
		var err error
		userAssetKey := chainName + address
		if userAsset, ok := userAssetMap[userAssetKey]; !ok {
			userAsset, err = doHandleUserAsset(chainName, client, uid, address, "", mainDecimals, mainSymbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				userAsset, err = doHandleUserAsset(chainName, client, uid, address, "", mainDecimals, mainSymbol, now)
			}
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s查询用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"查询用户资产失败", zap.Any("address", address), zap.Any("error", err))
				return
			}
			if userAsset != nil {
				userAssetMap[userAssetKey] = userAsset
			}
		}
	}

	for key, tokenDecimalsMap := range addressTokenMap {
		uidAddress := strings.Split(key, ",")
		userAssetsList, err := doHandleUserTokenAsset(chainName, client, uidAddress[0], uidAddress[1], tokenDecimalsMap, tokenSymbolMap, now)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			userAssetsList, err = doHandleUserTokenAsset(chainName, client, uidAddress[0], uidAddress[1], tokenDecimalsMap, tokenSymbolMap, now)
		}
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s查询用户资产失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"查询用户资产失败", zap.Any("address", uidAddress[1]), zap.Any("tokenAddress", tokenDecimalsMap), zap.Any("error", err))
			return
		}
		for _, userAsset := range userAssetsList {
			if userAsset != nil {
				userAssetKey := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress
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
		log.Error(chainName+"更新用户资产，将数据插入到数据库中失败", zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserAsset(chainName string, client Client, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	balance, err := client.GetBalance(address)
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

func doHandleUserTokenAsset(chainName string, client Client, uid string, address string,
	tokenDecimalsMap map[string]int, tokenSymbolMap map[string]string, nowTime int64) ([]*data.UserAsset, error) {
	var userAssets []*data.UserAsset
	balanceList, err := client.BatchTokenBalance(address, tokenDecimalsMap)
	if err != nil {
		log.Error("query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenDecimalsMap), zap.Any("error", err))
		return nil, err
	}
	for tokenAddress, balancei := range balanceList {
		balance := fmt.Sprintf("%v", balancei)
		decimals := int32(tokenDecimalsMap[tokenAddress])
		symbol := tokenSymbolMap[tokenAddress]

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
		userAssets = append(userAssets, userAsset)
	}
	return userAssets, nil
}

func handleUserStatistic(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
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
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
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

		if strings.HasPrefix(chainName, "Polygon") && tokenAddress == POLYGON_CODE {
			tokenAddress = ""
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
			log.Error(chainName+"交易记录统计，解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
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
		log.Error(chainName+"交易记录统计，将数据插入到数据库中失败", zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func handleTokenPush(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
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
		if record.TransactionType == biz.CONTRACT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.PaseGetTokenInfo(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType == biz.ERC721 || tokenType == biz.ERC1155 {
			continue
		}
		decimals := tokenInfo.Decimals
		symbol := tokenInfo.Symbol

		tokenAddress := record.ContractAddress
		address := record.ToAddress
		uid := record.ToUid
		if !(strings.HasPrefix(chainName, "Polygon") && tokenAddress == POLYGON_CODE) && tokenAddress != "" && address != "" && uid != "" {
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

func HandleNftRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleNftRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleNftRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT交易履历失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT {
			continue
		}
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		tokenInfo, err := biz.PaseTokenInfo(record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
			continue
		}
		if !((record.FromAddress != "" && record.FromUid != "") || (record.ToAddress != "" && record.ToUid != "")) {
			continue
		}

		tokenId := tokenInfo.TokenId
		tokenAddress := tokenInfo.Address
		log.Info(chainName+"添加NFT交易履历", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
			zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
		//if !GetETHNftHistoryByBlockspan(chainName, tokenAddress, tokenId) {
		//	if !GetETHNftHistoryByNftgo(chainName, tokenAddress, tokenId, client) {
		//		GetETHNftHistoryByOpenSea(chainName, tokenAddress, tokenId)
		//	}
		//}
	}
}

func handleUserNftAsset(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("handleUserNftAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("handleUserNftAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
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
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.PaseTokenInfo(record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"解析parseData失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
			continue
		}
		if !((record.FromAddress != "" && record.FromUid != "") || (record.ToAddress != "" && record.ToUid != "")) {
			continue
		}

		tokenId := tokenInfo.TokenId
		tokenAddress := tokenInfo.Address
		nftInfo, err := biz.GetRawNftInfoDirectly(nil, chainName, tokenAddress, tokenId)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			nftInfo, err = biz.GetRawNftInfoDirectly(nil, chainName, tokenAddress, tokenId)
		}
		if err != nil {
			// nodeProxy出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中NFT信息失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"更新用户资产，从nodeProxy中获取NFT信息失败", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			continue
		}
		if nftInfo == nil {
			log.Error(chainName+"更新用户资产，从nodeProxy中获取NFT信息为空", zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
			continue
		}

		if record.FromAddress != "" && record.FromUid != "" {
			if tokenInfo.TokenType == biz.ERC721 {
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
					TokenType:        nftInfo.TokenType,
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
			} else if tokenInfo.TokenType == biz.ERC1155 {
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
		}

		if record.ToAddress != "" && record.ToUid != "" {
			if tokenInfo.TokenType == biz.ERC721 {
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
					TokenType:        nftInfo.TokenType,
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
			} else if tokenInfo.TokenType == biz.ERC1155 {
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
			alarmMsg := fmt.Sprintf("请注意：%s查询用户资产失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"查询用户资产失败", zap.Any("address", uidAddress[1]), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
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
		alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"更新用户资产，将数据插入到数据库中失败", zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserNftAsset(chainName string, client Client, uid string, address string,
	tokenAddressIdMap map[string]map[string]*v1.GetNftReply_NftInfoResp, nowTime int64) ([]*data.UserNftAsset, error) {
	var userAssets []*data.UserNftAsset
	for tokenAddress, tokenIdMap := range tokenAddressIdMap {
		for tokenId, nftInfo := range tokenIdMap {
			var err error
			var balance string
			if nftInfo.TokenType == biz.ERC721 {
				balance, err = client.Erc721Balance(address, tokenAddress, tokenId)
			} else if nftInfo.TokenType == biz.ERC1155 {
				balance, err = client.Erc1155Balance(address, tokenAddress, tokenId)
			}
			if err != nil {
				log.Error("query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
				return nil, err
			}

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
