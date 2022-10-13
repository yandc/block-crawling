package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

var urlMap = map[string]string{
	"DOGE": "http://haotech:jHoNTnHnZZY6pXuiUWoUwZKC@47.244.138.206:22555",
	"BTC":  "http://haotech:phzxiTvtjqHikHTBTnTthqg3@47.244.138.206:8332",
	"LTC":  "http://haotech:BFHGDCQHbaTZBvHJER4fyHy@47.75.184.192:9332",
}

func HandleRecord(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
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

	go handleUserAsset(chainName, client, txRecords)
	go handleUserStatistic(chainName, client, txRecords)
	go UnspentTx(chainName, client, txRecords)
}

func UnspentTx(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("unspentTx error, chainName:"+chainName, e)
			} else {
				log.Errore("unspentTx panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更用户utxo失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	baseClient := client.DispatchClient
	baseClient.StreamURL = urlMap[chainName]
	p1 := decimal.NewFromInt(100000000)

	for _, record := range txRecords {

		if record.Status != biz.SUCCESS {
			continue
		}

		ret := strings.Split(record.TransactionHash, "#")[0]

		txRecord, err := baseClient.GetTransactionsByTXHash(ret)
		log.Info(chainName, zap.Any("ydUTXO", txRecord))
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			txRecord, err = baseClient.GetTransactionsByTXHash(ret)
		}
		if txRecord.Error != nil {
			log.Error(chainName, zap.Any("err", txRecord.Error))
			continue
		}
		for _, vout := range txRecord.Result.Vout {
			//增加 未消费的utxo
			var fromAdd string

			if chainName == "BTC" {
				fromAdd = vout.ScriptPubKey.Address
			}

			if chainName == "LTC" || chainName == "DOGE" {
				if len(vout.ScriptPubKey.Addresses) > 0 {
					fromAdd = vout.ScriptPubKey.Addresses[0]
				}
			}
			fromUid := ""
			userMeta, err := pCommon.MatchUser(fromAdd, "", chainName)
			if err == nil {
				fromUid = userMeta.FromUid
			}
			if fromUid == "" {
				continue
			}
			value := decimal.NewFromFloat(vout.Value)
			var utxoUnspentRecord = &data.UtxoUnspentRecord{
				Uid:       fromUid,
				Hash:      record.TransactionHash,
				N:         vout.N,
				ChainName: chainName,
				Address:   fromAdd,
				Script:    vout.ScriptPubKey.Hex,
				Unspent:   1, //1 未花费 2 已花费 联合索引
				Amount:    value.Mul(p1).String(),
				TxTime:    int64(txRecord.Result.Time),
				UpdatedAt: time.Now().Unix(),
			}
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
		}

		//vin 消耗的utxo
		for _, vin := range txRecord.Result.Vin {

			txVinRecord, err1 := baseClient.GetTransactionsByTXHash(vin.Txid)
			for i := 0; i < 10 && err1 != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				txVinRecord, err1 = baseClient.GetTransactionsByTXHash(vin.Txid)
			}
			if txVinRecord.Error != nil {
				log.Error(chainName, zap.Any("err", txRecord.Error))
				continue
			}
			// 角标
			inVout := txVinRecord.Result.Vout[vin.Vout]
			spentValue := decimal.NewFromFloat(inVout.Value)

			var fromAdd string

			if chainName == "BTC" {
				fromAdd = inVout.ScriptPubKey.Address
			}

			if chainName == "LTC" || chainName == "DOGE" {
				if len(inVout.ScriptPubKey.Addresses) > 0 {
					fromAdd = inVout.ScriptPubKey.Addresses[0]
				}
			}

			uid := ""
			userMeta, err := pCommon.MatchUser(fromAdd, "", chainName)
			if err == nil {
				uid = userMeta.FromUid
			}

			var utxoUnspentRecord = &data.UtxoUnspentRecord{
				Uid:       uid,
				Hash:      vin.Txid,
				N:         vin.Vout,
				ChainName: chainName,
				Address:   fromAdd,
				Script:    inVout.ScriptPubKey.Hex,
				Unspent:   2, //1 未花费 2 已花费 联合索引
				Amount:    spentValue.Mul(p1).String(),
				TxTime:    int64(txVinRecord.Result.Time),
				UpdatedAt: time.Now().Unix(),
			}
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
		}
	}
}

func handleUserAsset(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
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
	var decimals int32 = 8
	var symbol = "BTC"
	for _, record := range txRecords {
		var err error
		fromUserAssetKey := chainName + record.FromAddress
		if fromUserAsset, ok := userAssetMap[fromUserAssetKey]; !ok {
			fromUserAsset, err = doHandleUserAsset(chainName, client, record.FromUid, record.FromAddress, decimals, symbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				fromUserAsset, err = doHandleUserAsset(chainName, client, record.FromUid, record.FromAddress, decimals, symbol, now)
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

		toUserAssetKey := chainName + record.ToAddress
		if toUserAsset, ok := userAssetMap[toUserAssetKey]; !ok {
			toUserAsset, err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, symbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				toUserAsset, err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, symbol, now)
			}
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s更新用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"更新用户资产失败", zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
				return
			}
			if toUserAsset != nil {
				userAssetMap[toUserAssetKey] = toUserAsset
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

func doHandleUserAsset(chainName string, client Client, uid string, address string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	balance, err := client.GetBalance(address)
	if err != nil {
		log.Error("query balance error", zap.Any("address", address), zap.Any("error", err))
		return nil, err
	}

	var userAsset = &data.UserAsset{
		ChainName: chainName,
		Uid:       uid,
		Address:   address,
		Balance:   balance,
		Decimals:  decimals,
		Symbol:    symbol,
		CreatedAt: nowTime,
		UpdatedAt: nowTime,
	}
	return userAsset, nil
}

func handleUserStatistic(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
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
	var decimals int32 = 8
	var dt = utils.GetDayTime(&tm)
	//资金流向: 1:充值, 2:提现, 3:内部转账

	//资金类型: 1:小单提现, 2:次中单提现, 3:中单提现, 4:大单提现, 5:超大单提现
	//小单：金额<1K
	//次中单：1K=<金额<1W
	//中单：1W=<金额<10W
	//大单：10W=<金额<100W
	//超大单：100W=<金额

	price, err := biz.GetTokenPrice(nil, chainName, biz.CNY, "")
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		price, err = biz.GetTokenPrice(nil, chainName, biz.CNY, "")
	}
	if err != nil {
		// 调用nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币价格失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Errore(chainName+"交易记录统计，从nodeProxy中获取代币价格失败", err)
		return
	}

	var transactionStatisticMap = make(map[string]*data.TransactionStatistic)
	var transactionStatisticList []*data.TransactionStatistic
	for _, record := range txRecords {
		var fundDirection int16
		var fundType int16
		var amount = record.Amount
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
				TokenAddress:        "",
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
	_, err = data.TransactionStatisticRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionStatisticList, biz.PAGE_SIZE)
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
