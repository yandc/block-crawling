package bitcoin

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"golang.org/x/exp/slices"
	"strconv"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
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
	go HandleTransactionCount(chainName, client, txRecords)
	go UnspentTx(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
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

	go handleUserAsset(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
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
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		from := record.FromAddress
		fromUid := record.FromUid
		to := record.ToAddress
		toUid := record.ToUid

		//判断 是否是 本站用户
		if fromUid != "" {
			HandleUTXOAsset(chainName, from, record.TransactionHash)
		}

		if toUid != "" {
			HandleUTXOAsset(chainName, to, record.TransactionHash)
		}
	}
}

func HandleUTXOAsset(chainName string, address string, transactionHash string) error {
	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetBalance(address)
	})

	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，update utxo query balance error", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，update utxo query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("txHash", transactionHash), zap.Any("error", err))
		return err
	}
	balance := result.(string)

	//查询数据库中 UTXO
	dbUtxos, _ := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{
		ChainName: chainName,
		IsUnspent: strconv.Itoa(data.UtxoStatusUnSpend),
		Address:   address,
	})

	utxoBalance := 0
	for _, dbUtxo := range dbUtxos {
		amount, err := strconv.Atoi(dbUtxo.Amount)
		if err != nil {
			continue
		}
		utxoBalance = utxoBalance + amount
	}

	ub := utils.StringDecimals(strconv.Itoa(utxoBalance), 8)

	//如果数据库中 UTXO 总和与 balance 不相等，则更新 UTXO
	if ub != balance {
		err = RefreshUserUTXO(chainName, address, transactionHash, false)
	}

	return err
}

func RefreshUserUTXO(chainName, address, txHash string, refreshAll bool) (err error) {
	userInfo, err := biz.GetUserInfo(address)
	if err != nil {
		return err
	}

	platInfo, _ := biz.GetChainPlatInfo(chainName)
	decimals := int(platInfo.Decimal)

	//从 oklink 获取 utxo
	var utxos []types.OklinkUTXO
	if chainName == "DOGE" || chainName == "LTC" {
		utxoInterface, _ := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
			return client.oklinkClient.GetUTXO(address)
		})
		utxos = utxoInterface.([]types.OklinkUTXO)
	} else if chainName == "BTC" {
		client := NewOklinkClient("BTC", "https://83c5939d-9051-46d9-9e72-ed69d5855209@www.oklink.com")
		utxos, err = client.GetUTXO(address)
	}

	//如果查出来没有，则不更新
	if len(utxos) == 0 {
		return nil
	}
	spentTxhashUtxosMap := make(map[string]struct{})
	if refreshAll {
		_ = data.UtxoUnspentRecordRepoClient.DeleteByAddress(nil, chainName, address)
	} else {
		_, _ = data.UtxoUnspentRecordRepoClient.DeleteByAddressWithNotPending(nil, chainName, address)
		spentTxhashUtxos, _ := data.UtxoUnspentRecordRepoClient.FindNotPendingByTxHash(nil, chainName, address, txHash)
		for _, utxo := range spentTxhashUtxos {
			spentTxhashUtxosMap[utxo.Hash] = struct{}{}
		}

	}

	for _, utxo := range utxos {
		if _, ok := spentTxhashUtxosMap[utxo.Txid]; ok && !refreshAll {
			continue
		}
		index, _ := strconv.Atoi(utxo.Index)
		amountDecimal, _ := decimal.NewFromString(utxo.UnspentAmount)
		amount := biz.Pow10(amountDecimal, decimals).BigInt().String()
		txTime, _ := strconv.ParseInt(utxo.BlockTime, 10, 64)
		var utxoUnspentRecord = &data.UtxoUnspentRecord{
			Uid:       userInfo.Uid,
			Hash:      utxo.Txid,
			N:         index,
			ChainName: chainName,
			Address:   address,
			Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
			Amount:    amount,
			TxTime:    txTime,
			UpdatedAt: time.Now().Unix(),
		}
		//插入所有未花费的UTXO
		r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
		if err != nil {
			log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
		}
	}

	//查询pending的UTXO，与已有的UTXO取差值，差值为已花费的UTXO
	userUTXOs := make([]string, len(utxos))
	for i, utxo := range utxos {
		userUTXOs[i] = fmt.Sprintf("%s#%s", utxo.Txid, utxo.Index)
	}

	pendingUTXOs, err := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{IsUnspent: strconv.Itoa(data.UtxoStatusPending), Address: address})
	for _, utxo := range pendingUTXOs {
		u := fmt.Sprintf("%s#%d", utxo.Hash, utxo.N)
		if !slices.Contains(userUTXOs, u) {
			utxo.Unspent = data.UtxoStatusSpent
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxo)
		}
	}
	return nil
}

//
//func HandleUTXOAssetByBlockcypher(chainName, address, uid, script string, balance string) {
//	re, err := btc.GetUnspentUtxoByBlockcypher(chainName, address)
//	for i := 0; i < 3 && err != nil; i++ {
//		re, err = btc.GetUnspentUtxoByBlockcypher(chainName, address)
//	}
//	if err != nil {
//		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，GetUnspentUtxoByBlockcypher error", chainName)
//		alarmOpts := biz.WithMsgLevel("FATAL")
//		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//		log.Error("更新用户UTXO，GetUnspentUtxoByBlockcypher error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
//	}
//
//	if re.Balance > 0 {
//		retryUtxoBalance := 0
//		for _, tx := range re.Txrefs {
//			//if tx.TxInputN == -1 && !tx.Spent {
//			retryUtxoBalance = retryUtxoBalance + tx.Value
//			//}
//		}
//		ru := utils.StringDecimals(strconv.Itoa(retryUtxoBalance), 8)
//		log.Info("@@@@@@@@@@@@@@@@@@@", zap.String("utxo balance", ru), zap.String("node balance", balance))
//		if ru != balance {
//			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，GetUnspentUtxoByBlockcypher 余额不准确 error", chainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			log.Error("更新用户UTXO，GetUnspentUtxoByBlockcypher 余额不准确 error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
//			return
//		}
//
//		_, err := data.UtxoUnspentRecordRepoClient.DeleteByAddressWithNotPending(nil, chainName, address)
//		if err != nil {
//			// postgres出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，删除数据库utxo数据失败", chainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			log.Error("更新用户UTXO，删除数据库utxo数据失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
//			return
//		}
//
//		for _, tx := range re.Txrefs {
//			//if tx.TxInputN == -1 && !tx.Spent {
//			var utxoUnspentRecord = &data.UtxoUnspentRecord{
//				Uid:       uid,
//				Hash:      tx.TxHash,
//				N:         tx.TxOutputN,
//				ChainName: chainName,
//				Address:   address,
//				Script:    script,
//				Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
//				Amount:    strconv.Itoa(tx.Value),
//				UpdatedAt: time.Now().Unix(),
//			}
//			log.Info(address, zap.Any("插入utxo对象", utxoUnspentRecord))
//			r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
//			if err != nil {
//				log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
//			}
//		}
//	}
//
//	//查询pending的UTXO，与已有的UTXO取差值，差值为已花费的UTXO
//	userUTXOs := make([]string, len(re.Txrefs))
//	for i, utxo := range re.Txrefs {
//		userUTXOs[i] = fmt.Sprintf("%s#%d", utxo.TxHash, utxo.TxOutputN)
//	}
//
//	pendingUTXOs, err := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{IsUnspent: strconv.Itoa(data.UtxoStatusPending), Address: address})
//	for _, utxo := range pendingUTXOs {
//		u := fmt.Sprintf("%s#%d", utxo.Hash, utxo.N)
//		if !slices.Contains(userUTXOs, u) {
//			utxo.Unspent = data.UtxoStatusSpent
//			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxo)
//		}
//	}
//}

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
	var decimals int32
	var symbol string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		decimals = platInfo.Decimal
		symbol = platInfo.NativeCurrency
	} else {
		return
	}

	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

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

		toUserAssetKey := chainName + record.ToAddress
		if toUserAsset, ok := userAssetMap[toUserAssetKey]; !ok {
			toUserAsset, err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, symbol, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				toUserAsset, err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, symbol, now)
			}
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，查询用户资产失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
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

func doHandleUserAsset(chainName string, client Client, uid string, address string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetBalance(address)
	})
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return nil, err
	}
	balance := result.(string)

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
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var decimals int32
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		decimals = platInfo.Decimal
	}

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS {
			continue
		}

		var userAssetStatistic = biz.UserAssetStatistic{
			ChainName:    chainName,
			FromUid:      record.FromUid,
			ToUid:        record.ToUid,
			Amount:       record.Amount,
			TokenAddress: "",
			Decimals:     decimals,
		}
		userAssetStatisticList = append(userAssetStatisticList, userAssetStatistic)
	}
	biz.HandleUserAssetStatistic(chainName, userAssetStatisticList)
}

func HandleTransactionCount(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
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
		if record.Status != biz.SUCCESS {
			continue
		}
		if record.FromAddress == "" || record.ToAddress == "" || (record.FromUid == "" && record.ToUid == "") {
			continue
		}

		var transactionInfo = biz.TransactionInfo{
			ChainName:       chainName,
			FromAddress:     record.FromAddress,
			ToAddress:       record.ToAddress,
			TransactionType: biz.NATIVE,
			TransactionHash: record.TransactionHash,
		}
		transactionInfoList = append(transactionInfoList, transactionInfo)
	}
	biz.HandleTransactionCount(chainName, transactionInfoList)
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}
