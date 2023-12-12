package bitcoin

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin/btc"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/exp/slices"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

var urlMap = map[string]string{
	"DOGE": "http://haotech:jHoNTnHnZZY6pXuiUWoUwZKC@47.244.138.206:22555",
	"BTC":  "http://haotech:phzxiTvtjqHikHTBTnTthqg3@47.244.138.206:8332",
	"LTC":  "http://haotech:BFHGDCQHbaTZBvHJER4fyHy@47.75.184.192:9332",
}
var btcUrls = []string{
	"https://zpka_a96686455d374da6a418f65eaca8a0a5_1652e9a7@svc.blockdaemon.com/universal/v1/",
	"https://zpka_2991d45e50054722ba547e54e739a7e8_41cff525@svc.blockdaemon.com/universal/v1/",
	"https://zpka_630c27c7492847b18b0b21aa346fc0ab_011359a9@svc.blockdaemon.com/universal/v1/",
	"https://zpka_8976bc36b6c84ef6b493460b1dc4a8ce_4e56d164@svc.blockdaemon.com/universal/v1/",
	"https://zpka_ea8d8727c06349e6b93d939d328e0e04_4085b24f@svc.blockdaemon.com/universal/v1/",
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
		var flag string
		if chainName == "BTC" {
			flag = "/bitcoin/mainnet/"
		} else if chainName == "LTC" {
			flag = "/litecoin/mainnet/"
		} else if chainName == "DOGE" {
			flag = "/dogecoin/mainnet/"
		} else {
			flag = ""
		}
		from := record.FromAddress
		fromUid := record.FromUid
		to := record.ToAddress
		toUid := record.ToUid

		//判断 是否是 本站用户
		if fromUid != "" {
			f, s, b := HandleUTXOAsset(chainName, from, record.TransactionHash, flag, fromUid)
			if !f {
				HandleUTXOAssetByBlockcypher(chainName, from, fromUid, s, b)
			}
		}

		if toUid != "" {
			f, s, b := HandleUTXOAsset(chainName, to, record.TransactionHash, flag, toUid)
			if !f {
				HandleUTXOAssetByBlockcypher(chainName, to, toUid, s, b)
			}
		}

		//********** 自建节点 测试环境不可以用****************** 切换成 公共节点
		/*ret := record.TransactionHash
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
		*/
		//*********btc节点不同, 其余链节点可用client*****************
	}
}

func HandleUTXOAsset(chainName string, address string, transactionHash string, flag string, uid string) (bool, string, string) {
	script := ""
	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetBalance(address)
	})

	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，update utxo query balance error", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，update utxo query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("txHash", transactionHash), zap.Any("error", err))
		return false, script, ""
	}
	balance := result.(string)

	//判断总金额 与 balance是否 一至 不一致 则再去差unspentutxo 并 报警
	//删除原来 记录， 更新 未花费记录
	list, err := btc.GetUnspentUtxo(btcUrls[0]+flag, address)
	for i := 0; i < len(btcUrls) && err != nil; i++ {
		list, err = btc.GetUnspentUtxo(btcUrls[i]+flag, address)
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，query utxo balance error", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，update utxo query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("txHash", transactionHash), zap.Any("error", err))
		return false, script, balance
	}
	utxoBalance := 0
	for _, u := range list {
		script = u.Mined.Meta.Script
		utxoBalance = utxoBalance + u.Value
	}
	ub := utils.StringDecimals(strconv.Itoa(utxoBalance), 8)

	checkFlag := ub != balance
	log.Info("======666=====", zap.Any("ub", ub), zap.Any("balance", balance))

	if checkFlag {
		retryList, retryErr := btc.GetUnspentUtxo(btcUrls[0]+flag, address)
		for i := 0; i < len(btcUrls) && retryErr != nil; i++ {
			retryList, retryErr = btc.GetUnspentUtxo(btcUrls[i]+flag, address)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，retry query utxo balance error", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户UTXO，retry update utxo query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("txHash", transactionHash), zap.Any("error", err))
			return false, script, balance
		}
		retryUtxoBalance := 0
		for _, u := range retryList {
			retryUtxoBalance = retryUtxoBalance + u.Value
		}
		ru := utils.StringDecimals(strconv.Itoa(retryUtxoBalance), 8)

		checkFlag = ru != balance

	}
	if checkFlag {
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，query utxo balance not equal utxo value error", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，query utxo balance not equal utxo value error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("txHash", transactionHash), zap.Any("error", err))
		return false, script, balance
	}

	//删除所有非pending的UTXO
	ret, err := data.UtxoUnspentRecordRepoClient.DeleteByUid(nil, uid, chainName, address)

	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，删除数据库utxo数据失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，删除数据库utxo数据失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return false, script, balance
	}

	log.Info(address, zap.Any("删除utxo条数", ret))
	if len(list) > 0 {
		for _, d := range list {
			var utxoUnspentRecord = &data.UtxoUnspentRecord{
				Uid:       uid,
				Hash:      d.Mined.TxId,
				N:         d.Mined.Index,
				ChainName: chainName,
				Address:   address,
				Script:    d.Mined.Meta.Script,
				Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
				Amount:    strconv.Itoa(d.Value),
				TxTime:    int64(d.Mined.Date),
				UpdatedAt: time.Now().Unix(),
			}
			//插入所有未花费的UTXO
			r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
			if err != nil {
				log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
			}
		}
	}

	//查询pending的UTXO，与已有的UTXO取差值，差值为已花费的UTXO
	userUTXOs := make([]string, len(list))
	for i, utxo := range list {
		userUTXOs[i] = fmt.Sprintf("%s#%d", utxo.Mined.TxId, utxo.Mined.Index)
	}

	pendingUTXOs, err := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{IsUnspent: strconv.Itoa(data.UtxoStatusPending), Address: address})
	for _, utxo := range pendingUTXOs {
		u := fmt.Sprintf("%s#%d", utxo.Hash, utxo.N)
		if !slices.Contains(userUTXOs, u) {
			utxo.Unspent = data.UtxoStatusSpent
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxo)
		}
	}

	return true, script, balance
}

func HandleUTXOAssetByBlockcypher(chainName, address, uid, script string, balance string) {
	re, err := btc.GetUnspentUtxoByBlockcypher(chainName, address)
	for i := 0; i < 3 && err != nil; i++ {
		re, err = btc.GetUnspentUtxoByBlockcypher(chainName, address)
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，GetUnspentUtxoByBlockcypher error", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，GetUnspentUtxoByBlockcypher error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
	}

	if re.Balance > 0 {
		retryUtxoBalance := 0
		for _, tx := range re.Txrefs {
			//if tx.TxInputN == -1 && !tx.Spent {
			retryUtxoBalance = retryUtxoBalance + tx.Value
			//}
		}
		ru := utils.StringDecimals(strconv.Itoa(retryUtxoBalance), 8)
		log.Info("@@@@@@@@@@@@@@@@@@@", zap.String("utxo balance", ru), zap.String("node balance", balance))
		if ru != balance {
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，GetUnspentUtxoByBlockcypher 余额不准确 error", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户UTXO，GetUnspentUtxoByBlockcypher 余额不准确 error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return
		}

		_, err := data.UtxoUnspentRecordRepoClient.DeleteByUid(nil, uid, chainName, address)
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，删除数据库utxo数据失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户UTXO，删除数据库utxo数据失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return
		}

		for _, tx := range re.Txrefs {
			//if tx.TxInputN == -1 && !tx.Spent {
			var utxoUnspentRecord = &data.UtxoUnspentRecord{
				Uid:       uid,
				Hash:      tx.TxHash,
				N:         tx.TxOutputN,
				ChainName: chainName,
				Address:   address,
				Script:    script,
				Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
				Amount:    strconv.Itoa(tx.Value),
				UpdatedAt: time.Now().Unix(),
			}
			log.Info(address, zap.Any("插入utxo对象", utxoUnspentRecord))
			r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
			if err != nil {
				log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
			}
		}
	}

	//查询pending的UTXO，与已有的UTXO取差值，差值为已花费的UTXO
	userUTXOs := make([]string, len(re.Txrefs))
	for i, utxo := range re.Txrefs {
		userUTXOs[i] = fmt.Sprintf("%s#%d", utxo.TxHash, utxo.TxOutputN)
	}

	pendingUTXOs, err := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{IsUnspent: strconv.Itoa(data.UtxoStatusPending), Address: address})
	for _, utxo := range pendingUTXOs {
		u := fmt.Sprintf("%s#%d", utxo.Hash, utxo.N)
		if !slices.Contains(userUTXOs, u) {
			utxo.Unspent = data.UtxoStatusSpent
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxo)
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
