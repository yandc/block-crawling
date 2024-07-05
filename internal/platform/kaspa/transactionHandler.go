package kaspa

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
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

	go HandleUserAsset(chainName, client, txRecords)
	go HandleUserStatistic(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
	go HandleUserUtxo(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
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

	go HandleUserAsset(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
	go HandleUserUtxo(chainName, client, txRecords)
}

func HandleUserAsset(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
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
				log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
					zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
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
				log.Error("更新用户资产，查询用户资产失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
					zap.Any("toAddress", record.ToAddress), zap.Any("error", err))
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
		ChainName:     chainName,
		Uid:           uid,
		Address:       address,
		Balance:       balance,
		Decimals:      decimals,
		Symbol:        symbol,
		CreatedAt:     nowTime,
		IsSyncToChain: true,
		SyncToChainTs: nowTime,
		UpdatedAt:     nowTime,
	}
	return userAsset, nil
}

func HandleUserStatistic(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
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

	var decimals int32
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		decimals = platInfo.Decimal
	}

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
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

func HandleTransactionCount(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
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

func HandleUserUtxo(chainName string, client Client, txRecords []*data.KasTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUTXO error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUTXO panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新UTXO失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	now := time.Now().Unix()
	var utxoRecords []*data.UtxoUnspentRecord
	var deleteAddressList []string
	deleteUtxoRecordMap := make(map[string]*data.UtxoUnspentRecord)
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		fromUtxoRecords, err := doHandleUserUtxo(chainName, client, record.FromUid, record.FromAddress, record.TxTime, now)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			fromUtxoRecords, err = doHandleUserUtxo(chainName, client, record.FromUid, record.FromAddress, record.TxTime, now)
		}
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，请求节点查询用户UTXO失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户UTXO，请求节点查询用户UTXO失败", zap.Any("chainName", chainName), zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
			return
		}
		if fromUtxoRecords != nil {
			utxoRecords = append(utxoRecords, fromUtxoRecords...)
		}

		fromUserAssetKey := chainName + record.FromAddress
		if _, ok := deleteUtxoRecordMap[fromUserAssetKey]; !ok {
			deleteUtxoRecordMap[fromUserAssetKey] = &data.UtxoUnspentRecord{
				ChainName: chainName,
				Address:   record.FromAddress,
				Unspent:   data.UtxoStatusPending,
			}
		}

		if record.FromAddress != record.ToAddress { //from == to时，from 在前面更新过，这里直接跳过
			toUtxoRecords, err := doHandleUserUtxo(chainName, client, record.ToUid, record.ToAddress, record.TxTime, now)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				toUtxoRecords, err = doHandleUserUtxo(chainName, client, record.ToUid, record.ToAddress, record.TxTime, now)
			}
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，请求节点查询用户UTXO失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户UTXO，请求节点查询用户UTXO失败", zap.Any("chainName", chainName), zap.Any("toAddress", record.ToAddress), zap.Any("error", err))
				return
			}
			if toUtxoRecords != nil {
				utxoRecords = append(utxoRecords, toUtxoRecords...)
			}
		}

		toUserAssetKey := chainName + record.ToAddress
		if _, ok := deleteUtxoRecordMap[toUserAssetKey]; !ok {
			deleteUtxoRecordMap[toUserAssetKey] = &data.UtxoUnspentRecord{
				ChainName: chainName,
				Address:   record.ToAddress,
				Unspent:   data.UtxoStatusPending,
			}
		}
	}

	if len(deleteUtxoRecordMap) == 0 {
		return
	}
	for _, deleteUtxoRecord := range deleteUtxoRecordMap {
		deleteAddressList = append(deleteAddressList, deleteUtxoRecord.Address)
	}
	deleteUserUtxos := &data.UserUtxo{
		ChainName:       chainName,
		AddressList:     deleteAddressList,
		UnspentNotEqual: data.UtxoStatusPending,
	}
	_, err := data.UtxoUnspentRecordRepoClient.Delete(nil, deleteUserUtxos)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UtxoUnspentRecordRepoClient.Delete(nil, deleteUserUtxos)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，删除数据库中数据失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，删除数据库中数据失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}

	_, err = data.UtxoUnspentRecordRepoClient.PageBatchSaveOrUpdate(nil, utxoRecords, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UtxoUnspentRecordRepoClient.PageBatchSaveOrUpdate(nil, utxoRecords, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户UTXO，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserUtxo(chainName string, client Client, uid string, address string, txTime, nowTime int64) ([]*data.UtxoUnspentRecord, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	var utxoRecords []*data.UtxoUnspentRecord

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetUtxo(address)
	})
	if err != nil {
		log.Error("query utxo error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return nil, err
	}
	utxoList := result.([]*KaspaUtxoResp)

	for _, utxo := range utxoList {
		var utxoUnspentRecord = &data.UtxoUnspentRecord{
			ChainName: chainName,
			Uid:       uid,
			Address:   address,
			Hash:      utxo.Outpoint.TransactionId,
			N:         utxo.Outpoint.Index,
			Script:    utxo.UtxoEntry.ScriptPublicKey.ScriptPublicKey,
			Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
			Amount:    utxo.UtxoEntry.Amount,
			TxTime:    txTime,
			UpdatedAt: nowTime,
		}
		utxoRecords = append(utxoRecords, utxoUnspentRecord)
	}
	return utxoRecords, nil
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}
