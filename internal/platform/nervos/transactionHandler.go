package nervos

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"

	"gitlab.bixin.com/mili/node-driver/chain"

	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/address"
	"github.com/nervosnetwork/ckb-sdk-go/types"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
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
		//handleUserAsset(chainName, txRecords)
		HandleUTXO(chainName, client, txRecords)
	}()
	go handleUserStatistic(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
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
		//handleUserAsset(chainName, txRecords)
		HandleUTXO(chainName, client, txRecords)
	}()
	go HandleTransactionCount(chainName, client, txRecords)
}

func handleUserAsset(chainName string, userAssetList []*data.UserAsset, addresses []string) {
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
	userAssetMap := make(map[string]*data.UserAsset)

	for _, add := range addresses {
		_, err := data.UserAssetRepoClient.UpdateZeroByAddress(nil, add)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.UserAssetRepoClient.UpdateZeroByAddress(nil, add)
		}
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，清空用户资产失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户资产，清空用户资产失败", zap.Any("chainName", chainName), zap.Any("error", err))
		}
	}
	log.Info("更新用户资产，DDDYYY", zap.Any("init", userAssetList))

	for _, userAsset := range userAssetList {
		if userAsset == nil {
			continue
		}

		var decimals int64
		var symbol string
		//代币 累加
		userAssetKey := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress
		if userAsset.TokenAddress != "" {
			tokenInfo, err := biz.GetTokenInfoRetryAlert(nil, chainName, userAsset.TokenAddress)
			if err != nil {
				log.Error("更新用户资产，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("error", err))
				continue
			}
			decimals = tokenInfo.Decimals
			symbol = tokenInfo.Symbol
		} else {
			if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
				decimals = int64(platInfo.Decimal)
				symbol = platInfo.NativeCurrency
			} else {
				continue
			}
		}

		userAsset.Decimals = int32(decimals)
		userAsset.Symbol = symbol
		userAsset.Balance = utils.StringDecimals(userAsset.Balance, int(decimals))
		userAsset.CreatedAt = now
		userAsset.UpdatedAt = now
		oldUserAsset, ok := userAssetMap[userAssetKey]
		if ok {
			userAssetBalance, _ := decimal.NewFromString(userAsset.Balance)
			oldUserAssetBalance, _ := decimal.NewFromString(oldUserAsset.Balance)
			oldUserAsset.Balance = userAssetBalance.Add(oldUserAssetBalance).String()
		} else {
			userAssetMap[userAssetKey] = userAsset
		}
		log.Info("更新用户资产，DDDYYY", zap.Any("chainName", chainName), zap.Any("map", userAssetMap), zap.Any("len", len(userAssetMap)))

		if len(userAssetMap) == 0 {
			return
		}

		for _, u := range userAssetMap {
			uidType, _ := biz.GetUidTypeCode(userAsset.Address)
			userAsset.UidType = uidType
			_, err := data.UserAssetRepoClient.SaveOrUpdate(nil, u)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，将数据插入到数据库中失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
			}
		}
	}
}

func handleUserStatistic(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
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

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS {
			continue
		}

		decimals, _, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额，解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("统计交易金额，解析parseData失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
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

func HandleTransactionCount(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
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
		if record.TransactionType != biz.NATIVE && record.TransactionType != biz.TRANSFER && record.TransactionType != biz.TRANSFERNFT {
			transactionType = biz.CONTRACT
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

func handleTokenPush(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
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

		decimals, symbol, err := biz.GetDecimalsSymbol(chainName, record.ParseData)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息，解析parseData失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("推送token信息，解析parseData失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("parseData", record.ParseData), zap.Any("error", err))
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

func HandleUTXO(chainName string, client Client, txRecords []*data.CkbTransactionRecord) {
	now := time.Now().Unix()
	var addressList []string

	for _, record := range txRecords {
		ret := strings.Split(record.TransactionHash, "#")[0]

		result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
			return client.GetUTXOByHash(ret)
		})
		if err != nil {
			log.Error("更新用户UTXO，调用GetCellByHash失败！", zap.Any("chainName", chainName), zap.Any("error", err))
			continue
		}
		tx := result.(*types.TransactionWithStatus)

		if record.Status == "success" {
			log.Info("更新用户UTXO，zydghg", zap.Any("chainName", chainName), zap.Any(record.TransactionHash, tx))
			if record.FromUid != "" {
				log.Info("更新用户UTXO，zydghg1", zap.Any("chainName", chainName), zap.Any(record.TransactionHash, tx))

				// 标记成 已用
				cellInputs := tx.Transaction.Inputs
				for _, ci := range cellInputs {
					if ci.PreviousOutput.TxHash.Hex() == zeroHex {
						continue
					}
					index := ci.PreviousOutput.Index
					preTxHash := ci.PreviousOutput.TxHash
					//preTx, err := client.GetUTXOByHash(preTxHash.String())
					//if err != nil {
					//	log.Error(chainName+"调用GetCellByHash失败！", zap.Any("error", err))
					//	continue
					//}
					//output := preTx.Transaction.Outputs[index]
					var nervosCellRecord = &data.NervosCellRecord{
						Uid: record.FromUid,
						//Capacity:           fmt.Sprint(output.Capacity),
						Index:              int(index),
						TransactionHash:    preTxHash.String(),
						UseTransactionHash: ret,
						Address:            record.FromAddress,
						//Data:               types.BytesToHash(preTx.Transaction.OutputsData[index]).String(),
						Status:    "2", // 1 未花费 2 已花费 3 pending 4 cancel 由pending触发
						CreatedAt: now,
						UpdatedAt: now,
					}
					//if client.isTokenTransfer(output.Type) {
					//	nervosCellRecord.ContractAddress = types.BytesToHash(output.Lock.Args).String()
					//}
					//
					//if output.Lock != nil {
					//	nervosCellRecord.LockCodeHash = output.Lock.CodeHash.String()
					//	nervosCellRecord.LockHashType = string(output.Lock.HashType)
					//	nervosCellRecord.LockArgs = types.BytesToHash(output.Lock.Args).String()
					//}
					//if output.Type != nil {
					//	nervosCellRecord.TypeCodeHash = output.Type.CodeHash.String()
					//	nervosCellRecord.TypeHashType = string(output.Type.HashType)
					//	nervosCellRecord.TypeArgs = types.BytesToHash(output.Type.Args).String()
					//}
					r, _ := data.NervosCellRecordRepoClient.SaveOrUpdate(nil, nervosCellRecord)
					log.Info("更新用户UTXO，zydghg2", zap.Any("chainName", chainName), zap.Any(record.TransactionHash, r))
				}
				if record.FromAddress != "" {
					addressList = append(addressList, record.FromAddress)
				}
			}

			// 标记成 未花费
			for index, toTxOutput := range tx.Transaction.Outputs {
				//判断地址是否是 用户中心
				toAddr, err := address.ConvertScriptToAddress(client.mode, toTxOutput.Lock)
				if err != nil {
					log.Error("更新用户UTXO，解析to地址失败", zap.Any("chainName", chainName), zap.Any("toAddr", toAddr))
					continue
				}
				var toAddrUid = ""
				userMeta, err := pCommon.MatchUser(toAddr, "", chainName)
				if err == nil {
					toAddrUid = userMeta.FromUid
				}
				if toAddrUid == "" {
					continue
				}

				amount := fmt.Sprint(toTxOutput.Capacity)

				var nervosCellRecord = &data.NervosCellRecord{
					Uid:             toAddrUid,
					Capacity:        amount,
					Index:           index,
					TransactionHash: tx.Transaction.Hash.String(),
					Address:         toAddr,
					Data:            types.BytesToHash(tx.Transaction.OutputsData[index]).String(),
					Status:          "1", // 1 未花费 2 已花费 3 pending 4 cancel 由pending触发
					CreatedAt:       now,
					UpdatedAt:       now,
				}
				if client.isTokenTransfer(toTxOutput.Type) {
					nervosCellRecord.ContractAddress = types.BytesToHash(toTxOutput.Type.Args).Hex()
				}

				if toTxOutput.Lock != nil {
					nervosCellRecord.LockCodeHash = toTxOutput.Lock.CodeHash.String()
					nervosCellRecord.LockHashType = string(toTxOutput.Lock.HashType)
					nervosCellRecord.LockArgs = types.BytesToHash(toTxOutput.Lock.Args).String()
				}
				if toTxOutput.Type != nil {
					nervosCellRecord.TypeCodeHash = toTxOutput.Type.CodeHash.String()
					nervosCellRecord.TypeHashType = string(toTxOutput.Type.HashType)
					nervosCellRecord.TypeArgs = types.BytesToHash(toTxOutput.Type.Args).String()
				}
				data.NervosCellRecordRepoClient.SaveOrUpdate(nil, nervosCellRecord)
				if toAddr != "" {
					addressList = append(addressList, toAddr)
				}
			}
		}

		if record.Status == biz.FAIL || record.Status == biz.DROPPED_REPLACED || record.Status == biz.DROPPED {
			ret, _ := data.NervosCellRecordRepoClient.UpdateStatusByUseTransactionHash(nil, record.TransactionHash, "1")
			if ret == 0 {
				log.Error("更新用户UTXO，更新状态失败", zap.Any("chainName", chainName), zap.String("txHash", record.TransactionHash))
			}
		}
	}

	//更新资产
	tempList := utils.RemoveDuplicate(addressList)
	var userAssets []*data.UserAsset
	log.Info("更新用户UTXO，fffff", zap.Any("chainName", chainName), zap.Any("地址", tempList))
	for _, address := range tempList {
		req := &pb.UnspentReq{}
		req.Address = address
		req.IsUnspent = "1"
		cells, err := data.NervosCellRecordRepoClient.FindByCondition(nil, req)
		log.Info("更新用户UTXO，fffff", zap.Any("chainName", chainName), zap.Any(address, cells))

		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户UTXO，查询Cell失败，address：%s", chainName, address)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户UTXO，查询Cell失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			continue
		}
		if len(cells) > 0 {
			for _, c := range cells {
				ua := &data.UserAsset{
					ChainName:    chainName,
					Uid:          c.Uid,
					Address:      c.Address,
					TokenAddress: c.ContractAddress,
					Balance:      c.Capacity,
				}
				userAssets = append(userAssets, ua)
			}
		}
	}
	log.Info("更新用户UTXO，fffff", zap.Any("chainName", chainName), zap.Any(chainName, userAssets))

	go handleUserAsset(chainName, userAssets, tempList)
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}
