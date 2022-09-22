package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

func DappApproveFilter(chainName string, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("DappApproveFilter error, chainName:"+chainName, e)
			} else {
				log.Errore("DappApproveFilter panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理Dapp授权失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	txhashMap := make(map[string]*data.EvmTransactionRecord)
	txhashEventLogMap := make(map[string][]*data.EvmTransactionRecord)

	for _, record := range txRecords {
		if record.TransactionType == "approve" {
			if record.ContractAddress == "0xa042fb0e60125A4022670014AC121931e7501Af4" && record.ToAddress == "0xED7d5F38C79115ca12fe6C0041abb22F0A06C300" {
				log.Info("heco:", zap.Any("zyd", record.Id), zap.Any("zyd", record.Amount))
			}

			dar := &data.DappApproveRecord{
				Uid:        record.FromUid,
				LastTxhash: record.TransactionHash,
				ChainName:  chainName,
				Address:    record.FromAddress,
				Token:      record.ContractAddress,
				ToAddress:  record.ToAddress,
				TxTime:     record.TxTime,
			}

			paseJson := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(record.ParseData), &paseJson); jsonErr == nil {
				tokenMap := paseJson["token"]
				if tokenMap != nil {
					ret := tokenMap.(map[string]interface{})
					if ret != nil {
						am := ret["amount"].(string)
						sy := ret["symbol"].(string)
						if _, ok := ret["decimals"].(float64); ok {
							decimal := ret["decimals"].(float64)
							deciamlStr := strconv.FormatFloat(decimal, 'f', 0, 64)
							ds, _ := strconv.Atoi(deciamlStr)
							dar.Decimals = int64(ds)
						}
						dar.Amount = am
						dar.Original = am
						dar.Symbol = sy
						dar.Original = am
					}
				}

			}
			data.DappApproveRecordRepoClient.SaveOrUpdate(nil, dar)
			continue
		}

		if record.TransactionType == "contract" {
			txhashMap[record.TransactionHash] = record
			continue
		}

		if record.TransactionType == "eventLog" {
			rets := strings.Split(record.TransactionHash, "#")
			records := txhashEventLogMap[rets[0]]
			records = append(records, record)
			txhashEventLogMap[rets[0]] = records
			continue
		}
	}
	//更新敞口
	var dars []*data.DappApproveRecord
	for k, v := range txhashMap {
		eventLogs := txhashEventLogMap[k]
		for _, log_ := range eventLogs {
			dar := &data.DappApproveRecord{}
			dar.Address = v.FromAddress
			dar.ToAddress = v.ToAddress
			dar.Token = log_.ContractAddress
			dar.LastTxhash = v.TransactionHash
			dar.TxTime = v.TxTime
			paseJson := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(log_.ParseData), &paseJson); jsonErr == nil {
				tokenMap := paseJson["token"]
				ret := tokenMap.(map[string]interface{})
				dar.Amount = ret["amount"].(string)
			}
			dars = append(dars, dar)
		}
	}
	data.DappApproveRecordRepoClient.UpdateAmout(nil, dars)
}