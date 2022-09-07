package dogecoin

//import (
//	"block-crawling/internal/biz"
//	"block-crawling/internal/data"
//	"block-crawling/internal/log"
//	"fmt"
//	"github.com/shopspring/decimal"
//	"go.uber.org/zap"
//	"time"
//)
//
//func HandleRecord(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
//	handleUserAsset(chainName, client, txRecords)
//}
//
//
//func handleUserAsset(chainName string, client Client, txRecords []*data.BtcTransactionRecord) {
//	now := time.Now().Unix()
//	for _, record := range txRecords {
//		var decimals int32 = 8
//		err := doHandleUserAsset(chainName, client, record.FromUid, record.FromAddress, decimals, chainName, now)
//		for i := 0; i < 10 && err != nil; i++ {
//			time.Sleep(time.Duration(i*5) * time.Second)
//			err = doHandleUserAsset(chainName, client, record.FromUid, record.FromAddress, decimals, chainName, now)
//		}
//		if err != nil {
//			// 更新用户资产出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s更新用户资产失败", chainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			log.Error(chainName+"更新用户资产失败", zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
//			return
//		}
//		err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, chainName, now)
//		for i := 0; i < 10 && err != nil; i++ {
//			time.Sleep(time.Duration(i*5) * time.Second)
//			err = doHandleUserAsset(chainName, client, record.ToUid, record.ToAddress, decimals, chainName, now)
//		}
//		if err != nil {
//			// 更新用户资产出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s更新用户资产失败", chainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			log.Error(chainName+"更新用户资产失败", zap.Any("fromAddress", record.FromAddress), zap.Any("error", err))
//			return
//		}
//	}
//}
//
//func doHandleUserAsset(chainName string, client Client, uid string, address string, decimals int32, symbol string, nowTime int64) error {
//
//	var balance decimal.Decimal
//	var balances string
//	var err error
//	balances, err = client.GetBalance(address)
//	if err != nil {
//		log.Error("query balance error", zap.Any("address", address), zap.Any("error", err))
//		return err
//	}
//	balance, err = decimal.NewFromString(balances)
//	if err != nil {
//		log.Error("format balance error", zap.Any("balance", balances), zap.Any("error", err))
//		return err
//	}
//
//	var userAsset = &data.UserAsset{
//		ChainName: chainName,
//		Uid:       uid,
//		Address:   address,
//		Amount:    balance,
//		Decimals:  decimals,
//		Symbol:    symbol,
//		CreatedAt: nowTime,
//		UpdatedAt: nowTime,
//	}
//	_, err = data.UserAssetRepoClient.SaveOrUpdate(nil, userAsset)
//	if err != nil {
//		log.Errore("insert or update balance error", err)
//		return err
//	}
//	return nil
//}