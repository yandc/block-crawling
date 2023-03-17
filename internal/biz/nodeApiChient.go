package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var pageSize = 60
var timeout = 10_000 * time.Millisecond

func GetTxByAddress(chainName string, address string, urls []string) (err error) {
	switch chainName {
	case "Osmosis":
		err = OsmosisGetTxByAddress(chainName, address, urls)
	}

	return
}

func OsmosisGetTxByAddress(chainName string, address string, urls []string) (err error) {
	var starIndex = 0
	url := urls[0]
	url = url + address + "/txs?"

	var chainRecords []types.OsmsiomBrowserInfo
	for {
		var out []types.OsmsiomBrowserInfo
		reqUrl := url + "limit=" + strconv.Itoa(pageSize) + "&from=" + strconv.Itoa(starIndex)

		err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			return err
		}

		chainRecords = append(chainRecords, out...)
		if len(out) < pageSize {
			break
		}
		starIndex += pageSize
	}

	var transactionHashList []string
	for _, txRecord := range chainRecords {
		txHash := txRecord.Data.Txhash
		transactionHashList = append(transactionHashList, txHash)
	}
	transactionRequest := &data.TransactionRequest{
		TransactionHashList: transactionHashList,
		Nonce:               -1,
	}
	dbRecords, err := data.AtomTransactionRecordRepoClient.List(nil, GetTableName(chainName), transactionRequest)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"链查询数据库交易记录失败", zap.Any("address", address), zap.Any("transactionRequest", transactionRequest), zap.Any("error", err))
		return err
	}
	dbRecordMap := make(map[string]*data.AtomTransactionRecord)
	for _, record := range dbRecords {
		dbRecordMap[record.TransactionHash] = record
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Data.Txhash
		if _, ok := dbRecordMap[txHash]; !ok {
			atomRecord := &data.AtomTransactionRecord{
				TransactionHash: txHash,
				Status:          PENDING,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
		}
	}

	if len(atomTransactionRecordList) > 0 {
		_, err = data.AtomTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), atomTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链插入链上交易记录数据到数据库中失败", zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}
