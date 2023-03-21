package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var pageSize = 60
var timeout = 10_000 * time.Millisecond

func GetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("GetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	switch chainName {
	case "Osmosis":
		err = OsmosisGetTxByAddress(chainName, address, urls)
	case "Solana":
		err = SolanaGetTxByAddress(chainName, address, urls)
	}

	return
}

func OsmosisGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("OsmosisGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("OsmosisGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

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
			break
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

type SolanaBrowserResponse struct {
	Succcess bool                `json:"succcess"`
	Data     []SolanaBrowserInfo `json:"data"`
}
type SolanaBrowserInfo struct {
	BlockTime         int      `json:"blockTime"`
	Slot              int      `json:"slot"`
	TxHash            string   `json:"txHash"`
	Fee               int      `json:"fee"`
	Status            string   `json:"status"`
	Lamport           int      `json:"lamport"`
	Signer            []string `json:"signer"`
	ParsedInstruction []struct {
		ProgramId string `json:"programId"`
		Type      string `json:"type"`
	} `json:"parsedInstruction"`
}

func SolanaGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("SolanaGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("SolanaGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var pageSize = 10
	var beforeTxHash string
	url := urls[0]
	url = url + "/account/transaction?address=" + address

	var chainRecords []SolanaBrowserInfo
	for len(chainRecords) < 60 {
		var out SolanaBrowserResponse
		reqUrl := url + "&before=" + beforeTxHash

		err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out.Data)
		if dataLen > 0 {
			chainRecords = append(chainRecords, out.Data...)
		}
		if dataLen < pageSize {
			break
		}
		beforeTxHash = out.Data[dataLen-1].TxHash
	}

	var transactionHashList []string
	for _, txRecord := range chainRecords {
		txHash := txRecord.TxHash
		transactionHashList = append(transactionHashList, txHash)
	}
	transactionRequest := &data.TransactionRequest{
		TransactionHashList: transactionHashList,
		Nonce:               -1,
	}
	dbRecords, err := data.SolTransactionRecordRepoClient.List(nil, GetTableName(chainName), transactionRequest)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"链查询数据库交易记录失败", zap.Any("address", address), zap.Any("transactionRequest", transactionRequest), zap.Any("error", err))
		return err
	}
	dbRecordMap := make(map[string]*data.SolTransactionRecord)
	for _, record := range dbRecords {
		dbRecordMap[record.TransactionHash] = record
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TxHash
		if _, ok := dbRecordMap[txHash]; !ok {
			solRecord := &data.SolTransactionRecord{
				TransactionHash: txHash,
				Status:          PENDING,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			solTransactionRecordList = append(solTransactionRecordList, solRecord)
		}
	}

	if len(solTransactionRecordList) > 0 {
		_, err = data.SolTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), solTransactionRecordList)
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
