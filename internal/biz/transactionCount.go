package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type TransactionInfo struct {
	ChainName       string `json:"chainName"`
	FromAddress     string `json:"fromAddress"`
	ToAddress       string `json:"toAddress"`
	TransactionType string `json:"transactionType"`
	TransactionHash string `json:"transactionHash"`
}

func HandleTransactionCount(chainName string, txRecords []TransactionInfo) {
	tm := time.Now()
	nowTime := tm.Unix()

	var transactionCountMap = make(map[string]*data.TransactionCount)
	var transactionCountList []*data.TransactionCount
	for _, record := range txRecords {
		key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
		if statistic, ok := transactionCountMap[key]; ok {
			statistic.TransactionQuantity += 1
		} else {
			var transactionCount = &data.TransactionCount{
				ChainName:           chainName,
				FromAddress:         record.FromAddress,
				ToAddress:           record.ToAddress,
				TransactionType:     record.TransactionType,
				TransactionQuantity: 1,
				TransactionHash:     record.TransactionHash,
				CreatedAt:           nowTime,
				UpdatedAt:           nowTime,
			}

			transactionCountMap[key] = transactionCount
		}
	}

	if len(transactionCountMap) == 0 {
		return
	}
	for _, transactionCount := range transactionCountMap {
		transactionCountList = append(transactionCountList, transactionCount)
	}
	_, err := data.TransactionCountRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionCountList, PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.TransactionCountRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionCountList, PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链统计交易次数，将数据插入到数据库中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计交易次数，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
	}
}
