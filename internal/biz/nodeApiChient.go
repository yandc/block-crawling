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

func GetTxByAddress(address string, url string, chainName string) (err error) {
	starIndex := 0
	pageSize := 60
	timeout := 10_000 * time.Millisecond
	url = url + address + "/txs?"

	var result []types.OsmsiomBrowserInfo
	for {
		var out []types.OsmsiomBrowserInfo
		reqUrl := url + "limit=" + strconv.Itoa(pageSize) + "&from=" + strconv.Itoa(starIndex)

		err := httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s 链查询交易交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+" : "+address+"查询交易记录失败", zap.Any("error", err))
			return err
		}

		starIndex++
		result = append(result, out...)
		if len(out) < pageSize {
			break
		}
	}

	//插入pending
	for _, txRecord := range result {
		txHash := txRecord.Data.Txhash
		ret, _ := data.AtomTransactionRecordRepoClient.FindByTxhash(nil, GetTableName(chainName), txHash)
		if ret == nil {
			now := time.Now().Unix()
			atomRecord := &data.AtomTransactionRecord{
				TransactionHash: txHash,
				Status:          PENDING,
				CreatedAt:       now,
				DappData:        "",
				ClientData:      "",
				UpdatedAt:       now,
			}
			data.AtomTransactionRecordRepoClient.Save(nil, GetTableName(chainName), atomRecord)
		}

	}
	return nil
}
