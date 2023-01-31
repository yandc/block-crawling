package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const BLOCKSPAN_URL = "https://api.blockspan.com/v1/transfers/contract/"
const BLOCKSPAN_KEY = "QEhydRzXWozNUV5r1AgiPmySQSRdi09Y"

func GetETHNftHistoryByBlockspan(chainName string, contractAddress string, tokenId string) bool {
	var nftRecords []*data.NftRecordHistory
	now := time.Now().Unix()

	var events types.Blockspan
	var param = make(map[string]string)
	param["chain"] = "eth-main"
	param["page_size"] = "25"

	url := BLOCKSPAN_URL + contractAddress + "/token/" + tokenId

	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(url, param, map[string]string{"X-API-KEY": BLOCKSPAN_KEY}, &events, &timeoutMS)
	log.Info("YYDS", zap.Any("blockspan", events))
	for i := 0; err != nil && i < 3; i++ {
		err = httpclient.HttpsSignGetForm(url, param, map[string]string{"X-API-KEY": BLOCKSPAN_KEY}, &events, &timeoutMS)
		log.Info("YYDS", zap.Any("blockspan-1", events))
	}

	if err != nil {
		// 更新用户资产出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链nft流转记录获取失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		return false
	}

	for _, event := range events.Results {
		fromAddress := event.FromAddress
		toAddress := event.ToAddress
		var fromUid, toUid string

		userMeta, err := pCommon.MatchUser(fromAddress, toAddress, chainName)
		if err == nil {
			fromUid = userMeta.FromUid
			toUid = userMeta.ToUid
		}

		bn, _ := strconv.Atoi(event.BlockNumber)

		tt := event.BlockTimestamp.Unix() * 1000

		nrh := &data.NftRecordHistory{
			ChainName:       chainName,
			BlockNumber:     bn,
			TransactionHash: event.TransactionHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         fromUid,
			ToUid:           toUid,
			TxTime:          strconv.Itoa(int(tt)),
			Quantity:        event.Quantity,
			ContractAddress: contractAddress,
			TokenId:         tokenId,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		nftRecords = append(nftRecords, nrh)
	}

	//if len(nftRecords) > 0 {
	//	_, err := data.NftRecordHistoryRepoClient.SaveOrUpdate(nil, nftRecords)
	//	if err != nil {
	//		// 更新用户资产出错 接入lark报警
	//		alarmMsg := fmt.Sprintf("请注意：%s链nft流转记录插入数据库失败", chainName)
	//		alarmOpts := biz.WithMsgLevel("FATAL")
	//		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	//	}
	//
	//}
	return true
}
