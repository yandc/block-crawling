package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	httpclient2 "block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const NFTGO_URL = "https://data-api.nftgo.io/eth/v1/history/nft/transactions"
const NFTGO_KEY = "c156869d-fac6-4118-a0d3-d0d9af1dbe17"

func GetETHNftHistoryByNftgo(chainName string, contractAddress string, tokenId string, client Client) bool {

	var nftRecords []*data.NftRecordHistory
	now := time.Now().Unix()

	var events types.NFtgoEvents
	var param = make(map[string]string)
	param["token_id"] = tokenId
	param["contract_address"] = contractAddress
	param["event_type"] = "all"

	param["scroll"] = strconv.Itoa(int(now))

	err := httpclient2.HttpsOpenSeaGetForm(NFTGO_URL, param, NFTGO_KEY, &events)
	log.Info("YYDS",zap.Any("nftgo",events))
	for i := 0; err != nil && i < 3; i++ {
		err = httpclient2.HttpsOpenSeaGetForm(NFTGO_URL, param, NFTGO_KEY, &events)
		log.Info("YYDS",zap.Any("nftgo-1",events))

	}

	if err != nil {
		// 更新用户资产出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链nft流转记录获取失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		return false
	}

	for _, event := range events.Transactions {


		fromAddress := event.Sender.Address
		toAddress := event.Receiver.Address
		var fromUid, toUid string

		userMeta, err := pCommon.MatchUser(fromAddress, toAddress, chainName)
		if err == nil {
			fromUid = userMeta.FromUid
			toUid = userMeta.ToUid
		}
		txInfo,err := client.GetTransactionReceipt(context.Background(), common.HexToHash(event.TxHash))
		for i := 0; err != nil && i < 3; i++ {
			txInfo,err = client.GetTransactionReceipt(context.Background(), common.HexToHash(event.TxHash))
		}

		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链nft流转记录获取失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return false
		}

		bn, _ := strconv.Atoi(txInfo.BlockNumber)
		tt := strconv.Itoa(int(event.Time))
		//txTime :=event.Time/1000
		//t := time.Unix(txTime,0).Format("2006-01-02 15:04:05")
		nrh := &data.NftRecordHistory{
			ChainName:       chainName,
			BlockNumber:     bn,
			TransactionHash: event.TxHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         fromUid,
			ToUid:           toUid,
			TxTime:          tt,
			Quantity:        strconv.Itoa(event.Quantity),
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