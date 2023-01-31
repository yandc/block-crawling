package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"fmt"
	"strconv"
	"time"
)

const OPENSEA_URL = "https://api.opensea.io/api/v1/events"
const OPENSEA_KEY = "207e09c24d49409ca949578d7e3bde27"

func GetETHNftHistoryByOpenSea(chainName string, contractAddress string, tokenId string) bool {
	var nftRecords []*data.NftRecordHistory
	now := time.Now().Unix()

	var events types.OpenSeaEvents
	var param = make(map[string]string)
	param["token_id"] = tokenId
	param["asset_contract_address"] = contractAddress
	param["event_type"] = "transfer"

	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(OPENSEA_URL, param, map[string]string{"X-API-KEY": OPENSEA_KEY}, &events, &timeoutMS)

	if err != nil {
		// 更新用户资产出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链nft流转记录获取失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		return false
	}

	for _, event := range events.AssetEvents {
		fromAddress := event.FromAccount.Address
		toAddress := event.ToAccount.Address
		var fromUid, toUid string

		userMeta, err := pCommon.MatchUser(fromAddress, toAddress, chainName)
		if err == nil {
			fromUid = userMeta.FromUid
			toUid = userMeta.ToUid
		}
		bn, _ := strconv.Atoi(event.Transaction.BlockNumber)
		ft, _ := time.Parse("2006-01-02T15:04:05", event.EventTimestamp)
		tt := ft.Unix() * 1000

		nrh := &data.NftRecordHistory{
			ChainName:       chainName,
			BlockNumber:     bn,
			TransactionHash: event.Transaction.TransactionHash,
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
	//}

	return true
}
