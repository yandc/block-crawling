package biz

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

func HandleTokenPush(chainName string, userAssetList []*data.UserAsset) {
	var assetRequest = &pb.PageListAssetRequest{
		ChainName: chainName,
	}
	var addressList []string
	var tokenInfoList []string
	for _, record := range userAssetList {
		tokenAddress := record.TokenAddress
		address := record.Address
		uid := record.Uid
		symbol := record.Symbol
		decimals := record.Decimals
		tokenInfo := uid + "," + address + "," + tokenAddress + "," + symbol + "," + strconv.Itoa(int(decimals))
		addressList = append(addressList, address)
		tokenInfoList = append(tokenInfoList, tokenInfo)
	}
	if len(addressList) == 0 {
		return
	}

	assetRequest.AddressList = addressList
	list, _, err := data.UserAssetRepoClient.PageList(nil, assetRequest)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		list, _, err = data.UserAssetRepoClient.PageList(nil, assetRequest)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中用户资产数据失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"推送token信息，查询数据库中用户资产数据失败", zap.Any("error", err))
		return
	}
	assetMap := make(map[string][]string)
	for _, asset := range list {
		tokenAddressList, ok := assetMap[asset.Address]
		if !ok {
			tokenAddressList = make([]string, 0)
			assetMap[asset.Address] = tokenAddressList
		}
		tokenAddressList = append(tokenAddressList, asset.TokenAddress)
	}

	for _, tokenInfo := range tokenInfoList {
		tokens := strings.Split(tokenInfo, ",")
		address := tokens[1]
		tokenAddress := tokens[2]
		tokenAddressList := assetMap[address]
		var exist bool
		for _, tokenAddr := range tokenAddressList {
			if tokenAddr == tokenAddress {
				exist = true
			}
		}

		if !exist {
			tokenInfo = chainName + "," + tokenInfo
			err = data.RedisQueueManager.QueuePublish(&data.QueueSendMessage{
				Topic:     TOKEN_INFO_QUEUE_TOPIC,
				Partition: TOKEN_INFO_QUEUE_PARTITION,
				Body:      tokenInfo,
			})
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				err = data.RedisQueueManager.QueuePublish(&data.QueueSendMessage{
					Topic:     TOKEN_INFO_QUEUE_TOPIC,
					Partition: TOKEN_INFO_QUEUE_PARTITION,
					Body:      tokenInfo,
				})
			}
			if err != nil {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链推送token信息到redis中失败", chainName)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"推送token信息，推送token信息到redis中失败", zap.Any("error", err))
				return
			}
		}
	}
}
