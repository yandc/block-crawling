package biz

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type UserTokenPush struct {
	ChainName    string `json:"chainName"`
	Uid          string `json:"uid"`
	Address      string `json:"address"`
	TokenAddress string `json:"tokenAddress"`
	Decimals     int32  `json:"decimals"`
	Symbol       string `json:"symbol"`
}

func HandleTokenPush(chainName string, userTokenPushList []UserTokenPush) {
	if len(userTokenPushList) == 0 {
		return
	}

	var userTokenPushs []UserTokenPush
	for _, userTokenPush := range userTokenPushList {
		if userTokenPush.Decimals == 0 && (userTokenPush.Symbol == "" || userTokenPush.Symbol == "Unknown Token") {
			alarmMsg := fmt.Sprintf("请注意：%s链推送用户token信息失败，tokenAddress:%s，symbol:%s", chainName, userTokenPush.TokenAddress, userTokenPush.Symbol)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"推送，推送用户token信息失败", zap.Any("address", userTokenPush.Address), zap.Any("tokenAddress", userTokenPush.TokenAddress), zap.Any("symbol", userTokenPush.Symbol))
			continue
		}

		userTokenPushs = append(userTokenPushs, userTokenPush)
	}
	if len(userTokenPushs) == 0 {
		return
	}
	userTokenPushList = userTokenPushs

	var assetRequest = &pb.PageListAssetRequest{
		ChainName: chainName,
	}
	var addressList []string
	for _, record := range userTokenPushList {
		address := record.Address
		addressList = append(addressList, address)
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
		if asset.Decimals == 0 && (asset.Symbol == "" || asset.Symbol == "Unknown Token") {
			continue
		}

		tokenAddressList, ok := assetMap[asset.Address]
		if !ok {
			tokenAddressList = make([]string, 0)
		}
		tokenAddressList = append(tokenAddressList, asset.TokenAddress)
		assetMap[asset.Address] = tokenAddressList
	}

	for _, userTokenPush := range userTokenPushList {
		address := userTokenPush.Address
		tokenAddress := userTokenPush.TokenAddress
		tokenAddressList := assetMap[address]
		var exist bool
		for _, tokenAddr := range tokenAddressList {
			if tokenAddr == tokenAddress {
				exist = true
			}
		}

		if !exist {
			tokenInfo, _ := utils.JsonEncode(userTokenPush)
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
				continue
			}
			log.Info(chainName+"推送token信息，推送token信息到redis中成功", zap.Any("tokenInfo", tokenInfo))
		}
	}
}
