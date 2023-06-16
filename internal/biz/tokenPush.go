package biz

import (
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
	if chainName == "SeiTEST" {
		return
	}

	if len(userTokenPushList) == 0 {
		return
	}

	var userTokenPushs []UserTokenPush
	var userTokenPushMap = make(map[string]UserTokenPush)
	for _, userTokenPush := range userTokenPushList {
		if userTokenPush.Decimals == 0 && (userTokenPush.Symbol == "" || userTokenPush.Symbol == "Unknown Token") {
			alarmMsg := fmt.Sprintf("请注意：%s链推送用户token信息失败，tokenAddress:%s，symbol:%s", chainName, userTokenPush.TokenAddress, userTokenPush.Symbol)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"推送，推送用户token信息失败", zap.Any("address", userTokenPush.Address), zap.Any("tokenAddress", userTokenPush.TokenAddress), zap.Any("symbol", userTokenPush.Symbol))
			continue
		}

		key := userTokenPush.ChainName + userTokenPush.Address + userTokenPush.TokenAddress
		userTokenPushMap[key] = userTokenPush
	}
	if len(userTokenPushMap) == 0 {
		return
	}
	for _, userTokenPush := range userTokenPushMap {
		userTokenPushs = append(userTokenPushs, userTokenPush)
	}
	userTokenPushList = userTokenPushs

	var chainNameAddressTokenAddressList []*data.AssetRequest
	for _, record := range userTokenPushList {
		chainNameAddressTokenAddressList = append(chainNameAddressTokenAddressList, &data.AssetRequest{
			ChainName:    record.ChainName,
			Address:      record.Address,
			TokenAddress: record.TokenAddress,
		})
	}

	var assetRequest = &data.AssetRequest{
		ChainNameAddressTokenAddressList: chainNameAddressTokenAddressList,
	}
	list, err := data.UserAssetRepoClient.List(nil, assetRequest)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		list, err = data.UserAssetRepoClient.List(nil, assetRequest)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中用户资产数据失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"推送token信息，查询数据库中用户资产数据失败", zap.Any("error", err))
		return
	}
	assetMap := make(map[string]string)
	for _, asset := range list {
		if asset.Decimals == 0 && (asset.Symbol == "" || asset.Symbol == "Unknown Token") {
			continue
		}

		key := asset.ChainName + asset.Address + asset.TokenAddress
		assetMap[key] = ""
	}

	for _, userTokenPush := range userTokenPushList {
		key := userTokenPush.ChainName + userTokenPush.Address + userTokenPush.TokenAddress
		if _, ok := assetMap[key]; !ok {
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
