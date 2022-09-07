package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/subhandle"
	"strconv"
)

var InnerPlatforms []subhandle.Platform

func NewInnerNodeList(c map[string]*conf.PlatInfo) {
	//var c InnerConfig
	//if err := config.GetConfig("innerNodeList").Unmarshal(&c.InnerPlatform); err != nil {
	//	log.Panic("get innerNodeList  error:", zap.Error(err))
	//}

	for _, value := range c {
		platform := GetInnerPlatform(value.Chain, value.Handler, value.Chain, value.RpcURL[0])
		InnerPlatforms = append(InnerPlatforms, platform)
	}
}

func GetInnerPlatform(typ, chain, chainName string, nodeURL string) subhandle.Platform {
	height := -1
	redisHeight, _ := data.RedisClient.Get(data.CHAINNAME + chainName).Result()
	if redisHeight != "" {
		height, _ = strconv.Atoi(redisHeight)
	}

	var nodeURLS []string
	nodeURLS = make([]string, 0)
	nodeURLS = append(nodeURLS, nodeURL)

	switch typ {
	case biz.STC:
		return starcoin.Init(coins.Starcoin().Handle, chain, chainName, nodeURLS, height)
	case biz.EVM:
		return ethereum.Init(coins.Ethereum().Handle, chain, chainName, nodeURLS, height)
	case biz.BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, chain, chainName, nodeURLS, height)
	case biz.TVM:
		return tron.Init(coins.Tron().Handle, chain, chainName, nodeURLS, height)
	case biz.APTOS:
		return aptos.Init(coins.Tron().Handle, chain, chainName, nodeURLS, height)
	}
	return nil
}
