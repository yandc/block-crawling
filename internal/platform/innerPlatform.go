package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
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
		platform := GetInnerPlatform(value)
		InnerPlatforms = append(InnerPlatforms, platform)
	}
}

func GetInnerPlatform(value *conf.PlatInfo) subhandle.Platform {
	typ, chainName, nodeURL := value.Type, value.Chain, value.RpcURL[0]
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
		return starcoin.Init(coins.Starcoin().Handle, value, nodeURLS, height)
	case biz.EVM:
		return ethereum.Init(coins.Ethereum().Handle, value, nodeURLS, height)
	case biz.BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, value, nodeURLS, height)
	case biz.TVM:
		return tron.Init(coins.Tron().Handle, value, nodeURLS, height)
	case biz.APTOS:
		return aptos.Init(coins.Aptos().Handle, value, nodeURLS, height)
	case biz.SUI:
		return sui.Init(coins.Sui().Handle, value, nodeURLS, height)
	case biz.SOLANA:
		return solana.Init(coins.Solana().Handle, value, nodeURLS, height)
	}
	return nil
}
