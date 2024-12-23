package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/cosmos"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/kaspa"
	"block-crawling/internal/platform/nervos"
	"block-crawling/internal/platform/polkadot"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/tron"
)

var InnerPlatforms []biz.Platform

type InnerPlatformContainer []biz.Platform

func NewInnerNodeList(bc *conf.Bootstrap, bundle *data.Bundle) InnerPlatformContainer {
	c := bc.InnerNodeList
	//var c InnerConfig
	//if err := config.GetConfig("innerNodeList").Unmarshal(&c.InnerPlatform); err != nil {
	//	log.Panic("get innerNodeList  error:", zap.Error(err))
	//}

	for _, value := range c {
		platform := GetInnerPlatform(value)
		InnerPlatforms = append(InnerPlatforms, platform)
	}
	return InnerPlatforms
}

func GetInnerPlatform(value *conf.PlatInfo) biz.Platform {
	typ, nodeURL := value.Type, value.RpcURL[0]

	var nodeURLS []string
	nodeURLS = make([]string, 0)
	nodeURLS = append(nodeURLS, nodeURL)

	switch typ {
	case biz.STC:
		return starcoin.Init(coins.Starcoin().Handle, value, nodeURLS)
	case biz.EVM:
		return ethereum.Init(coins.Ethereum().Handle, value, nodeURLS)
	case biz.BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, value, nodeURLS)
	case biz.TVM:
		return tron.Init(coins.Tron().Handle, value, nodeURLS)
	case biz.APTOS:
		return aptos.Init(coins.Aptos().Handle, value, nodeURLS)
	case biz.SUI:
		return sui.Init(coins.Sui().Handle, value, nodeURLS)
	case biz.SOLANA:
		return solana.Init(coins.Solana().Handle, value, nodeURLS)
	case biz.NERVOS:
		return nervos.Init(coins.Nervos().Handle, value, nodeURLS)
	case biz.COSMOS:
		return cosmos.Init(coins.Cosmos().Handle, value, nodeURLS)
	case biz.POLKADOT:
		return polkadot.Init(coins.Polkadot().Handle, value, nodeURLS)
	case biz.KASPA:
		return kaspa.Init(coins.Kaspa().Handle, value, nodeURLS)
	}
	return nil
}
