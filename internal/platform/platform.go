package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/subhandle"
	"strconv"
	"strings"
)

var Platforms []subhandle.Platform
var PlatInfos []conf.PlatInfo
var PlatformChan chan subhandle.Platform

const (
	STC = "STC"
	BTC = "BTC"
	EVM = "EVM"
	TVM = "TVM"


)

func NewPlatform(confInnerPublicNodeList map[string]*conf.PlatInfo, c map[string]*conf.PlatInfo, testConfig map[string]*conf.PlatInfo) {
	//var c Config
	//if err := config.GetChainConfig("platform").Unmarshal(&c.Platform); err != nil {
	//	log.Panic("get platform  error:", zap.Error(err))
	//}
	if biz.AppConfig.Mode != "" {
		if strings.ToLower(biz.AppConfig.Mode) == "debug" {
			//var testConfig Config
			//if err := config.GetChainConfig("platformTest").Unmarshal(&testConfig.Platform); err != nil {
			//	log.Panic("get platform test  error:", zap.Error(err))
			//}
			for key, platInfo := range testConfig {
				c[key] = platInfo
			}
		} else {
			mode := strings.ToUpper(biz.AppConfig.Mode)
			modes := strings.Split(mode, ",")
			for _, chainName := range modes {
				if platInfo := testConfig[chainName]; platInfo != nil {
					c[chainName] = platInfo
				}
			}
		}
	}

	for key, value := range confInnerPublicNodeList {
		webConfig := c[key]
		if webConfig != nil {
			webConfig.RpcURL = append(webConfig.RpcURL, value.RpcURL...)
			c[key] = webConfig
		}
	}

	PlatformChan = make(chan subhandle.Platform, len(c))

	for _, value := range c {
		PlatInfos = append(PlatInfos, *value)
		platform := GetPlatform(value.Type, value.Handler, value.Chain, value.RpcURL)
		Platforms = append(Platforms, platform)
		go SetPlatform(platform)
	}
	DynamicCreateTable(PlatInfos)
}

func DynamicCreateTable(platInfos []conf.PlatInfo)  {
	for _, platInfo := range PlatInfos {
		chain := strings.ToLower(platInfo.Chain) + biz.TABLE_POSTFIX
		switch platInfo.Type {
		case STC:
			data.GormlDb.Table(chain).AutoMigrate(&data.StcTransactionRecord{})
		case EVM:
			data.GormlDb.Table(chain).AutoMigrate(&data.EvmTransactionRecord{})
		case BTC:
			data.GormlDb.Table(chain).AutoMigrate(&data.BtcTransactionRecord{})
		case TVM:
			data.GormlDb.Table(chain).AutoMigrate(&data.TrxTransactionRecord{})

		}

	}
}

func GetPlatform(typ, chain, chainName string, nodeURL []string) subhandle.Platform {
	height := -1
	redisHeight, _ := data.RedisClient.Get(data.CHAINNAME + chainName).Result()
	if redisHeight != "" {
		height, _ = strconv.Atoi(redisHeight)
	}
	switch typ {
	case STC:
		return starcoin.Init(coins.Starcoin().Handle, chain, chainName, nodeURL, height)
	case EVM:
		return ethereum.Init(coins.Ethereum().Handle, chain, chainName, nodeURL, height)
	case BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, chain, chainName, nodeURL, height)
	case TVM:
		return tron.Init(coins.Tron().Handle, chain, chainName, nodeURL, height)

	}
	return nil
}

func SetPlatform(platform subhandle.Platform) {
	PlatformChan <- platform
}
