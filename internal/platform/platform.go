package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/casper"
	"block-crawling/internal/platform/cosmos"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/nervos"
	"block-crawling/internal/platform/polkadot"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/subhandle"
	"strconv"
	"strings"
)

var Platforms []subhandle.Platform
var grpcPlatformInfos []biz.GrpcPlatformInfo

type PlatformContainer []subhandle.Platform

func NewPlatform(bc *conf.Bootstrap, bundle *data.Bundle, appConfig biz.AppConf) PlatformContainer {
	c := bc.Platform
	confInnerPublicNodeList := bc.InnerPublicNodeList
	testConfig := bc.PlatformTest
	if appConfig.Mode != "" {
		if strings.ToLower(appConfig.Mode) == "debug" {
			for key, platInfo := range testConfig {
				c[key] = platInfo
			}
		} else {
			modes := strings.Split(appConfig.Mode, ",")
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
		} else {
			c[key] = value
		}
	}

	var PlatInfos []*conf.PlatInfo
	var chainNameType = make(map[string]string)
	for _, value := range c {
		PlatInfos = append(PlatInfos, value)

		platform := GetPlatform(value)
		Platforms = append(Platforms, platform)
		if p, ok := platform.(*nervos.Platform); ok {
			biz.GetNervosUTXOTransaction = p.GetUTXOByHash
		}
		if p, ok := platform.(*bitcoin.Platform); ok {
			biz.GetUTXOByHash[value.Chain] = p.GetUTXOByHash
		}

		chainNameType[value.Chain] = value.Type
		biz.Init(value.Handler, value.TokenPrice, value.Chain, value.Type)
	}
	biz.PlatInfos = PlatInfos
	biz.ChainNameType = chainNameType
	biz.PlatInfoMap = c
	DynamicCreateTable(PlatInfos)
	return Platforms
}

func DynamicCreateTable(platInfos []*conf.PlatInfo) {
	for _, platInfo := range platInfos {
		chain := strings.ToLower(platInfo.Chain) + biz.TABLE_POSTFIX
		switch platInfo.Type {
		case biz.STC:
			data.GormlDb.Table(chain).AutoMigrate(&data.StcTransactionRecord{})
		case biz.EVM:
			data.GormlDb.Table(chain).AutoMigrate(&data.EvmTransactionRecord{})
		case biz.BTC:
			data.GormlDb.Table(chain).AutoMigrate(&data.BtcTransactionRecord{})
		case biz.TVM:
			data.GormlDb.Table(chain).AutoMigrate(&data.TrxTransactionRecord{})
		case biz.APTOS:
			data.GormlDb.Table(chain).AutoMigrate(&data.AptTransactionRecord{})
		case biz.SUI:
			data.GormlDb.Table(chain).AutoMigrate(&data.SuiTransactionRecord{})
		case biz.SOLANA:
			data.GormlDb.Table(chain).AutoMigrate(&data.SolTransactionRecord{})
		case biz.NERVOS:
			data.GormlDb.Table(chain).AutoMigrate(&data.CkbTransactionRecord{})
		case biz.CASPER:
			data.GormlDb.Table(chain).AutoMigrate(&data.CsprTransactionRecord{})
		case biz.COSMOS:
			data.GormlDb.Table(chain).AutoMigrate(&data.AtomTransactionRecord{})
		case biz.POLKADOT:
			data.GormlDb.Table(chain).AutoMigrate(&data.DotTransactionRecord{})

		}
	}
}

func GetPlatform(value *conf.PlatInfo) subhandle.Platform {
	typ := value.Type        // EVM
	chainName := value.Chain // ETH
	nodeURL := value.RpcURL

	height := -1
	redisHeight, _ := data.RedisClient.Get(data.CHAINNAME + chainName).Result()
	if redisHeight != "" {
		height, _ = strconv.Atoi(redisHeight)
	}
	switch typ {
	case biz.STC:
		return starcoin.Init(coins.Starcoin().Handle, value, nodeURL, height)
	case biz.EVM:
		return ethereum.Init(coins.Ethereum().Handle, value, nodeURL, height)
	case biz.BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, value, nodeURL, height)
	case biz.TVM:
		return tron.Init(coins.Tron().Handle, value, nodeURL, height)
	case biz.APTOS:
		return aptos.Init(coins.Aptos().Handle, value, nodeURL, height)
	case biz.SUI:
		return sui.Init(coins.Sui().Handle, value, nodeURL, height)
	case biz.SOLANA:
		return solana.Init(coins.Solana().Handle, value, nodeURL, height)
	case biz.NERVOS:
		return nervos.Init(coins.Nervos().Handle, value, nodeURL, height)
	case biz.CASPER:
		return casper.Init(coins.Casper().Handle, value, nodeURL, height)
	case biz.COSMOS:
		return cosmos.Init(coins.Cosmos().Handle, value, nodeURL, height)
	case biz.POLKADOT:
		return polkadot.Init(coins.Polkadot().Handle, value, nodeURL, height)
	}
	return nil
}
