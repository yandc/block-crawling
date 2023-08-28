package platform

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/casper"
	"block-crawling/internal/platform/cosmos"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/kaspa"
	"block-crawling/internal/platform/nervos"
	"block-crawling/internal/platform/polkadot"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/tron"
	in "block-crawling/internal/types"
	"strings"

	"github.com/nervosnetwork/ckb-sdk-go/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var Platforms []biz.Platform

type PlatformContainer []biz.Platform

func NewPlatform(bc *conf.Bootstrap, bundle *data.Bundle, appConfig biz.AppConf, db *gorm.DB, l biz.Larker, provider CustomConfigProvider) Server {
	log.Info("BOOTSTRAP", zap.String("stage", "before"))
	defer log.Info("BOOTSTRAP", zap.String("stage", "after"))
	c := bc.Platform
	if c == nil {
		c = map[string]*conf.PlatInfo{}
	}
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

	bs := newServer(provider)

	var PlatInfos []*conf.PlatInfo
	var chainNameType = make(map[string]string)
	var platformMap = make(map[string]biz.Platform)
	for _, value := range c {
		PlatInfos = append(PlatInfos, value)

		platform := GetPlatform(value)
		bt := NewBootstrap(platform, value, db)
		bs.inner[value.Chain] = bt
		Platforms = append(Platforms, platform)
		if p, ok := platform.(*nervos.Platform); ok {
			biz.GetNervosUTXOTransaction = func(txHash string) (tx *types.TransactionWithStatus, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}
		if p, ok := platform.(*bitcoin.Platform); ok {
			biz.GetUTXOByHash[value.Chain] = func(txHash string) (tx in.TX, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}
		if p, ok := platform.(*kaspa.Platform); ok {
			biz.GetKaspaUTXOTransaction = func(txHash string) (tx *in.KaspaTransactionInfo, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}

		chainNameType[value.Chain] = value.Type
		platformMap[value.Chain] = platform
	}
	biz.ChainNameType = chainNameType
	biz.PlatInfoMap = c
	biz.PlatformMap = platformMap
	return bs
}

func GetPlatform(value *conf.PlatInfo) biz.Platform {
	typ := value.Type // EVM
	nodeURL := value.RpcURL

	switch typ {
	case biz.STC:
		return starcoin.Init(coins.Starcoin().Handle, value, nodeURL)
	case biz.EVM:
		return ethereum.Init(coins.Ethereum().Handle, value, nodeURL)
	case biz.BTC:
		return bitcoin.Init(coins.Bitcoin().Handle, value, nodeURL)
	case biz.TVM:
		return tron.Init(coins.Tron().Handle, value, nodeURL)
	case biz.APTOS:
		return aptos.Init(coins.Aptos().Handle, value, nodeURL)
	case biz.SUI:
		return sui.Init(coins.Sui().Handle, value, nodeURL)
	case biz.SOLANA:
		return solana.Init(coins.Solana().Handle, value, nodeURL)
	case biz.NERVOS:
		return nervos.Init(coins.Nervos().Handle, value, nodeURL)
	case biz.CASPER:
		return casper.Init(coins.Casper().Handle, value, nodeURL)
	case biz.COSMOS:
		return cosmos.Init(coins.Cosmos().Handle, value, nodeURL)
	case biz.POLKADOT:
		return polkadot.Init(coins.Polkadot().Handle, value, nodeURL)
	case biz.KASPA:
		return kaspa.Init(coins.Kaspa().Handle, value, nodeURL)
	}
	return nil
}
