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
	"block-crawling/internal/platform/ton"
	"block-crawling/internal/platform/tron"
	in "block-crawling/internal/types"
	"strings"

	"github.com/nervosnetwork/ckb-sdk-go/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var Platforms []biz.Platform

type PlatformContainer []biz.Platform

func NewPlatform(bc *conf.Bootstrap, bundle *data.Bundle, appConfig biz.AppConf, db *gorm.DB, l biz.Larker, provider CustomConfigProvider, mr data.MigrationRepo) Server {
	log.Info("BOOTSTRAP", zap.String("stage", "before"))
	defer log.Info("BOOTSTRAP", zap.String("stage", "after"))
	c := bc.Platform
	if c == nil {
		c = map[string]*conf.PlatInfo{}
	}
	confInnerPublicNodeList := bc.InnerPublicNodeList
	testConfig := bc.PlatformTest

	if appConfig.Mode != "" {
		enabledTestChains := make(map[string]bool)
		for _, chain := range strings.Split(appConfig.Mode, ",") {
			enabledTestChains[strings.TrimSpace(chain)] = true
		}

		for chainName, platInfo := range testConfig {
			if _, ok := enabledTestChains[chainName]; ok {
				c[chainName] = platInfo
			}
			if strings.ToLower(appConfig.Mode) == "debug" {
				c[chainName] = platInfo
			}
			if biz.IsBenfenStandalone() && biz.IsBenfenNet(chainName) {
				c[chainName] = platInfo
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

	bs := newServer(provider, mr)

	for chainName, value := range c {
		if biz.IsBenfenStandalone() && !biz.IsBenfenNet(chainName) {
			continue
		}
		platform := GetPlatform(value)
		bt := NewBootstrap(platform, value, db, mr)
		bs.inner[value.Chain] = bt
		Platforms = append(Platforms, platform)
		if p, ok := platform.(*nervos.Platform); ok {
			biz.GetNervosUTXOTransaction = func(txHash string) (tx *types.TransactionWithStatus, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}
		if p, ok := platform.(*bitcoin.Platform); ok {
			biz.GetTxByHashFuncMap[value.Chain] = func(txHash string) (tx in.TX, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}
		if p, ok := platform.(*kaspa.Platform); ok {
			biz.GetKaspaUTXOTransaction = func(txHash string) (tx *in.KaspaTransactionInfo, err error) {
				return p.GetUTXOByHash(txHash)
			}
		}

		biz.SetChainNameType(value.Chain, value.Type)
		biz.SetChainPlatform(value.Chain, platform)
		biz.SetChainPlatInfo(value.Chain, value)
	}

	// Some of the testnets aren't enable.
	for _, value := range testConfig {
		biz.SetChainNameType(value.Chain, value.Type)
		biz.SetChainPlatInfo(value.Chain, value)
	}

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
	case biz.TON:
		return ton.Init(coins.Ton().Handle, value, nodeURL)
	}
	return nil
}
