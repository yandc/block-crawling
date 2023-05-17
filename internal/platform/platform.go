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
	in "block-crawling/internal/types"
	"strconv"
	"strings"
	"sync"

	"github.com/nervosnetwork/ckb-sdk-go/types"
	"gorm.io/gorm"
)

var Platforms []biz.Platform

type PlatformContainer []biz.Platform

func NewPlatform(bc *conf.Bootstrap, bundle *data.Bundle, appConfig biz.AppConf, db *gorm.DB, l biz.Larker) Server {
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

	bs := make(Server)

	var PlatInfos []*conf.PlatInfo
	var chainNameType = make(map[string]string)
	var platformMap = make(map[string]biz.Platform)
	for _, value := range c {
		PlatInfos = append(PlatInfos, value)

		platform := GetPlatform(value)
		bt := NewBootstrap(platform, value)
		bs[value.Chain] = bt
		Platforms = append(Platforms, platform)
		if p, ok := platform.(*nervos.Platform); ok {
			spider := bt.Spider
			biz.GetNervosUTXOTransaction = func(txHash string) (tx *types.TransactionWithStatus, err error) {
				return p.GetUTXOByHash(spider, txHash)
			}
		}
		if p, ok := platform.(*bitcoin.Platform); ok {
			spider := bt.Spider
			biz.GetUTXOByHash[value.Chain] = func(txHash string) (tx in.TX, err error) {
				return p.GetUTXOByHash(spider, txHash)
			}
		}

		chainNameType[value.Chain] = value.Type
		platformMap[value.Chain] = platform
	}
	biz.PlatInfos = PlatInfos
	biz.ChainNameType = chainNameType
	biz.PlatInfoMap = c
	biz.PlatformMap = platformMap
	DynamicCreateTable(db, PlatInfos)
	return bs
}

var migrated sync.Map

func DynamicCreateTable(gormDb *gorm.DB, platInfos []*conf.PlatInfo) {
	for _, platInfo := range platInfos {
		chain := strings.ToLower(platInfo.Chain) + biz.TABLE_POSTFIX
		_, loaded := migrated.LoadOrStore(chain, true)
		if loaded {
			continue
		}

		switch platInfo.Type {
		case biz.STC:
			gormDb.Table(chain).AutoMigrate(&data.StcTransactionRecord{})
		case biz.EVM:
			gormDb.Table(chain).AutoMigrate(&data.EvmTransactionRecord{})
		case biz.BTC:
			gormDb.Table(chain).AutoMigrate(&data.BtcTransactionRecord{})
		case biz.TVM:
			gormDb.Table(chain).AutoMigrate(&data.TrxTransactionRecord{})
		case biz.APTOS:
			gormDb.Table(chain).AutoMigrate(&data.AptTransactionRecord{})
		case biz.SUI:
			gormDb.Table(chain).AutoMigrate(&data.SuiTransactionRecord{})
		case biz.SOLANA:
			gormDb.Table(chain).AutoMigrate(&data.SolTransactionRecord{})
		case biz.NERVOS:
			gormDb.Table(chain).AutoMigrate(&data.CkbTransactionRecord{})
		case biz.CASPER:
			gormDb.Table(chain).AutoMigrate(&data.CsprTransactionRecord{})
		case biz.COSMOS:
			gormDb.Table(chain).AutoMigrate(&data.AtomTransactionRecord{})
		case biz.POLKADOT:
			gormDb.Table(chain).AutoMigrate(&data.DotTransactionRecord{})

		}
	}
}

func GetPlatform(value *conf.PlatInfo) biz.Platform {
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
