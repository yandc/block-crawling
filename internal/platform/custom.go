package platform

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/transport"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var customBootstrapMap = &sync.Map{}

type CustomConfigProvider interface {
	transport.Server

	Load() []*conf.PlatInfo
	Updated() <-chan *conf.PlatInfo
}

type customConfigProviderImpl struct {
	ctx         context.Context
	cancel      func()
	response    *v1.GetChainNodeInUsedListResp
	platInfos   []*conf.PlatInfo
	updatedPlat map[string]*conf.PlatInfo
	updateCh    chan *conf.PlatInfo
	db          *gorm.DB
	mr          data.MigrationRepo
}

// Start implements CustomConfigProvider
func (p *customConfigProviderImpl) Start(context.Context) error {
	p.fetchAndProvide()
	go func() {
		customChainPlan := time.NewTicker(time.Duration(10) * time.Second)
		for true {
			select {
			case <-customChainPlan.C:
				p.fetchAndProvide()
			case <-p.ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (p *customConfigProviderImpl) fetchAndProvide() {
	chainNodeInUsedList, err := biz.GetCustomChainList(nil)

	if err != nil {
		log.Error("CUSTOM-CHAIN: 获取自定义链信息失败", zap.Error(err))
		return
	}
	p.response = chainNodeInUsedList
	p.platInfos = p.provide()
}

// Provide implements CustomConfigProvider
func (p *customConfigProviderImpl) provide() []*conf.PlatInfo {
	var platInfos []*conf.PlatInfo
	for _, chainInfo := range p.response.Data {
		oldUrls := []string{}
		if v, ok := p.updatedPlat[chainInfo.Chain]; ok {
			oldUrls = v.RpcURL
		}

		var mhat int32 = 1000
		var mc int32 = 1
		var scbd int32 = 500
		cp := &conf.PlatInfo{
			Chain:                      chainInfo.Chain,
			Type:                       chainInfo.Type,
			RpcURL:                     chainInfo.Urls,
			ChainId:                    chainInfo.ChainId,
			Decimal:                    int32(chainInfo.Decimals),
			NativeCurrency:             chainInfo.CurrencyName,
			Source:                     biz.SOURCE_REMOTE,
			MonitorHeightAlarmThr:      &mhat,
			MaxConcurrency:             &mc,
			SafelyConcurrentBlockDelta: &scbd,
			Handler:                    chainInfo.Chain,
		}
		biz.ChainNameType[cp.Chain] = cp.Type
		platform := GetPlatform(cp)
		biz.PlatformMap[cp.Chain] = platform
		bt := NewBootstrap(platform, cp, p.db, p.mr)
		customBootstrapMap.Store(cp.Chain, bt)
		biz.PlatInfoMap[cp.Chain] = cp
		platInfos = append(platInfos, cp)

		if !reflect.DeepEqual(oldUrls, chainInfo.Urls) {
			select {
			case p.updateCh <- cp:
				log.Info(
					"CUSTOM-CHAIN: RPC URLS HAVE BEEN CHANGED",
					zap.Strings("old", oldUrls),
					zap.Strings("new", chainInfo.Urls),
					zap.Any("chainInfo", chainInfo),
					zap.String("chainName", cp.Chain),
				)
				p.updatedPlat[chainInfo.Chain] = cp
			default:
			}
		}
	}
	return platInfos
}

// Stop implements CustomConfigProvider
func (p *customConfigProviderImpl) Stop(context.Context) error {
	p.cancel()
	return nil
}

func (p *customConfigProviderImpl) Load() []*conf.PlatInfo {
	return p.platInfos
}

func (p *customConfigProviderImpl) Updated() <-chan *conf.PlatInfo {
	return p.updateCh
}

func NewCustomConfigProvider(db *gorm.DB, mr data.MigrationRepo) CustomConfigProvider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &customConfigProviderImpl{
		ctx:         ctx,
		cancel:      cancel,
		updateCh:    make(chan *conf.PlatInfo, 512),
		updatedPlat: make(map[string]*conf.PlatInfo),
		db:          db,
		mr:          mr,
	}
	return p
}
