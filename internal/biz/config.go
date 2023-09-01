package biz

import (
	"block-crawling/internal/conf"
	in "block-crawling/internal/types"
	"net/url"
	"sync"

	"github.com/nervosnetwork/ckb-sdk-go/types"
)

var platInfoMap = &sync.Map{}
var chainNameType = &sync.Map{}
var platformMap = &sync.Map{}
var HTTPProxies []*url.URL

func GetChainPlatInfoMap() map[string]*conf.PlatInfo {
	results := make(map[string]*conf.PlatInfo)
	platInfoMap.Range(func(key, value interface{}) bool {
		results[key.(string)] = value.(*conf.PlatInfo)
		return true
	})
	return results
}

func SetChainPlatInfo(chainName string, info *conf.PlatInfo) {
	platInfoMap.Store(chainName, info)
}

func GetChainPlatInfo(chainName string) (*conf.PlatInfo, bool) {
	if v, ok := platInfoMap.Load(chainName); ok {
		return v.(*conf.PlatInfo), ok
	}
	return nil, false
}

func SetChainNameType(chainName string, chainType string) {
	chainNameType.Store(chainName, chainType)
}

func GetChainNameTypeMap() map[string]string {
	results := make(map[string]string)
	chainNameType.Range(func(key, value interface{}) bool {
		results[key.(string)] = value.(string)
		return true
	})
	return results
}

func SetChainPlatform(chainName string, p Platform) {
	platformMap.Store(chainName, p)
}

func GetChainNameType(chainName string) (string, bool) {
	v, ok := chainNameType.Load(chainName)
	if ok {
		return v.(string), ok
	}
	return "", ok
}

func GetChainPlatform(chainName string) (Platform, bool) {
	v, ok := platformMap.Load(chainName)
	if ok {
		return v.(Platform), ok
	}
	return nil, ok
}

var AppConfig *conf.App
var GetNervosUTXOTransaction func(string) (*types.TransactionWithStatus, error)
var GetUTXOByHash = make(map[string]func(string) (tx in.TX, err error))
var GetKaspaUTXOTransaction func(string) (*in.KaspaTransactionInfo, error)

type AppConf *conf.App

func NewConfig(conf *conf.App) AppConf {
	AppConfig = conf
	for _, rawURL := range conf.HttpProxies {
		proxy, err := url.Parse(rawURL)
		if err != nil {
			panic(err)

		}
		HTTPProxies = append(HTTPProxies, proxy)
	}
	return conf
}
