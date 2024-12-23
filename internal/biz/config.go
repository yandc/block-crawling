package biz

import (
	"block-crawling/internal/conf"
	in "block-crawling/internal/types"
	"net/url"
	"strings"
	"sync"

	"github.com/nervosnetwork/ckb-sdk-go/types"
)

var platInfoMap = &sync.Map{}
var chainNameType = &sync.Map{}
var platformMap = &sync.Map{}
var customChainMap = &sync.Map{}
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

func SetCustomChain(chainName string) {
	customChainMap.Store(chainName, true)
}

func IsCustomChain(chainName string) bool {
	_, loaded := customChainMap.Load(chainName)
	return loaded
}

func ShouldChainAlarm(chainName string) bool {
	if IsCustomChain(chainName) && !IsCustomChainFeatured(chainName) {
		return false
	}
	return true
}

func IsCustomChainFeatured(chainName string) bool {
	_, featured := AppConfig.FeaturedCustomChain[chainName]
	return featured

}

func IsBenfenNet(chainName string) bool {
	return strings.HasPrefix(chainName, "Benfen")
}

func IsEventLogWhiteChain(chainName string) bool {
	_, ok := eventLogWhiteList[chainName]
	return ok
}

var AppConfig *conf.App
var GetNervosUTXOTransaction func(string) (*types.TransactionWithStatus, error)
var GetTxByHashFuncMap = make(map[string]func(string) (tx in.TX, err error))
var GetKaspaUTXOTransaction func(string) (*in.KaspaTransactionInfo, error)

const (
	// OkLinkURLWithAPIKey = "https://83c5939d-9051-46d9-9e72-ed69d5855209@www.oklink.com"
	OkLinkURLWithAPIKey = "https://aad5a190-adff-4fb1-8e2f-728a84f58a1f@www.oklink.com"
)

type AppConf *conf.App

var eventLogWhiteList = make(map[string]string)

func NewConfig(conf *conf.App, bootstrap *conf.Bootstrap) AppConf {
	AppConfig = conf
	eventLogWhiteList = bootstrap.EventLogWhiteList
	for _, rawURL := range conf.HttpProxies {
		proxy, err := url.Parse(rawURL)
		if err != nil {
			panic(err)

		}
		HTTPProxies = append(HTTPProxies, proxy)
	}
	return conf
}

func IsBenfenStandalone() bool {
	return strings.ToLower(AppConfig.Mode) == strings.ToLower("BenfenStandalone")
}
