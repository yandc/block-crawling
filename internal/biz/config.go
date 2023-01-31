package biz

import (
	"block-crawling/internal/conf"
	in "block-crawling/internal/types"

	"github.com/nervosnetwork/ckb-sdk-go/types"
)

var PlatInfos []*conf.PlatInfo
var PlatInfoMap map[string]*conf.PlatInfo
var ChainNameType map[string]string

var AppConfig *conf.App
var GetNervosUTXOTransaction func(string) (*types.TransactionWithStatus, error)
var GetUTXOByHash = make(map[string]func(string) (tx in.TX, err error))

type AppConf *conf.App

func NewConfig(conf *conf.App) AppConf {
	AppConfig = conf
	return conf
}
