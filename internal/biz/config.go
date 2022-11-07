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
var GetUTXOByHash func(string) (tx in.TX, err error)

func NewConfig(conf *conf.App) {
	AppConfig = conf
}
