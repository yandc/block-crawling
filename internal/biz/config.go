package biz

import "block-crawling/internal/conf"

var PlatInfos []*conf.PlatInfo
var PlatInfoMap map[string]*conf.PlatInfo
var ChainNameType map[string]string

var AppConfig *conf.App

func NewConfig(conf *conf.App) {
	AppConfig = conf
}
