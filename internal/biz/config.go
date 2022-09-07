package biz

import "block-crawling/internal/conf"

var PlatInfos []*conf.PlatInfo
var PlatInfoMap map[string]*conf.PlatInfo

var AppConfig *conf.App

func NewConfig(conf *conf.App) {
	AppConfig = conf
}
