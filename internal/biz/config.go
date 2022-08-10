package biz

import "block-crawling/internal/conf"

var AppConfig *conf.App

func NewConfig(conf *conf.App) {
	AppConfig = conf
}
