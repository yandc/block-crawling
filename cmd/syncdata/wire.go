//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/server"
	"block-crawling/internal/service"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp( *conf.Logger, *conf.Data, confApp *conf.App, map[string]*conf.PlatInfo, map[string]*conf.PlatInfo, map[string]*conf.PlatInfo,
	map[string]*conf.PlatInfo) (func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, newApp))
}
