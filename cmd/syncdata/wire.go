//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Logger, *conf.Data, *conf.App, *conf.Bootstrap) (*kratos.App, func(), error) {
	panic(wire.Build(
		data.ProviderSet,
		biz.ProviderSet,
		service.ProviderSet,
		newApp,
	))
}
