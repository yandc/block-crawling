//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	dataKanban "block-crawling/internal/data/kanban"
	"block-crawling/internal/kanban"
	"block-crawling/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.App, *conf.AddressServer, *conf.Lark, *conf.Logger,
	*conf.Transaction, *conf.Bootstrap, log.Logger, Subcommand, *kanban.Options) (*kratos.App, func(), error) {
	panic(wire.Build(
		data.ConnProviderSet,
		data.ProviderSet,
		dataKanban.ConnProviderSet,
		dataKanban.ProviderSet,
		kanban.ProviderSet,
		biz.LarkProviderSet,
		biz.ProviderSet,
		service.ProviderSet,
		newApp,
	))
}
