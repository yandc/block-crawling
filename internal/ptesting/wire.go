//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package ptesting

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/server"
	"block-crawling/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.App, *conf.AddressServer, *conf.Lark, *conf.Logger,
	*conf.Transaction, *conf.Bootstrap, log.Logger, Preparation, *Cancellation) (*kratos.App, func(), error) {
	panic(wire.Build(
		data.ConnProviderSet,
		RunnerProviderSet,
		LarkProviderSet,
		data.ProviderSet,
		biz.ProviderSet,
		service.ProviderSet,
		server.ProviderSet,
		newApp,
	))
}
