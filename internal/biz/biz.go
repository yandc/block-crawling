package biz

import (
	"block-crawling/internal/log"
	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(log.NewLogger, NewConfig, NewLark, NewGreeterUsecase)
