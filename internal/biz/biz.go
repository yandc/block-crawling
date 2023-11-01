package biz

import (
	"github.com/google/wire"
)

var LarkProviderSet = wire.NewSet(NewLark, wire.Bind(new(Larker), new(*Lark)))

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewConfig, NewTransactionUsecase, NewBFStationUsecase, NewTransactionRecordRepo)
