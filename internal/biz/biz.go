package biz

import (
	v1 "block-crawling/internal/client"
	"block-crawling/internal/conf"
	"github.com/google/wire"
	"google.golang.org/grpc"
)

var LarkProviderSet = wire.NewSet(NewLark, wire.Bind(new(Larker), new(*Lark)))

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewConfig, NewTransactionUsecase, NewBFStationUsecase, NewTransactionRecordRepo, NewChainListClient)

var ChainListClient v1.ChainListClient

func NewChainListClient(conf *conf.App) v1.ChainListClient {
	conn, err := grpc.Dial(conf.Addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := v1.NewChainListClient(conn)
	ChainListClient = client
	return client
}
