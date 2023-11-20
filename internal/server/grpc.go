package server

import (
	bfstation "block-crawling/api/bfstation/v1"
	transaction "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/validate"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

// NewGRPCServer new a gRPC server.
func NewGRPCServer(c *conf.Server, ts *service.TransactionService, logger log.Logger, bs *service.BFStationService) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(recovery.WithHandler(common.HandlerFunction)),
			validate.Validator(),
			logging.Server(logger),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	srv := grpc.NewServer(opts...)
	transaction.RegisterTransactionServer(srv, ts)
	bfstation.RegisterBFStationServer(srv, bs)
	return srv
}
