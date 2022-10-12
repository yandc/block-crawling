package data

import "github.com/google/wire"

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewGormDB, NewRedisClient, NewGreeterRepo, NewBtcTransactionRecordRepo, NewEvmTransactionRecordRepo, NewStcTransactionRecordRepo, NewTrxTransactionRecordRepo, NewAptTransactionRecordRepo, NewSuiTransactionRecordRepo, NewSolTransactionRecordRepo)
