package data

import (
	"github.com/google/wire"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewGormDB, NewRedisClient, NewBtcTransactionRecordRepo, NewEvmTransactionRecordRepo, NewStcTransactionRecordRepo, NewTrxTransactionRecordRepo, NewAptTransactionRecordRepo, NewSuiTransactionRecordRepo, NewSolTransactionRecordRepo, NewCkbTransactionRecordRepo, NewCsprTransactionRecordRepo, NewUserGormDB, NewUserRecordRepo, NewUserAssetRepo, NewAtomTransactionRecordRepo, NewDappApproveRecordRepo, NewDotTransactionRecordRepo, NewTransactionStatisticRepo, NewNervosCellRecordRepo, NewUtxoUnspentRecordRepo, NewUserNftAssetRepo, NewNftRecordHistoryRepo,NewUserSendRawHistoryRepo, NewBundle)
