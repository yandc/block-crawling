package data

import (
	"github.com/google/wire"
)

var ConnProviderSet = wire.NewSet(NewGormDB, NewUserGormDB, NewRedisClient)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewBtcTransactionRecordRepo, NewEvmTransactionRecordRepo, NewStcTransactionRecordRepo, NewTrxTransactionRecordRepo, NewAptTransactionRecordRepo, NewSuiTransactionRecordRepo, NewSolTransactionRecordRepo, NewCkbTransactionRecordRepo, NewCsprTransactionRecordRepo, NewUserRecordRepo, NewUserAssetRepo, NewAtomTransactionRecordRepo, NewDappApproveRecordRepo, NewDotTransactionRecordRepo, NewTransactionStatisticRepo, NewNervosCellRecordRepo, NewUtxoUnspentRecordRepo, NewUserNftAssetRepo, NewNftRecordHistoryRepo, NewUserSendRawHistoryRepo, NewBundle)
