package data

import (
	"github.com/google/wire"
)

var ConnProviderSet = wire.NewSet(NewGormDB, NewUserGormDB, NewRedisClient)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewBtcTransactionRecordRepo, NewEvmTransactionRecordRepo, NewStcTransactionRecordRepo, NewTrxTransactionRecordRepo, NewAptTransactionRecordRepo, NewSuiTransactionRecordRepo, NewSolTransactionRecordRepo, NewCkbTransactionRecordRepo, NewCsprTransactionRecordRepo, NewUserRecordRepo, NewUserAssetRepo, NewUserAssetHistoryRepo, NewChainTypeAssetRepo, NewChainTypeAddressAmountRepo, NewAtomTransactionRecordRepo, NewDappApproveRecordRepo, NewDotTransactionRecordRepo, NewTransactionStatisticRepo, NewTransactionCountRepo, NewNervosCellRecordRepo, NewUtxoUnspentRecordRepo, NewUserNftAssetRepo, NewNftRecordHistoryRepo, NewUserSendRawHistoryRepo, NewMarketCoinHistoryRepo, NewBundle, NewKasTransactionRecordRepo, NewMigrationRepo, NewSwapContractRepo, NewBFCStationRepo, NewUserWalletAssetHistoryRepo)
