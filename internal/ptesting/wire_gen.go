// Code generated by Wire. DO NOT EDIT.

//go:generate go run github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package ptesting

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"block-crawling/internal/platform"
	"block-crawling/internal/server"
	"block-crawling/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

// Injectors from wire.go:

// wireApp init kratos application.
func wireApp(confServer *conf.Server, confData *conf.Data, app *conf.App, addressServer *conf.AddressServer, lark *conf.Lark, logger *conf.Logger, transaction *conf.Transaction, bootstrap *conf.Bootstrap, logLogger log.Logger, preparation Preparation, cancellation *Cancellation) (*kratos.App, func(), error) {
	db, cleanup, err := data.NewGormDB(confData)
	if err != nil {
		return nil, nil, err
	}
	atomTransactionRecordRepo := data.NewAtomTransactionRecordRepo(db)
	btcTransactionRecordRepo := data.NewBtcTransactionRecordRepo(db)
	dotTransactionRecordRepo := data.NewDotTransactionRecordRepo(db)
	evmTransactionRecordRepo := data.NewEvmTransactionRecordRepo(db)
	stcTransactionRecordRepo := data.NewStcTransactionRecordRepo(db)
	trxTransactionRecordRepo := data.NewTrxTransactionRecordRepo(db)
	aptTransactionRecordRepo := data.NewAptTransactionRecordRepo(db)
	suiTransactionRecordRepo := data.NewSuiTransactionRecordRepo(db)
	solTransactionRecordRepo := data.NewSolTransactionRecordRepo(db)
	ckbTransactionRecordRepo := data.NewCkbTransactionRecordRepo(db)
	csprTransactionRecordRepo := data.NewCsprTransactionRecordRepo(db)
	kasTransactionRecordRepo := data.NewKasTransactionRecordRepo(db)
	userNftAssetRepo := data.NewUserNftAssetRepo(db)
	nftRecordHistoryRepo := data.NewNftRecordHistoryRepo(db)
	transactionStatisticRepo := data.NewTransactionStatisticRepo(db)
	nervosCellRecordRepo := data.NewNervosCellRecordRepo(db)
	utxoUnspentRecordRepo := data.NewUtxoUnspentRecordRepo(db)
	userGormDB, cleanup2, err := data.NewUserGormDB(confData)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	userRecordRepo := data.NewUserRecordRepo(userGormDB)
	userAssetRepo := data.NewUserAssetRepo(db)
	userAssetHistoryRepo := data.NewUserAssetHistoryRepo(db)
	chainTypeAssetRepo := data.NewChainTypeAssetRepo(db)
	chainTypeAddressAmountRepo := data.NewChainTypeAddressAmountRepo(db)
	dappApproveRecordRepo := data.NewDappApproveRecordRepo(db)
	client := data.NewRedisClient(confData)
	userSendRawHistoryRepo := data.NewUserSendRawHistoryRepo(db)
	marketCoinHistoryRepo := data.NewMarketCoinHistoryRepo(db)
	bundle := data.NewBundle(atomTransactionRecordRepo, btcTransactionRecordRepo, dotTransactionRecordRepo, evmTransactionRecordRepo, stcTransactionRecordRepo, trxTransactionRecordRepo, aptTransactionRecordRepo, suiTransactionRecordRepo, solTransactionRecordRepo, ckbTransactionRecordRepo, csprTransactionRecordRepo, kasTransactionRecordRepo, userNftAssetRepo, nftRecordHistoryRepo, transactionStatisticRepo, nervosCellRecordRepo, utxoUnspentRecordRepo, userRecordRepo, userAssetRepo, userAssetHistoryRepo, chainTypeAssetRepo, chainTypeAddressAmountRepo, dappApproveRecordRepo, client, userSendRawHistoryRepo, marketCoinHistoryRepo)
	appConf := biz.NewConfig(app)
	ptestingDummyLark := NewDummyLark()
	customConfigProvider := platform.NewCustomConfigProvider()
	platformServer := platform.NewPlatform(bootstrap, bundle, appConf, db, ptestingDummyLark, customConfigProvider)
	kanbanGormDB, cleanup3, err := kanban.NewGormDB(confData)
	if err != nil {
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	kanbanEvmTransactionRecordRepo := kanban.NewEvmTransactionRecordRepo(kanbanGormDB)
	walletRepo := kanban.NewWalletRepo(kanbanGormDB)
	trendingRepo := kanban.NewTrendingRepo(kanbanGormDB)
	kanbanBundle := kanban.NewBundle(kanbanEvmTransactionRecordRepo, walletRepo, trendingRepo)
	transactionUsecase := biz.NewTransactionUsecase(db, ptestingDummyLark, bundle, kanbanBundle)
	runner := NewRunner(platformServer, preparation, transactionUsecase, cancellation)
	innerPlatformContainer := platform.NewInnerNodeList(bootstrap, bundle)
	transactionService := service.NewTransactionService(transactionUsecase, platformServer, innerPlatformContainer)
	grpcServer := server.NewGRPCServer(confServer, transactionService, logLogger)
	kratosApp := newApp(logLogger, runner, grpcServer)
	return kratosApp, func() {
		cleanup3()
		cleanup2()
		cleanup()
	}, nil
}
