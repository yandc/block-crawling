// Code generated by Wire. DO NOT EDIT.

//go:generate go run -mod=mod github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package main

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/scheduling"
	"block-crawling/internal/server"
	"block-crawling/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

// Injectors from wire.go:

// wireApp init kratos application.
func wireApp(confServer *conf.Server, confData *conf.Data, app *conf.App, addressServer *conf.AddressServer, lark *conf.Lark, logger *conf.Logger, transaction *conf.Transaction, bootstrap *conf.Bootstrap, logLogger log.Logger) (*kratos.App, func(), error) {
	db, cleanup, err := data.NewGormDB(confData)
	if err != nil {
		return nil, nil, err
	}
	bizLark := biz.NewLark(lark)
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
	tonTransactionRecordRepo := data.NewTonTransactionRecordRepo(db)
	userNftAssetRepo := data.NewUserNftAssetRepo(db)
	nftRecordHistoryRepo := data.NewNftRecordHistoryRepo(db)
	transactionStatisticRepo := data.NewTransactionStatisticRepo(db)
	transactionCountRepo := data.NewTransactionCountRepo(db)
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
	swapContractRepo := data.NewSwapContractRepo(db)
	bfcStationRepo := data.NewBFCStationRepo(db)
	userWalletAssetHistoryRepo := data.NewUserWalletAssetHistoryRepo(db)
	deFiAssetRepo := data.NewDeFiAssetRepo(db)
	bundle := data.NewBundle(atomTransactionRecordRepo, btcTransactionRecordRepo, dotTransactionRecordRepo, evmTransactionRecordRepo, stcTransactionRecordRepo, trxTransactionRecordRepo, aptTransactionRecordRepo, suiTransactionRecordRepo, solTransactionRecordRepo, ckbTransactionRecordRepo, csprTransactionRecordRepo, kasTransactionRecordRepo, tonTransactionRecordRepo, userNftAssetRepo, nftRecordHistoryRepo, transactionStatisticRepo, transactionCountRepo, nervosCellRecordRepo, utxoUnspentRecordRepo, userRecordRepo, userAssetRepo, userAssetHistoryRepo, chainTypeAssetRepo, chainTypeAddressAmountRepo, dappApproveRecordRepo, client, userSendRawHistoryRepo, marketCoinHistoryRepo, swapContractRepo, bfcStationRepo, userWalletAssetHistoryRepo, deFiAssetRepo)
	transactionRecordRepo := biz.NewTransactionRecordRepo(db)
	chainListClient := biz.NewChainListClient(app)
	transactionUsecase := biz.NewTransactionUsecase(db, bizLark, bundle, transactionRecordRepo, chainListClient)
	appConf := biz.NewConfig(app, bootstrap)
	migrationRepo := data.NewMigrationRepo(db)
	customConfigProvider := platform.NewCustomConfigProvider(db, migrationRepo)
	platformServer := platform.NewPlatform(bootstrap, bundle, appConf, db, bizLark, customConfigProvider, migrationRepo)
	innerPlatformContainer := platform.NewInnerNodeList(bootstrap, bundle)
	transactionService := service.NewTransactionService(transactionUsecase, platformServer, innerPlatformContainer)
	bfStationUsecase := biz.NewBFStationUsecase(db)
	bfStationService := service.NewBFStationService(bfStationUsecase)
	userWalletAssetUsecase := biz.NewUserWalletAssetUsecase(db, bizLark)
	userWalletAssetService := service.NewUserWalletAssetService(userWalletAssetUsecase)
	grpcServer := server.NewGRPCServer(confServer, transactionService, logLogger, bfStationService, userWalletAssetService)
	httpServer := server.NewHTTPServer(confServer, transactionService, logLogger, bfStationService)
	scheduledTask := scheduling.NewScheduledTask()
	kratosApp := newApp(logLogger, grpcServer, httpServer, platformServer, customConfigProvider, scheduledTask)
	return kratosApp, func() {
		cleanup2()
		cleanup()
	}, nil
}
