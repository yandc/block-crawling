// Code generated by Wire. DO NOT EDIT.

//go:generate go run github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package main

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	kanban2 "block-crawling/internal/data/kanban"
	"block-crawling/internal/kanban"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

// Injectors from wire.go:

// wireApp init kratos application.
func wireApp(server *conf.Server, confData *conf.Data, app *conf.App, addressServer *conf.AddressServer, lark *conf.Lark, logger *conf.Logger, transaction *conf.Transaction, bootstrap *conf.Bootstrap, logLogger log.Logger, options *kanban.Options) (*kratos.App, func(), error) {
	db, cleanup, err := data.NewGormDB(confData)
	if err != nil {
		return nil, nil, err
	}
	migrationRepo := data.NewMigrationRepo(db)
	kanbanGormDB, cleanup2, err := kanban2.NewGormDB(confData)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	evmTransactionRecordRepo := kanban2.NewEvmTransactionRecordRepo(kanbanGormDB)
	walletRepo := kanban2.NewWalletRepo(kanbanGormDB)
	trendingRepo := kanban2.NewTrendingRepo(kanbanGormDB)
	bundle := kanban2.NewBundle(evmTransactionRecordRepo, walletRepo, trendingRepo)
	bizLark := biz.NewLark(lark)
	atomTransactionRecordRepo := data.NewAtomTransactionRecordRepo(db)
	btcTransactionRecordRepo := data.NewBtcTransactionRecordRepo(db)
	dotTransactionRecordRepo := data.NewDotTransactionRecordRepo(db)
	dataEvmTransactionRecordRepo := data.NewEvmTransactionRecordRepo(db)
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
	userGormDB, cleanup3, err := data.NewUserGormDB(confData)
	if err != nil {
		cleanup2()
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
	dataBundle := data.NewBundle(atomTransactionRecordRepo, btcTransactionRecordRepo, dotTransactionRecordRepo, dataEvmTransactionRecordRepo, stcTransactionRecordRepo, trxTransactionRecordRepo, aptTransactionRecordRepo, suiTransactionRecordRepo, solTransactionRecordRepo, ckbTransactionRecordRepo, csprTransactionRecordRepo, kasTransactionRecordRepo, userNftAssetRepo, nftRecordHistoryRepo, transactionStatisticRepo, nervosCellRecordRepo, utxoUnspentRecordRepo, userRecordRepo, userAssetRepo, userAssetHistoryRepo, chainTypeAssetRepo, chainTypeAddressAmountRepo, dappApproveRecordRepo, client, userSendRawHistoryRepo, marketCoinHistoryRepo)
	transactionUsecase := biz.NewTransactionUsecase(db, bizLark, dataBundle, bundle)
	migrateScheduler := kanban.NewMigrateScheduler(migrationRepo, bootstrap, bundle, transactionUsecase, kanbanGormDB, options)
	aggerator := kanban.NewAggerator(bootstrap, bundle, options)
	kratosApp := newApp(logLogger, migrateScheduler, aggerator)
	return kratosApp, func() {
		cleanup3()
		cleanup2()
		cleanup()
	}, nil
}
