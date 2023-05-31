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
	"block-crawling/internal/platform"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

// Injectors from wire.go:

// wireApp init kratos application.
func wireApp(server *conf.Server, confData *conf.Data, app *conf.App, addressServer *conf.AddressServer, lark *conf.Lark, logger *conf.Logger, transaction *conf.Transaction, bootstrap *conf.Bootstrap, logLogger log.Logger, options *kanban.Options) (*kratos.App, func(), error) {
	kanbanGormDB, cleanup, err := kanban2.NewGormDB(confData)
	if err != nil {
		return nil, nil, err
	}
	db, cleanup2, err := data.NewGormDB(confData)
	if err != nil {
		cleanup()
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
	userGormDB, cleanup3, err := data.NewUserGormDB(confData)
	if err != nil {
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	userRecordRepo := data.NewUserRecordRepo(userGormDB)
	userAssetRepo := data.NewUserAssetRepo(db)
	dappApproveRecordRepo := data.NewDappApproveRecordRepo(db)
	client := data.NewRedisClient(confData)
	userSendRawHistoryRepo := data.NewUserSendRawHistoryRepo(db)
	bundle := data.NewBundle(atomTransactionRecordRepo, btcTransactionRecordRepo, dotTransactionRecordRepo, evmTransactionRecordRepo, stcTransactionRecordRepo, trxTransactionRecordRepo, aptTransactionRecordRepo, suiTransactionRecordRepo, solTransactionRecordRepo, ckbTransactionRecordRepo, csprTransactionRecordRepo, kasTransactionRecordRepo, userNftAssetRepo, nftRecordHistoryRepo, transactionStatisticRepo, nervosCellRecordRepo, utxoUnspentRecordRepo, userRecordRepo, userAssetRepo, dappApproveRecordRepo, client, userSendRawHistoryRepo)
	appConf := biz.NewConfig(app)
	bizLark := biz.NewLark(lark)
	platformServer := platform.NewPlatform(bootstrap, bundle, appConf, db, bizLark)
	kanbanEvmTransactionRecordRepo := kanban2.NewEvmTransactionRecordRepo(kanbanGormDB)
	walletRepo := kanban2.NewWalletRepo(kanbanGormDB)
	trendingRepo := kanban2.NewTrendingRepo(kanbanGormDB)
	kanbanBundle := kanban2.NewBundle(kanbanEvmTransactionRecordRepo, walletRepo, trendingRepo)
	timeMachine := kanban.NewTimeMachine(bootstrap, kanbanGormDB, platformServer, kanbanBundle, options)
	kratosApp := newApp(logLogger, timeMachine)
	return kratosApp, func() {
		cleanup3()
		cleanup2()
		cleanup()
	}, nil
}
