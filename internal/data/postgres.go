package data

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type UserGormDB *gorm.DB

var BlockCreawlingDB *gorm.DB

// NewGormDB use grom connect to postgres
func NewGormDB(conf *conf.Data) (*gorm.DB, func(), error) {
	log.Info("opening connection to postgres")
	gormlDb, err := gorm.Open(postgres.Open(conf.Database.Source), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})

	if err != nil {
		log.Fatale("failed opening connection to postgres", err)
		return nil, nil, err
	}

	err = gormlDb.AutoMigrate(&DappApproveRecord{}, &UserAsset{}, &UserAssetHistory{}, &ChainTypeAsset{}, &ChainTypeAddressAmount{}, &UserNftAsset{}, &TransactionStatistic{}, &TransactionCount{}, &UtxoUnspentRecord{}, &NervosCellRecord{}, &NftRecordHistory{}, &UserSendRawHistory{}, &MarketCoinHistory{}, &Migration{})
	if err != nil {
		log.Fatale("failed execute autoMigrate to postgres", err)
		return nil, nil, err
	}

	sqlDb, err := gormlDb.DB()
	if err != nil {
		log.Fatale("failed get database from postgres", err)
		return nil, nil, err
	}

	sqlDb.SetConnMaxLifetime(time.Minute * time.Duration(conf.Database.Pool.ConnMaxLifetime))
	sqlDb.SetMaxOpenConns(int(conf.Database.Pool.MaxOpenConns))
	sqlDb.SetMaxIdleConns(int(conf.Database.Pool.MaxIdleConns))

	cleanup := func() {
		log.Info("closing the postgres resources")
		err = sqlDb.Close()
		if err != nil {
			log.Errore("failed closeing postgres", err)
		}
	}

	log.Info("opened connection to postgres")
	BlockCreawlingDB = gormlDb
	return gormlDb, cleanup, nil
}

func NewUserGormDB(conf *conf.Data) (UserGormDB, func(), error) {
	log.Info("opening connection to postgres")
	gormlDb, err := gorm.Open(postgres.Open(conf.User.Source), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})

	if err != nil {
		log.Fatale("failed opening connection to postgres of usercenter", err)
	}

	sqlDb, err := gormlDb.DB()
	if err != nil {
		log.Fatale("failed opening connection to postgres of usercenter", err)
	}

	sqlDb.SetConnMaxLifetime(time.Minute * time.Duration(conf.User.Pool.ConnMaxLifetime))
	sqlDb.SetMaxOpenConns(int(conf.User.Pool.MaxOpenConns))
	sqlDb.SetMaxIdleConns(int(conf.User.Pool.MaxIdleConns))

	cleanup := func() {
		log.Info("closing the postgres resources of usercenter")
		err = sqlDb.Close()
		if err != nil {
			log.Errore("failed closeing postgres of usercenter", err)
		}
	}

	log.Info("opened connection to postgres of usercenter")
	return gormlDb, cleanup, nil
}
