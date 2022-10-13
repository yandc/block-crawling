package data

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

var GormlDb *gorm.DB

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
	}

	gormlDb.AutoMigrate(&DappApproveRecord{}, &UserAsset{}, &TransactionStatistic{}, &UtxoUnspentRecord{})

	sqlDb, err := gormlDb.DB()
	if err != nil {
		log.Fatale("failed opening connection to postgres", err)
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
	GormlDb = gormlDb
	return gormlDb, cleanup, nil
}
