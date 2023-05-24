package kanban

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type KanbanGormDB *gorm.DB

func NewGormDB(conf *conf.Data) (KanbanGormDB, func(), error) {
	log.Info("opening connection to postgres")
	gormlDb, err := gorm.Open(postgres.Open(conf.Kanban.Source), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})

	if err != nil {
		log.Fatale("failed opening connection to postgres of kanban", err)
	}

	sqlDb, err := gormlDb.DB()
	if err != nil {
		log.Fatale("failed opening connection to postgres of kanban", err)
	}

	sqlDb.SetConnMaxLifetime(time.Minute * time.Duration(conf.Kanban.Pool.ConnMaxLifetime))
	sqlDb.SetMaxOpenConns(int(conf.Kanban.Pool.MaxOpenConns))
	sqlDb.SetMaxIdleConns(int(conf.Kanban.Pool.MaxIdleConns))

	cleanup := func() {
		log.Info("closing the postgres resources of kanban")
		err = sqlDb.Close()
		if err != nil {
			log.Errore("failed closeing postgres of kanban", err)
		}
	}

	log.Info("opened connection to postgres of kanban")
	return gormlDb, cleanup, nil
}
