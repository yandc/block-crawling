package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

func DynamicCreateTable(mr data.MigrationRepo, gormDb *gorm.DB, table string, chainType string) {
	var record interface{}
	switch chainType {
	case STC:
		record = &data.StcTransactionRecord{}
	case EVM:
		record = &data.EvmTransactionRecord{}
	case BTC:
		record = &data.BtcTransactionRecord{}
	case TVM:
		record = &data.TrxTransactionRecord{}
	case APTOS:
		record = &data.AptTransactionRecord{}
	case SUI:
		record = &data.SuiTransactionRecord{}
	case SOLANA:
		record = &data.SolTransactionRecord{}
	case NERVOS:
		record = &data.CkbTransactionRecord{}
	case CASPER:
		record = &data.CsprTransactionRecord{}
	case COSMOS:
		record = &data.AtomTransactionRecord{}
	case POLKADOT:
		record = &data.DotTransactionRecord{}
	case KASPA:
		record = &data.KasTransactionRecord{}
	}
	if record == nil {
		// Should we panic here?
		return
	}
	if v, ok := record.(data.VersionMarker); ok {
		version := v.Version()
		migrated, err := mr.FindOne(context.Background(), table, version)
		if err == nil && migrated != nil {
			log.Info(
				"TABLE WITH VERSION HAS ALREADY MIGRATED",
				zap.String("table", table),
				zap.String("version", version),
				zap.Int64("createdAt", migrated.CreatedAt),
			)
			return
		}
		log.Info(
			"TABLE WITH VERSION HASN'T MIGRATED",
			zap.String("table", table),
			zap.String("version", version),
			zap.Error(err),
		)
		if err := gormDb.Table(table).AutoMigrate(record); err != nil {
			log.Error(
				"MIGRATION FAILED",
				zap.String("table", table),
				zap.String("version", version),
				zap.Error(err),
			)
			return
		}
		mr.Create(context.Background(), &data.Migration{
			Table:     table,
			Version:   version,
			CreatedAt: time.Now().Unix(),
		})
		return
	}
	gormDb.Table(table).AutoMigrate(record)
}
