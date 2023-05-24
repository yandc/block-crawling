package biz

import (
	"block-crawling/internal/data"

	"gorm.io/gorm"
)

func DynamicCreateTable(gormDb *gorm.DB, table string, chainType string) {
	switch chainType {
	case STC:
		gormDb.Table(table).AutoMigrate(&data.StcTransactionRecord{})
	case EVM:
		gormDb.Table(table).AutoMigrate(&data.EvmTransactionRecord{})
	case BTC:
		gormDb.Table(table).AutoMigrate(&data.BtcTransactionRecord{})
	case TVM:
		gormDb.Table(table).AutoMigrate(&data.TrxTransactionRecord{})
	case APTOS:
		gormDb.Table(table).AutoMigrate(&data.AptTransactionRecord{})
	case SUI:
		gormDb.Table(table).AutoMigrate(&data.SuiTransactionRecord{})
	case SOLANA:
		gormDb.Table(table).AutoMigrate(&data.SolTransactionRecord{})
	case NERVOS:
		gormDb.Table(table).AutoMigrate(&data.CkbTransactionRecord{})
	case CASPER:
		gormDb.Table(table).AutoMigrate(&data.CsprTransactionRecord{})
	case COSMOS:
		gormDb.Table(table).AutoMigrate(&data.AtomTransactionRecord{})
	case POLKADOT:
		gormDb.Table(table).AutoMigrate(&data.DotTransactionRecord{})
	}
}
