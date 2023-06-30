package data

import (
	"context"

	"gorm.io/gorm"
)

type OutTxCounter interface {
	CountOut(ctx context.Context, tableName string, address string, toAddress string) (int64, error)
}

func countOutTx(gormDB *gorm.DB, ctx context.Context, tableName string, address string, toAddress string) (int64, error) {
	var result int64
	ret := gormDB.WithContext(ctx).Table(tableName).Where(
		"from_address = ? AND to_address = ? AND amount::DECIMAL > 0 AND transaction_type IN ('native', 'transfer')",
		address, toAddress,
	).Count(&result)
	if ret.Error != nil {
		return 0, ret.Error
	}
	return result, nil
}
