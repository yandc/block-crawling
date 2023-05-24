package data

import "strings"

const (
	POSTGRES_NOT_FOUND     = "record not found"
	POSTGRES_DUPLICATE_KEY = "duplicate key value"
	PAGE_SIZE              = 10
	MAX_PAGE_SIZE          = 500
	TABLE_POSTFIX          = "_transaction_record"
	MAIN_ADDRESS_PARAM     = "0x"
)

func GetTableName(chainName string) string {
	return strings.ToLower(chainName) + TABLE_POSTFIX
}
