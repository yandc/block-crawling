package kanban

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EvmTransactionRecordRepo is a repo for kanban.
type EvmTransactionRecordRepo interface {
	TxRecordCursor

	BatchSaveOrUpdateSelective(context.Context, string, []*data.EvmTransactionRecord) (int64, error)
}

var EvmTransactionRecordRepoClient EvmTransactionRecordRepo

func NewEvmTransactionRecordRepo(db KanbanGormDB) EvmTransactionRecordRepo {
	EvmTransactionRecordRepoClient = &evmTransactionRecordRepoImpl{
		db: db,
	}
	return EvmTransactionRecordRepoClient
}

type evmTransactionRecordRepoImpl struct {
	db *gorm.DB
}

// CursorList implements EvmTransactionRecordRepo
func (r *evmTransactionRecordRepoImpl) CursorList(ctx context.Context, txTime int64, chainName string, limit int, cursor *uint64) ([]*TxRecord, error) {
	tableName := GetShardingTable(data.GetTableName(chainName), txTime)
	db := r.db.Table(tableName)
	if cursor != nil && *cursor > 0 {
		db = db.Where("id > ?", *cursor)
	}
	var evmTransactionRecords []*data.EvmTransactionRecord
	ret := db.Order("id ASC").Limit(limit).Find(&evmTransactionRecords)
	if ret.Error != nil {
		return nil, ret.Error
	}
	results := make([]*TxRecord, 0, len(evmTransactionRecords))
	for _, item := range evmTransactionRecords {
		results = append(results, EVMRecordIntoTxRecord(item))
		*cursor = uint64(item.Id)
	}
	return results, nil
}

func EVMRecordIntoTxRecord(item *data.EvmTransactionRecord) *TxRecord {
	return &TxRecord{
		FromAddress:     item.FromAddress,
		ToAddress:       item.ToAddress,
		Amount:          item.Amount,
		TxTime:          item.TxTime,
		Status:          item.Status,
		ContractAddress: item.ContractAddress,
		TransactionType: item.TransactionType,
	}
}

func (r *evmTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*data.EvmTransactionRecord) (int64, error) {
	byTables := make(map[string][]*data.EvmTransactionRecord)
	for _, item := range evmTransactionRecords {
		shardingTable := GetShardingTable(tableName, item.TxTime)
		if _, ok := byTables[shardingTable]; !ok {
			byTables[shardingTable] = nil
		}
		byTables[shardingTable] = append(byTables[shardingTable], item)
	}
	var rows int64
	for table, records := range byTables {
		if nrows, err := r.doBatchSaveOrUpdateSelective(ctx, table, records); err != nil {
			return rows, err
		} else {
			rows += nrows
		}
	}
	return rows, nil
}

func (r *evmTransactionRecordRepoImpl) doBatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*data.EvmTransactionRecord) (int64, error) {
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":               clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":             clause.Column{Table: "excluded", Name: "block_number"},
			"nonce":                    clause.Column{Table: "excluded", Name: "nonce"},
			"transaction_hash":         clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":             clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":               gorm.Expr("case when '" + tableName + "' = 'ronin_transaction_record' and " + tableName + ".to_address like 'ronin:%' and excluded.to_address != '' then 'ronin:' || substring (excluded.to_address, 3) else excluded.to_address end"),
			"from_uid":                 clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":                   clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":               clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":                   clause.Column{Table: "excluded", Name: "amount"},
			"status":                   gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') then " + tableName + ".status else excluded.status end"),
			"tx_time":                  clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address":         clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":               clause.Column{Table: "excluded", Name: "parse_data"},
			"type":                     clause.Column{Table: "excluded", Name: "type"},
			"gas_limit":                clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":                 clause.Column{Table: "excluded", Name: "gas_used"},
			"gas_price":                clause.Column{Table: "excluded", Name: "gas_price"},
			"base_fee":                 clause.Column{Table: "excluded", Name: "base_fee"},
			"max_fee_per_gas":          clause.Column{Table: "excluded", Name: "max_fee_per_gas"},
			"max_priority_fee_per_gas": clause.Column{Table: "excluded", Name: "max_priority_fee_per_gas"},
			"data":                     clause.Column{Table: "excluded", Name: "data"},
			"event_log":                clause.Column{Table: "excluded", Name: "event_log"},
			"log_address":              clause.Column{Table: "excluded", Name: "log_address"},
			"transaction_type":         clause.Column{Table: "excluded", Name: "transaction_type"},
			"operate_type":             gorm.Expr("case when excluded.operate_type != '' then excluded.operate_type when " + tableName + ".transaction_type in('cancel', 'speed_up') then " + tableName + ".transaction_type else " + tableName + ".operate_type end"),
			"dapp_data":                gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":              gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":               gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}
