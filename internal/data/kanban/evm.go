package kanban

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type KanbanEvmTransactionRecord struct {
	Id                   int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash            string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber          int             `json:"blockNumber" form:"blockNumber"`
	Nonce                int64           `json:"nonce" form:"nonce"`
	TransactionHash      string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress          string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(42);index"`
	ToAddress            string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(46);index"`
	FromUid              string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid                string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount            decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount               decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status               string          `json:"status" form:"status" gorm:"type:character varying(20);index"`
	TxTime               int64           `json:"txTime" form:"txTime"`
	ContractAddress      string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(42);index"`
	ParseData            string          `json:"parseData" form:"parseData"`
	Type                 string          `json:"type" form:"type" gorm:"type:character varying(2)"`
	GasLimit             string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(32)"`
	GasUsed              string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(32)"`
	GasPrice             string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(32)"`
	BaseFee              string          `json:"baseFee" form:"baseFee" gorm:"type:character varying(32)"`
	MaxFeePerGas         string          `json:"maxFeePerGas" form:"maxFeePerGas" gorm:"type:character varying(32)"`
	MaxPriorityFeePerGas string          `json:"maxPriorityFeePerGas" form:"maxPriorityFeePerGas" gorm:"type:character varying(32)"`
	Data                 string          `json:"data" form:"data"`
	EventLog             string          `json:"eventLog" form:"eventLog"`
	LogAddress           datatypes.JSON  `json:"logAddress" form:"logAddress" gorm:"type:jsonb"`
	TransactionType      string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	OperateType          string          `json:"operateType" form:"operateType" gorm:"type:character varying(8)"`
	DappData             string          `json:"dappData" form:"dappData"`
	ClientData           string          `json:"clientData" form:"clientData"`
	CreatedAt            int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt            int64           `json:"updatedAt" form:"updatedAt"`
}

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
	var evmTransactionRecords []*KanbanEvmTransactionRecord
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

func EVMRecordIntoTxRecord(item *KanbanEvmTransactionRecord) *TxRecord {
	return &TxRecord{
		FromAddress:     item.FromAddress,
		ToAddress:       item.ToAddress,
		Amount:          item.Amount,
		TxTime:          item.TxTime,
		Status:          item.Status,
		ContractAddress: item.ContractAddress,
		TransactionType: item.TransactionType,
		Data:            item.Data,
	}
}

func (r *evmTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*data.EvmTransactionRecord) (int64, error) {
	byTables := make(map[string][]*KanbanEvmTransactionRecord)
	for _, item := range evmTransactionRecords {
		shardingTable := GetShardingTable(tableName, item.TxTime)
		if _, ok := byTables[shardingTable]; !ok {
			byTables[shardingTable] = nil
		}
		byTables[shardingTable] = append(byTables[shardingTable], &KanbanEvmTransactionRecord{
			BlockHash:            item.BlockHash,
			BlockNumber:          item.BlockNumber,
			Nonce:                item.Nonce,
			TransactionHash:      item.TransactionHash,
			FromAddress:          item.FromAddress,
			ToAddress:            item.ToAddress,
			FromUid:              item.FromUid,
			ToUid:                item.ToUid,
			FeeAmount:            item.FeeAmount,
			Amount:               item.Amount,
			Status:               item.Status,
			TxTime:               item.TxTime,
			ContractAddress:      item.ContractAddress,
			ParseData:            item.ParseData,
			Type:                 item.Type,
			GasLimit:             item.GasLimit,
			GasUsed:              item.GasUsed,
			GasPrice:             item.GasPrice,
			BaseFee:              item.BaseFee,
			MaxFeePerGas:         item.MaxFeePerGas,
			MaxPriorityFeePerGas: item.MaxPriorityFeePerGas,
			Data:                 item.Data,
			EventLog:             item.EventLog,
			LogAddress:           item.LogAddress,
			TransactionType:      item.TransactionType,
			OperateType:          item.OperateType,
			DappData:             item.DappData,
			ClientData:           item.ClientData,
			CreatedAt:            item.CreatedAt,
			UpdatedAt:            item.UpdatedAt,
		})
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

func (r *evmTransactionRecordRepoImpl) doBatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*KanbanEvmTransactionRecord) (int64, error) {
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
			"status":                   gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') or (" + tableName + ".status in('success', 'fail') and excluded.status = 'dropped_replaced') then " + tableName + ".status else excluded.status end"),
			"tx_time":                  clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address":         clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":               gorm.Expr("case when excluded.status = 'success' or excluded.parse_data not like '%\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}' or " + tableName + ".parse_data = '' then excluded.parse_data else " + tableName + ".parse_data end"),
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
	}, clause.Returning{
		Columns: []clause.Column{
			{Name: "id"},
			{Name: "transaction_hash"},
		},
	}).Create(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}
