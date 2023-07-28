package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EvmTransactionRecord is a EvmTransactionRecord model.
type EvmTransactionRecord struct {
	Id                   int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash            string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber          int             `json:"blockNumber" form:"blockNumber"`
	Nonce                int64           `json:"nonce" form:"nonce"`
	TransactionHash      string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	OriginalHash         string          `json:"originalHash" form:"originalHash" gorm:"type:character varying(80);default:null;index"`
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

type EvmTransactionRecordWrapper struct {
	EvmTransactionRecord
	Total int64 `json:"total,omitempty"`
}

// EvmTransactionRecordRepo is a Greater repo.
type EvmTransactionRecordRepo interface {
	OutTxCounter

	Save(context.Context, string, *EvmTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrIgnore(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*EvmTransactionRecord) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*EvmTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, string, []*EvmTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveByTransactionHash(context.Context, string, []*EvmTransactionRecord, int) (int64, error)
	UpdateNotSuccessNotFail(context.Context, string, *EvmTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*EvmTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*EvmTransactionRecord, error)
	FindByNonceAndAddress(context.Context, string, string, int64) (*EvmTransactionRecord, error)
	UpdateByNonceAndAddressAndStatus(context.Context, string, string, int64, string) (int64, error)
	ListByID(context.Context, string, int64) ([]*EvmTransactionRecord, error)
	ListAll(context.Context, string) ([]*EvmTransactionRecord, error)
	PageListRecord(context.Context, string, *TransactionRequest) ([]*EvmTransactionRecordWrapper, int64, error)
	PageList(context.Context, string, *TransactionRequest) ([]*EvmTransactionRecord, int64, error)
	PendingByAddress(context.Context, string, string) ([]*EvmTransactionRecord, error)
	PendingByFromAddress(context.Context, string, string) ([]*EvmTransactionRecord, error)
	List(context.Context, string, *TransactionRequest) ([]*EvmTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	Delete(context.Context, string, *TransactionRequest) (int64, error)
	FindLast(context.Context, string) (*EvmTransactionRecord, error)
	FindLastBlockNumberByAddress(context.Context, string, string) (*EvmTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*EvmTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*EvmTransactionRecord, error)
	FindByTxhashLike(context.Context, string, string) ([]*EvmTransactionRecord, error)
	FindParseDataByTxHashAndToken(context.Context, string, string, string) (*EvmTransactionRecord, error)
	ListByTransactionType(context.Context, string, string) ([]*EvmTransactionRecord, error)
	FindFromAddress(context.Context, string) ([]string, error)
	FindLastNonceByAddress(context.Context, string, string) (int64, error)
	ListIncompleteNft(context.Context, string, *TransactionRequest) ([]*EvmTransactionRecord, error)
	CursorListAll(ctx context.Context, tableName string, cursor *int64, pageLimit int) ([]*EvmTransactionRecord, error)
	FindLastNonce(context.Context, string, string) (*EvmTransactionRecord, error)
}

type EvmTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var EvmTransactionRecordRepoClient EvmTransactionRecordRepo

// NewEvmTransactionRecordRepo new a EvmTransactionRecord repo.
func NewEvmTransactionRecordRepo(gormDB *gorm.DB) EvmTransactionRecordRepo {
	EvmTransactionRecordRepoClient = &EvmTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return EvmTransactionRecordRepoClient
}

func (r *EvmTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, evmTransactionRecord *EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(evmTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(evmTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert "+tableName+" failed", err)
		} else {
			log.Errore("insert "+tableName+" failed", err)
		}
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(evmTransactionRecords, len(evmTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(evmTransactionRecords)), Data: 0}
			log.Warne("batch insert "+tableName+" failed", err)
		} else {
			log.Errore("batch insert "+tableName+" failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, tableName string, atomTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		DoNothing: true,
	}).Create(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or ignore "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":               clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":             clause.Column{Table: "excluded", Name: "block_number"},
			"nonce":                    clause.Column{Table: "excluded", Name: "nonce"},
			"transaction_hash":         clause.Column{Table: "excluded", Name: "transaction_hash"},
			"original_hash":            gorm.Expr("case when excluded.original_hash != '' then excluded.original_hash else " + tableName + ".original_hash end"),
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
			"transaction_type":         gorm.Expr("case when " + tableName + ".transaction_type in('mint', 'swap') and excluded.transaction_type not in('mint', 'swap') then " + tableName + ".transaction_type else excluded.transaction_type end"),
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

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	var columnList []clause.Column
	for _, column := range columns {
		columnList = append(columnList, clause.Column{Name: column})
	}
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   columnList,
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":               gorm.Expr("case when excluded.block_hash != '' then excluded.block_hash else " + tableName + ".block_hash end"),
			"block_number":             gorm.Expr("case when excluded.block_number != 0 then excluded.block_number else " + tableName + ".block_number end"),
			"nonce":                    gorm.Expr("case when excluded.nonce != 0 then excluded.nonce else " + tableName + ".nonce end"),
			"transaction_hash":         gorm.Expr("case when excluded.transaction_hash != '' then excluded.transaction_hash else " + tableName + ".transaction_hash end"),
			"original_hash":            gorm.Expr("case when excluded.original_hash != '' then excluded.original_hash else " + tableName + ".original_hash end"),
			"from_address":             gorm.Expr("case when excluded.from_address != '' then excluded.from_address else " + tableName + ".from_address end"),
			"to_address":               gorm.Expr("case when excluded.to_address != '' then excluded.to_address else " + tableName + ".to_address end"),
			"from_uid":                 gorm.Expr("case when excluded.from_uid != '' then excluded.from_uid else " + tableName + ".from_uid end"),
			"to_uid":                   gorm.Expr("case when excluded.to_uid != '' then excluded.to_uid else " + tableName + ".to_uid end"),
			"fee_amount":               gorm.Expr("case when excluded.fee_amount != '' and excluded.fee_amount != '0' then excluded.fee_amount else " + tableName + ".fee_amount end"),
			"amount":                   gorm.Expr("case when excluded.amount != '' and excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":                   gorm.Expr("case when excluded.status != '' then excluded.status else " + tableName + ".status end"),
			"tx_time":                  gorm.Expr("case when excluded.tx_time != 0 then excluded.tx_time else " + tableName + ".tx_time end"),
			"contract_address":         gorm.Expr("case when excluded.contract_address != '' then excluded.contract_address else " + tableName + ".contract_address end"),
			"parse_data":               gorm.Expr("case when excluded.parse_data != '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"type":                     gorm.Expr("case when excluded.type != '' then excluded.type else " + tableName + ".type end"),
			"gas_limit":                gorm.Expr("case when excluded.gas_limit != '' then excluded.gas_limit else " + tableName + ".gas_limit end"),
			"gas_used":                 gorm.Expr("case when excluded.gas_used != '' then excluded.gas_used else " + tableName + ".gas_used end"),
			"gas_price":                gorm.Expr("case when excluded.gas_price != '' then excluded.gas_price else " + tableName + ".gas_price end"),
			"base_fee":                 gorm.Expr("case when excluded.base_fee != '' then excluded.base_fee else " + tableName + ".base_fee end"),
			"max_fee_per_gas":          gorm.Expr("case when excluded.max_fee_per_gas != '' then excluded.max_fee_per_gas else " + tableName + ".max_fee_per_gas end"),
			"max_priority_fee_per_gas": gorm.Expr("case when excluded.max_priority_fee_per_gas != '' then excluded.max_priority_fee_per_gas else " + tableName + ".max_priority_fee_per_gas end"),
			"data":                     gorm.Expr("case when excluded.data != '' then excluded.data else " + tableName + ".data end"),
			"event_log":                gorm.Expr("case when excluded.event_log != '' then excluded.event_log else " + tableName + ".event_log end"),
			"log_address":              gorm.Expr("case when excluded.log_address is not null then excluded.log_address else " + tableName + ".log_address end"),
			"transaction_type":         gorm.Expr("case when excluded.transaction_type != '' then excluded.transaction_type else " + tableName + ".transaction_type end"),
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

func (r *EvmTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, txRecords []*EvmTransactionRecord, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(txRecords)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, subTxRecords)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *EvmTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, tableName string, txRecords []*EvmTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"id"}, txRecords, pageSize)
}

func (r *EvmTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByTransactionHash(ctx context.Context, tableName string, txRecords []*EvmTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"transaction_hash"}, txRecords, pageSize)
}

func (r *EvmTransactionRecordRepoImpl) UpdateNotSuccessNotFail(ctx context.Context, tableName string, evmTransactionRecord *EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id = ? and status not in('success', 'fail')", evmTransactionRecord.Id).Updates(evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&evmTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query "+tableName+" failed", err)
		}
		return nil, err
	}

	return evmTransactionRecord, nil
}

func (r *EvmTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByNonceAndAddress(ctx context.Context, tableName string, fromAddress string, nonce int64) (*EvmTransactionRecord, error) {
	var evmTransactionRecords []*EvmTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName)
	if fromAddress != "" {
		db = db.Where("from_address = ?", fromAddress)
	}
	if nonce >= 0 {
		db = db.Where("nonce = ?", nonce)
	}
	db.Order("id DESC ")
	ret := db.Find(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("findByNonceAndAddress "+tableName+" failed", err)
		return nil, err
	}

	if len(evmTransactionRecords) > 0 {
		return evmTransactionRecords[0], nil

	} else {
		return nil, err
	}
}
func (r *EvmTransactionRecordRepoImpl) UpdateByNonceAndAddressAndStatus(ctx context.Context, tableName string, fromAddress string, nonce int64, status string) (int64, error) {
	ret := r.gormDB.Table(tableName).Where(" status in  ('pending','no_status') and nonce >=0 and nonce <= ? and from_address = ? and client_data is not null and client_data != ''", nonce, fromAddress).Update("status", status)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return 0, err
	}
	return ret.RowsAffected, nil
}

func (r *EvmTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) PageListRecord(ctx context.Context, tableName string, req *TransactionRequest) ([]*EvmTransactionRecordWrapper, int64, error) {
	var evmTransactionRecordList []*EvmTransactionRecordWrapper
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)

	sqlStr := "with t as("
	sqlStr += "select * from (" +
		"select row_number() over (partition by (case when original_hash != '' then original_hash else transaction_hash end) order by tx_time desc), * " +
		"from " + tableName +
		" where 1=1 "
	if req.FromUid != "" {
		sqlStr += " and from_uid = '" + req.FromUid + "'"
	}
	if req.ToUid != "" {
		sqlStr += " and to_uid = '" + req.ToUid + "'"
	}
	if req.FromAddress != "" {
		sqlStr += " and (from_address = '" + req.FromAddress + "' or (log_address is not null and log_address->0 ? '" + req.FromAddress + "'))"
	}
	if req.ToAddress != "" {
		sqlStr += " and (to_address = '" + req.ToAddress + "' or (log_address is not null and log_address->1 ? '" + req.ToAddress + "'))"
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		sqlStr += " and (from_address in(" + fromAddressList + ") or (log_address is not null and log_address->0 ?| array[" + fromAddressList + "]))"
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		sqlStr += " and (to_address in(" + toAddressList + ") or (log_address is not null and log_address->1 ?| array[" + toAddressList + "]))"
	}
	if req.Uid != "" {
		sqlStr += " and (from_uid = '" + req.Uid + "' or to_uid = '" + req.Uid + "')"
	}
	if req.Address != "" {
		sqlStr += " and (from_address = '" + req.Address + "' or to_address = '" + req.Address + "' or (log_address is not null and (log_address->0 ? '" + req.Address + "' or log_address->1 ? '" + req.Address + "')))"
	}
	if req.ContractAddress != "" {
		sqlStr += " and contract_address = '" + req.ContractAddress + "'"
	}
	if len(req.ContractAddressList) > 0 {
		contractAddressList := strings.ReplaceAll(utils.ListToString(req.ContractAddressList), "\"", "'")
		sqlStr += " and contract_address in(" + contractAddressList + ")"
	}
	if len(req.StatusList) > 0 {
		statusList := strings.ReplaceAll(utils.ListToString(req.StatusList), "\"", "'")
		sqlStr += " and status in(" + statusList + ")"
	}
	if len(req.StatusNotInList) > 0 {
		statusNotInList := strings.ReplaceAll(utils.ListToString(req.StatusNotInList), "\"", "'")
		sqlStr += " and status not in(" + statusNotInList + ")"
	}
	if req.TransactionType != "" {
		sqlStr += " and transaction_type = '" + req.TransactionType + "'"
	}
	if req.TransactionTypeNotEqual != "" {
		sqlStr += " and transaction_type != '" + req.TransactionTypeNotEqual + "'"
	}
	if len(req.TransactionTypeList) > 0 {
		transactionTypeList := strings.ReplaceAll(utils.ListToString(req.TransactionTypeList), "\"", "'")
		sqlStr += " and transaction_type in(" + transactionTypeList + ")"
	}
	if len(req.TransactionTypeNotInList) > 0 {
		transactionTypeNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionTypeNotInList), "\"", "'")
		sqlStr += " and transaction_type not in(" + transactionTypeNotInList + ")"
	}
	if req.OperateTypeEmpty {
		sqlStr += " and (operate_type is null or operate_type = '')"
	}
	if req.TransactionHash != "" {
		sqlStr += " and transaction_hash = '" + req.TransactionHash + "'"
	}
	if len(req.TransactionHashList) > 0 {
		transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
		sqlStr += " and transaction_hash in(" + transactionHashList + ")"
	}
	if len(req.TransactionHashNotInList) > 0 {
		transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
		sqlStr += " and transaction_hash not in(" + transactionHashNotInList + ")"
	}
	if req.TransactionHashLike != "" {
		sqlStr += " and transaction_hash like '" + req.TransactionHashLike + "%'"
	}
	if len(req.OriginalHashList) > 0 {
		originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
		sqlStr += " and original_hash in(" + originalHashList + ")"
	}
	if req.Nonce >= 0 {
		sqlStr += " and nonce = " + strconv.Itoa(int(req.Nonce))
	}
	if req.DappDataEmpty {
		sqlStr += " and (dapp_data is null or dapp_data = '')"
	}
	if req.ClientDataNotEmpty {
		sqlStr += " and client_data is not null and client_data != ''"
	}
	if req.StartTime > 0 {
		sqlStr += " and created_at >= " + strconv.Itoa(int(req.StartTime))
	}
	if req.StopTime > 0 {
		sqlStr += " and created_at < " + strconv.Itoa(int(req.StopTime))
	}
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		sqlStr += " and ((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))"
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			sqlStr += " and (transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))"
		} else if req.AssetType == NFT {
			sqlStr += " and (transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))"
		}
	}

	for _, req := range req.OrParamList {
		orSql := ""
		if req.TransactionHash != "" {
			orSql += " or transaction_hash = '" + req.TransactionHash + "'"
		}
		if len(req.TransactionHashList) > 0 {
			transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
			orSql += " or transaction_hash in(" + transactionHashList + ")"
		}
		if len(req.TransactionHashNotInList) > 0 {
			transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
			orSql += " or transaction_hash not in(" + transactionHashNotInList + ")"
		}
		if req.TransactionHashLike != "" {
			orSql += " or transaction_hash like '" + req.TransactionHashLike + "%'"
		}
		if len(req.OriginalHashList) > 0 {
			originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
			orSql += " or original_hash in(" + originalHashList + ")"
		}
		if orSql != "" {
			orSql = "(" + orSql[4:len(orSql)] + ")"
			sqlStr += " and " + orSql
		}
	}

	sqlStr += ") as t1 where row_number = 1" +
		")"

	sqlStr += " select t.* "
	if req.Total {
		sqlStr += ", t1.* "
	}
	sqlStr += " from t "
	if req.Total {
		sqlStr += " inner join (select count(*) as total from t) as t1 on 1=1 "
	}

	sqlStr += " where 1=1 "
	if req.DataDirection > 0 {
		dataDirection := ">"
		if req.DataDirection == 1 {
			dataDirection = "<"
		}
		if req.OrderBy == "" {
			sqlStr += " and t.id " + dataDirection + " " + strconv.Itoa(int(req.StartIndex))
		} else {
			orderBys := strings.Split(req.OrderBy, " ")
			sqlStr += " and t." + orderBys[0] + " " + dataDirection + " " + strconv.Itoa(int(req.StartIndex))
		}
	}

	if req.OrderBy != "" {
		sqlStr += " order by t." + req.OrderBy
	}

	if req.DataDirection == 0 {
		if req.PageNum > 0 {
			sqlStr += " offset " + strconv.Itoa(int(req.PageNum-1)*int(req.PageSize))
		} else {
			sqlStr += " offset 0 "
		}
	}
	sqlStr += " limit " + strconv.Itoa(int(req.PageSize))

	ret := db.Raw(sqlStr).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query "+tableName+" failed", err)
		return nil, 0, err
	}
	if len(evmTransactionRecordList) > 0 {
		total = evmTransactionRecordList[0].Total
	}
	return evmTransactionRecordList, total, nil
}

func (r *EvmTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*EvmTransactionRecord, int64, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
	}
	if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}
	if len(req.ContractAddressList) > 0 {
		db = db.Where("contract_address in(?)", req.ContractAddressList)
	}
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	if req.TransactionType != "" {
		db = db.Where("transaction_type = ?", req.TransactionType)
	}
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}
	if req.OperateTypeEmpty {
		db = db.Where("(operate_type is null or operate_type = '')")
	}
	if req.TransactionHash != "" {
		db = db.Where("transaction_hash = ?", req.TransactionHash)
	}
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
	}
	if len(req.TransactionHashNotInList) > 0 {
		db = db.Where("transaction_hash not in(?)", req.TransactionHashNotInList)
	}
	if req.TransactionHashLike != "" {
		db = db.Where("transaction_hash like ?", req.TransactionHashLike+"%")
	}
	if len(req.OriginalHashList) > 0 {
		db = db.Where("original_hash in(?)", req.OriginalHashList)
	}
	if req.Nonce >= 0 {
		db = db.Where("nonce = ?", req.Nonce)
	}
	if req.DappDataEmpty {
		db = db.Where("(dapp_data is null or dapp_data = '')")
	}
	if req.ClientDataNotEmpty {
		db = db.Where("client_data is not null and client_data != ''")
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
	}
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
		}
	}

	for _, req := range req.OrParamList {
		orSql := ""
		if req.TransactionHash != "" {
			orSql += " or transaction_hash = '" + req.TransactionHash + "'"
		}
		if len(req.TransactionHashList) > 0 {
			transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
			orSql += " or transaction_hash in(" + transactionHashList + ")"
		}
		if len(req.TransactionHashNotInList) > 0 {
			transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
			orSql += " or transaction_hash not in(" + transactionHashNotInList + ")"
		}
		if req.TransactionHashLike != "" {
			orSql += " or transaction_hash like '" + req.TransactionHashLike + "%'"
		}
		if len(req.OriginalHashList) > 0 {
			originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
			orSql += " or original_hash in(" + originalHashList + ")"
		}
		if orSql != "" {
			orSql = "(" + orSql[4:len(orSql)] + ")"
			db = db.Where(orSql)
		}
	}

	if req.Total {
		// 统计总记录数
		db.Count(&total)
	}

	if req.DataDirection > 0 {
		dataDirection := ">"
		if req.DataDirection == 1 {
			dataDirection = "<"
		}
		if req.OrderBy == "" {
			db = db.Where("id "+dataDirection+" ?", req.StartIndex)
		} else {
			orderBys := strings.Split(req.OrderBy, " ")
			db = db.Where(orderBys[0]+" "+dataDirection+" ?", req.StartIndex)
		}
	}

	db = db.Order(req.OrderBy)

	if req.DataDirection == 0 {
		if req.PageNum > 0 {
			db = db.Offset(int(req.PageNum-1) * int(req.PageSize))
		} else {
			db = db.Offset(0)
		}
	}
	db = db.Limit(int(req.PageSize))

	ret := db.Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query "+tableName+" failed", err)
		return nil, 0, err
	}
	return evmTransactionRecordList, total, nil
}

func (r *EvmTransactionRecordRepoImpl) List(ctx context.Context, tableName string, req *TransactionRequest) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
	}
	if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}
	if len(req.ContractAddressList) > 0 {
		db = db.Where("contract_address in(?)", req.ContractAddressList)
	}
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	if req.TransactionType != "" {
		db = db.Where("transaction_type = ?", req.TransactionType)
	}
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}
	if req.OperateTypeEmpty {
		db = db.Where("(operate_type is null or operate_type = '')")
	}
	if req.TransactionHash != "" {
		db = db.Where("transaction_hash = ?", req.TransactionHash)
	}
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
	}
	if len(req.TransactionHashNotInList) > 0 {
		db = db.Where("transaction_hash not in(?)", req.TransactionHashNotInList)
	}
	if req.TransactionHashLike != "" {
		db = db.Where("transaction_hash like ?", req.TransactionHashLike+"%")
	}
	if len(req.OriginalHashList) > 0 {
		db = db.Where("original_hash in(?)", req.OriginalHashList)
	}
	if req.Nonce >= 0 {
		db = db.Where("nonce = ?", req.Nonce)
	}
	if req.DappDataEmpty {
		db = db.Where("(dapp_data is null or dapp_data = '')")
	}
	if req.ClientDataNotEmpty {
		db = db.Where("client_data is not null and client_data != ''")
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
	}
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
		}
	}

	for _, req := range req.OrParamList {
		orSql := ""
		if req.TransactionHash != "" {
			orSql += " or transaction_hash = '" + req.TransactionHash + "'"
		}
		if len(req.TransactionHashList) > 0 {
			transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
			orSql += " or transaction_hash in(" + transactionHashList + ")"
		}
		if len(req.TransactionHashNotInList) > 0 {
			transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
			orSql += " or transaction_hash not in(" + transactionHashNotInList + ")"
		}
		if req.TransactionHashLike != "" {
			orSql += " or transaction_hash like '" + req.TransactionHashLike + "%'"
		}
		if len(req.OriginalHashList) > 0 {
			originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
			orSql += " or original_hash in(" + originalHashList + ")"
		}
		if orSql != "" {
			orSql = "(" + orSql[4:len(orSql)] + ")"
			db = db.Where(orSql)
		}
	}

	db = db.Order(req.OrderBy)

	ret := db.Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("list query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&EvmTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&EvmTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) Delete(ctx context.Context, tableName string, req *TransactionRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
	}
	if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}
	if len(req.ContractAddressList) > 0 {
		db = db.Where("contract_address in(?)", req.ContractAddressList)
	}
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	if req.TransactionType != "" {
		db = db.Where("transaction_type = ?", req.TransactionType)
	}
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}
	if req.OperateTypeEmpty {
		db = db.Where("(operate_type is null or operate_type = '')")
	}
	if req.TransactionHash != "" {
		db = db.Where("transaction_hash = ?", req.TransactionHash)
	}
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
	}
	if len(req.TransactionHashNotInList) > 0 {
		db = db.Where("transaction_hash not in(?)", req.TransactionHashNotInList)
	}
	if req.TransactionHashLike != "" {
		db = db.Where("transaction_hash like ?", req.TransactionHashLike+"%")
	}
	if len(req.OriginalHashList) > 0 {
		db = db.Where("original_hash in(?)", req.OriginalHashList)
	}
	if req.Nonce >= 0 {
		db = db.Where("nonce = ?", req.Nonce)
	}
	if req.DappDataEmpty {
		db = db.Where("(dapp_data is null or dapp_data = '')")
	}
	if req.ClientDataNotEmpty {
		db = db.Where("client_data is not null and client_data != ''")
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
	}
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
		}
	}

	for _, req := range req.OrParamList {
		orSql := ""
		if req.TransactionHash != "" {
			orSql += " or transaction_hash = '" + req.TransactionHash + "'"
		}
		if len(req.TransactionHashList) > 0 {
			transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
			orSql += " or transaction_hash in(" + transactionHashList + ")"
		}
		if len(req.TransactionHashNotInList) > 0 {
			transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
			orSql += " or transaction_hash not in(" + transactionHashNotInList + ")"
		}
		if req.TransactionHashLike != "" {
			orSql += " or transaction_hash like '" + req.TransactionHashLike + "%'"
		}
		if len(req.OriginalHashList) > 0 {
			originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
			orSql += " or original_hash in(" + originalHashList + ")"
		}
		if orSql != "" {
			orSql = "(" + orSql[4:len(orSql)] + ")"
			db = db.Where(orSql)
		}
	}

	ret := db.Delete(&EvmTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return evmTransactionRecord, nil
		}
	}
}

func (r *EvmTransactionRecordRepoImpl) FindLastBlockNumberByAddress(ctx context.Context, tableName string, address string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName)
	if address != "" {
		db = db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return evmTransactionRecord, nil
		}
	}

}

func (r *EvmTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one "+tableName+" by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return evmTransactionRecord, nil
		}
	}
}

func (r *EvmTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
	var amount string
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" || len(req.FromAddressList) > 0 || req.ToUid != "" || len(req.ToAddressList) > 0 {
		if req.FromUid == "" && len(req.FromAddressList) == 0 {
			if req.ToUid != "" {
				db = db.Where("to_uid = ?", req.ToUid)
			}
			if len(req.ToAddressList) > 0 {
				db = db.Where("to_address in(?)", req.ToAddressList)
			}
		} else if req.ToUid == "" && len(req.ToAddressList) == 0 {
			if req.FromUid != "" {
				db = db.Where("from_uid = ?", req.FromUid)
			}
			if len(req.FromAddressList) > 0 {
				db = db.Where("from_address in(?)", req.FromAddressList)
			}
		} else {
			fromToSql := "(("

			if req.FromUid != "" && len(req.FromAddressList) > 0 {
				fromToSql += "from_uid = '" + req.FromUid + "'"
				addressLists := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
				fromToSql += " and from_address in(" + addressLists + ")"
			} else if req.FromUid != "" {
				fromToSql += "from_uid = '" + req.FromUid + "'"
			} else if len(req.FromAddressList) > 0 {
				addressLists := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
				fromToSql += "from_address in(" + addressLists + ")"
			}

			fromToSql += ") or ("

			if req.ToUid != "" && len(req.ToAddressList) > 0 {
				fromToSql += "to_uid = '" + req.ToUid + "'"
				addressLists := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
				fromToSql += " and to_address in(" + addressLists + ")"
			} else if req.ToUid != "" {
				fromToSql += "to_uid = '" + req.ToUid + "'"
			} else if len(req.ToAddressList) > 0 {
				addressLists := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
				fromToSql += "to_address in(" + addressLists + ")"
			}

			fromToSql += "))"
			db = db.Where(fromToSql)
		}
	}
	db = db.Where("status = ?", status)
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}

	ret := db.Pluck("coalesce(sum(cast(amount as numeric)), 0)", &amount)
	err := ret.Error
	if err != nil {
		log.Errore("query amount failed", err)
		return "", err
	}
	return amount, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" by txHash failed", err)
		return nil, err
	}
	if evmTransactionRecord.Id != 0 {
		return evmTransactionRecord, nil
	} else {
		return nil, nil
	}
}
func (r *EvmTransactionRecordRepoImpl) FindByTxhashLike(ctx context.Context, tableName string, txhash string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecord []*EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash like ?", txhash+"%").Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" by txHash failed", err)
		return nil, err
	}
	return evmTransactionRecord, nil

}

func (r *EvmTransactionRecordRepoImpl) FindParseDataByTxHashAndToken(ctx context.Context, tableName string, txhash string, token string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash like ? and contract_address = ?", txhash+"%", token).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" by txHash failed", err)
		return nil, err
	}
	if evmTransactionRecord.Id != 0 {
		return evmTransactionRecord, nil
	} else {
		return nil, nil
	}
}

func (r *EvmTransactionRecordRepoImpl) ListByTransactionType(ctx context.Context, tableName string, transactionType string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecords []*EvmTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = 'success' and block_hash != '' and transaction_type = ?", transactionType).Order("block_number asc").Find(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" by txType failed", err)
		return nil, err
	}
	return evmTransactionRecords, nil
}

func (r *EvmTransactionRecordRepoImpl) FindFromAddress(ctx context.Context, tableName string) ([]string, error) {
	var addresses []string
	ret := r.gormDB.Select("from_address").Table(tableName).Group("from_address").Find(&addresses)
	err := ret.Error
	if err != nil {
		log.Errore("findAddress "+tableName+" failed", err)
		return nil, err
	}

	return addresses, nil
}

func (r *EvmTransactionRecordRepoImpl) FindLastNonceByAddress(ctx context.Context, tableName string, fromAddress string) (int64, error) {
	var nonce int64
	ret := r.gormDB.Select("NONCE").Table(tableName).Where("from_address = ? and (status = 'success' or status = 'fail')", fromAddress).Order("NONCE DESC").Limit(1).Find(&nonce)
	err := ret.Error
	if err != nil {
		log.Errore("findAddress "+tableName+" failed", err)
		return 0, err
	}
	return nonce, nil
}

func (r *EvmTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	db := r.gormDB.Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query evmTransactionRecord failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) PendingByFromAddress(ctx context.Context, tableName string, address string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	db := r.gormDB.Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("from_address = ? ", address)
	}
	ret := db.Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query evmTransactionRecord failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) ListIncompleteNft(ctx context.Context, tableName string, req *TransactionRequest) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecords []*EvmTransactionRecord

	sqlStr := "select transaction_type, transaction_hash, amount, parse_data, event_log from " + tableName +
		" where 1=1 " +
		"and (" +
		"(" +
		"(" +
		"(parse_data not like '%\"collection_name\":\"%' and parse_data not like '%\"item_name\":%') " +
		"or (parse_data like '%\"collection_name\":\"\"%' and parse_data like '%\"item_name\":\"\"%')" +
		") and (" +
		"parse_data like '%\"token_type\":\"ERC721\"%' " +
		"or parse_data like '%\"token_type\":\"ERC1155\"%'" +
		")" +
		") or (" +
		"(" +
		"(event_log not like '%\"collection_name\":\"%' and event_log not like '%\"item_name\":%') " +
		"or (event_log like '%\"collection_name\":\"\"%' and event_log like '%\"item_name\":\"\"%')" +
		") and (" +
		"event_log like '%\"token_type\":\"ERC721\"%' " +
		"or event_log like '%\"token_type\":\"ERC1155\"%'" +
		")" +
		")" +
		")"

	if len(req.StatusNotInList) > 0 {
		statusNotInList := strings.ReplaceAll(utils.ListToString(req.StatusNotInList), "\"", "'")
		sqlStr += " and status not in (" + statusNotInList + ")"
	}
	if len(req.TransactionTypeNotInList) > 0 {
		transactionTypeNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionTypeNotInList), "\"", "'")
		sqlStr += " and transaction_type not in (" + transactionTypeNotInList + ")"
	}

	ret := r.gormDB.WithContext(ctx).Table(tableName).Raw(sqlStr).Find(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("list query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecords, nil
}

func (r *EvmTransactionRecordRepoImpl) CursorListAll(ctx context.Context, tableName string, cursor *int64, pageLimit int) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", cursor).Order("id ASC").Limit(pageLimit).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}

	// Set cursor
	if len(evmTransactionRecordList) > 0 {
		*cursor = evmTransactionRecordList[len(evmTransactionRecordList)-1].Id
	}

	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) FindLastNonce(ctx context.Context, tableName string, fromAddress string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in('success', 'fail') and transaction_type not in('transfer', 'eventLog') and from_address = ?", fromAddress).Order("nonce desc").Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last nonce from "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return evmTransactionRecord, nil
		}
	}
}

func (r *EvmTransactionRecordRepoImpl) CountOut(ctx context.Context, tableName string, address string, toAddress string) (int64, error) {
	return countOutTx(r.gormDB, ctx, tableName, address, toAddress)
}
