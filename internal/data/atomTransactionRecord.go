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
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// AtomTransactionRecord is a AtomTransactionRecord model.
type AtomTransactionRecord struct {
	DefaultVersionMarkerIn

	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	Nonce           int64           `json:"nonce" form:"nonce"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(68);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(68);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"` // gorm:"type:decimal.Decimal"
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(20);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(128);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	GasLimit        string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed         string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice        string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	Data            string          `json:"data" form:"data"`
	EventLog        string          `json:"eventLog" form:"eventLog"`
	LogAddress      datatypes.JSON  `json:"logAddress,omitempty" form:"logAddress" gorm:"type:jsonb"` //gorm:"type:jsonb;index:,type:gin"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	TokenInfo       string          `json:"tokenInfo" form:"tokenInfo"`
	SendTime        int64           `json:"sendTime" form:"sendTime"`
	SessionId       string          `json:"sessionId" form:"sessionId" gorm:"type:character varying(36);default:null;index:,unique"`
	ShortHost       string          `json:"shortHost" form:"shortHost" gorm:"type:character varying(200);default:null;"`
	Memo            string          `json:"memo" form:"memo"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

func (r *AtomTransactionRecord) Version() string {
	return "20240104"
}

// AtomTransactionRecordRepo is a Greater repo.
type AtomTransactionRecordRepo interface {
	Save(context.Context, string, *AtomTransactionRecord) (int64, error)
	SaveOrUpdateClient(context.Context, string, *AtomTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*AtomTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*AtomTransactionRecord) (int64, error)
	BatchSaveOrIgnore(context.Context, string, []*AtomTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*AtomTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*AtomTransactionRecord) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*AtomTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, string, []*AtomTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveByTransactionHash(context.Context, string, []*AtomTransactionRecord, int) (int64, error)
	Update(context.Context, string, *AtomTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*AtomTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*AtomTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*AtomTransactionRecord, error)
	ListAll(context.Context, string) ([]*AtomTransactionRecord, error)
	PageList(context.Context, string, *TransactionRequest) ([]*AtomTransactionRecord, int64, error)
	PendingByAddress(context.Context, string, string) ([]*AtomTransactionRecord, error)
	List(context.Context, string, *TransactionRequest) ([]*AtomTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*AtomTransactionRecord, error)
	PageListAllCallBack(context.Context, string, *TransactionRequest, func(list []*AtomTransactionRecord) error, ...time.Duration) error
	PageListAll(context.Context, string, *TransactionRequest, ...time.Duration) ([]*AtomTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*AtomTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxHash(context.Context, string, string) (*AtomTransactionRecord, error)
	SelectColumnByTxHash(context.Context, string, string, []string) (*AtomTransactionRecord, error)
	ListByTransactionType(context.Context, string, string) ([]*AtomTransactionRecord, error)
	FindLastNonce(context.Context, string, string) (*AtomTransactionRecord, error)
	ListIncompleteNft(context.Context, string, *TransactionRequest) ([]*AtomTransactionRecord, error)
}

type AtomTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var AtomTransactionRecordRepoClient AtomTransactionRecordRepo

// NewAtomTransactionRecordRepo new a AtomTransactionRecord repo.
func NewAtomTransactionRecordRepo(gormDB *gorm.DB) AtomTransactionRecordRepo {
	AtomTransactionRecordRepoClient = &AtomTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return AtomTransactionRecordRepoClient
}

func (r *AtomTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, atomTransactionRecord *AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(atomTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(atomTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert atomTransactionRecord failed", err)
		} else {
			log.Errore("insert "+tableName+" failed", err)
		}
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) SaveOrUpdateClient(ctx context.Context, tableName string, atomTransactionRecord *AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"dapp_data":   gorm.Expr("excluded.dapp_data"),
			"client_data": gorm.Expr("excluded.client_data"),
			"send_time":   gorm.Expr("excluded.send_time"),
			"session_id":  gorm.Expr("excluded.session_id"),
			"short_host":  gorm.Expr("excluded.short_host"),
			//"memo":                             gorm.Expr("case when + " + tableName + ".memo != '' then excluded.memo else " + tableName + ".memo end"),
			"memo":       gorm.Expr("case when " + tableName + ".status in('success', 'fail') then " + tableName + ".memo else excluded.memo end"),
			"updated_at": gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("更新atom "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, atomTransactionRecords []*AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(atomTransactionRecords, len(atomTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(atomTransactionRecords)), Data: 0}
			log.Warne("batch insert "+tableName+" failed", err)
		} else {
			log.Errore("batch insert "+tableName+" failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, atomTransactionRecords []*AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, tableName string, atomTransactionRecords []*AtomTransactionRecord) (int64, error) {
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

func (r *AtomTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, atomTransactionRecords []*AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":       clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":     clause.Column{Table: "excluded", Name: "block_number"},
			"nonce":            clause.Column{Table: "excluded", Name: "nonce"},
			"transaction_hash": clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":     clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":       clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":         clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":           clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":       clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":           clause.Column{Table: "excluded", Name: "amount"},
			"status":           gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') or (" + tableName + ".status in('success', 'fail') and excluded.status = 'dropped_replaced') then " + tableName + ".status else excluded.status end"),
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address": clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":       gorm.Expr("case when excluded.status = 'success' or excluded.parse_data not like '%\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}' or " + tableName + ".parse_data = '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"gas_limit":        clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":         clause.Column{Table: "excluded", Name: "gas_used"},
			"gas_price":        clause.Column{Table: "excluded", Name: "gas_price"},
			"data":             clause.Column{Table: "excluded", Name: "data"},
			"event_log":        clause.Column{Table: "excluded", Name: "event_log"},
			"log_address":      clause.Column{Table: "excluded", Name: "log_address"},
			"transaction_type": gorm.Expr("case when " + tableName + ".transaction_type in('mint', 'swap') and excluded.transaction_type not in('mint', 'swap') then " + tableName + ".transaction_type else excluded.transaction_type end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"token_info":       gorm.Expr("case when excluded.status = 'success' or excluded.token_info != '{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}' or " + tableName + ".token_info = '' then excluded.token_info else " + tableName + ".token_info end"),
			"send_time":        gorm.Expr("case when excluded.send_time != 0 then excluded.send_time else " + tableName + ".send_time end"),
			"session_id":       gorm.Expr("case when excluded.session_id != '' then excluded.session_id else " + tableName + ".session_id end"),
			"short_host":       gorm.Expr("case when excluded.short_host != '' then excluded.short_host else " + tableName + ".short_host end"),
			"memo":             gorm.Expr("case when excluded.memo != '' then excluded.memo else " + tableName + ".memo end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective atomTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, atomTransactionRecords []*AtomTransactionRecord) (int64, error) {
	var columnList []clause.Column
	for _, column := range columns {
		columnList = append(columnList, clause.Column{Name: column})
	}
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   columnList,
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":       gorm.Expr("case when excluded.block_hash != '' then excluded.block_hash else " + tableName + ".block_hash end"),
			"block_number":     gorm.Expr("case when excluded.block_number != 0 then excluded.block_number else " + tableName + ".block_number end"),
			"nonce":            gorm.Expr("case when excluded.nonce != 0 then excluded.nonce else " + tableName + ".nonce end"),
			"transaction_hash": gorm.Expr("case when excluded.transaction_hash != '' then excluded.transaction_hash else " + tableName + ".transaction_hash end"),
			"from_address":     gorm.Expr("case when excluded.from_address != '' then excluded.from_address else " + tableName + ".from_address end"),
			"to_address":       gorm.Expr("case when excluded.to_address != '' then excluded.to_address else " + tableName + ".to_address end"),
			"from_uid":         gorm.Expr("case when excluded.from_uid != '' then excluded.from_uid else " + tableName + ".from_uid end"),
			"to_uid":           gorm.Expr("case when excluded.to_uid != '' then excluded.to_uid else " + tableName + ".to_uid end"),
			"fee_amount":       gorm.Expr("case when excluded.fee_amount != '' and excluded.fee_amount != '0' then excluded.fee_amount else " + tableName + ".fee_amount end"),
			"amount":           gorm.Expr("case when excluded.amount != '' and excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":           gorm.Expr("case when excluded.status != '' then excluded.status else " + tableName + ".status end"),
			"tx_time":          gorm.Expr("case when excluded.tx_time != 0 then excluded.tx_time else " + tableName + ".tx_time end"),
			"contract_address": gorm.Expr("case when excluded.contract_address != '' then excluded.contract_address else " + tableName + ".contract_address end"),
			"parse_data":       gorm.Expr("case when excluded.parse_data != '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"gas_limit":        gorm.Expr("case when excluded.gas_limit != '' then excluded.gas_limit else " + tableName + ".gas_limit end"),
			"gas_used":         gorm.Expr("case when excluded.gas_used != '' then excluded.gas_used else " + tableName + ".gas_used end"),
			"gas_price":        gorm.Expr("case when excluded.gas_price != '' then excluded.gas_price else " + tableName + ".gas_price end"),
			"data":             gorm.Expr("case when excluded.data != '' then excluded.data else " + tableName + ".data end"),
			"event_log":        gorm.Expr("case when excluded.event_log != '' then excluded.event_log else " + tableName + ".event_log end"),
			"log_address":      gorm.Expr("case when excluded.log_address is not null then excluded.log_address else " + tableName + ".log_address end"),
			"transaction_type": gorm.Expr("case when excluded.transaction_type != '' then excluded.transaction_type else " + tableName + ".transaction_type end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"token_info":       gorm.Expr("case when excluded.token_info != '' then excluded.token_info else " + tableName + ".token_info end"),
			"send_time":        gorm.Expr("case when excluded.send_time != 0 then excluded.send_time else " + tableName + ".send_time end"),
			"session_id":       gorm.Expr("case when excluded.session_id != '' then excluded.session_id else " + tableName + ".session_id end"),
			"short_host":       gorm.Expr("case when excluded.short_host != '' then excluded.short_host else " + tableName + ".short_host end"),
			"memo":             gorm.Expr("case when excluded.memo != '' then excluded.memo else " + tableName + ".memo end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AtomTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, txRecords []*AtomTransactionRecord, pageSize int) (int64, error) {
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

func (r *AtomTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, tableName string, txRecords []*AtomTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"id"}, txRecords, pageSize)
}

func (r *AtomTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByTransactionHash(ctx context.Context, tableName string, txRecords []*AtomTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"transaction_hash"}, txRecords, pageSize)
}

func (r *AtomTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, atomTransactionRecord *AtomTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id = ?", atomTransactionRecord.Id).Updates(atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *AtomTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&atomTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query "+tableName+" failed", err)
		}
		return nil, err
	}

	return atomTransactionRecord, nil
}

func (r *AtomTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return atomTransactionRecordList, nil
}

func (r *AtomTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return atomTransactionRecordList, nil
}

func (r *AtomTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return atomTransactionRecordList, nil
}

func (r *AtomTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*AtomTransactionRecord, int64, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
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
	if req.TransactionTypeNotEquals != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEquals)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
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

	ret := db.Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query atomTransactionRecord failed", err)
		return nil, 0, err
	}
	return atomTransactionRecordList, total, nil
}

func (r *AtomTransactionRecordRepoImpl) PageListAllCallBack(ctx context.Context, tableName string, req *TransactionRequest, fn func(list []*AtomTransactionRecord) error, timeDuration ...time.Duration) error {
	var timeout time.Duration
	if len(timeDuration) > 0 {
		timeout = timeDuration[0]
	} else {
		timeout = 1_000 * time.Millisecond
	}
	req.DataDirection = 2
	for {
		atomTransactionRecords, _, err := r.PageList(ctx, tableName, req)
		if err != nil {
			return err
		}
		dataLen := int32(len(atomTransactionRecords))
		if dataLen == 0 {
			break
		}

		err = fn(atomTransactionRecords)
		if err != nil {
			return err
		}
		if dataLen < req.PageSize {
			break
		}
		req.StartIndex = atomTransactionRecords[dataLen-1].Id
		time.Sleep(timeout)
	}
	return nil
}

func (r *AtomTransactionRecordRepoImpl) PageListAll(ctx context.Context, tableName string, req *TransactionRequest, timeDuration ...time.Duration) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
	err := r.PageListAllCallBack(nil, tableName, req, func(atomTransactionRecords []*AtomTransactionRecord) error {
		atomTransactionRecordList = append(atomTransactionRecordList, atomTransactionRecords...)
		return nil
	}, timeDuration...)
	return atomTransactionRecordList, err
}

func (r *AtomTransactionRecordRepoImpl) List(ctx context.Context, tableName string, req *TransactionRequest) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
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
	if req.TransactionTypeNotEquals != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEquals)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
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

	db = db.Order(req.OrderBy)

	ret := db.Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query atomTransactionRecord failed", err)
		return nil, err
	}
	return atomTransactionRecordList, nil
}

func (r *AtomTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&AtomTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *AtomTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&AtomTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *AtomTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return atomTransactionRecord, nil
		}
	}
}

func (r *AtomTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one atomTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return atomTransactionRecord, nil
		}
	}
}

func (r *AtomTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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

func (r *AtomTransactionRecordRepoImpl) FindByTxHash(ctx context.Context, tableName string, txHash string) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Find(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query atomTransactionRecord by txHash failed", err)
		return nil, err
	}
	if atomTransactionRecord.Id != 0 {
		return atomTransactionRecord, nil
	} else {
		return nil, nil
	}
}

func (r *AtomTransactionRecordRepoImpl) SelectColumnByTxHash(ctx context.Context, tableName string, txHash string, selectColumn []string) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Select(selectColumn).Find(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" for column by txHash failed", err)
		return nil, err
	}
	return atomTransactionRecord, nil
}

func (r *AtomTransactionRecordRepoImpl) ListByTransactionType(ctx context.Context, tableName string, transactionType string) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecords []*AtomTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = 'success' and block_hash != '' and transaction_type = ?", transactionType).Order("block_number asc").Find(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("query atomTransactionRecord by txType failed", err)
		return nil, err
	}
	return atomTransactionRecords, nil
}

func (r *AtomTransactionRecordRepoImpl) FindLastNonce(ctx context.Context, tableName string, fromAddress string) (*AtomTransactionRecord, error) {
	var atomTransactionRecord *AtomTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in('success', 'fail') and from_address = ?", fromAddress).Order("nonce desc").Limit(1).Find(&atomTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last nonce from "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return atomTransactionRecord, nil
		}
	}
}

func (r *AtomTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecordList []*AtomTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&atomTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query atomTransactionRecord failed", err)
		return nil, err
	}
	return atomTransactionRecordList, nil
}

func (r *AtomTransactionRecordRepoImpl) ListIncompleteNft(ctx context.Context, tableName string, req *TransactionRequest) ([]*AtomTransactionRecord, error) {
	var atomTransactionRecords []*AtomTransactionRecord

	sqlStr := "select transaction_type, transaction_hash, amount, token_info, event_log from " + tableName +
		" where 1=1 " +
		"and (" +
		"(" +
		"(" +
		"(token_info not like '%\"collection_name\":\"%' and token_info not like '%\"item_name\":%') " +
		"or (token_info like '%\"collection_name\":\"\"%' and token_info like '%\"item_name\":\"\"%')" +
		") and (" +
		"token_info like '%\"token_type\":\"CosmosNFT\"%' " +
		")" +
		") or (" +
		"(" +
		"(event_log not like '%\"collection_name\":\"%' and event_log not like '%\"item_name\":%') " +
		"or (event_log like '%\"collection_name\":\"\"%' and event_log like '%\"item_name\":\"\"%')" +
		") and (" +
		"event_log like '%\"token_type\":\"CosmosNFT\"%'" +
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

	ret := r.gormDB.WithContext(ctx).Table(tableName).Raw(sqlStr).Find(&atomTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("list query atomTransactionRecord failed", err)
		return nil, err
	}
	return atomTransactionRecords, nil
}
