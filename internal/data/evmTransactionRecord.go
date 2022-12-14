package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

// EvmTransactionRecord is a EvmTransactionRecord model.
type EvmTransactionRecord struct {
	Id                   int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash            string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber          int             `json:"blockNumber" form:"blockNumber"`
	Nonce                int64           `json:"nonce" form:"nonce"`
	TransactionHash      string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress          string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(42);index"`
	ToAddress            string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(42);index"`
	FromUid              string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid                string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount            decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"` // gorm:"type:decimal.Decimal"
	Amount               decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status               string          `json:"status" form:"status" gorm:"type:character varying(20);index"`
	TxTime               int64           `json:"txTime" form:"txTime"`
	ContractAddress      string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(42);index"`
	ParseData            string          `json:"parseData" form:"parseData"`
	Type                 string          `json:"type" form:"type" gorm:"type:character varying(2)"`
	GasLimit             string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed              string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice             string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	BaseFee              string          `json:"baseFee" form:"baseFee" gorm:"type:character varying(20)"`
	MaxFeePerGas         string          `json:"maxFeePerGas" form:"maxFeePerGas" gorm:"type:character varying(20)"`
	MaxPriorityFeePerGas string          `json:"maxPriorityFeePerGas" form:"maxPriorityFeePerGas" gorm:"type:character varying(20)"`
	Data                 string          `json:"data" form:"data"`
	EventLog             string          `json:"eventLog" form:"eventLog"`
	LogAddress           datatypes.JSON  `json:"logAddress" form:"logAddress" gorm:"type:jsonb"` //gorm:"type:jsonb;index:,type:gin"`
	TransactionType      string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData             string          `json:"dappData" form:"dappData"`
	ClientData           string          `json:"clientData" form:"clientData"`
	CreatedAt            int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt            int64           `json:"updatedAt" form:"updatedAt"`
}

// EvmTransactionRecordRepo is a Greater repo.
type EvmTransactionRecordRepo interface {
	Save(context.Context, string, *EvmTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*EvmTransactionRecord) (int64, error)
	Update(context.Context, string, *EvmTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*EvmTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*EvmTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*EvmTransactionRecord, error)
	ListAll(context.Context, string) ([]*EvmTransactionRecord, error)
	PageList(context.Context, string, *pb.PageListRequest) ([]*EvmTransactionRecord, int64, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*EvmTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*EvmTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*EvmTransactionRecord, error)
	FindParseDataByTxHashAndToken(context.Context, string, string, string) (*EvmTransactionRecord, error)
	ListByTransactionType(context.Context, string, string) ([]*EvmTransactionRecord, error)
	UpdateStatusByNonce(context.Context, string, string, int64, string, string, decimal.Decimal, string) (int64, error)
	UpdateCancelByNonce(context.Context, string, string, int64, string, string) (int64, error)
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
			log.Warne("insert evmTransactionRecord failed", err)
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

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":               clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":             clause.Column{Table: "excluded", Name: "block_number"},
			"nonce":                    clause.Column{Table: "excluded", Name: "nonce"},
			"transaction_hash":         clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":             clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":               clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":                 clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":                   clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":               clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":                   clause.Column{Table: "excluded", Name: "amount"},
			"status":                   clause.Column{Table: "excluded", Name: "status"},
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
			"dapp_data":                gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":              gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":               gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&evmTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective evmTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, evmTransactionRecord *EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id = ?", evmTransactionRecord.Id).Updates(evmTransactionRecord)
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

	//rows := ret.RowsAffected

	return evmTransactionRecordList, nil
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

func (r *EvmTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *pb.PageListRequest) ([]*EvmTransactionRecord, int64, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)

	/*if req.FromUid != "" || len(req.FromAddressList) > 0 || req.ToUid != "" || len(req.ToAddressList) > 0 {
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
	}*/
	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
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
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
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
		log.Errore("page query evmTransactionRecord failed", err)
		return nil, 0, err
	}
	return evmTransactionRecordList, total, nil
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

func (r *EvmTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one evmTransactionRecord by blockNumber failed", err)
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
		log.Errore("query evmTransactionRecord by txHash failed", err)
		return nil, err
	}
	if evmTransactionRecord.Id != 0 {
		return evmTransactionRecord, nil
	} else {
		return nil, nil
	}
}


func (r *EvmTransactionRecordRepoImpl) FindParseDataByTxHashAndToken(ctx context.Context, tableName string, txhash string, token string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash like ? and contract_address = ?", txhash+"%",token).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query evmTransactionRecord by txHash failed", err)
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
		log.Errore("query evmTransactionRecord by txType failed", err)
		return nil, err
	}
	return evmTransactionRecords, nil

}

func (r *EvmTransactionRecordRepoImpl) UpdateStatusByNonce(ctx context.Context, tableName string, address string, nonce int64, transactionHash string, toAddress string, amount decimal.Decimal,data string) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("status != 'dropped' and transaction_type != 'eventLog' and from_address = ? and nonce = ?  and transaction_hash not like ? and to_address = ?  and amount = ? and data = ? ", address, nonce, transactionHash+"%",toAddress,amount,data).Update("status", "dropped_replaced")
	err := ret.Error
	if err != nil {
		log.Errore("update "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) UpdateCancelByNonce(ctx context.Context, tableName string, address string, nonce int64, transactionHash string, toAddress string) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("status != 'dropped' and transaction_type != 'eventLog' and from_address = ? and nonce = ?  and transaction_hash not like ? and to_address = ?   ", address, nonce, transactionHash+"%",toAddress).Update("status", "dropped_replaced")
	err := ret.Error
	if err != nil {
		log.Errore("update "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}