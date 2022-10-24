package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

type AptTransactionRecord struct {
	Id                  int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash           string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber         int             `json:"blockNumber" form:"blockNumber"`
	Nonce               int64           `json:"nonce" form:"nonce"`
	TransactionVersion  int             `json:"transactionVersion" form:"transactionVersion"`
	TransactionHash     string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress         string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(66);index"`
	ToAddress           string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(66);index"`
	FromUid             string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid               string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount           decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"` // gorm:"type:decimal.Decimal"
	Amount              decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status              string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime              int64           `json:"txTime" form:"txTime"`
	ContractAddress     string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(1024);index"`
	ParseData           string          `json:"parseData" form:"parseData"`
	StateRootHash       string          `json:"stateRootHash" form:"stateRootHash" gorm:"type:character varying(66)"`
	EventRootHash       string          `json:"eventRootHash" form:"eventRootHash" gorm:"type:character varying(66)"`
	AccumulatorRootHash string          `json:"accumulatorRootHash" form:"accumulatorRootHash" gorm:"type:character varying(66)"`
	GasLimit            string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed             string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice            string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	Data                string          `json:"data" form:"data"`
	EventLog            string          `json:"eventLog" form:"eventLog"`
	TransactionType     string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData            string          `json:"dappData" form:"dappData"`
	ClientData          string          `json:"clientData" form:"clientData"`
	CreatedAt           int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt           int64           `json:"updatedAt" form:"updatedAt"`
}

// AptTransactionRecordRepo is a Greater repo.
type AptTransactionRecordRepo interface {
	Save(context.Context, string, *AptTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*AptTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*AptTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*AptTransactionRecord) (int64, error)
	Update(context.Context, string, *AptTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*AptTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*AptTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*AptTransactionRecord, error)
	ListAll(context.Context, string) ([]*AptTransactionRecord, error)
	PageList(context.Context, string, *pb.PageListRequest) ([]*AptTransactionRecord, int64, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*AptTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*AptTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*AptTransactionRecord, error)
}

type AptTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var AptTransactionRecordRepoClient AptTransactionRecordRepo

// NewAptTransactionRecordRepo new a AptTransactionRecord repo.
func NewAptTransactionRecordRepo(gormDB *gorm.DB) AptTransactionRecordRepo {
	AptTransactionRecordRepoClient = &AptTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return AptTransactionRecordRepoClient
}

func (r *AptTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, aptTransactionRecord *AptTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(aptTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(aptTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert aptTransactionRecord failed", err)
		} else {
			log.Errore("insert aptTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AptTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, aptTransactionRecords []*AptTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(aptTransactionRecords, len(aptTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(aptTransactionRecords)), Data: 0}
			log.Warne("insert aptTransactionRecord failed", err)
		} else {
			log.Errore("insert aptTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AptTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, aptTransactionRecords []*AptTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&aptTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("insert aptTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AptTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, aptTransactionRecords []*AptTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":            clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":          clause.Column{Table: "excluded", Name: "block_number"},
			"nonce":                 clause.Column{Table: "excluded", Name: "nonce"},
			"transaction_version":   clause.Column{Table: "excluded", Name: "transaction_version"},
			"transaction_hash":      clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":          clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":            clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":              clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":                clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":            clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":                gorm.Expr("case when excluded.amount != '' and excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":                clause.Column{Table: "excluded", Name: "status"},
			"tx_time":               clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address":      clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":            clause.Column{Table: "excluded", Name: "parse_data"},
			"state_root_hash":       clause.Column{Table: "excluded", Name: "state_root_hash"},
			"event_root_hash":       clause.Column{Table: "excluded", Name: "event_root_hash"},
			"accumulator_root_hash": clause.Column{Table: "excluded", Name: "accumulator_root_hash"},
			"gas_limit":             clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":              clause.Column{Table: "excluded", Name: "gas_used"},
			"gas_price":             clause.Column{Table: "excluded", Name: "gas_price"},
			"data":                  clause.Column{Table: "excluded", Name: "data"},
			"event_log":             clause.Column{Table: "excluded", Name: "event_log"},
			"transaction_type":      clause.Column{Table: "excluded", Name: "transaction_type"},
			"dapp_data":             gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":           gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":            gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&aptTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective aptTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *AptTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, aptTransactionRecord *AptTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&AptTransactionRecord{}).Where("id = ?", aptTransactionRecord.Id).Updates(aptTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update aptTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *AptTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*AptTransactionRecord, error) {
	var aptTransactionRecord *AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&aptTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query aptTransactionRecord failed", err)
		}
		return nil, err
	}

	return aptTransactionRecord, nil
}

func (r *AptTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*AptTransactionRecord, error) {
	var aptTransactionRecordList []*AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&aptTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query aptTransactionRecord failed", err)
		return nil, err
	}

	return aptTransactionRecordList, nil
}

func (r *AptTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*AptTransactionRecord, error) {
	var aptTransactionRecordList []*AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&aptTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query aptTransactionRecord failed", err)
		return nil, err
	}

	return aptTransactionRecordList, nil
}

func (r *AptTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*AptTransactionRecord, error) {
	var aptTransactionRecordList []*AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&aptTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query aptTransactionRecord failed", err)
		return nil, err
	}

	return aptTransactionRecordList, nil
}

func (r *AptTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *pb.PageListRequest) ([]*AptTransactionRecord, int64, error) {
	var aptTransactionRecordList []*AptTransactionRecord
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
		db = db.Where("from_address in(?)", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		db = db.Where("to_address in(?)", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ?)", req.Address, req.Address)
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

	ret := db.Find(&aptTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query aptTransactionRecord failed", err)
		return nil, 0, err
	}
	return aptTransactionRecordList, total, nil
}

func (r *AptTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&AptTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete aptTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *AptTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&AptTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete aptTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *AptTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*AptTransactionRecord, error) {
	var aptTransactionRecord *AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number IS NOT NULL").Order("block_number DESC").Limit(1).Find(&aptTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last aptTransactionRecord failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return aptTransactionRecord, nil
		}
	}
}

func (r *AptTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*AptTransactionRecord, error) {
	var aptTransactionRecord *AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&aptTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one aptTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return aptTransactionRecord, nil
		}
	}
}

func (r *AptTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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

func (r *AptTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*AptTransactionRecord, error) {
	var aptTransactionRecord *AptTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(aptTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query aptTransactionRecord by txHash failed", err)
		return nil, err
	}
	if aptTransactionRecord.Id != 0 {
		return aptTransactionRecord, nil
	} else {
		return nil, nil
	}
}
