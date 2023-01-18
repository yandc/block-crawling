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

type SuiTransactionRecord struct {
	Id                 int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	TransactionVersion int             `json:"transactionVersion" form:"transactionVersion"`
	TransactionHash    string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress        string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(66);index"`
	ToAddress          string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(66);index"`
	FromUid            string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid              string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount          decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount             decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status             string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime             int64           `json:"txTime" form:"txTime"`
	ContractAddress    string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(1024);index"`
	ParseData          string          `json:"parseData" form:"parseData"`
	GasLimit           string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed            string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	Data               string          `json:"data" form:"data"`
	EventLog           string          `json:"eventLog" form:"eventLog"`
	LogAddress         datatypes.JSON  `json:"logAddress" form:"logAddress" gorm:"type:jsonb"` //gorm:"type:jsonb;index:,type:gin"`
	TransactionType    string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData           string          `json:"dappData" form:"dappData"`
	ClientData         string          `json:"clientData" form:"clientData"`
	CreatedAt          int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt          int64           `json:"updatedAt" form:"updatedAt"`
}

// SuiTransactionRecordRepo is a Greater repo.
type SuiTransactionRecordRepo interface {
	Save(context.Context, string, *SuiTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*SuiTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*SuiTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*SuiTransactionRecord) (int64, error)
	Update(context.Context, string, *SuiTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*SuiTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*SuiTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*SuiTransactionRecord, error)
	ListAll(context.Context, string) ([]*SuiTransactionRecord, error)
	PageList(context.Context, string, *pb.PageListRequest) ([]*SuiTransactionRecord, int64, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*SuiTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*SuiTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*SuiTransactionRecord, error)
}

type SuiTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var SuiTransactionRecordRepoClient SuiTransactionRecordRepo

// NewSuiTransactionRecordRepo new a SuiTransactionRecord repo.
func NewSuiTransactionRecordRepo(gormDB *gorm.DB) SuiTransactionRecordRepo {
	SuiTransactionRecordRepoClient = &SuiTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return SuiTransactionRecordRepoClient
}

func (r *SuiTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, suiTransactionRecord *SuiTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(suiTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(suiTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert suiTransactionRecord failed", err)
		} else {
			log.Errore("insert suiTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SuiTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, suiTransactionRecords []*SuiTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(suiTransactionRecords, len(suiTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(suiTransactionRecords)), Data: 0}
			log.Warne("insert suiTransactionRecord failed", err)
		} else {
			log.Errore("insert suiTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SuiTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, suiTransactionRecords []*SuiTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&suiTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("insert suiTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SuiTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, suiTransactionRecords []*SuiTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"transaction_version": clause.Column{Table: "excluded", Name: "transaction_version"},
			"transaction_hash":    clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":        clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":          clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":            clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":              clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":          clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":              gorm.Expr("case when excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":              clause.Column{Table: "excluded", Name: "status"},
			"tx_time":             clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address":    clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":          clause.Column{Table: "excluded", Name: "parse_data"},
			"gas_limit":           clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":            clause.Column{Table: "excluded", Name: "gas_used"},
			"data":                clause.Column{Table: "excluded", Name: "data"},
			"event_log":           clause.Column{Table: "excluded", Name: "event_log"},
			"log_address":         clause.Column{Table: "excluded", Name: "log_address"},
			"transaction_type":    clause.Column{Table: "excluded", Name: "transaction_type"},
			"dapp_data":           gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":         gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":          gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&suiTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective suiTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SuiTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, suiTransactionRecord *SuiTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&SuiTransactionRecord{}).Where("id = ?", suiTransactionRecord.Id).Updates(suiTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update suiTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SuiTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*SuiTransactionRecord, error) {
	var suiTransactionRecord *SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&suiTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query suiTransactionRecord failed", err)
		}
		return nil, err
	}

	return suiTransactionRecord, nil
}

func (r *SuiTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*SuiTransactionRecord, error) {
	var suiTransactionRecordList []*SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&suiTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query suiTransactionRecord failed", err)
		return nil, err
	}

	return suiTransactionRecordList, nil
}

func (r *SuiTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*SuiTransactionRecord, error) {
	var suiTransactionRecordList []*SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&suiTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query suiTransactionRecord failed", err)
		return nil, err
	}

	return suiTransactionRecordList, nil
}

func (r *SuiTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*SuiTransactionRecord, error) {
	var suiTransactionRecordList []*SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&suiTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query suiTransactionRecord failed", err)
		return nil, err
	}

	return suiTransactionRecordList, nil
}

func (r *SuiTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *pb.PageListRequest) ([]*SuiTransactionRecord, int64, error) {
	var suiTransactionRecordList []*SuiTransactionRecord
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)

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

	ret := db.Find(&suiTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query suiTransactionRecord failed", err)
		return nil, 0, err
	}
	return suiTransactionRecordList, total, nil
}

func (r *SuiTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&SuiTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete suiTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SuiTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_version >= ?", blockNumber).Delete(&SuiTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete suiTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SuiTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*SuiTransactionRecord, error) {
	var suiTransactionRecord *SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_version IS NOT NULL").Order("transaction_version DESC").Limit(1).Find(&suiTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last suiTransactionRecord failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return suiTransactionRecord, nil
		}
	}
}

func (r *SuiTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*SuiTransactionRecord, error) {
	var suiTransactionRecord *SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_version = ?", blockNumber).Limit(1).Find(&suiTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one suiTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return suiTransactionRecord, nil
		}
	}
}

func (r *SuiTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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

func (r *SuiTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*SuiTransactionRecord, error) {
	var suiTransactionRecord *SuiTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(suiTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query suiTransactionRecord by txHash failed", err)
		return nil, err
	}
	if suiTransactionRecord.Id != 0 {
		return suiTransactionRecord, nil
	} else {
		return nil, nil
	}
}
