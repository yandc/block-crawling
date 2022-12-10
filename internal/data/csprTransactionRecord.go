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

type CsprTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(516);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(516);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	Data            string          `json:"data" form:"data"`
	ParseData       string          `json:"parseData" form:"parseData"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}


type CsprTransactionRecordRepo interface {

	Save(context.Context, string, *CsprTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*CsprTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*CsprTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*CsprTransactionRecord) (int64, error)
	Update(context.Context, string, *CsprTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*CsprTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*CsprTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*CsprTransactionRecord, error)
	ListAll(context.Context, string) ([]*CsprTransactionRecord, error)
	PageList(context.Context, string, *pb.PageListRequest) ([]*CsprTransactionRecord, int64, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*CsprTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*CsprTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*CsprTransactionRecord, error)
}





type CsprTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var CsprTransactionRecordRepoClient CsprTransactionRecordRepo

func NewCsprTransactionRecordRepo(gormDB *gorm.DB) CsprTransactionRecordRepo {
	CsprTransactionRecordRepoClient = &CsprTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return CsprTransactionRecordRepoClient
}


func (r *CsprTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, CsprTransactionRecord *CsprTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(CsprTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(CsprTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert CsprTransactionRecord failed", err)
		} else {
			log.Errore("insert CsprTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *CsprTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, CsprTransactionRecords []*CsprTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(CsprTransactionRecords, len(CsprTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(CsprTransactionRecords)), Data: 0}
			log.Warne("batch insert CsprTransactionRecord failed", err)
		} else {
			log.Errore("batch insert CsprTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *CsprTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, CsprTransactionRecords []*CsprTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&CsprTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update CsprTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *CsprTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, CsprTransactionRecords []*CsprTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":       clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":     clause.Column{Table: "excluded", Name: "block_number"},
			"transaction_hash": clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":     clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":       clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":         clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":           clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":       clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":           clause.Column{Table: "excluded", Name: "amount"},
			"status":           clause.Column{Table: "excluded", Name: "status"},
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"parse_data":       clause.Column{Table: "excluded", Name: "parse_data"},
			"transaction_type": clause.Column{Table: "excluded", Name: "transaction_type"},
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&CsprTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective CsprTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *CsprTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, csprTransactionRecord *CsprTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&CsprTransactionRecord{}).Where("id = ?", csprTransactionRecord.Id).Updates(csprTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update CsprTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *CsprTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*CsprTransactionRecord, error) {
	var csprTransactionRecord *CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&csprTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query CsprTransactionRecord failed", err)
		}
		return nil, err
	}
	return csprTransactionRecord, nil
}

func (r *CsprTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*CsprTransactionRecord, error) {
	var csprTransactionRecordList []*CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&csprTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query CsprTransactionRecord failed", err)
		return nil, err
	}
	return csprTransactionRecordList, nil
}

func (r *CsprTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*CsprTransactionRecord, error) {
	var csprTransactionRecordList []*CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&csprTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query CsprTransactionRecord failed", err)
		return nil, err
	}
	return csprTransactionRecordList, nil
}

func (r *CsprTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*CsprTransactionRecord, error) {
	var csprTransactionRecordList []*CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&csprTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query CsprTransactionRecord failed", err)
		return nil, err
	}
	return csprTransactionRecordList, nil
}

func (r *CsprTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *pb.PageListRequest) ([]*CsprTransactionRecord, int64, error) {
	var csprTransactionRecordList []*CsprTransactionRecord
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
	/*if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}*/
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	/*if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}*/
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

	ret := db.Find(&csprTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query CsprTransactionRecord failed", err)
		return nil, 0, err
	}
	return csprTransactionRecordList, total, nil
}

func (r *CsprTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&CsprTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete CsprTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *CsprTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&CsprTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete CsprTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *CsprTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*CsprTransactionRecord, error) {
	var csprTransactionRecord *CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&csprTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last CsprTransactionRecord failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return csprTransactionRecord, nil
		}
	}
}

func (r *CsprTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*CsprTransactionRecord, error) {
	var csprTransactionRecord *CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&csprTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one CsprTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return csprTransactionRecord, nil
		}
	}
}

func (r *CsprTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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
	/*if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}*/

	ret := db.Pluck("coalesce(sum(cast(amount as numeric)), 0)", &amount)
	err := ret.Error
	if err != nil {
		log.Errore("query amount failed", err)
		return "", err
	}
	return amount, nil
}

func (r *CsprTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*CsprTransactionRecord, error) {
	var csprTransactionRecord *CsprTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(csprTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query CsprTransactionRecord by txHash failed", err)
		return nil, err
	}
	if csprTransactionRecord.Id != 0 {
		return csprTransactionRecord, nil
	} else {
		return nil, nil
	}
}