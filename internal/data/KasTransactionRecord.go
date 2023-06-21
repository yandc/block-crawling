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

// KasTransactionRecord is a KasTransactionRecord model.
type KasTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(72);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(72);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ConfirmCount    int32           `json:"confirmCount" form:"confirmCount"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

// KasTransactionRecordRepo is a Greater repo.
type KasTransactionRecordRepo interface {
	Save(context.Context, string, *KasTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*KasTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*KasTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*KasTransactionRecord) (int64, error)
	Update(context.Context, string, *KasTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*KasTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*KasTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*KasTransactionRecord, error)
	ListAll(context.Context, string) ([]*KasTransactionRecord, error)
	PageList(context.Context, string, *TransactionRequest) ([]*KasTransactionRecord, int64, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*KasTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*KasTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*KasTransactionRecord, error)
	DeleteByTxHash(context.Context, string, string) (int64, error)
	PendingByAddress(context.Context, string, string) ([]*KasTransactionRecord, error)
}

type KasTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var KasTransactionRecordRepoClient KasTransactionRecordRepo

// NewKasTransactionRecordRepo new a KasTransactionRecord repo.
func NewKasTransactionRecordRepo(gormDB *gorm.DB) KasTransactionRecordRepo {
	KasTransactionRecordRepoClient = &KasTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return KasTransactionRecordRepoClient
}

func (r *KasTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, kasTransactionRecord *KasTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(kasTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(kasTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert kasTransactionRecord failed", err)
		} else {
			log.Errore("insert kasTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *KasTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, kasTransactionRecords []*KasTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(kasTransactionRecords, len(kasTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(kasTransactionRecords)), Data: 0}
			log.Warne("batch insert kasTransactionRecord failed", err)
		} else {
			log.Errore("batch insert kasTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *KasTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, kasTransactionRecords []*KasTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&kasTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update kasTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *KasTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, kasTransactionRecords []*KasTransactionRecord) (int64, error) {
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
			"status":           gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') then " + tableName + ".status else excluded.status end"),
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"confirm_count":    clause.Column{Table: "excluded", Name: "confirm_count"},
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&kasTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective kasTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *KasTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, kasTransactionRecord *KasTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&KasTransactionRecord{}).Where("id = ?", kasTransactionRecord.Id).Updates(kasTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update kasTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *KasTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*KasTransactionRecord, error) {
	var kasTransactionRecord *KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&kasTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query kasTransactionRecord failed", err)
		}
		return nil, err
	}
	return kasTransactionRecord, nil
}

func (r *KasTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*KasTransactionRecord, error) {
	var kasTransactionRecordList []*KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&kasTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query kasTransactionRecord failed", err)
		return nil, err
	}
	return kasTransactionRecordList, nil
}

func (r *KasTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*KasTransactionRecord, error) {
	var kasTransactionRecordList []*KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&kasTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query kasTransactionRecord failed", err)
		return nil, err
	}
	return kasTransactionRecordList, nil
}

func (r *KasTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*KasTransactionRecord, error) {
	var kasTransactionRecordList []*KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&kasTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query kasTransactionRecord failed", err)
		return nil, err
	}
	return kasTransactionRecordList, nil
}

func (r *KasTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*KasTransactionRecord, int64, error) {
	var kasTransactionRecordList []*KasTransactionRecord
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
	if req.FromAddress != "" {
		db = db.Where("from_address = ?", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("to_address = ?", req.ToAddress)
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
	if req.TransactionHashLike != "" {
		db = db.Where("transaction_hash like ?", req.TransactionHashLike+"%")
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

	ret := db.Find(&kasTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query kasTransactionRecord failed", err)
		return nil, 0, err
	}
	return kasTransactionRecordList, total, nil
}

func (r *KasTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&KasTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete kasTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *KasTransactionRecordRepoImpl) DeleteByTxHash(ctx context.Context, tableName string, txHash string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Delete(&KasTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete kasTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil

}

func (r *KasTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&KasTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete kasTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *KasTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*KasTransactionRecord, error) {
	var kasTransactionRecord *KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&kasTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last kasTransactionRecord failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return kasTransactionRecord, nil
		}
	}
}

func (r *KasTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*KasTransactionRecord, error) {
	var kasTransactionRecord *KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&kasTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one kasTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return kasTransactionRecord, nil
		}
	}
}

func (r *KasTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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

func (r *KasTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*KasTransactionRecord, error) {
	var kasTransactionRecord *KasTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(&kasTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query kasTransactionRecord by txHash failed", err)
		return nil, err
	}
	if kasTransactionRecord.Id != 0 {
		return kasTransactionRecord, nil
	} else {
		return nil, nil
	}
}

func (r *KasTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*KasTransactionRecord, error) {
	var kasTransactionRecordList []*KasTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&kasTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query kaTransactionRecord failed", err)
		return nil, err
	}
	return kasTransactionRecordList, nil
}
