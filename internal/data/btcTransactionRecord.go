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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// BtcTransactionRecord is a BtcTransactionRecord model.
type BtcTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(64);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(64);index"`
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

// BtcTransactionRecordRepo is a Greater repo.
type BtcTransactionRecordRepo interface {
	OutTxCounter

	Save(context.Context, string, *BtcTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrIgnore(context.Context, string, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*BtcTransactionRecord) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*BtcTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, string, []*BtcTransactionRecord, int) (int64, error)
	Update(context.Context, string, *BtcTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*BtcTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*BtcTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*BtcTransactionRecord, error)
	ListAll(context.Context, string) ([]*BtcTransactionRecord, error)
	PageList(context.Context, string, *TransactionRequest) ([]*BtcTransactionRecord, int64, error)
	PendingByAddress(context.Context, string, string) ([]*BtcTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*BtcTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*BtcTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*BtcTransactionRecord, error)
	DeleteByTxHash(context.Context, string, string) (int64, error)
}

type BtcTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var BtcTransactionRecordRepoClient BtcTransactionRecordRepo

// NewBtcTransactionRecordRepo new a BtcTransactionRecord repo.
func NewBtcTransactionRecordRepo(gormDB *gorm.DB) BtcTransactionRecordRepo {
	BtcTransactionRecordRepoClient = &BtcTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return BtcTransactionRecordRepoClient
}

func (r *BtcTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(btcTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(btcTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert btcTransactionRecord failed", err)
		} else {
			log.Errore("insert btcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(btcTransactionRecords, len(btcTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(btcTransactionRecords)), Data: 0}
			log.Warne("batch insert btcTransactionRecord failed", err)
		} else {
			log.Errore("batch insert btcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&btcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update btcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		DoNothing: true,
	}).Create(&btcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or ignore "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
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
	}).Create(&btcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective btcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
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
			"transaction_hash": gorm.Expr("case when excluded.transaction_hash != '' then excluded.transaction_hash else " + tableName + ".transaction_hash end"),
			"from_address":     gorm.Expr("case when excluded.from_address != '' then excluded.from_address else " + tableName + ".from_address end"),
			"to_address":       gorm.Expr("case when excluded.to_address != '' then excluded.to_address else " + tableName + ".to_address end"),
			"from_uid":         gorm.Expr("case when excluded.from_uid != '' then excluded.from_uid else " + tableName + ".from_uid end"),
			"to_uid":           gorm.Expr("case when excluded.to_uid != '' then excluded.to_uid else " + tableName + ".to_uid end"),
			"fee_amount":       gorm.Expr("case when excluded.fee_amount != '' and excluded.fee_amount != '0' then excluded.fee_amount else " + tableName + ".fee_amount end"),
			"amount":           gorm.Expr("case when excluded.amount != '' and excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":           gorm.Expr("case when excluded.status != '' then excluded.status else " + tableName + ".status end"),
			"tx_time":          gorm.Expr("case when excluded.tx_time != 0 then excluded.tx_time else " + tableName + ".tx_time end"),
			"confirm_count":    gorm.Expr("case when excluded.confirm_count != 0 then excluded.confirm_count else " + tableName + ".confirm_count end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&btcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective btcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, txRecords []*BtcTransactionRecord, pageSize int) (int64, error) {
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

func (r *BtcTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, tableName string, txRecords []*BtcTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"id"}, txRecords, pageSize)
}

func (r *BtcTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&BtcTransactionRecord{}).Where("id = ?", btcTransactionRecord.Id).Updates(btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&btcTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query btcTransactionRecord failed", err)
		}
		return nil, err
	}
	return btcTransactionRecord, nil
}

func (r *BtcTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*BtcTransactionRecord, int64, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
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
	if len(req.TransactionHashNotInList) > 0 {
		db = db.Where("transaction_hash not in(?)", req.TransactionHashNotInList)
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

	ret := db.Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query btcTransactionRecord failed", err)
		return nil, 0, err
	}
	return btcTransactionRecordList, total, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&BtcTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByTxHash(ctx context.Context, tableName string, txHash string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Delete(&BtcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil

}

func (r *BtcTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&BtcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last btcTransactionRecord failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return btcTransactionRecord, nil
		}
	}
}

func (r *BtcTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one btcTransactionRecord by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return btcTransactionRecord, nil
		}
	}
}

func (r *BtcTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
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

func (r *BtcTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(&btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord by txHash failed", err)
		return nil, err
	}
	if btcTransactionRecord.Id != 0 {
		return btcTransactionRecord, nil
	} else {
		return nil, nil
	}
}

func (r *BtcTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query CsprTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

// CountOut implements AtomTransactionRecordRepo
func (r *BtcTransactionRecordRepoImpl) CountOut(ctx context.Context, tableName string, address string, toAddress string) (int64, error) {
	return countOutTx(r.gormDB, ctx, tableName, address, toAddress)
}
