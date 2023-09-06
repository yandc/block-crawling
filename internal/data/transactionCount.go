package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

// TransactionCount is a TransactionCount model.
type TransactionCount struct {
	Id                  int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName           string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_from_address_to_address_transaction_type"`
	FromAddress         string `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(512);index:,unique,composite:unique_chain_name_from_address_to_address_transaction_type"`
	ToAddress           string `json:"toAddress" form:"toAddress" gorm:"type:character varying(512);index:,unique,composite:unique_chain_name_from_address_to_address_transaction_type"`
	TransactionType     string `json:"transactionType" form:"transactionType" gorm:"type:character varying(42);index:,unique,composite:unique_chain_name_from_address_to_address_transaction_type"`
	TransactionQuantity int64  `json:"transactionQuantity" form:"transactionQuantity"`
	TransactionHash     string `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(98);default:null"`
	CreatedAt           int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt           int64  `json:"updatedAt" form:"updatedAt"`
}

func (transactionCount TransactionCount) TableName() string {
	return "transaction_count"
}

// TransactionCountRepo is a Greater repo.
type TransactionCountRepo interface {
	Save(context.Context, *TransactionCount) (int64, error)
	BatchSave(context.Context, []*TransactionCount) (int64, error)
	SaveOrUpdate(context.Context, *TransactionCount) (int64, error)
	BatchSaveOrUpdate(context.Context, []*TransactionCount) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*TransactionCount, int) (int64, error)
	IncrementBatchSaveOrUpdate(context.Context, []*TransactionCount) (int64, error)
	PageIncrementBatchSaveOrUpdate(context.Context, []*TransactionCount, int) (int64, error)
	Update(context.Context, *TransactionCount) (int64, error)
	FindByID(context.Context, int64) (*TransactionCount, error)
	ListByID(context.Context, int64) ([]*TransactionCount, error)
	ListAll(context.Context) ([]*TransactionCount, error)
	PageList(context.Context, *CountRequest) ([]*TransactionCount, int64, error)
	List(context.Context, *CountRequest) ([]*TransactionCount, error)
	CountTransactionQuantity(context.Context, *CountRequest) (int64, error)
	DeleteByID(context.Context, int64) (int64, error)
	Delete(context.Context, *CountRequest) (int64, error)
}

type CountRequest struct {
	ChainName           string
	FromAddress         string
	ToAddress           string
	TransactionTypeList []string
	StartTime           int64
	StopTime            int64
	SelectColumn        string
	GroupBy             string
	OrderBy             string
	DataDirection       int32
	StartIndex          int64
	PageNum             int32
	PageSize            int32
	Total               bool
}

type TransactionCountRepoImpl struct {
	gormDB *gorm.DB
}

var TransactionCountRepoClient TransactionCountRepo

// NewTransactionCountRepo new a TransactionCount repo.
func NewTransactionCountRepo(gormDB *gorm.DB) TransactionCountRepo {
	TransactionCountRepoClient = &TransactionCountRepoImpl{
		gormDB: gormDB,
	}
	return TransactionCountRepoClient
}

func (r *TransactionCountRepoImpl) Save(ctx context.Context, transactionCount *TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(transactionCount)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(transactionCount.Id, 10), Data: 0}
			log.Warne("insert transactionCount failed", err)
		} else {
			log.Errore("insert transactionCount failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionCountRepoImpl) BatchSave(ctx context.Context, transactionCounts []*TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(transactionCounts, len(transactionCounts))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(transactionCounts)), Data: 0}
			log.Warne("batch insert transactionCount failed", err)
		} else {
			log.Errore("batch insert transactionCount failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionCountRepoImpl) SaveOrUpdate(ctx context.Context, transactionCount *TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "transaction_type"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"transaction_quantity", "transaction_hash", "updated_at"}),
	}).Create(&transactionCount)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update transactionCount failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionCountRepoImpl) BatchSaveOrUpdate(ctx context.Context, transactionCounts []*TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "transaction_type"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"transaction_quantity", "transaction_hash", "updated_at"}),
	}).Create(&transactionCounts)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update transactionCount failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionCountRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, transactionCounts []*TransactionCount, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(transactionCounts)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTransactionCounts := transactionCounts[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subTransactionCounts)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *TransactionCountRepoImpl) IncrementBatchSaveOrUpdate(ctx context.Context, transactionCounts []*TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "transaction_type"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"transaction_quantity": gorm.Expr("case when excluded.transaction_hash != transaction_count.transaction_hash then excluded.transaction_quantity + transaction_count.transaction_quantity else transaction_count.transaction_quantity end"),
			"transaction_hash":     clause.Column{Table: "excluded", Name: "transaction_hash"},
			"updated_at":           clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&transactionCounts)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update transactionCount failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionCountRepoImpl) PageIncrementBatchSaveOrUpdate(ctx context.Context, transactionCounts []*TransactionCount, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(transactionCounts)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTransactionCounts := transactionCounts[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.IncrementBatchSaveOrUpdate(ctx, subTransactionCounts)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *TransactionCountRepoImpl) Update(ctx context.Context, transactionCount *TransactionCount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&TransactionCount{}).Where("id = ?", transactionCount.Id).Updates(transactionCount)
	err := ret.Error
	if err != nil {
		log.Errore("update transactionCount failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TransactionCountRepoImpl) FindByID(ctx context.Context, id int64) (*TransactionCount, error) {
	var transactionCount *TransactionCount
	ret := r.gormDB.WithContext(ctx).First(&transactionCount, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query transactionCount failed", err)
		}
		return nil, err
	}
	return transactionCount, nil
}

func (r *TransactionCountRepoImpl) ListByID(ctx context.Context, id int64) ([]*TransactionCount, error) {
	var transactionCountList []*TransactionCount
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&transactionCountList)
	err := ret.Error
	if err != nil {
		log.Errore("query transactionCount failed", err)
		return nil, err
	}
	return transactionCountList, nil
}

func (r *TransactionCountRepoImpl) ListAll(ctx context.Context) ([]*TransactionCount, error) {
	var transactionCountList []*TransactionCount
	ret := r.gormDB.WithContext(ctx).Find(&transactionCountList)
	err := ret.Error
	if err != nil {
		log.Errore("query transactionCount failed", err)
		return nil, err
	}
	return transactionCountList, nil
}

func (r *TransactionCountRepoImpl) PageList(ctx context.Context, req *CountRequest) ([]*TransactionCount, int64, error) {
	var transactionCountList []*TransactionCount
	var total int64
	db := r.gormDB.WithContext(ctx).Table("transaction_count")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.FromAddress != "" {
		db = db.Where("from_address = ?", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("to_address = ?", req.ToAddress)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if req.StartTime > 0 {
		db = db.Where("dt >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("dt < ?", req.StopTime)
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

	ret := db.Find(&transactionCountList)
	err := ret.Error
	if err != nil {
		log.Errore("page query transactionCount failed", err)
		return nil, 0, err
	}
	return transactionCountList, total, nil
}

func (r *TransactionCountRepoImpl) List(ctx context.Context, req *CountRequest) ([]*TransactionCount, error) {
	var transactionCountList []*TransactionCount
	db := r.gormDB.WithContext(ctx).Table("transaction_count")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.FromAddress != "" {
		db = db.Where("from_address = ?", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("to_address = ?", req.ToAddress)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if req.StartTime > 0 {
		db = db.Where("dt >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("dt < ?", req.StopTime)
	}

	ret := db.Find(&transactionCountList)
	err := ret.Error
	if err != nil {
		log.Errore("list query transactionCount failed", err)
		return nil, err
	}
	return transactionCountList, nil
}

func (r *TransactionCountRepoImpl) CountTransactionQuantity(ctx context.Context, req *CountRequest) (int64, error) {
	var transactionCount *TransactionCount

	sqlStr := "select sum(transaction_quantity) as transaction_quantity " +
		"from transaction_count " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.FromAddress != "" {
		sqlStr += " and from_address = '" + req.FromAddress + "'"
	}
	if req.ToAddress != "" {
		sqlStr += " and to_address = '" + req.ToAddress + "'"
	}
	if len(req.TransactionTypeList) > 0 {
		transactionTypeList := strings.ReplaceAll(utils.ListToString(req.TransactionTypeList), "\"", "'")
		sqlStr += " and transaction_type in (" + transactionTypeList + ")"
	}

	ret := r.gormDB.WithContext(ctx).Table("transaction_count").Raw(sqlStr).Find(&transactionCount)
	err := ret.Error
	if err != nil {
		log.Errore("count transactionQuantity failed", err)
		return 0, err
	}
	return transactionCount.TransactionQuantity, nil
}

func (r *TransactionCountRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&TransactionCount{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete transactionCount failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TransactionCountRepoImpl) Delete(ctx context.Context, req *CountRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("transaction_count")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.FromAddress != "" {
		db = db.Where("from_address = ?", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("to_address = ?", req.ToAddress)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if req.StartTime > 0 {
		db = db.Where("dt >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("dt < ?", req.StopTime)
	}

	ret := db.Delete(&UserAsset{})
	err := ret.Error
	if err != nil {
		log.Errore("delete transactionCount failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
