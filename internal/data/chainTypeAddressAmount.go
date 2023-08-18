package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ChainTypeAddressAmount is a ChainTypeAddressAmount model.
type ChainTypeAddressAmount struct {
	Id            int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName     string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_dt_chain_name_uid_type"`
	UidType       int8   `json:"uidType" form:"uidType" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_uid_type"`
	AddressAmount int64  `json:"addressAmount" form:"addressAmount"`
	Dt            int64  `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_uid_type"`
	CreatedAt     int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt     int64  `json:"updatedAt" form:"updatedAt"`
}

func (chainTypeAddressAmount ChainTypeAddressAmount) TableName() string {
	return "chain_type_address_amount"
}

// ChainTypeAddressAmountRepo is a Greater repo.
type ChainTypeAddressAmountRepo interface {
	Save(context.Context, *ChainTypeAddressAmount) (int64, error)
	BatchSave(context.Context, []*ChainTypeAddressAmount) (int64, error)
	SaveOrUpdate(context.Context, *ChainTypeAddressAmount) (int64, error)
	BatchSaveOrUpdate(context.Context, []*ChainTypeAddressAmount) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*ChainTypeAddressAmount, int) (int64, error)
	Update(context.Context, *ChainTypeAddressAmount) (int64, error)
	FindByID(context.Context, int64) (*ChainTypeAddressAmount, error)
	ListByID(context.Context, int64) ([]*ChainTypeAddressAmount, error)
	ListAll(context.Context) ([]*ChainTypeAddressAmount, error)
	PageList(context.Context, *AssetRequest) ([]*ChainTypeAddressAmount, int64, error)
	List(context.Context, *AssetRequest) ([]*ChainTypeAddressAmount, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
	Delete(context.Context, *AssetRequest) (int64, error)
}

type ChainTypeAddressAmountRepoImpl struct {
	gormDB *gorm.DB
}

var ChainTypeAddressAmountRepoClient ChainTypeAddressAmountRepo

// NewChainTypeAddressAmountRepo new a ChainTypeAddressAmount repo.
func NewChainTypeAddressAmountRepo(gormDB *gorm.DB) ChainTypeAddressAmountRepo {
	ChainTypeAddressAmountRepoClient = &ChainTypeAddressAmountRepoImpl{
		gormDB: gormDB,
	}
	return ChainTypeAddressAmountRepoClient
}

func (r *ChainTypeAddressAmountRepoImpl) Save(ctx context.Context, chainTypeAddressAmount *ChainTypeAddressAmount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(chainTypeAddressAmount)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(chainTypeAddressAmount.Id, 10), Data: 0}
			log.Warne("insert chainTypeAddressAmount failed", err)
		} else {
			log.Errore("insert chainTypeAddressAmount failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAddressAmountRepoImpl) BatchSave(ctx context.Context, chainTypeAddressAmounts []*ChainTypeAddressAmount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(chainTypeAddressAmounts, len(chainTypeAddressAmounts))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(chainTypeAddressAmounts)), Data: 0}
			log.Warne("batch insert chainTypeAddressAmount failed", err)
		} else {
			log.Errore("batch insert chainTypeAddressAmount failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAddressAmountRepoImpl) SaveOrUpdate(ctx context.Context, chainTypeAddressAmount *ChainTypeAddressAmount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "uid_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"address_amount", "updated_at"}),
	}).Create(&chainTypeAddressAmount)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update chainTypeAddressAmount failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAddressAmountRepoImpl) BatchSaveOrUpdate(ctx context.Context, chainTypeAddressAmounts []*ChainTypeAddressAmount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "uid_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"address_amount", "updated_at"}),
	}).Create(&chainTypeAddressAmounts)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update chainTypeAddressAmount failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAddressAmountRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, chainTypeAddressAmounts []*ChainTypeAddressAmount, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(chainTypeAddressAmounts)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subChainTypeAddressAmountss := chainTypeAddressAmounts[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subChainTypeAddressAmountss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *ChainTypeAddressAmountRepoImpl) Update(ctx context.Context, chainTypeAddressAmount *ChainTypeAddressAmount) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&ChainTypeAddressAmount{}).Where("id = ?", chainTypeAddressAmount.Id).Updates(chainTypeAddressAmount)
	err := ret.Error
	if err != nil {
		log.Errore("update chainTypeAddressAmount failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAddressAmountRepoImpl) FindByID(ctx context.Context, id int64) (*ChainTypeAddressAmount, error) {
	var chainTypeAddressAmount *ChainTypeAddressAmount
	ret := r.gormDB.WithContext(ctx).First(&chainTypeAddressAmount, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query chainTypeAddressAmount failed", err)
		}
		return nil, err
	}
	return chainTypeAddressAmount, nil
}

func (r *ChainTypeAddressAmountRepoImpl) ListByID(ctx context.Context, id int64) ([]*ChainTypeAddressAmount, error) {
	var chainTypeAddressAmountList []*ChainTypeAddressAmount
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&chainTypeAddressAmountList)
	err := ret.Error
	if err != nil {
		log.Errore("query chainTypeAddressAmount failed", err)
		return nil, err
	}
	return chainTypeAddressAmountList, nil
}

func (r *ChainTypeAddressAmountRepoImpl) ListAll(ctx context.Context) ([]*ChainTypeAddressAmount, error) {
	var chainTypeAddressAmountList []*ChainTypeAddressAmount
	ret := r.gormDB.WithContext(ctx).Find(&chainTypeAddressAmountList)
	err := ret.Error
	if err != nil {
		log.Errore("query chainTypeAddressAmount failed", err)
		return nil, err
	}
	return chainTypeAddressAmountList, nil
}

func (r *ChainTypeAddressAmountRepoImpl) PageList(ctx context.Context, req *AssetRequest) ([]*ChainTypeAddressAmount, int64, error) {
	var chainTypeAddressAmountList []*ChainTypeAddressAmount
	var total int64
	db := r.gormDB.WithContext(ctx).Table("chain_type_address_amount")

	if req.ChainName != "" {
		if req.ChainName != "all" {
			db = db.Where("chain_name = ?", req.ChainName)
		} else {
			db = db.Where("chain_name = ''")
		}
	}
	if req.UidType != 0 {
		if req.UidType != -1 {
			db = db.Where("uid_type = ?", req.UidType)
		} else {
			db = db.Where("uid_type = 0")
		}
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

	ret := db.Find(&chainTypeAddressAmountList)
	err := ret.Error
	if err != nil {
		log.Errore("page query chainTypeAddressAmount failed", err)
		return nil, 0, err
	}
	return chainTypeAddressAmountList, total, nil
}

func (r *ChainTypeAddressAmountRepoImpl) List(ctx context.Context, req *AssetRequest) ([]*ChainTypeAddressAmount, error) {
	var chainTypeAddressAmountList []*ChainTypeAddressAmount
	db := r.gormDB.WithContext(ctx).Table("chain_type_address_amount")

	if req.ChainName != "" {
		if req.ChainName != "all" {
			db = db.Where("chain_name = ?", req.ChainName)
		} else {
			db = db.Where("chain_name = ''")
		}
	}
	if req.UidType != 0 {
		if req.UidType != -1 {
			db = db.Where("uid_type = ?", req.UidType)
		} else {
			db = db.Where("uid_type = 0")
		}
	}
	if req.StartTime > 0 {
		db = db.Where("dt >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("dt < ?", req.StopTime)
	}

	if req.OrderBy != "" {
		db = db.Order(req.OrderBy)
	}

	ret := db.Find(&chainTypeAddressAmountList)
	err := ret.Error
	if err != nil {
		log.Errore("list query chainTypeAddressAmount failed", err)
		return nil, err
	}
	return chainTypeAddressAmountList, nil
}

func (r *ChainTypeAddressAmountRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&ChainTypeAddressAmount{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAddressAmount failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAddressAmountRepoImpl) DeleteByIDs(ctx context.Context, ids []int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&ChainTypeAddressAmount{}, ids)
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAddressAmounts failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAddressAmountRepoImpl) Delete(ctx context.Context, req *AssetRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("chain_type_address_amount")

	if req.ChainName != "" {
		if req.ChainName != "all" {
			db = db.Where("chain_name = ?", req.ChainName)
		} else {
			db = db.Where("chain_name = ''")
		}
	}
	if req.UidType != 0 {
		if req.UidType != -1 {
			db = db.Where("uid_type = ?", req.UidType)
		} else {
			db = db.Where("uid_type = 0")
		}
	}
	if req.StartTime > 0 {
		db = db.Where("dt >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("dt < ?", req.StopTime)
	}

	ret := db.Delete(&ChainTypeAddressAmount{})
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAddressAmounts failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
