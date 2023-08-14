package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ChainTypeAsset is a ChainTypeAsset model.
type ChainTypeAsset struct {
	Id        int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_dt_chain_name_uid_type"`
	UidType   int8            `json:"uidType" form:"uidType" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_uid_type"`
	CnyAmount decimal.Decimal `json:"cnyAmount" form:"cnyAmount" gorm:"type:decimal(256,2);"`
	UsdAmount decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	Dt        int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_uid_type"`
	CreatedAt int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt int64           `json:"updatedAt" form:"updatedAt"`
}

func (chainTypeAsset ChainTypeAsset) TableName() string {
	return "chain_type_asset"
}

// ChainTypeAssetRepo is a Greater repo.
type ChainTypeAssetRepo interface {
	Save(context.Context, *ChainTypeAsset) (int64, error)
	BatchSave(context.Context, []*ChainTypeAsset) (int64, error)
	SaveOrUpdate(context.Context, *ChainTypeAsset) (int64, error)
	BatchSaveOrUpdate(context.Context, []*ChainTypeAsset) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*ChainTypeAsset, int) (int64, error)
	Update(context.Context, *ChainTypeAsset) (int64, error)
	FindByID(context.Context, int64) (*ChainTypeAsset, error)
	ListByID(context.Context, int64) ([]*ChainTypeAsset, error)
	ListAll(context.Context) ([]*ChainTypeAsset, error)
	PageList(context.Context, *AssetRequest) ([]*ChainTypeAsset, int64, error)
	List(context.Context, *AssetRequest) ([]*ChainTypeAsset, error)
	ListAmountGroupByDt(context.Context, *AssetRequest) ([]*pb.AssetHistoryFundAmountResponse, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
	Delete(context.Context, *AssetRequest) (int64, error)
}

type ChainTypeAssetRepoImpl struct {
	gormDB *gorm.DB
}

var ChainTypeAssetRepoClient ChainTypeAssetRepo

// NewChainTypeAssetRepo new a ChainTypeAsset repo.
func NewChainTypeAssetRepo(gormDB *gorm.DB) ChainTypeAssetRepo {
	ChainTypeAssetRepoClient = &ChainTypeAssetRepoImpl{
		gormDB: gormDB,
	}
	return ChainTypeAssetRepoClient
}

func (r *ChainTypeAssetRepoImpl) Save(ctx context.Context, chainTypeAsset *ChainTypeAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(chainTypeAsset)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(chainTypeAsset.Id, 10), Data: 0}
			log.Warne("insert chainTypeAsset failed", err)
		} else {
			log.Errore("insert chainTypeAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAssetRepoImpl) BatchSave(ctx context.Context, chainTypeAssets []*ChainTypeAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(chainTypeAssets, len(chainTypeAssets))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(chainTypeAssets)), Data: 0}
			log.Warne("batch insert chainTypeAsset failed", err)
		} else {
			log.Errore("batch insert chainTypeAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAssetRepoImpl) SaveOrUpdate(ctx context.Context, chainTypeAsset *ChainTypeAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "uid_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"cny_amount", "usd_amount", "updated_at"}),
	}).Create(&chainTypeAsset)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update chainTypeAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAssetRepoImpl) BatchSaveOrUpdate(ctx context.Context, chainTypeAssets []*ChainTypeAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "uid_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"cny_amount", "usd_amount", "updated_at"}),
	}).Create(&chainTypeAssets)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update chainTypeAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *ChainTypeAssetRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, chainTypeAssets []*ChainTypeAsset, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(chainTypeAssets)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subChainTypeAssetss := chainTypeAssets[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subChainTypeAssetss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *ChainTypeAssetRepoImpl) Update(ctx context.Context, chainTypeAsset *ChainTypeAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&ChainTypeAsset{}).Where("id = ?", chainTypeAsset.Id).Updates(chainTypeAsset)
	err := ret.Error
	if err != nil {
		log.Errore("update chainTypeAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAssetRepoImpl) FindByID(ctx context.Context, id int64) (*ChainTypeAsset, error) {
	var chainTypeAsset *ChainTypeAsset
	ret := r.gormDB.WithContext(ctx).First(&chainTypeAsset, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query chainTypeAsset failed", err)
		}
		return nil, err
	}
	return chainTypeAsset, nil
}

func (r *ChainTypeAssetRepoImpl) ListByID(ctx context.Context, id int64) ([]*ChainTypeAsset, error) {
	var chainTypeAssetList []*ChainTypeAsset
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&chainTypeAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query chainTypeAsset failed", err)
		return nil, err
	}
	return chainTypeAssetList, nil
}

func (r *ChainTypeAssetRepoImpl) ListAll(ctx context.Context) ([]*ChainTypeAsset, error) {
	var chainTypeAssetList []*ChainTypeAsset
	ret := r.gormDB.WithContext(ctx).Find(&chainTypeAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query chainTypeAsset failed", err)
		return nil, err
	}
	return chainTypeAssetList, nil
}

func (r *ChainTypeAssetRepoImpl) PageList(ctx context.Context, req *AssetRequest) ([]*ChainTypeAsset, int64, error) {
	var chainTypeAssetList []*ChainTypeAsset
	var total int64
	db := r.gormDB.WithContext(ctx).Table("chain_type_asset")

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

	ret := db.Find(&chainTypeAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query chainTypeAsset failed", err)
		return nil, 0, err
	}
	return chainTypeAssetList, total, nil
}

func (r *ChainTypeAssetRepoImpl) List(ctx context.Context, req *AssetRequest) ([]*ChainTypeAsset, error) {
	var chainTypeAssetList []*ChainTypeAsset
	db := r.gormDB.WithContext(ctx).Table("chain_type_asset")

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

	ret := db.Find(&chainTypeAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("list query chainTypeAsset failed", err)
		return nil, err
	}
	return chainTypeAssetList, nil
}

//维度：日期
func (r *ChainTypeAssetRepoImpl) ListAmountGroupByDt(ctx context.Context, req *AssetRequest) ([]*pb.AssetHistoryFundAmountResponse, error) {
	var assetHistoryFundAmountResponse []*pb.AssetHistoryFundAmountResponse

	sqlStr := "select sum(cny_amount) as cny_amount, sum(usd_amount) as usd_amount, dt " +
		"from chain_type_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		if req.ChainName != "all" {
			sqlStr += " and chain_name = '" + req.ChainName + "'"
		} else {
			sqlStr += " and chain_name = ''"
		}
	}
	if req.UidType != 0 {
		if req.UidType != -1 {
			sqlStr += " and uid_type = " + strconv.Itoa(int(req.UidType))
		} else {
			sqlStr += " and uid_type = 0"
		}
	}
	if req.StartTime > 0 {
		sqlStr += " and dt >= " + strconv.Itoa(int(req.StartTime))
	}
	if req.StopTime > 0 {
		sqlStr += " and dt < " + strconv.Itoa(int(req.StopTime))
	}
	sqlStr += " group by dt"

	ret := r.gormDB.WithContext(ctx).Table("chain_type_asset").Raw(sqlStr).Find(&assetHistoryFundAmountResponse)
	err := ret.Error
	if err != nil {
		log.Errore("page query chainTypeAsset failed", err)
		return nil, err
	}
	return assetHistoryFundAmountResponse, nil
}

func (r *ChainTypeAssetRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&ChainTypeAsset{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAssetRepoImpl) DeleteByIDs(ctx context.Context, ids []int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&ChainTypeAsset{}, ids)
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *ChainTypeAssetRepoImpl) Delete(ctx context.Context, req *AssetRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("chain_type_asset")

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

	ret := db.Delete(&ChainTypeAsset{})
	err := ret.Error
	if err != nil {
		log.Errore("delete chainTypeAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
