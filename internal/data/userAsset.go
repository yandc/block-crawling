package data

import (
	pb "block-crawling/api/transaction/v1"
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

// UserAsset is a UserAsset model.
type UserAsset struct {
	Id           int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName    string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_address_token_address"`
	Uid          string `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address      string `json:"address" form:"address" gorm:"type:character varying(66);index:,unique,composite:unique_chain_name_address_token_address"`
	TokenAddress string `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_name_address_token_address"`
	Balance      string `json:"balance" form:"balance" gorm:"type:character varying(256);"`
	Decimals     int32  `json:"decimals" form:"decimals"`
	Symbol       string `json:"symbol" form:"symbol" gorm:"type:character varying(72);index"`
	CreatedAt    int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt    int64  `json:"updatedAt" form:"updatedAt"`
}

func (userAsset UserAsset) TableName() string {
	return "user_asset"
}

// UserAssetRepo is a Greater repo.
type UserAssetRepo interface {
	Save(context.Context, *UserAsset) (int64, error)
	BatchSave(context.Context, []*UserAsset) (int64, error)
	SaveOrUpdate(context.Context, *UserAsset) (int64, error)
	BatchSaveOrUpdate(context.Context, []*UserAsset) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*UserAsset, int) (int64, error)
	Update(context.Context, *UserAsset) (int64, error)
	FindByID(context.Context, int64) (*UserAsset, error)
	ListByID(context.Context, int64) ([]*UserAsset, error)
	ListAll(context.Context) ([]*UserAsset, error)
	PageList(context.Context, *pb.PageListAssetRequest) ([]*UserAsset, int64, error)
	GroupListBalance(context.Context, *pb.PageListAssetRequest) ([]*UserAsset, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
}

type UserAssetRepoImpl struct {
	gormDB *gorm.DB
}

var UserAssetRepoClient UserAssetRepo

// NewUserAssetRepo new a UserAsset repo.
func NewUserAssetRepo(gormDB *gorm.DB) UserAssetRepo {
	UserAssetRepoClient = &UserAssetRepoImpl{
		gormDB: gormDB,
	}
	return UserAssetRepoClient
}

func (r *UserAssetRepoImpl) Save(ctx context.Context, userAsset *UserAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(userAsset)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(userAsset.Id, 10), Data: 0}
			log.Warne("insert userAsset failed", err)
		} else {
			log.Errore("insert userAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetRepoImpl) BatchSave(ctx context.Context, userAssets []*UserAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(userAssets, len(userAssets))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(userAssets)), Data: 0}
			log.Warne("batch insert userAsset failed", err)
		} else {
			log.Errore("batch insert userAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetRepoImpl) SaveOrUpdate(ctx context.Context, userAsset *UserAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"balance", "updated_at"}),
	}).Create(&userAsset)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update userAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetRepoImpl) BatchSaveOrUpdate(ctx context.Context, userAssets []*UserAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"balance", "updated_at"}),
	}).Create(&userAssets)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update userAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, userAssets []*UserAsset, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(userAssets)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subUserAssetss := userAssets[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subUserAssetss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UserAssetRepoImpl) Update(ctx context.Context, userAsset *UserAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserAsset{}).Where("id = ?", userAsset.Id).Updates(userAsset)
	err := ret.Error
	if err != nil {
		log.Errore("update userAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetRepoImpl) FindByID(ctx context.Context, id int64) (*UserAsset, error) {
	var userAsset *UserAsset
	ret := r.gormDB.WithContext(ctx).First(&userAsset, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query userAsset failed", err)
		}
		return nil, err
	}
	return userAsset, nil
}

func (r *UserAssetRepoImpl) ListByID(ctx context.Context, id int64) ([]*UserAsset, error) {
	var userAssetList []*UserAsset
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) ListAll(ctx context.Context) ([]*UserAsset, error) {
	var userAssetList []*UserAsset
	ret := r.gormDB.WithContext(ctx).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) PageList(ctx context.Context, req *pb.PageListAssetRequest) ([]*UserAsset, int64, error) {
	var userAssetList []*UserAsset
	var total int64
	db := r.gormDB.WithContext(ctx).Table("user_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
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

	ret := db.Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, 0, err
	}
	return userAssetList, total, nil
}

func (r *UserAssetRepoImpl) GroupListBalance(ctx context.Context, req *pb.PageListAssetRequest) ([]*UserAsset, error) {
	var userAssetList []*UserAsset

	sqlStr := "select chain_name, token_address, sum(cast(balance as numeric)) as balance " +
		"from user_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.Uid != "" {
		sqlStr += " and uid = '" + req.Uid + "'"
	}
	if len(req.AddressList) > 0 {
		addressList := strings.ReplaceAll(utils.ListToString(req.AddressList), "\"", "'")
		sqlStr += " and address in (" + addressList + ")"
	}
	if len(req.TokenAddressList) > 0 {
		tokenAddressList := strings.ReplaceAll(utils.ListToString(req.TokenAddressList), "\"", "'")
		sqlStr += " and token_address in (" + tokenAddressList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	sqlStr += " group by chain_name, token_address"

	ret := r.gormDB.WithContext(ctx).Table("user_asset").Raw(sqlStr).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserAsset{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete userAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetRepoImpl) DeleteByIDs(ctx context.Context, ids []int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserAsset{}, ids)
	err := ret.Error
	if err != nil {
		log.Errore("delete userAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
