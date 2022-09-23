package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
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
	PageList(context.Context, *pb.PageListRequest) ([]*UserAsset, int64, error)
	DeleteByID(context.Context, int64) (int64, error)
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
		DoUpdates: clause.AssignmentColumns([]string{"amount", "updated_at"}),
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
		DoUpdates: clause.AssignmentColumns([]string{"amount", "updated_at"}),
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

func (r *UserAssetRepoImpl) PageList(ctx context.Context, req *pb.PageListRequest) ([]*UserAsset, int64, error) {
	var userAssetList []*UserAsset
	var total int64
	db := r.gormDB.WithContext(ctx)

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
	/*if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}*/
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
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

	if req.Total {
		// 统计总记录数
		db.Count(&total)
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
