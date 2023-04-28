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

// UserNftAsset is a UserNftAsset model.
type UserNftAsset struct {
	Id               int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName        string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_address_token_address_token_id"`
	Uid              string `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address          string `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_chain_name_address_token_address_token_id"`
	TokenAddress     string `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_name_address_token_address_token_id"`
	TokenUri         string `json:"tokenUri" form:"tokenUri" gorm:"type:character varying(256)"`
	TokenId          string `json:"tokenId" form:"tokenId" gorm:"type:character varying(128);index:,unique,composite:unique_chain_name_address_token_address_token_id"`
	Balance          string `json:"balance" form:"balance" gorm:"type:character varying(256);"`
	TokenType        string `json:"tokenType" form:"tokenType" gorm:"type:character varying(12);index"`
	CollectionName   string `json:"collectionName" form:"collectionName" gorm:"type:character varying(128)"`
	Symbol           string `json:"symbol" form:"symbol" gorm:"type:character varying(72);index"`
	Name             string `json:"name" form:"name" gorm:"type:character varying(128)"`
	ItemName         string `json:"itemName" form:"itemName" gorm:"type:character varying(128)"`
	ItemUri          string `json:"itemUri" form:"itemUri" gorm:"type:character varying(256)"`
	ItemOriginalUri  string `json:"itemOriginalUri" form:"itemOriginalUri" gorm:"type:character varying(256)"`
	ItemAnimationUri string `json:"itemAnimationUri" form:"itemAnimationUri" gorm:"type:character varying(256)"`
	Data             string `json:"data" form:"data"`
	CreatedAt        int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt        int64  `json:"updatedAt" form:"updatedAt"`
}

type UserNftAssetGroup struct {
	ChainName      string `json:"chainName,omitempty"`
	Uid            string `json:"uid,omitempty"`
	Address        string `json:"address,omitempty"`
	TokenAddress   string `json:"tokenAddress,omitempty"`
	TokenUri       string `json:"tokenUri,omitempty"`
	TokenId        string `json:"tokenId,omitempty"`
	Balance        string `json:"balance,omitempty"`
	TokenType      string `json:"tokenType,omitempty"`
	CollectionName string `json:"collectionName,omitempty"`
	TokenIdAmount  int64  `json:"tokenIdAmount,omitempty"`
	Total          int64  `json:"total,omitempty"`
	TotalBalance   int64  `json:"totalBalance,omitempty"`
}

type NftAssetRequest struct {
	ChainName          string
	Uid                string
	UidList            []string
	Address            string
	AddressList        []string
	TokenAddressList   []string
	TokenIdList        []string
	AmountType         int32
	CollectionNameLike string
}

func (userNftAsset UserNftAsset) TableName() string {
	return "user_nft_asset"
}

// UserNftAssetRepo is a Greater repo.
type UserNftAssetRepo interface {
	Save(context.Context, *UserNftAsset) (int64, error)
	BatchSave(context.Context, []*UserNftAsset) (int64, error)
	SaveOrUpdate(context.Context, *UserNftAsset) (int64, error)
	BatchSaveOrUpdate(context.Context, []*UserNftAsset) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*UserNftAsset, int) (int64, error)
	Update(context.Context, *UserNftAsset) (int64, error)
	FindByID(context.Context, int64) (*UserNftAsset, error)
	FindByUniqueKey(context.Context, *pb.NftAssetRequest) (*UserNftAsset, error)
	ListByID(context.Context, int64) ([]*UserNftAsset, error)
	ListAll(context.Context) ([]*UserNftAsset, error)
	PageList(context.Context, *pb.PageListNftAssetRequest) ([]*UserNftAsset, int64, error)
	List(context.Context, *NftAssetRequest) ([]*UserNftAsset, error)
	ListBalanceGroup(context.Context, *pb.PageListNftAssetRequest) ([]*UserNftAsset, error)
	PageListGroup(context.Context, *pb.PageListNftAssetRequest) ([]*UserNftAssetGroup, int64, int64, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
	Delete(context.Context, *NftAssetRequest) (int64, error)
}

type UserNftAssetRepoImpl struct {
	gormDB *gorm.DB
}

var UserNftAssetRepoClient UserNftAssetRepo

// NewUserNftAssetRepo new a UserNftAsset repo.
func NewUserNftAssetRepo(gormDB *gorm.DB) UserNftAssetRepo {
	UserNftAssetRepoClient = &UserNftAssetRepoImpl{
		gormDB: gormDB,
	}
	return UserNftAssetRepoClient
}

func (r *UserNftAssetRepoImpl) Save(ctx context.Context, userNftAsset *UserNftAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(userNftAsset)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(userNftAsset.Id, 10), Data: 0}
			log.Warne("insert userNftAsset failed", err)
		} else {
			log.Errore("insert userNftAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserNftAssetRepoImpl) BatchSave(ctx context.Context, userNftAssets []*UserNftAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(userNftAssets, len(userNftAssets))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(userNftAssets)), Data: 0}
			log.Warne("batch insert userNftAsset failed", err)
		} else {
			log.Errore("batch insert userNftAsset failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserNftAssetRepoImpl) SaveOrUpdate(ctx context.Context, userNftAsset *UserNftAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}, {Name: "token_id"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"token_uri", "balance", "collection_name", "item_name", "item_uri",
			"item_original_uri", "item_animation_uri", "updated_at"}),
	}).Create(&userNftAsset)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update userNftAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserNftAssetRepoImpl) BatchSaveOrUpdate(ctx context.Context, userNftAssets []*UserNftAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}, {Name: "token_id"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"token_uri", "balance", "collection_name", "item_name", "item_uri",
			"item_original_uri", "item_animation_uri", "updated_at"}),
	}).Create(&userNftAssets)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update userNftAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserNftAssetRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, userNftAssets []*UserNftAsset, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(userNftAssets)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subUserNftAssetss := userNftAssets[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subUserNftAssetss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UserNftAssetRepoImpl) Update(ctx context.Context, userNftAsset *UserNftAsset) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserNftAsset{}).Where("id = ?", userNftAsset.Id).Updates(userNftAsset)
	err := ret.Error
	if err != nil {
		log.Errore("update userNftAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserNftAssetRepoImpl) FindByID(ctx context.Context, id int64) (*UserNftAsset, error) {
	var userNftAsset *UserNftAsset
	ret := r.gormDB.WithContext(ctx).First(&userNftAsset, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query userNftAsset failed", err)
		}
		return nil, err
	}
	return userNftAsset, nil
}

func (r *UserNftAssetRepoImpl) FindByUniqueKey(ctx context.Context, req *pb.NftAssetRequest) (*UserNftAsset, error) {
	var userNftAsset *UserNftAsset
	ret := r.gormDB.WithContext(ctx).First(&userNftAsset, req)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query userNftAsset failed", err)
		}
		return nil, err
	}
	return userNftAsset, nil
}

func (r *UserNftAssetRepoImpl) ListByID(ctx context.Context, id int64) ([]*UserNftAsset, error) {
	var userNftAssetList []*UserNftAsset
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query userNftAsset failed", err)
		return nil, err
	}
	return userNftAssetList, nil
}

func (r *UserNftAssetRepoImpl) ListAll(ctx context.Context) ([]*UserNftAsset, error) {
	var userNftAssetList []*UserNftAsset
	ret := r.gormDB.WithContext(ctx).Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("query userNftAsset failed", err)
		return nil, err
	}
	return userNftAssetList, nil
}

func (r *UserNftAssetRepoImpl) PageList(ctx context.Context, req *pb.PageListNftAssetRequest) ([]*UserNftAsset, int64, error) {
	var userNftAssetList []*UserNftAsset
	var total int64
	db := r.gormDB.WithContext(ctx).Table("user_nft_asset")

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
	if len(req.TokenIdList) > 0 {
		db = db.Where("token_id in(?)", req.TokenIdList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
	}
	if req.CollectionNameLike != "" {
		db = db.Where("collection_name like ?", "%"+req.CollectionNameLike+"%")
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

	ret := db.Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userNftAsset failed", err)
		return nil, 0, err
	}
	return userNftAssetList, total, nil
}

func (r *UserNftAssetRepoImpl) List(ctx context.Context, req *NftAssetRequest) ([]*UserNftAsset, error) {
	var userNftAssetList []*UserNftAsset
	db := r.gormDB.WithContext(ctx).Table("user_nft_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if len(req.TokenIdList) > 0 {
		db = db.Where("token_id in(?)", req.TokenIdList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
	}
	if req.CollectionNameLike != "" {
		db = db.Where("collection_name like ?", "%"+req.CollectionNameLike+"%")
	}

	ret := db.Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("list query userNftAssets failed", err)
		return nil, err
	}
	return userNftAssetList, nil
}

func (r *UserNftAssetRepoImpl) ListBalanceGroup(ctx context.Context, req *pb.PageListNftAssetRequest) ([]*UserNftAsset, error) {
	var userNftAssetList []*UserNftAsset

	sqlStr := "select chain_name, token_address, sum(cast(balance as numeric)) as balance " +
		"from user_nft_asset " +
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
	if len(req.TokenIdList) > 0 {
		tokenIdList := strings.ReplaceAll(utils.ListToString(req.TokenIdList), "\"", "'")
		sqlStr += " and token_id in (" + tokenIdList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	if req.CollectionNameLike != "" {
		sqlStr += " and collection_name like '%" + req.CollectionNameLike + "%'"
	}
	sqlStr += " group by chain_name, token_address"

	ret := r.gormDB.WithContext(ctx).Table("user_nft_asset").Raw(sqlStr).Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userNftAsset failed", err)
		return nil, err
	}
	return userNftAssetList, nil
}

func (r *UserNftAssetRepoImpl) PageListGroup(ctx context.Context, req *pb.PageListNftAssetRequest) ([]*UserNftAssetGroup, int64, int64, error) {
	var userNftAssetList []*UserNftAssetGroup
	var total, totalBalance int64

	sqlStr := "with t as(" +
		"select chain_name, uid, address, token_address, token_type, (array_agg(token_id))[1] as token_id, " +
		"sum(cast(balance as numeric)) as balance, count(token_id) as token_id_amount " +
		"from user_nft_asset " +
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
	if len(req.TokenIdList) > 0 {
		tokenIdList := strings.ReplaceAll(utils.ListToString(req.TokenIdList), "\"", "'")
		sqlStr += " and token_id in (" + tokenIdList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	if req.CollectionNameLike != "" {
		sqlStr += " and collection_name like '%" + req.CollectionNameLike + "%'"
	}
	sqlStr += " group by chain_name, uid, address, token_address, token_type" +
		")"

	sqlStr += " select t.* "
	if req.Total {
		sqlStr += ", t1.* "
	}
	sqlStr += " from t "
	if req.Total {
		sqlStr += " inner join (select count(*) as total, sum(balance) as total_balance from t) as t1 on 1=1 "
	}
	sqlStr += " order by t." + req.OrderBy

	if req.DataDirection == 0 {
		if req.PageNum > 0 {
			sqlStr += " offset " + strconv.Itoa(int(req.PageNum-1)*int(req.PageSize))
		} else {
			sqlStr += " offset 0 "
		}
	}
	sqlStr += " limit " + strconv.Itoa(int(req.PageSize))

	ret := r.gormDB.WithContext(ctx).Table("user_nft_asset").Raw(sqlStr).Find(&userNftAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userNftAsset failed", err)
		return nil, 0, 0, err
	}
	if len(userNftAssetList) > 0 {
		total = userNftAssetList[0].Total
		totalBalance = userNftAssetList[0].TotalBalance
	}
	return userNftAssetList, total, totalBalance, nil
}

func (r *UserNftAssetRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserNftAsset{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete userNftAsset failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserNftAssetRepoImpl) DeleteByIDs(ctx context.Context, ids []int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserNftAsset{}, ids)
	err := ret.Error
	if err != nil {
		log.Errore("delete userNftAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserNftAssetRepoImpl) Delete(ctx context.Context, req *NftAssetRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("user_nft_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if len(req.TokenIdList) > 0 {
		db = db.Where("token_id in(?)", req.TokenIdList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
	}
	if req.CollectionNameLike != "" {
		db = db.Where("collection_name like ?", "%"+req.CollectionNameLike+"%")
	}

	ret := db.Delete(&UserAsset{})
	err := ret.Error
	if err != nil {
		log.Errore("delete userNftAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
