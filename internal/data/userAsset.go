package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UserAsset is a UserAsset model.
type UserAsset struct {
	Id           int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName    string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_address_token_address"`
	Uid          string `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address      string `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_chain_name_address_token_address"`
	TokenAddress string `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_name_address_token_address"`
	Balance      string `json:"balance" form:"balance" gorm:"type:character varying(256);"`
	Decimals     int32  `json:"decimals" form:"decimals"`
	Symbol       string `json:"symbol" form:"symbol" gorm:"type:character varying(72);index"`
	UidType      int8   `json:"uidType" form:"uidType"`
	CreatedAt    int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt    int64  `json:"updatedAt" form:"updatedAt"`
}

type AssetRequest struct {
	ChainName                        string
	Uid                              string
	UidList                          []string
	Address                          string
	AddressList                      []string
	TokenAddress                     string
	TokenAddressList                 []string
	UidType                          int32
	UidTypeList                      []int32
	AmountType                       int32
	ChainNameAddressTokenAddressList []*AssetRequest
	StartTime                        int64
	StopTime                         int64
	GroupBy                          string
	OrderBy                          string
	DataDirection                    int32
	StartIndex                       int64
	PageNum                          int32
	PageSize                         int32
	Total                            bool
}

type UserAssetWrapper struct {
	UserAsset
	AddressAmount int64 `json:"addressAmount,omitempty"`
	Total         int64 `json:"total,omitempty"`
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
	BatchSaveOrUpdateSelectiveByColumns(context.Context, []string, []*UserAsset) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, []string, []*UserAsset, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, []*UserAsset, int) (int64, error)
	Update(context.Context, *UserAsset) (int64, error)
	UpdateZeroByAddress(context.Context, string) (int64, error)
	UpdateUidTypeByUid(context.Context, string, int8) (int64, error)
	FindByID(context.Context, int64) (*UserAsset, error)
	ListByID(context.Context, int64) ([]*UserAsset, error)
	ListAll(context.Context) ([]*UserAsset, error)
	PageList(context.Context, *AssetRequest) ([]*UserAsset, int64, error)
	List(context.Context, *AssetRequest) ([]*UserAsset, error)
	ListBalance(context.Context, *AssetRequest) ([]*UserAsset, error)
	ListBalanceGroup(context.Context, *AssetRequest) ([]*UserAsset, error)
	PageListBalanceGroup(context.Context, *AssetRequest) ([]*UserAssetWrapper, int64, error)
	ListAddressAmountGroup(context.Context, *AssetRequest) ([]*UserAssetWrapper, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
	Delete(context.Context, *AssetRequest) (int64, error)
	ListByChainNames(context.Context, []string) ([]*UserAsset, error)
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
		DoUpdates: clause.AssignmentColumns([]string{"decimals", "symbol", "balance", "updated_at"}),
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

func (r *UserAssetRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, columns []string, userAssets []*UserAsset) (int64, error) {
	var columnList []clause.Column
	for _, column := range columns {
		columnList = append(columnList, clause.Column{Name: column})
	}
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   columnList,
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"chain_name":    gorm.Expr("case when excluded.chain_name != '' then excluded.chain_name else user_asset.chain_name end"),
			"uid":           gorm.Expr("case when excluded.uid != '' then excluded.uid else user_asset.uid end"),
			"address":       gorm.Expr("case when excluded.address != '' then excluded.address else user_asset.address end"),
			"token_address": gorm.Expr("case when excluded.token_address != '' then excluded.token_address else user_asset.token_address end"),
			"balance":       gorm.Expr("case when excluded.balance != '' then excluded.balance else user_asset.balance end"),
			"decimals":      gorm.Expr("case when excluded.decimals != 0 then excluded.decimals else user_asset.decimals end"),
			"symbol":        gorm.Expr("case when excluded.symbol != '' then excluded.symbol else user_asset.symbol end"),
			"uid_type":      gorm.Expr("case when excluded.uid_type != 0 then excluded.uid_type else user_asset.uid_type end"),
			"updated_at":    gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&userAssets)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective userAsset failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, columns []string, userAssets []*UserAsset, pageSize int) (int64, error) {
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

		affected, err := r.BatchSaveOrUpdateSelectiveByColumns(ctx, columns, subUserAssetss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UserAssetRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, userAssets []*UserAsset, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, []string{"id"}, userAssets, pageSize)
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

func (r *UserAssetRepoImpl) UpdateUidTypeByUid(ctx context.Context, uid string, uidType int8) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserAsset{}).Where("uid = ?", uid).Update("uid_type", uidType)
	err := ret.Error
	if err != nil {
		log.Errore("update uidType by uid", err)
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

func (r *UserAssetRepoImpl) PageList(ctx context.Context, req *AssetRequest) ([]*UserAsset, int64, error) {
	var userAssetList []*UserAsset
	var total int64
	db := r.gormDB.WithContext(ctx).Table("user_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.UidList) > 0 {
		db = db.Where("uid in(?)", req.UidList)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if req.UidType != 0 {
		db = db.Where("uid_type = ?", req.UidType)
	}
	if len(req.UidTypeList) > 0 {
		db = db.Where("uid_type in(?)", req.UidTypeList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
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

	ret := db.Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, 0, err
	}
	return userAssetList, total, nil
}

func (r *UserAssetRepoImpl) List(ctx context.Context, req *AssetRequest) ([]*UserAsset, error) {
	var userAssetList []*UserAsset
	db := r.gormDB.WithContext(ctx).Table("user_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.UidList) > 0 {
		db = db.Where("uid in(?)", req.UidList)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if req.TokenAddress != "" {
		db = db.Where("token_address = ?", req.TokenAddress)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if req.UidType != 0 {
		db = db.Where("uid_type = ?", req.UidType)
	}
	if len(req.UidTypeList) > 0 {
		db = db.Where("uid_type in(?)", req.UidTypeList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
	}
	if len(req.ChainNameAddressTokenAddressList) > 0 {
		chainNameAddressTokenAddressList := req.ChainNameAddressTokenAddressList
		chainNameAddressTokenAddress := "("
		for _, record := range chainNameAddressTokenAddressList {
			chainNameAddressTokenAddress += "('" + record.ChainName + "','" + record.Address + "','" + record.TokenAddress + "'),"
		}
		chainNameAddressTokenAddress = chainNameAddressTokenAddress[:len(chainNameAddressTokenAddress)-1]
		chainNameAddressTokenAddress += ")"
		db = db.Where("(chain_name, address, token_address) in" + chainNameAddressTokenAddress)
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
	}

	ret := db.Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("list query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) ListBalance(ctx context.Context, req *AssetRequest) ([]*UserAsset, error) {
	var userAssetList []*UserAsset

	sqlStr := "select chain_name, uid, token_address, balance " +
		"from user_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.Uid != "" {
		sqlStr += " and uid = '" + req.Uid + "'"
	}
	if req.Address != "" {
		sqlStr += " and address = '" + req.Address + "'"
	}
	if len(req.UidList) > 0 {
		uidList := strings.ReplaceAll(utils.ListToString(req.UidList), "\"", "'")
		sqlStr += " and uid in (" + uidList + ")"
	}
	if len(req.AddressList) > 0 {
		addressList := strings.ReplaceAll(utils.ListToString(req.AddressList), "\"", "'")
		sqlStr += " and address in (" + addressList + ")"
	}
	if len(req.TokenAddressList) > 0 {
		tokenAddressList := strings.ReplaceAll(utils.ListToString(req.TokenAddressList), "\"", "'")
		sqlStr += " and token_address in (" + tokenAddressList + ")"
	}
	if req.UidType != 0 {
		sqlStr += " and uid_type = " + strconv.Itoa(int(req.UidType))
	}
	if len(req.UidTypeList) > 0 {
		uidTypeList := strings.ReplaceAll(utils.ListToString(req.UidTypeList), "\"", "")
		sqlStr += " and uid_type in (" + uidTypeList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}

	ret := r.gormDB.WithContext(ctx).Table("user_asset").Raw(sqlStr).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) ListBalanceGroup(ctx context.Context, req *AssetRequest) ([]*UserAsset, error) {
	var userAssetList []*UserAsset

	groupBy := req.GroupBy
	if groupBy != "" {
		groupBy += ", "
	}
	sqlStr := "select " + groupBy + "sum(cast(balance as numeric)) as balance " +
		"from user_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.Uid != "" {
		sqlStr += " and uid = '" + req.Uid + "'"
	}
	if req.Address != "" {
		sqlStr += " and address = '" + req.Address + "'"
	}
	if len(req.UidList) > 0 {
		uidList := strings.ReplaceAll(utils.ListToString(req.UidList), "\"", "'")
		sqlStr += " and uid in (" + uidList + ")"
	}
	if len(req.AddressList) > 0 {
		addressList := strings.ReplaceAll(utils.ListToString(req.AddressList), "\"", "'")
		sqlStr += " and address in (" + addressList + ")"
	}
	if len(req.TokenAddressList) > 0 {
		tokenAddressList := strings.ReplaceAll(utils.ListToString(req.TokenAddressList), "\"", "'")
		sqlStr += " and token_address in (" + tokenAddressList + ")"
	}
	if req.UidType != 0 {
		sqlStr += " and uid_type = " + strconv.Itoa(int(req.UidType))
	}
	if len(req.UidTypeList) > 0 {
		uidTypeList := strings.ReplaceAll(utils.ListToString(req.UidTypeList), "\"", "")
		sqlStr += " and uid_type in (" + uidTypeList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	if req.GroupBy != "" {
		sqlStr += " group by " + req.GroupBy
	}

	ret := r.gormDB.WithContext(ctx).Table("user_asset").Raw(sqlStr).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}

func (r *UserAssetRepoImpl) PageListBalanceGroup(ctx context.Context, req *AssetRequest) ([]*UserAssetWrapper, int64, error) {
	var userAssetList []*UserAssetWrapper
	var total int64

	groupBy := req.GroupBy
	if groupBy != "" {
		groupBy += ", "
	}
	sqlStr := "with t as(" +
		"select " + groupBy + "sum(cast(balance as numeric)) as balance "
	sqlStr += "from user_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.Uid != "" {
		sqlStr += " and uid = '" + req.Uid + "'"
	}
	if req.Address != "" {
		sqlStr += " and address = '" + req.Address + "'"
	}
	if len(req.UidList) > 0 {
		uidList := strings.ReplaceAll(utils.ListToString(req.UidList), "\"", "'")
		sqlStr += " and uid in (" + uidList + ")"
	}
	if len(req.AddressList) > 0 {
		addressList := strings.ReplaceAll(utils.ListToString(req.AddressList), "\"", "'")
		sqlStr += " and address in (" + addressList + ")"
	}
	if len(req.TokenAddressList) > 0 {
		tokenAddressList := strings.ReplaceAll(utils.ListToString(req.TokenAddressList), "\"", "'")
		sqlStr += " and token_address in (" + tokenAddressList + ")"
	}
	if req.UidType != 0 {
		sqlStr += " and uid_type = " + strconv.Itoa(int(req.UidType))
	}
	if len(req.UidTypeList) > 0 {
		uidTypeList := strings.ReplaceAll(utils.ListToString(req.UidTypeList), "\"", "")
		sqlStr += " and uid_type in (" + uidTypeList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}

	if req.GroupBy != "" {
		sqlStr += " group by " + req.GroupBy
	}
	sqlStr += ")"

	sqlStr += " select t.* "
	if req.Total {
		sqlStr += ", t1.* "
	}
	sqlStr += " from t "
	if req.Total {
		sqlStr += " inner join (select count(*) as total from t) as t1 on 1=1 "
	}
	if req.OrderBy != "" {
		sqlStr += " order by t." + req.OrderBy
	}

	if req.DataDirection == 0 {
		if req.PageNum > 0 {
			sqlStr += " offset " + strconv.Itoa(int(req.PageNum-1)*int(req.PageSize))
		} else {
			sqlStr += " offset 0 "
		}
	}
	sqlStr += " limit " + strconv.Itoa(int(req.PageSize))

	ret := r.gormDB.WithContext(ctx).Table("user_asset").Raw(sqlStr).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, 0, err
	}
	if len(userAssetList) > 0 {
		total = userAssetList[0].Total
	}
	return userAssetList, total, nil
}

func (r *UserAssetRepoImpl) ListAddressAmountGroup(ctx context.Context, req *AssetRequest) ([]*UserAssetWrapper, error) {
	var userAssetList []*UserAssetWrapper

	groupBy := req.GroupBy
	if groupBy != "" {
		groupBy += ", "
	}
	sqlStr := "select " + groupBy + "count(distinct address) as address_amount " +
		"from user_asset " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if req.Uid != "" {
		sqlStr += " and uid = '" + req.Uid + "'"
	}
	if req.Address != "" {
		sqlStr += " and address = '" + req.Address + "'"
	}
	if len(req.UidList) > 0 {
		uidList := strings.ReplaceAll(utils.ListToString(req.UidList), "\"", "'")
		sqlStr += " and uid in (" + uidList + ")"
	}
	if len(req.AddressList) > 0 {
		addressList := strings.ReplaceAll(utils.ListToString(req.AddressList), "\"", "'")
		sqlStr += " and address in (" + addressList + ")"
	}
	if len(req.TokenAddressList) > 0 {
		tokenAddressList := strings.ReplaceAll(utils.ListToString(req.TokenAddressList), "\"", "'")
		sqlStr += " and token_address in (" + tokenAddressList + ")"
	}
	if req.UidType != 0 {
		sqlStr += " and uid_type = " + strconv.Itoa(int(req.UidType))
	}
	if len(req.UidTypeList) > 0 {
		uidTypeList := strings.ReplaceAll(utils.ListToString(req.UidTypeList), "\"", "")
		sqlStr += " and uid_type in (" + uidTypeList + ")"
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	if req.GroupBy != "" {
		sqlStr += " group by " + req.GroupBy
	}

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

func (r *UserAssetRepoImpl) Delete(ctx context.Context, req *AssetRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("user_asset")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.UidList) > 0 {
		db = db.Where("uid in(?)", req.UidList)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if req.TokenAddress != "" {
		db = db.Where("token_address = ?", req.TokenAddress)
	}
	if len(req.TokenAddressList) > 0 {
		db = db.Where("token_address in(?)", req.TokenAddressList)
	}
	if req.UidType != 0 {
		db = db.Where("uid_type = ?", req.UidType)
	}
	if len(req.UidTypeList) > 0 {
		db = db.Where("uid_type in(?)", req.UidTypeList)
	}
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			db = db.Where("(balance is null or balance = '' or balance = '0')")
		} else if req.AmountType == 2 {
			db = db.Where("(balance is not null and balance != '' and balance != '0')")
		}
	}
	if len(req.ChainNameAddressTokenAddressList) > 0 {
		chainNameAddressTokenAddressList := req.ChainNameAddressTokenAddressList
		chainNameAddressTokenAddress := "("
		for _, record := range chainNameAddressTokenAddressList {
			chainNameAddressTokenAddress += "('" + record.ChainName + "','" + record.Address + "','" + record.TokenAddress + "'),"
		}
		chainNameAddressTokenAddress = chainNameAddressTokenAddress[:len(chainNameAddressTokenAddress)-1]
		chainNameAddressTokenAddress += ")"
		db = db.Where("(chain_name, address, token_address) in" + chainNameAddressTokenAddress)
	}

	ret := db.Delete(&UserAsset{})
	err := ret.Error
	if err != nil {
		log.Errore("delete userAssets failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetRepoImpl) UpdateZeroByAddress(ctx context.Context, address string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserAsset{}).Where("address = ?", address).Update("balance", "0")
	err := ret.Error
	if err != nil {
		log.Errore("update balance zero", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
func (r *UserAssetRepoImpl) ListByChainNames(ctx context.Context, chainNames []string) ([]*UserAsset, error) {
	var userAssetList []*UserAsset

	ret := r.gormDB.WithContext(ctx).Table("user_asset").Where("chain_name in ?", chainNames).Find(&userAssetList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAsset failed", err)
		return nil, err
	}
	return userAssetList, nil
}
