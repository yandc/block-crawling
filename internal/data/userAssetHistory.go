package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UserAssetHistory is a UserAssetHistory model.
type UserAssetHistory struct {
	Id           int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName    string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	Uid          string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address      string          `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	TokenAddress string          `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	Balance      string          `json:"balance" form:"balance" gorm:"type:character varying(256);"`
	Decimals     int32           `json:"decimals" form:"decimals"`
	Symbol       string          `json:"symbol" form:"symbol" gorm:"type:character varying(72);index"`
	CnyAmount    decimal.Decimal `json:"cnyAmount" form:"cnyAmount" gorm:"type:decimal(256,2);"`
	UsdAmount    decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	Dt           int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_address_token_address"`
	CreatedAt    int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt    int64           `json:"updatedAt" form:"updatedAt"`
}

type AssetHistoryRequest struct {
	ChainName                        string
	Uid                              string
	UidList                          []string
	Address                          string
	AddressList                      []string
	TokenAddress                     string
	TokenAddressList                 []string
	AmountType                       int32
	ChainNameAddressTokenAddressList []*AssetHistoryRequest
	StartTime                        int64
	StopTime                         int64
	OrderBy                          string
	DataDirection                    int32
	StartIndex                       int64
	PageNum                          int32
	PageSize                         int32
	Total                            bool
}

func (userAssetHistory UserAssetHistory) TableName() string {
	return "user_asset_history"
}

// UserAssetHistoryRepo is a Greater repo.
type UserAssetHistoryRepo interface {
	Save(context.Context, *UserAssetHistory) (int64, error)
	BatchSave(context.Context, []*UserAssetHistory) (int64, error)
	SaveOrUpdate(context.Context, *UserAssetHistory) (int64, error)
	BatchSaveOrUpdate(context.Context, []*UserAssetHistory) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*UserAssetHistory, int) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, []string, []*UserAssetHistory) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, []string, []*UserAssetHistory, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, []*UserAssetHistory, int) (int64, error)
	Update(context.Context, *UserAssetHistory) (int64, error)
	UpdateZeroByAddress(context.Context, string) (int64, error)
	FindByID(context.Context, int64) (*UserAssetHistory, error)
	ListByID(context.Context, int64) ([]*UserAssetHistory, error)
	ListAll(context.Context) ([]*UserAssetHistory, error)
	PageList(context.Context, *AssetHistoryRequest) ([]*UserAssetHistory, int64, error)
	List(context.Context, *AssetHistoryRequest) ([]*UserAssetHistory, error)
	ListBalance(context.Context, *AssetHistoryRequest) ([]*UserAssetHistory, error)
	ListBalanceGroup(context.Context, *AssetHistoryRequest) ([]*UserAssetHistory, error)
	ListBalanceGroupByUid(context.Context, *AssetHistoryRequest) ([]*UserAssetHistory, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByIDs(context.Context, []int64) (int64, error)
	Delete(context.Context, *AssetHistoryRequest) (int64, error)
	ListByChainNameAndDt(context.Context, []string, int64) ([]*UserAssetHistory, error)
	ListByRangeTimeAndAddressAndChainName(context.Context, int, int, string, string) ([]*UserAssetHistory, error)
}

type UserAssetHistoryRepoImpl struct {
	gormDB *gorm.DB
}

var UserAssetHistoryRepoClient UserAssetHistoryRepo

// NewUserAssetHistoryRepo new a UserAssetHistory repo.
func NewUserAssetHistoryRepo(gormDB *gorm.DB) UserAssetHistoryRepo {
	UserAssetHistoryRepoClient = &UserAssetHistoryRepoImpl{
		gormDB: gormDB,
	}
	return UserAssetHistoryRepoClient
}

func (r *UserAssetHistoryRepoImpl) Save(ctx context.Context, userAssetHistory *UserAssetHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(userAssetHistory)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(userAssetHistory.Id, 10), Data: 0}
			log.Warne("insert userAssetHistory failed", err)
		} else {
			log.Errore("insert userAssetHistory failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetHistoryRepoImpl) BatchSave(ctx context.Context, userAssetHistorys []*UserAssetHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(userAssetHistorys, len(userAssetHistorys))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(userAssetHistorys)), Data: 0}
			log.Warne("batch insert userAssetHistory failed", err)
		} else {
			log.Errore("batch insert userAssetHistory failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetHistoryRepoImpl) SaveOrUpdate(ctx context.Context, userAssetHistory *UserAssetHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"balance", "cny_amount", "usd_amount", "updated_at"}),
	}).Create(&userAssetHistory)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update userAssetHistory failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetHistoryRepoImpl) BatchSaveOrUpdate(ctx context.Context, userAssetHistorys []*UserAssetHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"decimals", "symbol", "balance", "cny_amount", "usd_amount", "updated_at"}),
	}).Create(&userAssetHistorys)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update userAssetHistory failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetHistoryRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, userAssetHistorys []*UserAssetHistory, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(userAssetHistorys)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subUserAssetHistoryss := userAssetHistorys[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subUserAssetHistoryss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UserAssetHistoryRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, columns []string, userAssetHistorys []*UserAssetHistory) (int64, error) {
	var columnList []clause.Column
	for _, column := range columns {
		columnList = append(columnList, clause.Column{Name: column})
	}
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   columnList,
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"chain_name":    gorm.Expr("case when excluded.chain_name != '' then excluded.chain_name else user_asset_history.chain_name end"),
			"uid":           gorm.Expr("case when excluded.uid != '' then excluded.uid else user_asset_history.uid end"),
			"address":       gorm.Expr("case when excluded.address != '' then excluded.address else user_asset_history.address end"),
			"token_address": gorm.Expr("case when excluded.token_address != '' then excluded.token_address else user_asset_history.token_address end"),
			"balance":       gorm.Expr("case when excluded.balance != '' then excluded.balance else user_asset_history.balance end"),
			"decimals":      gorm.Expr("case when excluded.decimals != 0 then excluded.decimals else user_asset_history.decimals end"),
			"symbol":        gorm.Expr("case when excluded.symbol != '' then excluded.symbol else user_asset_history.symbol end"),
			"cny_amount":    gorm.Expr("case when excluded.cny_amount != '' then excluded.cny_amount else user_asset_history.cny_amount end"),
			"usd_amount":    gorm.Expr("case when excluded.usd_amount != '' then excluded.usd_amount else user_asset_history.usd_amount end"),
			"updated_at":    gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&userAssetHistorys)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective userAssetHistory failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UserAssetHistoryRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, columns []string, userAssetHistorys []*UserAssetHistory, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(userAssetHistorys)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subUserAssetHistoryss := userAssetHistorys[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdateSelectiveByColumns(ctx, columns, subUserAssetHistoryss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UserAssetHistoryRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, userAssetHistorys []*UserAssetHistory, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, []string{"id"}, userAssetHistorys, pageSize)
}

func (r *UserAssetHistoryRepoImpl) Update(ctx context.Context, userAssetHistory *UserAssetHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserAssetHistory{}).Where("id = ?", userAssetHistory.Id).Updates(userAssetHistory)
	err := ret.Error
	if err != nil {
		log.Errore("update userAssetHistory failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetHistoryRepoImpl) FindByID(ctx context.Context, id int64) (*UserAssetHistory, error) {
	var userAssetHistory *UserAssetHistory
	ret := r.gormDB.WithContext(ctx).First(&userAssetHistory, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query userAssetHistory failed", err)
		}
		return nil, err
	}
	return userAssetHistory, nil
}

func (r *UserAssetHistoryRepoImpl) ListByID(ctx context.Context, id int64) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) ListAll(ctx context.Context) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory
	ret := r.gormDB.WithContext(ctx).Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) PageList(ctx context.Context, req *AssetHistoryRequest) ([]*UserAssetHistory, int64, error) {
	var userAssetHistoryList []*UserAssetHistory
	var total int64
	db := r.gormDB.WithContext(ctx).Table("user_asset_history")

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

	ret := db.Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAssetHistory failed", err)
		return nil, 0, err
	}
	return userAssetHistoryList, total, nil
}

func (r *UserAssetHistoryRepoImpl) List(ctx context.Context, req *AssetHistoryRequest) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory
	db := r.gormDB.WithContext(ctx).Table("user_asset_history")

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

	ret := db.Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("list query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) ListBalance(ctx context.Context, req *AssetHistoryRequest) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory

	sqlStr := "select chain_name, uid, token_address, balance " +
		"from user_asset_history " +
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
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}

	ret := r.gormDB.WithContext(ctx).Table("user_asset_history").Raw(sqlStr).Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) ListBalanceGroup(ctx context.Context, req *AssetHistoryRequest) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory

	sqlStr := "select chain_name, token_address, symbol, sum(cast(balance as numeric)) as balance " +
		"from user_asset_history " +
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
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	sqlStr += " group by chain_name, token_address, symbol"

	ret := r.gormDB.WithContext(ctx).Table("user_asset_history").Raw(sqlStr).Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) ListBalanceGroupByUid(ctx context.Context, req *AssetHistoryRequest) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory

	sqlStr := "select uid, sum(cast(balance as numeric)) as balance " +
		"from user_asset_history " +
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
	if req.AmountType > 0 {
		if req.AmountType == 1 {
			sqlStr += " and (balance is null or balance = '' or balance = '0')"
		} else if req.AmountType == 2 {
			sqlStr += " and (balance is not null and balance != '' and balance != '0')"
		}
	}
	sqlStr += " group by uid"

	ret := r.gormDB.WithContext(ctx).Table("user_asset_history").Raw(sqlStr).Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userAssetHistory failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}

func (r *UserAssetHistoryRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserAssetHistory{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete userAssetHistory failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetHistoryRepoImpl) DeleteByIDs(ctx context.Context, ids []int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&UserAssetHistory{}, ids)
	err := ret.Error
	if err != nil {
		log.Errore("delete userAssetHistorys failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetHistoryRepoImpl) Delete(ctx context.Context, req *AssetHistoryRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("user_asset_history")

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

	ret := db.Delete(&UserAssetHistory{})
	err := ret.Error
	if err != nil {
		log.Errore("delete userAssetHistorys failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetHistoryRepoImpl) UpdateZeroByAddress(ctx context.Context, address string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserAssetHistory{}).Where("address = ?", address).Update("balance", "0")
	err := ret.Error
	if err != nil {
		log.Errore("update balance zero", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UserAssetHistoryRepoImpl) ListByChainNameAndDt(ctx context.Context, chainNames []string, dt int64) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory
	ret := r.gormDB.WithContext(ctx).Model(&UserAssetHistory{}).Where("balance is not null and balance != '' and balance != '0' and chain_name in ? and dt = ? ", chainNames, dt).Find(&userAssetHistoryList)
	err := ret.Error

	if err != nil {
		log.Errore("ListByChainName failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}
func (r *UserAssetHistoryRepoImpl) ListByRangeTimeAndAddressAndChainName(ctx context.Context, startTime int, endTime int, address string, chainName string) ([]*UserAssetHistory, error) {
	var userAssetHistoryList []*UserAssetHistory
	ret := r.gormDB.WithContext(ctx).Model(&UserAssetHistory{}).Where("dt >= ? and dt <= ? and address = ? and chain_name = ? ", startTime, endTime, address, chainName).Order("dt asc ").Find(&userAssetHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("ListByRangeTimeAndAddressAndChainName failed", err)
		return nil, err
	}
	return userAssetHistoryList, nil
}