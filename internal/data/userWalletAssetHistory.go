package data

import (
	"context"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// UserWalletAssetHistory is a UserWalletAssetHistory model.
type UserWalletAssetHistory struct {
	Id        int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid       string          `json:"uid" form:"uid" gorm:"type:character varying(36);index:,unique,composite:unique_uid_dt"`
	UsdAmount decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	UsdChange decimal.Decimal `json:"usdChange" form:"usdChange" gorm:"type:decimal(256,2);"`
	BtcPrice  decimal.Decimal `json:"btcPrice" form:"btcPrice" gorm:"type:decimal(256,2);"`
	UsdtPrice decimal.Decimal `json:"usdtPrice" form:"usdtPrice" gorm:"type:decimal(256,2);"`
	CnyPrice  decimal.Decimal `json:"cnyPrice" form:"cnyPrice" gorm:"type:decimal(256,2);"`
	Dt        int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_uid_dt"`
	CreatedAt int64           `json:"createdAt" form:"createdAt"`
}

func (userWalletAssetHistory UserWalletAssetHistory) TableName() string {
	return "user_wallet_asset_history"
}

// UserWalletAssetHistoryRepo is a Greater repo.
type UserWalletAssetHistoryRepo interface {
	SaveBatch(context.Context, []*UserWalletAssetHistory) error
	FindByUidsAndDTRange(ctx context.Context, uids []string, start, end int64) ([]*UserWalletAssetHistory, error)
	FindByUidsAndDt(ctx context.Context, uids []string, dt int64) ([]*UserWalletAssetHistory, error)
	FindByUidsAndDts(ctx context.Context, uids []string, dts []int64) ([]*UserWalletAssetHistory, error)
}

type UserWalletAssetHistoryRepoImpl struct {
	gormDB *gorm.DB
}

var UserWalletAssetHistoryRepoClient UserWalletAssetHistoryRepo

// NewUserWalletAssetHistoryRepo new a UserWalletAssetHistory repo.
func NewUserWalletAssetHistoryRepo(gormDB *gorm.DB) UserWalletAssetHistoryRepo {
	UserWalletAssetHistoryRepoClient = &UserWalletAssetHistoryRepoImpl{
		gormDB: gormDB,
	}
	return UserWalletAssetHistoryRepoClient
}

func (r UserWalletAssetHistoryRepoImpl) SaveBatch(ctx context.Context, userWalletAssetHistories []*UserWalletAssetHistory) error {
	return r.gormDB.Create(userWalletAssetHistories).Error
}

func (r UserWalletAssetHistoryRepoImpl) FindByUidsAndDTRange(ctx context.Context, uids []string, start, end int64) ([]*UserWalletAssetHistory, error) {
	var histories []*UserWalletAssetHistory
	err := r.gormDB.Where("uid in ? and dt between ? and ?", uids, start, end).Find(&histories).Error
	if err != nil {
		return nil, err
	}
	return histories, nil
}

func (r UserWalletAssetHistoryRepoImpl) FindByUidsAndDt(ctx context.Context, uids []string, dt int64) ([]*UserWalletAssetHistory, error) {
	var histories []*UserWalletAssetHistory
	err := r.gormDB.Where("uid in ? and dt = ?", uids, dt).Find(&histories).Error
	if err != nil {
		return nil, err
	}
	return histories, nil
}

func (r UserWalletAssetHistoryRepoImpl) FindByUidsAndDts(ctx context.Context, uids []string, dts []int64) ([]*UserWalletAssetHistory, error) {
	var histories []*UserWalletAssetHistory
	err := r.gormDB.Where("uid in ? and dt in ?", uids, dts).Find(&histories).Error
	if err != nil {
		return nil, err
	}
	return histories, nil
}
