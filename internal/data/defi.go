package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "block-crawling/api/userWalletAsset/v1"
	"block-crawling/internal/log"
)

type DeFiAction string

const (
	DeFiActionStakedStake     DeFiAction = "stake"
	DeFiActionStakedUnstake   DeFiAction = "unstake"
	DeFiActionDebtBorrow      DeFiAction = "borrow"
	DeFiActionDebtRepay       DeFiAction = "repay"
	DeFiActionLPAdd           DeFiAction = "addLP"
	DeFiActionLPRemove        DeFiAction = "removeLP"
	DeFiActionDepositSupply   DeFiAction = "deposit"
	DeFiActionDepositWithdraw DeFiAction = "withdraw"
)

const (
	DeFiAssetTypeAll      = "all"
	DeFiAssetTypeStake    = "staked"
	DeFiAssetTypeDebt     = "debt"
	DeFiAssetTypeDeposits = "deposit"
	DeFiAssetTypeLP       = "lp"
)

const (
	DeFiAssetTypeDirRecv = "recv"
	DeFiAssetTypeDirSend = "send"
)

type UserDeFiPlatformer interface {
	GetPlatformID() int64
}

type DeFiPlatform struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	URL       string `json:"url" form:"url" gorm:"type:character varying(1024);index:,unique,composite:unique_url"`
	Icon      string `json:"icon" form:"icon"`
	Name      string `json:"name" form:"name" gorm:"type:character varying(256)"`
	CreatedAt int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt int64  `json:"updatedAt" form:"updatedAt"`

	InteractionTotal  int64 `json:"interactionTotal" form:"interactionTotal"`
	InteractionInWeek int64 `json:"interactionInWeek" form:"interactionInWeek"`
	InteractionInDay  int64 `json:"interactionInDay" form:"interactionInDay"`
	// nil: auto, 0: false, 1: true
	Disabled *bool `json:"disabled" form:"disabled"`
}

func (DeFiPlatform) TableName() string {
	return "defi_platform"
}

func (p *DeFiPlatform) Enabled(thr int64) bool {
	if p.Disabled == nil {
		return p.InteractionTotal > thr
	}
	return !*p.Disabled
}

type UserDeFiAsset struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid             string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);index"`
	ChainName       string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_address_platform_asset_type"`
	Address         string          `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_chain_address_platform_asset_type"`
	PlatformID      int64           `json:"platformId" form:"platformId" gorm:"type:BIGINT;index:,unique,composite:unique_chain_address_platform_asset_type"`
	Type            string          `json:"type" form:"type" gorm:"type: character varying(64);index:,unique,composite:unique_chain_address_platform_asset_type"`
	AssetAddress    string          `json:"assetAddress" form:"assetAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_address_platform_asset_type"`
	ValueUsd        decimal.Decimal `json:"valueUsd" form:"valueUsd" gorm:"type:decimal(256,2);default:0"`
	CostUsd         decimal.Decimal `json:"costUsd" form:"costUsd" gorm:"type:decimal(256,2);default:0"`
	ProfitUsd       decimal.Decimal `json:"profitUsd" form:"profitUsd" gorm:"type:decimal(256,2);default:0"`
	OpenedAt        int64           `json:"openedAt" form:"openedAt"`
	ClosedAt        int64           `json:"closedAt" form:"closedAt" gorm:"type:BIGINT;index;default:0"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
	Enabled         bool            `json:"enabled" form:"enabled"`
}

func (UserDeFiAsset) TableName() string {
	return "user_defi_asset"
}

func (h *UserDeFiAsset) UniqueKey() string {
	return fmt.Sprintf("%s-%s-%s-%s-%d", h.ChainName, h.Address, h.Type, h.AssetAddress, h.PlatformID)
}

func (u *UserDeFiAsset) GetPlatformID() int64 {
	return u.PlatformID
}

type UserDeFiAssetTxnHistory struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid             string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	BlockNumber     int64           `json:"blockNumber" form:"blockNumber" gorm:"type:BIGINT;index"`
	PlatformID      int64           `json:"platformId" form:"platformId" gorm:"type:BIGINT;index"`
	ChainName       string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_hash_address_aset"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);index:,unique,composite:unique_chain_hash_address_aset"`
	Address         string          `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_chain_hash_address_aset"`
	TokenAddress    string          `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_hash_address_aset"`
	Action          DeFiAction      `json:"action" form:"action" gorm:"type:character varying(32)"`
	AssetDirection  string          `json:"asssetDirection" form:"asssetDirection" gorm:"type:character varying(32)"`
	Type            string          `json:"type" form:"type" gorm:"type: character varying(64)"`
	Amount          string          `json:"amount" form:"amount" gorm:"type:character varying(256);"`
	RawAmount       decimal.Decimal `json:"rawAmount" form:"rawAmount" sql:"type:decimal(128,0);"`
	UsdPrice        decimal.Decimal `json:"usdPrice" form:"usdPrice" gorm:"type:decimal(256,4)"`
	TokenUri        string          `json:"tokenUri" form:"tokenUri" gorm:"type:character varying(256)"`
	Decimals        int32           `json:"decimals" form:"decimals"`
	Symbol          string          `json:"symbol" form:"symbol" gorm:"type:character varying(128);index"`
	TxTime          int64           `json:"txTime" form:"txTime" gorm:"type:BIGINT;index"`
	AssetAddress    string          `json:"assetTokenAddress" form:"assetTypeAddress" gorm:"type:character varying(1024);index"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
	Enabled         bool            `json:"enabled" form:"enabled"`
}

func (UserDeFiAssetTxnHistory) TableName() string {
	return "user_defi_asset_transaction_history"
}

func (h *UserDeFiAssetTxnHistory) UniqueKey() string {
	return fmt.Sprintf("%s-%s-%s-%s-%d", h.ChainName, h.Address, h.Type, h.AssetAddress, h.PlatformID)
}

func (u *UserDeFiAssetTxnHistory) GetPlatformID() int64 {
	return u.PlatformID
}

type UserWalletDeFiAssetHistory struct {
	Id         int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid        string          `json:"uid" form:"uid" gorm:"type:character varying(36);index:,unique,composite:unique_uid_plat_type_dt"`
	PlatformID int64           `json:"platformId" form:"platformId" gorm:"type:BIGINT;index:,unique,composite:unique_uid_plat_type_dt"`
	Type       string          `json:"type" form:"type" gorm:"type: character varying(64);index:,unique,composite:unique_uid_plat_type_dt"`
	UsdAmount  decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	UsdChange  decimal.Decimal `json:"usdChange" form:"usdChange" gorm:"type:decimal(256,2);"`
	BtcPrice   decimal.Decimal `json:"btcPrice" form:"btcPrice" gorm:"type:decimal(256,2);"`
	UsdtPrice  decimal.Decimal `json:"usdtPrice" form:"usdtPrice" gorm:"type:decimal(256,2);"`
	CnyPrice   decimal.Decimal `json:"cnyPrice" form:"cnyPrice" gorm:"type:decimal(256,2);"`
	Dt         int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_uid_plat_type_dt"`
	CreatedAt  int64           `json:"createdAt" form:"createdAt"`
}

func (UserWalletDeFiAssetHistory) TableName() string {
	return "user_wallet_defi_asset_history"
}

type DeFiOpenPostionReq struct {
	ChainName    string
	PlatformID   int64
	Address      string
	AssetAddress string
	TxTime       int64
}

type DeFiAssetRepo interface {
	// IsBorrowing returns true if we have borrowed this token and still need to repay.
	IsBorrowing(ctx context.Context, req *DeFiOpenPostionReq) (bool, error)

	// HasDeposit returns true if we have deposit this token and still don't withdraw.
	IsDepositting(ctx context.Context, req *DeFiOpenPostionReq) (bool, error)

	// HasStaked returns true if we have staked this token and still don't unstake.
	IsStaking(ctx context.Context, req *DeFiOpenPostionReq) (bool, error)

	// IsLiquidityAdding returns true if we have add liquidity to this pair and still don't remove.
	IsLiquidityAdding(ctx context.Context, req *DeFiOpenPostionReq) (bool, error)

	// GetPlatformTokenAmount returns the amount
	GetPlatformTokenAmount(ctx context.Context, req *DeFiOpenPostionReq, typ string) (decimal.Decimal, error)

	// SaveTxnHistory to save history.
	SaveTxnHistory(ctx context.Context, histories []*UserDeFiAssetTxnHistory) error

	// LoadOrSavePlatform
	LoadOrSavePlatform(ctx context.Context, plat *DeFiPlatform) error
	SaveFullPlatform(ctx context.Context, plat *DeFiPlatform) error
	EnablePlatformAssets(ctx context.Context, platformID int64) error

	// MaintainUserDeFiAsset maintain the
	MaintainDeFiTxnAsset(ctx context.Context, assetHistories []*UserDeFiAssetTxnHistory, valueUsd decimal.Decimal) error

	// SaveDeFiAsset
	SaveDeFiAsset(ctx context.Context, assets []*UserDeFiAsset) error
	DeleteDeFiAsset(ctx context.Context, txHashs []string) error

	LoadAssetHistories(ctx context.Context, req *DeFiOpenPostionReq, typ string) ([]*UserDeFiAssetTxnHistory, error)

	CursorListUids(ctx context.Context, cursor *int, limit int) ([]string, error)
	FindByUid(ctx context.Context, uid string) ([]*UserDeFiAsset, error)
	FindByUids(ctx context.Context, uids []string) ([]*UserDeFiAsset, error)
	GetValueUsd(histories []*UserDeFiAssetTxnHistory) decimal.Decimal

	SaveBatch(ctx context.Context, assets []*UserWalletDeFiAssetHistory) error

	// ListUserPlatforms
	ListUserPlatforms(ctx context.Context, uids []string, typ string) ([]*DeFiPlatform, error)

	ListUserDeFiAssets(ctx context.Context, req *pb.UserWalletDeFiAssetRequest) (int64, decimal.Decimal, []*UserDeFiAsset, error)

	LoadPlatforms(ctx context.Context, platformIDs []int64) ([]*DeFiPlatform, error)
	LoadPlatformsMap(ctx context.Context, items []*UserDeFiAsset) (map[int64]*DeFiPlatform, error)
	LoadAllPlatformsMap(ctx context.Context) (map[int64]*DeFiPlatform, error)
	FindByUidsAndDTRange(ctx context.Context, uids []string, start, end int64) ([]*UserWalletAssetHistory, error)
	FindByUidsAndDts(ctx context.Context, uids []string, dts []int64) ([]*UserWalletAssetHistory, error)
	CursorListTxnHistories(ctx context.Context, cursor *int64, limit int64) ([]*UserDeFiAssetTxnHistory, error)
	CursorListDeFiAssets(ctx context.Context, cursor *int64, limit int64) ([]*UserDeFiAsset, error)
	DeleteDuplicatedDeFiAsset(ctx context.Context, asset *UserDeFiAsset) error

	GetPlatformEarlistOpenedAt(ctx context.Context, chainName, address string, platformID int64) (*UserDeFiAsset, error)
	GetPlatformHistories(ctx context.Context, chainName, address string, platformID int64, earlistOpenAt int64) ([]*UserDeFiAssetTxnHistory, error)
}

type deFiAssetRepoImpl struct {
	db *gorm.DB
}

// GetPlatformEarlistOpenedAt implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) GetPlatformEarlistOpenedAt(ctx context.Context, chainName, address string, platformID int64) (*UserDeFiAsset, error) {
	var result *UserDeFiAsset
	err := de.db.WithContext(ctx).Where(
		"chain_name=? AND platform_id=? AND closed_at=0", chainName, platformID,
	).Order("opened_at ASC").First(&result).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetPlatformRecvTokenHistories implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) GetPlatformHistories(ctx context.Context, chainName, address string, platformID int64, earlistOpenAt int64) ([]*UserDeFiAssetTxnHistory, error) {
	var results []*UserDeFiAssetTxnHistory
	err := de.db.WithContext(ctx).Where(
		"chain_name = ? AND platform_id=? AND tx_time >= ?", chainName, platformID, earlistOpenAt,
	).Order("tx_time ASC").Find(&results).Error
	if err != nil {
		return nil, err
	}
	return results, nil
}

// CursorListDeFiAssets implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) CursorListDeFiAssets(ctx context.Context, cursor *int64, limit int64) ([]*UserDeFiAsset, error) {
	var results []*UserDeFiAsset
	err := de.db.Where("id > ?", *cursor).Order("id ASC").Find(&results).Error
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		*cursor = results[len(results)-1].Id
	}
	return results, nil
}

// DeleteDuplicatedDeFiAsset implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) DeleteDuplicatedDeFiAsset(ctx context.Context, asset *UserDeFiAsset) error {
	return de.db.WithContext(ctx).Table(asset.TableName()).Where(
		"chain_name = ? AND address = ? AND platform_id=? AND type=? AND asset_address=? AND id > ?",
		asset.ChainName, asset.Address, asset.PlatformID, asset.Type, asset.AssetAddress, asset.Id,
	).Delete(&UserDeFiAsset{}).Error
}

// DeleteDeFiAsset implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) DeleteDeFiAsset(ctx context.Context, txHashs []string) error {
	return de.db.WithContext(ctx).Where("transaction_hash in ?", txHashs).Delete(&UserDeFiAsset{}).Error
}

// CursorListTxnHistories implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) CursorListTxnHistories(ctx context.Context, cursor *int64, limit int64) ([]*UserDeFiAssetTxnHistory, error) {
	var results []*UserDeFiAssetTxnHistory
	err := de.db.Where("id > ?", *cursor).Order("id ASC").Find(&results).Error
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		*cursor = results[len(results)-1].Id
	}
	return results, nil
}

// FindByUidsAndDts implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) FindByUidsAndDts(ctx context.Context, uids []string, dts []int64) ([]*UserWalletAssetHistory, error) {
	var histories []*UserWalletAssetHistory
	err := de.db.Where("uid in ? and dt in ?", uids, dts).Find(&histories).Error
	if err != nil {
		return nil, err
	}
	return histories, nil
}

func (de *deFiAssetRepoImpl) FindByUidsAndDTRange(ctx context.Context, uids []string, start, end int64) ([]*UserWalletAssetHistory, error) {
	var results []*UserWalletDeFiAssetHistory
	err := de.db.Where("uid in ? and dt > ? and dt <= ? AND platform_id=0 AND type=?", uids, start, end, DeFiAssetTypeAll).Find(&results).Error
	if err != nil {
		return nil, err
	}
	histories := make([]*UserWalletAssetHistory, 0, len(results))
	for _, item := range results {
		histories = append(histories, &UserWalletAssetHistory{
			Id:        item.Id,
			Uid:       item.Uid,
			UsdAmount: item.UsdAmount,
			UsdChange: item.UsdChange,
			BtcPrice:  item.BtcPrice,
			UsdtPrice: item.UsdtPrice,
			CnyPrice:  item.CnyPrice,
			Dt:        item.Dt,
			CreatedAt: item.CreatedAt,
		})
	}

	return histories, nil

}

func (de *deFiAssetRepoImpl) LoadAllPlatformsMap(ctx context.Context) (map[int64]*DeFiPlatform, error) {
	var results []*DeFiPlatform
	err := de.db.WithContext(ctx).Find(&results).Error
	if err != nil {
		return nil, err
	}
	platforms := make(map[int64]*DeFiPlatform)
	for _, p := range results {
		platforms[p.Id] = p
	}
	return platforms, nil
}

// LoadPlatformsMap implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) LoadPlatformsMap(ctx context.Context, items []*UserDeFiAsset) (map[int64]*DeFiPlatform, error) {
	platformIDs := make([]int64, 0, len(items))
	for _, item := range items {
		platformIDs = append(platformIDs, item.GetPlatformID())
	}
	platforms, err := de.LoadPlatforms(ctx, platformIDs)
	if err != nil {
		return nil, err
	}
	platformsById := make(map[int64]*DeFiPlatform)
	for _, p := range platforms {
		platformsById[p.Id] = p
	}
	return platformsById, nil
}

// LoadPlatforms implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) LoadPlatforms(ctx context.Context, platformIDs []int64) ([]*DeFiPlatform, error) {
	var results []*DeFiPlatform
	if len(platformIDs) == 0 {
		return nil, nil
	}
	err := de.db.WithContext(ctx).Where("id IN ?", platformIDs).Find(&results).Error
	if err != nil {
		return nil, err
	}
	return results, nil
}

// ListUserDeFiAssets implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) ListUserDeFiAssets(ctx context.Context, req *pb.UserWalletDeFiAssetRequest) (int64, decimal.Decimal, []*UserDeFiAsset, error) {

	var total int64
	var totalAmount decimal.Decimal
	var results []*UserDeFiAsset
	db := de.createListDB(ctx, req)
	db.Select("SUM(CAST(value_usd AS DECIMAL)) as i").Row().Scan(&totalAmount)

	// Recreate db to clear select
	db = de.createListDB(ctx, req)
	db.Count(&total)

	orderBy := "id"
	order := "desc"
	switch req.OrderBy {
	case "value":
		orderBy = "value_usd"
	case "profit":
		orderBy = "profit_usd"
	}
	if req.Order != "" {
		order = req.Order
	}
	db = db.Order(fmt.Sprintf("%s %s", orderBy, order))
	err := db.Offset((int(req.PageSize) - 1) * int(req.PageNum)).Limit(int(req.PageNum)).Find(&results).Error
	return total, totalAmount, results, err
}

func (de *deFiAssetRepoImpl) createListDB(ctx context.Context, req *pb.UserWalletDeFiAssetRequest) *gorm.DB {
	a := &UserDeFiAsset{}

	db := de.db.WithContext(ctx).Where("type = ? AND closed_at = 0 AND enabled=true", req.Type).Table(a.TableName())
	if len(req.Uids) > 0 {
		db = db.Where("uid in ?", req.Uids)
	}
	if len(req.PlatformIds) > 0 {
		db = db.Where("platform_id in ?", req.PlatformIds)
	}
	if len(req.ChainNames) > 0 {
		db = db.Where("chain_name in ?", req.ChainNames)
	}
	return db
}

// ListUserPlatforms implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) ListUserPlatforms(ctx context.Context, uids []string, typ string) ([]*DeFiPlatform, error) {
	var platformIDs []int64
	asset := &UserDeFiAsset{}
	err := de.db.WithContext(ctx).Table(asset.TableName()).
		Distinct("platform_id").
		Where("uid in ? AND type = ? AND enabled=true", uids, typ).Find(&platformIDs).Error
	if err != nil {
		return nil, err
	}
	return de.LoadPlatforms(ctx, platformIDs)
}

// SaveBatch implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) SaveBatch(ctx context.Context, assets []*UserWalletDeFiAssetHistory) error {
	return de.db.Create(assets).Error
}

// FindByUid implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) FindByUid(ctx context.Context, uid string) ([]*UserDeFiAsset, error) {
	return de.FindByUids(ctx, []string{uid})
}

// FindByUid implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) FindByUids(ctx context.Context, uids []string) ([]*UserDeFiAsset, error) {
	var assets []*UserDeFiAsset
	ret := de.db.Where("uid IN ? AND closed_at = 0 AND enabled=true", uids).Find(&assets)
	return assets, ret.Error
}

// CursorListUids implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) CursorListUids(ctx context.Context, cursor *int, limit int) ([]string, error) {
	var assets []*UserDeFiAsset
	ret := de.db.Where("id > ?", *cursor).Limit(limit).Find(&assets)
	if ret.Error != nil {
		return nil, ret.Error
	}
	uids := make([]string, 0, limit)
	for _, r := range assets {
		*cursor = int(r.Id)
		uids = append(uids, r.Uid)
	}
	return uids, nil
}

// LoadAssetHistories implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) LoadAssetHistories(ctx context.Context, req *DeFiOpenPostionReq, typ string) ([]*UserDeFiAssetTxnHistory, error) {
	var results []*UserDeFiAssetTxnHistory
	err := de.db.WithContext(ctx).Model(&UserDeFiAssetTxnHistory{}).Where(
		"chain_name=? AND address=? AND asset_address=? AND type=? AND platform_id=? AND tx_time >= ?",
		req.ChainName, req.Address, req.AssetAddress, typ, req.PlatformID, req.TxTime,
	).Order("tx_time ASC").Find(&results).Error
	return results, err
}

// GetPlatformTokenAmount implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) GetPlatformTokenAmount(ctx context.Context, req *DeFiOpenPostionReq, typ string) (decimal.Decimal, error) {
	var openAction, closeAction DeFiAction
	var openDir, closeDir string
	switch typ {
	case DeFiAssetTypeStake:
		openAction = DeFiActionStakedStake
		closeAction = DeFiActionStakedUnstake

		openDir = DeFiAssetTypeDirSend
		closeDir = DeFiAssetTypeDirRecv
	case DeFiAssetTypeDebt:
		openAction = DeFiActionDebtBorrow
		closeAction = DeFiActionDebtRepay

		openDir = DeFiAssetTypeDirRecv
		closeDir = DeFiAssetTypeDirSend
	case DeFiAssetTypeDeposits:
		openAction = DeFiActionDepositSupply
		closeAction = DeFiActionDepositWithdraw

		openDir = DeFiAssetTypeDirSend
		closeDir = DeFiAssetTypeDirRecv
	case DeFiAssetTypeLP:
		openAction = DeFiActionLPAdd
		closeAction = DeFiActionLPRemove

		openDir = DeFiAssetTypeDirRecv
		closeDir = DeFiAssetTypeDirSend
	default:
		return decimal.Zero, errors.New("unknown type")
	}

	totalOpenAmount, err := de.getTotalAmount(ctx, req, openAction, openDir)
	if err != nil {
		log.Info(
			"DEFI: TOTAL OPEN/CLOSE AMOUNT FAILED", zap.Any("req", req),
			zap.String("openAction", string(openAction)), zap.String("openDir", openDir),
			zap.String("closeAction", string(closeAction)), zap.String("closeDir", closeDir),
			zap.String("err", err.Error()),
		)
		return decimal.Zero, err
	}
	totalCloseAmount, err := de.getTotalAmount(ctx, req, closeAction, closeDir)
	if err != nil {
		log.Info(
			"DEFI: TOTAL OPEN/CLOSE AMOUNT FAILED", zap.Any("req", req),
			zap.String("openAction", string(openAction)), zap.String("openDir", openDir),
			zap.String("closeAction", string(closeAction)), zap.String("closeDir", closeDir),
			zap.String("err", err.Error()),
		)
		return decimal.Zero, err
	}

	log.Info(
		"DEFI: TOTAL OPEN/CLOSE AMOUNT", zap.Any("req", req),
		zap.String("openAction", string(openAction)), zap.String("openDir", openDir),
		zap.String("closeAction", string(closeAction)), zap.String("closeDir", closeDir),
		zap.String("openAmount", totalOpenAmount.String()),
		zap.String("closeAmount", totalCloseAmount.String()),
	)
	total := totalOpenAmount.Sub(totalCloseAmount)
	if total.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, nil
	}
	return total, nil
}

// SaveDeFiAsset implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) SaveDeFiAsset(ctx context.Context, assets []*UserDeFiAsset) error {
	tableName := assets[0].TableName()
	for _, v := range assets {
		de.db.Where("transaction_hash = ? AND type != ?", v.TransactionHash, v.Type).Delete(&UserDeFiAsset{})
	}

	return de.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "chain_name"}, {Name: "address"}, {Name: "platform_id"}, {Name: "type"}, {Name: "asset_address"},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"uid":              clause.Column{Table: "excluded", Name: "uid"},
			"transaction_hash": gorm.Expr("case when " + tableName + ".closed_at != 0 then excluded.transaction_hash else " + tableName + ".transaction_hash end"),
			"opened_at":        gorm.Expr("case when " + tableName + ".closed_at != 0 then excluded.opened_at else " + tableName + ".opened_at end"),
			"closed_at":        clause.Column{Table: "excluded", Name: "closed_at"},
			"value_usd":        clause.Column{Table: "excluded", Name: "value_usd"},
			"cost_usd":         gorm.Expr("case when " + tableName + ".cost_usd::DECIMAL != 0 then " + tableName + ".cost_usd else excluded.cost_usd end"),
			"profit_usd":       clause.Column{Table: "excluded", Name: "profit_usd"},
			"updated_at":       clause.Column{Table: "excluded", Name: "updated_at"},
			"enabled":          clause.Column{Table: "excluded", Name: "enabled"},
		})}).Create(&assets).Error
}

func (de *deFiAssetRepoImpl) GetValueUsd(histories []*UserDeFiAssetTxnHistory) decimal.Decimal {
	var totalSentValueUsd, totalRecvValueUsd decimal.Decimal
	for _, h := range histories {
		amount, _ := decimal.NewFromString(h.Amount)
		amountUsd := amount.Mul(h.UsdPrice)
		if h.AssetDirection == DeFiAssetTypeDirRecv {
			totalRecvValueUsd = totalRecvValueUsd.Add(amountUsd)
		} else {
			totalSentValueUsd = totalSentValueUsd.Add(amountUsd)
		}
	}
	return totalRecvValueUsd.Sub(totalSentValueUsd).Abs()
}

// MaintainDeFiTxnAsset implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) MaintainDeFiTxnAsset(ctx context.Context, assetHistories []*UserDeFiAssetTxnHistory, valueUsd decimal.Decimal) error {
	shouldRemoveTxns := make([]string, 0, 4)
	isZero := valueUsd.LessThanOrEqual(decimal.Zero)
	for _, h := range assetHistories {
		switch h.Action {
		case DeFiActionStakedStake:
			if h.AssetDirection != DeFiAssetTypeDirSend || isZero {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{{
				Uid:             h.Uid,
				PlatformID:      h.PlatformID,
				ChainName:       h.ChainName,
				TransactionHash: h.TransactionHash,
				Address:         h.Address,
				Type:            DeFiAssetTypeStake,
				AssetAddress:    h.AssetAddress,
				ValueUsd:        valueUsd,
				CostUsd:         valueUsd,
				OpenedAt:        h.TxTime,
				CreatedAt:       time.Now().Unix(),
				UpdatedAt:       time.Now().Unix(),
				Enabled:         h.Enabled,
			}}); err != nil {
				return err
			}
		case DeFiActionStakedUnstake:
			opened, err := de.makeClosePosition(ctx, DeFiAssetTypeStake, h, valueUsd)
			if err != nil {
				return err
			}

			if opened == nil {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{opened}); err != nil {
				return err
			}
			shouldRemoveTxns = append(shouldRemoveTxns, h.TransactionHash)
		case DeFiActionDebtBorrow:
			if h.AssetDirection != DeFiAssetTypeDirRecv || isZero {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{{
				Uid:             h.Uid,
				PlatformID:      h.PlatformID,
				ChainName:       h.ChainName,
				TransactionHash: h.TransactionHash,
				Address:         h.Address,
				Type:            DeFiAssetTypeDebt,
				AssetAddress:    h.AssetAddress,
				ValueUsd:        valueUsd,
				CostUsd:         valueUsd,
				OpenedAt:        h.TxTime,
				ClosedAt:        0,
				CreatedAt:       time.Now().Unix(),
				UpdatedAt:       time.Now().Unix(),
				Enabled:         h.Enabled,
			}}); err != nil {
				return err
			}
		case DeFiActionDebtRepay:
			opened, err := de.makeClosePosition(ctx, DeFiAssetTypeDebt, h, valueUsd)
			if err != nil {
				return err
			}

			if opened == nil {
				continue
			}
			shouldRemoveTxns = append(shouldRemoveTxns, h.TransactionHash)

			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{opened}); err != nil {
				return err
			}
		case DeFiActionDepositSupply:
			if h.AssetDirection != DeFiAssetTypeDirSend || isZero {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{{
				Uid:             h.Uid,
				PlatformID:      h.PlatformID,
				ChainName:       h.ChainName,
				TransactionHash: h.TransactionHash,
				Address:         h.Address,
				Type:            DeFiAssetTypeDeposits,
				AssetAddress:    h.AssetAddress,
				ValueUsd:        valueUsd,
				CostUsd:         valueUsd,
				OpenedAt:        h.TxTime,
				ClosedAt:        0,
				CreatedAt:       time.Now().Unix(),
				UpdatedAt:       time.Now().Unix(),
				Enabled:         h.Enabled,
			}}); err != nil {
				return err
			}
		case DeFiActionDepositWithdraw:
			opened, err := de.makeClosePosition(ctx, DeFiAssetTypeDeposits, h, valueUsd)
			if err != nil {
				return err
			}
			if opened == nil {
				continue
			}

			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{opened}); err != nil {
				return err
			}
			shouldRemoveTxns = append(shouldRemoveTxns, h.TransactionHash)
		case DeFiActionLPAdd:
			if h.AssetDirection != DeFiAssetTypeDirRecv || isZero {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{{
				Uid:             h.Uid,
				PlatformID:      h.PlatformID,
				ChainName:       h.ChainName,
				TransactionHash: h.TransactionHash,
				Address:         h.Address,
				Type:            DeFiAssetTypeLP,
				AssetAddress:    h.AssetAddress,
				ValueUsd:        valueUsd,
				CostUsd:         valueUsd,
				OpenedAt:        h.TxTime,
				ClosedAt:        0,
				CreatedAt:       time.Now().Unix(),
				UpdatedAt:       time.Now().Unix(),
				Enabled:         h.Enabled,
			}}); err != nil {
				return err
			}
		case DeFiActionLPRemove:
			opened, err := de.makeClosePosition(ctx, DeFiAssetTypeLP, h, valueUsd)
			if err != nil {
				return err
			}

			if opened == nil {
				continue
			}
			if err := de.SaveDeFiAsset(ctx, []*UserDeFiAsset{opened}); err != nil {
				return err
			}
			shouldRemoveTxns = append(shouldRemoveTxns, h.TransactionHash)
		}
	}
	if len(shouldRemoveTxns) > 0 {
		if err := de.DeleteDeFiAsset(ctx, shouldRemoveTxns); err != nil {
			return fmt.Errorf("[cleanup] %w", err)
		}
	}
	return nil
}

func (de *deFiAssetRepoImpl) makeClosePosition(ctx context.Context, typ string, history *UserDeFiAssetTxnHistory, valueUsd decimal.Decimal) (*UserDeFiAsset, error) {
	opened, err := de.findOpenPosition(ctx, typ, history)
	if err != nil {
		return nil, err
	}
	if opened == nil {
		return nil, nil
	}
	opened.Id = 0
	opened.UpdatedAt = time.Now().Unix()
	opened.Enabled = history.Enabled
	if valueUsd.LessThanOrEqual(decimal.Zero) {
		opened.ClosedAt = history.TxTime
	}
	return opened, err
}

func (de *deFiAssetRepoImpl) findOpenPosition(ctx context.Context, typ string, history *UserDeFiAssetTxnHistory) (result *UserDeFiAsset, err error) {
	err = de.db.WithContext(ctx).Where(
		"platform_id = ? AND address = ? AND chain_name=? AND type = ? AND asset_address = ? AND closed_at=0 AND opened_at < ?",
		history.PlatformID, history.Address, history.ChainName, typ, history.AssetAddress, history.TxTime,
	).Order("opened_at ASC").First(&result).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return
}

// LoadOrSavePlatform implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) LoadOrSavePlatform(ctx context.Context, plat *DeFiPlatform) error {
	de.db.WithContext(ctx).Where("url = ?", plat.URL).First(&plat)
	if plat.Id != 0 {
		return nil
	}
	ret := de.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "url"},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"icon":       clause.Column{Table: "excluded", Name: "icon"},
			"name":       clause.Column{Table: "excluded", Name: "name"},
			"updated_at": clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(plat)
	return ret.Error
}

// SaveFullPlatform implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) SaveFullPlatform(ctx context.Context, plat *DeFiPlatform) error {
	ret := de.db.WithContext(ctx).Save(plat)
	return ret.Error
}

func (de *deFiAssetRepoImpl) EnablePlatformAssets(ctx context.Context, platformID int64) error {
	err := de.db.WithContext(ctx).Model(&UserDeFiAsset{}).Where(
		"platform_id=? AND (enabled=false OR enabled is NULL)", platformID,
	).Update("enabled", true).Error
	if err != nil {
		return err
	}
	return de.db.WithContext(ctx).Model(&UserDeFiAssetTxnHistory{}).Where(
		"platform_id=? AND (enabled=false OR enabled is NULL)", platformID,
	).Update("enabled", true).Error
}

// IsBorrowing implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) IsBorrowing(ctx context.Context, req *DeFiOpenPostionReq) (bool, error) {
	total, err := de.GetPlatformTokenAmount(context.Background(), req, DeFiAssetTypeDebt)
	if err != nil {
		return false, nil
	}
	return total.GreaterThan(decimal.Zero), nil
}

// IsDepositting implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) IsDepositting(ctx context.Context, req *DeFiOpenPostionReq) (bool, error) {
	total, err := de.GetPlatformTokenAmount(context.Background(), req, DeFiAssetTypeDeposits)
	if err != nil {
		return false, nil
	}
	return total.GreaterThan(decimal.Zero), nil
}

// IsLiquidityAdding implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) IsLiquidityAdding(ctx context.Context, req *DeFiOpenPostionReq) (bool, error) {
	total, err := de.GetPlatformTokenAmount(context.Background(), req, DeFiAssetTypeLP)
	if err != nil {
		return false, nil
	}
	return total.GreaterThan(decimal.Zero), nil
}

// IsStaking implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) IsStaking(ctx context.Context, req *DeFiOpenPostionReq) (bool, error) {
	total, err := de.GetPlatformTokenAmount(context.Background(), req, DeFiAssetTypeStake)
	if err != nil {
		return false, nil
	}
	return total.GreaterThan(decimal.Zero), nil
}

func (de *deFiAssetRepoImpl) getTotalAmount(ctx context.Context, req *DeFiOpenPostionReq, action DeFiAction, dir string) (decimal.Decimal, error) {
	var total *decimal.Decimal
	db := de.db.WithContext(ctx).Model(&UserDeFiAssetTxnHistory{}).Where(
		"chain_name=? AND address=? AND asset_address=? AND asset_direction=? AND action=? AND tx_time < ?",
		req.ChainName, req.Address, req.AssetAddress, dir, action, req.TxTime,
	)
	if req.PlatformID != 0 {
		db = db.Where("platform_id=?", req.PlatformID)
	}
	err := db.Select(`SUM(CAST(raw_amount AS DECIMAL)) as i`).Row().Scan(&total)
	if err != nil || total == nil {
		return decimal.Zero, err
	}
	return *total, err
}

// SaveTxnHistory implements DeFiAssetRepo
func (de *deFiAssetRepoImpl) SaveTxnHistory(ctx context.Context, histories []*UserDeFiAssetTxnHistory) error {
	for _, item := range histories {
		item.Type = de.actionToType(item.Action)
	}
	return de.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "chain_name"}, {Name: "transaction_hash"}, {Name: "address"}, {Name: "token_address"},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_number":    clause.Column{Table: "excluded", Name: "block_number"},
			"platform_id":     clause.Column{Table: "excluded", Name: "platform_id"},
			"asset_direction": clause.Column{Table: "excluded", Name: "asset_direction"},
			"action":          clause.Column{Table: "excluded", Name: "action"},
			"type":            clause.Column{Table: "excluded", Name: "type"},
			"amount":          clause.Column{Table: "excluded", Name: "amount"},
			"raw_amount":      clause.Column{Table: "excluded", Name: "raw_amount"},
			"token_uri":       clause.Column{Table: "excluded", Name: "token_uri"},
			"decimals":        clause.Column{Table: "excluded", Name: "decimals"},
			"symbol":          clause.Column{Table: "excluded", Name: "symbol"},
			"tx_time":         clause.Column{Table: "excluded", Name: "tx_time"},
			"asset_address":   clause.Column{Table: "excluded", Name: "asset_address"},
			"updated_at":      clause.Column{Table: "excluded", Name: "updated_at"},
			"enabled":         clause.Column{Table: "excluded", Name: "enabled"},
		})}).Create(&histories).Error
}

func (de *deFiAssetRepoImpl) actionToType(action DeFiAction) string {
	switch action {
	case DeFiActionStakedStake, DeFiActionStakedUnstake:
		return DeFiAssetTypeStake
	case DeFiActionDebtBorrow, DeFiActionDebtRepay:
		return DeFiAssetTypeDebt
	case DeFiActionLPAdd, DeFiActionLPRemove:
		return DeFiAssetTypeLP
	case DeFiActionDepositSupply, DeFiActionDepositWithdraw:
		return DeFiAssetTypeDeposits
	}
	panic(fmt.Errorf("unknown action %s", action))
}

var DeFiAssetRepoInst DeFiAssetRepo

func NewDeFiAssetRepo(db *gorm.DB) DeFiAssetRepo {
	DeFiAssetRepoInst = &deFiAssetRepoImpl{
		db: db.Debug(),
	}
	return DeFiAssetRepoInst
}
