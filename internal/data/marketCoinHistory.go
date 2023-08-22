package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

//每天资产履历

type MarketCoinHistory struct {
	Id                  int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid                 string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address             string          `json:"address" form:"address" gorm:"type:character varying(42);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	ChainName           string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	Symbol              string          `json:"symbol" form:"symbol" gorm:"type:character varying(32);"`
	TransactionQuantity int64           `json:"transactionQuantity" form:"transactionQuantity"`
	TransactionBalance  string          `json:"transactionBalance" form:"transactionBalance"`
	TokenAddress        string          `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(42);index:,unique,composite:unique_dt_chain_name_address_token_address"`
	CnyPrice            string          `json:"cnyPrice" form:"cnyPrice" gorm:"type:character varying(256);"` //均价   1： 10  2：15  3：20  (CnyAmount + inputBalance * inputcnyprice)/(balance+inputBalance)
	UsdPrice            string          `json:"usdPrice" form:"usdPrice" gorm:"type:character varying(256);"` //均价
	CnyAmount           decimal.Decimal `json:"cnyAmount" form:"cnyAmount" gorm:"type:decimal(256,2);"`       // CnyAmount + inputBalance * inputcnyprice
	UsdAmount           decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	Dt                  int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_address_token_address"`
	Balance             string          `json:"balance" form:"balance" gorm:"type:character varying(256);"` //当前余额 带小数点
	CreatedAt           int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt           int64           `json:"updatedAt" form:"updatedAt"`
}

func (marketCoinHistory MarketCoinHistory) TableName() string {
	return "market_coin_history"
}

type MarketCoinHistoryRepo interface {
	Save(context.Context, *MarketCoinHistory) (int64, error)
	SaveOrUpdate(context.Context, *MarketCoinHistory) (int64, error)
	ListByCondition(context.Context, *MarketCoinHistory) ([]*MarketCoinHistory, error)
	Update(context.Context, *MarketCoinHistory) (int64, error)
	ListByTimeRanges(context.Context, string, string, int, int) ([]*MarketCoinHistory, error)
	ListByTimeRangesAll(context.Context, string, string) ([]*MarketCoinHistory, error)
}

type MarketCoinHistoryRepoImpl struct {
	gormDB *gorm.DB
}

var MarketCoinHistoryRepoClient MarketCoinHistoryRepo

func NewMarketCoinHistoryRepo(gormDB *gorm.DB) MarketCoinHistoryRepo {
	MarketCoinHistoryRepoClient = &MarketCoinHistoryRepoImpl{
		gormDB: gormDB,
	}
	return MarketCoinHistoryRepoClient
}

func (r *MarketCoinHistoryRepoImpl) Save(ctx context.Context, marketCoinHistory *MarketCoinHistory) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(marketCoinHistory)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(marketCoinHistory.Id, 10), Data: 0}
			log.Warne("insert market_coin_history failed", err)
		} else {
			log.Errore("insert market_coin_history failed", err)
		}
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}
func (r *MarketCoinHistoryRepoImpl) SaveOrUpdate(ctx context.Context, marketCoinHistory *MarketCoinHistory) (int64, error) {

	ret := r.gormDB.WithContext(ctx).Table("market_coin_history").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "dt"}, {Name: "chain_name"}, {Name: "address"}, {Name: "token_address"}},
		UpdateAll: true,
	}).Create(marketCoinHistory)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update market_coin_history failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}
func (r *MarketCoinHistoryRepoImpl) ListByCondition(ctx context.Context, req *MarketCoinHistory) ([]*MarketCoinHistory, error) {
	var dars []*MarketCoinHistory
	tx := r.gormDB.Table("market_coin_history").Where("dt >= 1692633600")
	if req != nil {
		if req.ChainName != "" {
			tx = tx.Where("chain_name = ?", req.ChainName)
		}
		if req.Address != "" {
			tx = tx.Where("address = ?", req.Address)
		}
		if req.TokenAddress != "" {
			tx = tx.Where("token_address = ?", req.TokenAddress)
		}else {
			tx = tx.Where("token_address = '' ")
		}

	}
	ret := tx.Order("dt desc").Limit(1).Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("page query MarketCoinHistory failed", err)
		return nil, err
	}
	return dars, nil

}
func (r *MarketCoinHistoryRepoImpl) Update(ctx context.Context, marketCoinHistory *MarketCoinHistory) (int64, error) {

	ret := r.gormDB.Model(&MarketCoinHistory{}).Where("id = ?", marketCoinHistory.Id).Updates(marketCoinHistory)
	err := ret.Error
	if err != nil {
		log.Info("shouwuzudao", zap.Any("", marketCoinHistory))
		log.Errore("update MarketCoinHistory failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil

}
func (r *MarketCoinHistoryRepoImpl) ListByTimeRanges(ctx context.Context, address string, chainName string, startTime int, endTime int) ([]*MarketCoinHistory, error) {
	var dars []*MarketCoinHistory
	ret := r.gormDB.WithContext(ctx).Model(&MarketCoinHistory{}).Where("address = ? and  dt >= ? and dt <= ? and chain_name = ? ", address, startTime, endTime, chainName).Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("ListByTimeRanges failed", err)
		return nil, err
	}
	return dars, nil
}
func (r *MarketCoinHistoryRepoImpl) ListByTimeRangesAll(ctx context.Context, address string, chainName string) ([]*MarketCoinHistory, error) {
	var dars []*MarketCoinHistory
	ret := r.gormDB.WithContext(ctx).Model(&MarketCoinHistory{}).Where(" dt >= 1692633600  and address = ?  and chain_name = ? ", address, chainName).Order("dt desc").Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("ListByTimeRangesGroupByDt failed", err)
		return nil, err
	}
	return dars, nil
}
