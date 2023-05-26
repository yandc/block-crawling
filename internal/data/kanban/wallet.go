package kanban

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Denomination string

const (
	DenominationNative Denomination = "native"
	DenominationUSD                 = "usd"
)

type WalletSummaryRecord struct {
	Id               int64           `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Address          string          `json:"address" gorm:"type:character varying(512);index:,unique"`
	FirstTradeTime   int64           `json:"firstTxTime" gorm:"type:bigint"`
	TotalTxNum       int64           `json:"totalTxNum" gorm:"type:bigint;index"`
	TotalContractNum int64           `json:"totalContract" gorm:"type:bigint;index"`
	TotalTxAmount    decimal.Decimal `json:"totalTxAmount" gorm:"type:decimal(256,0);index"`
	TotalTxInAmount  decimal.Decimal `json:"totalTxInAmount" gorm:"type:decimal(256,0);index"`
	TotalTxOutAmount decimal.Decimal `json:"totalTxOutAmount" gorm:"type:decimal(256,0);index"`
	Denomination     Denomination    `json:"denomination" gorm:"type:character varying(20)"`
	CreatedAt        int64           `json:"createdAt" gorm:"type:bigint"`
	UpdatedAt        int64           `json:"updatedAt" gorm:"type:bigint"`
}

type WalletDaySummaryRecord struct {
	Id               int64           `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Address          string          `json:"address" gorm:"type:character varying(512);index:,unique,composite:unique_address_sharding"`
	Sharding         int64           `json:"sharding" gorm:"int;index:,unique,composite:unique_address_sharding"`
	FirstTradeTime   int64           `json:"firstTxTime" gorm:"type:bigint;index"`
	TotalTxNum       int64           `json:"totalTxNum"`
	TotalTxInAmount  decimal.Decimal `json:"totalTxAmount" sql:"type:decimal(256,0);"`
	TotalTxOutAmount decimal.Decimal `json:"totalTxOutAmount" sql:"type:decimal(256,0);"`
	TotalTxContract  int64           `json:"totalTxContract" gorm:"type:bigint"`
	Denomination     Denomination    `json:"denomination" gorm:"type:character varying(20)"`
	CreatedAt        int64           `json:"createdAt"  gorm:"type:bigint"`
	UpdatedAt        int64           `json:"updatedAt"  gorm:"type:bigint"`
}

func (r *WalletDaySummaryRecord) Key() string {
	return fmt.Sprint(r.Address, r.Sharding)
}

func (r *WalletDaySummaryRecord) Merge(o *WalletDaySummaryRecord) error {
	if r.Key() != o.Key() {
		return errors.New("cannot merge")
	}
	if (o.FirstTradeTime != 0 && o.FirstTradeTime < r.FirstTradeTime) || r.FirstTradeTime == 0 {
		r.FirstTradeTime = o.FirstTradeTime
	}
	r.TotalTxNum += o.TotalTxNum
	r.TotalTxContract += o.TotalTxContract
	r.TotalTxInAmount = r.TotalTxInAmount.Add(o.TotalTxInAmount)
	r.TotalTxOutAmount = r.TotalTxOutAmount.Add(o.TotalTxOutAmount)
	return nil
}

type WalletContractRecord struct {
	Id             int64  `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Address        string `json:"address" gorm:"type:character varying(512);index:,unique,composite:unique_address_contract_type"`
	Contract       string `json:"contract" gorm:"type:character varying(512);index:,unique,composite:unique_address_contract_type"`
	FirstTradeTime int64  `json:"firstTradeTime"`
	LastTradeTime  int64  `json:"lastTradeTime"`

	CreatedAt int64 `json:"createdAt" gorm:"type:bigint"`
	UpdatedAt int64 `json:"updatedAt" gorm:"type:bigint"`
}

func (c *WalletContractRecord) Key() string {
	return fmt.Sprint(c.Address, c.Contract)
}

func (c *WalletContractRecord) Merge(o *WalletContractRecord) error {
	if c.Key() != o.Key() {
		return errors.New("cannott merge")
	}
	if (o.FirstTradeTime != 0 && o.FirstTradeTime < c.FirstTradeTime) || c.FirstTradeTime == 0 {
		c.FirstTradeTime = o.FirstTradeTime
	}
	return nil
}

type WalletRepo interface {
	AutoMigrate(ctx context.Context, chainName string) error
	BatchSaveContract(ctx context.Context, chainName string, records []*WalletContractRecord) error
	BatchSaveDaySummary(ctx context.Context, chainName string, records []*WalletDaySummaryRecord) error
	// AnalyzeDaySummary to avoid index missing
	AnalyzeDaySummary(ctx context.Context, chainName string) error
	Accumulate(ctx context.Context, chainName string, address string) error
	Count(ctx context.Context, chainName, key string) (int64, error)
	GetTopPercentRank(ctx context.Context, chainName string, count int64, key string, percent int) (decimal.Decimal, error)
	RangeDaySummary(ctx context.Context, chainName, address string, startTime int64, endTime int64) ([]*WalletDaySummaryRecord, error)
	LoadSummary(ctx context.Context, chainName, address string) (*WalletSummaryRecord, error)
	LoadAllAddresses(ctx context.Context, chainName string, cursor *uint64, pageLimit int) ([]string, error)
}

type TxRecord struct {
	FromAddress     string
	ToAddress       string
	Amount          decimal.Decimal
	TxTime          int64
	Status          string
	ContractAddress string
	TransactionType string
}

func (r *TxRecord) IntoContracts() []*WalletContractRecord {
	results := make([]*WalletContractRecord, 0, 4)

	if r.ContractAddress != "" {
		results = append(results, &WalletContractRecord{
			Address:        r.FromAddress,
			Contract:       r.ContractAddress,
			FirstTradeTime: r.TxTime,
			LastTradeTime:  r.TxTime,
			CreatedAt:      time.Now().Unix(),
			UpdatedAt:      time.Now().Unix(),
		})
	}
	return results
}

func (r *TxRecord) IntoDaySummary() []*WalletDaySummaryRecord {
	results := make([]*WalletDaySummaryRecord, 0, 2)
	if r.TransactionType == "native" {
		results = append(results, &WalletDaySummaryRecord{
			Address:          r.FromAddress,
			Sharding:         TimeSharding(r.TxTime),
			FirstTradeTime:   r.TxTime,
			TotalTxNum:       1,
			TotalTxInAmount:  decimal.Zero,
			TotalTxOutAmount: r.Amount,
			Denomination:     DenominationNative,
			CreatedAt:        time.Now().Unix(),
			UpdatedAt:        time.Now().Unix(),
		}, &WalletDaySummaryRecord{
			Address:          r.ToAddress,
			Sharding:         TimeSharding(r.TxTime),
			FirstTradeTime:   r.TxTime,
			TotalTxNum:       1,
			TotalTxInAmount:  r.Amount,
			TotalTxOutAmount: decimal.Zero,
			Denomination:     DenominationNative,
			CreatedAt:        time.Now().Unix(),
			UpdatedAt:        time.Now().Unix(),
		})
	} else if r.TransactionType == "transfer" {
		results = append(results, &WalletDaySummaryRecord{
			Address:          r.FromAddress,
			Sharding:         TimeSharding(r.TxTime),
			FirstTradeTime:   r.TxTime,
			TotalTxNum:       1,
			TotalTxInAmount:  decimal.Zero,
			TotalTxOutAmount: decimal.Zero,
			TotalTxContract:  1,
			Denomination:     DenominationNative,
			CreatedAt:        time.Now().Unix(),
			UpdatedAt:        time.Now().Unix(),
		}, &WalletDaySummaryRecord{
			Address:          r.ToAddress,
			Sharding:         TimeSharding(r.TxTime),
			FirstTradeTime:   r.TxTime,
			TotalTxNum:       1,
			TotalTxInAmount:  decimal.Zero,
			TotalTxOutAmount: decimal.Zero,
			Denomination:     DenominationNative,
			CreatedAt:        time.Now().Unix(),
			UpdatedAt:        time.Now().Unix(),
		})
	} else {
		results = append(results, &WalletDaySummaryRecord{
			Address:          r.FromAddress,
			Sharding:         TimeSharding(r.TxTime),
			FirstTradeTime:   r.TxTime,
			TotalTxNum:       1,
			TotalTxInAmount:  decimal.Zero,
			TotalTxOutAmount: decimal.Zero,
			TotalTxContract:  1,
			Denomination:     DenominationNative,
			CreatedAt:        time.Now().Unix(),
			UpdatedAt:        time.Now().Unix(),
		})
	}
	return results
}

type TxRecordCursor interface {
	CursorList(ctx context.Context, txTime int64, chainName string, limit int, cursor *uint64) ([]*TxRecord, error)
}

type walletRepoImpl struct {
	db     *gorm.DB
	cursor TxRecordCursor
}

func NewWalletRepo(db KanbanGormDB) WalletRepo {
	return &walletRepoImpl{
		db: db,
	}
}

// SaveContract implements WalletRepo
func (r *walletRepoImpl) BatchSaveContract(ctx context.Context, chainName string, records []*WalletContractRecord) error {
	tableName := r.contractTable(chainName)
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{
				Name: "address",
			},
			{
				Name: "contract",
			},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"address":  clause.Column{Table: "excluded", Name: "address"},
			"contract": clause.Column{Table: "excluded", Name: "contract"},
			"first_trade_time": gorm.Expr("case when(" + tableName + ".first_trade_time > excluded.first_trade_time) then excluded.first_trade_time else " +
				tableName + ".first_trade_time end"),
			"last_trade_time": gorm.Expr("case when(" + tableName + ".last_trade_time < excluded.last_trade_time) then excluded.last_trade_time else " + tableName + ".last_trade_time end"),
			"updated_at":      gorm.Expr("excluded.updated_at"),
		}),
	}
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(onConflict).Create(&records)
	if ret.Error != nil {
		return ret.Error
	}
	return nil
}

func (r *walletRepoImpl) contractTable(chainName string) string {
	return strings.ToLower(chainName) + "_wallet_contract"
}

// SaveDaySummary implements WalletRepo
func (r *walletRepoImpl) BatchSaveDaySummary(ctx context.Context, chainName string, records []*WalletDaySummaryRecord) error {
	tableName := r.daySummaryTable(chainName)
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{
				Name: "address",
			},
			{
				Name: "sharding",
			},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"address":             clause.Column{Table: "excluded", Name: "address"},
			"sharding":            clause.Column{Table: "excluded", Name: "sharding"},
			"first_trade_time":    clause.Column{Table: "excluded", Name: "first_trade_time"},
			"total_tx_num":        clause.Column{Table: "excluded", Name: "total_tx_num"},
			"total_tx_contract":   clause.Column{Table: "excluded", Name: "total_tx_contract"},
			"total_tx_in_amount":  clause.Column{Table: "excluded", Name: "total_tx_in_amount"},
			"total_tx_out_amount": clause.Column{Table: "excluded", Name: "total_tx_out_amount"},
			"denomination":        gorm.Expr("excluded.denomination"),
			"updated_at":          gorm.Expr("excluded.updated_at"),
		}),
	}

	ret := r.db.WithContext(ctx).Table(tableName).Clauses(onConflict).Create(&records)
	if ret.Error != nil {
		return ret.Error
	}
	return nil
}

func (r *walletRepoImpl) daySummaryTable(chainName string) string {
	return strings.ToLower(chainName) + "_wallet_day_summary"
}

// Accumulate implements WalletRepo
func (r *walletRepoImpl) Accumulate(ctx context.Context, chainName string, address string) error {
	var contractNum int64
	err := r.db.WithContext(ctx).Table(r.contractTable(chainName)).Where("address = ?", address).Count(&contractNum).Error
	if err != nil {
		return err
	}

	var dayRecord *WalletDaySummaryRecord
	err = r.db.WithContext(ctx).Table(r.daySummaryTable(chainName)).Where("address = ?", address).Order("first_trade_time ASC").First(&dayRecord).Error
	if err != nil {
		return err
	}
	var totalTxNum int64
	var totalInAmount decimal.Decimal
	var totalOutAmount decimal.Decimal
	err = r.db.WithContext(ctx).Table(r.daySummaryTable(chainName)).Select(`SUM(total_tx_num) AS t,
SUM(CAST(total_tx_in_amount AS DECIMAL)) AS i,
SUM(CAST(total_tx_out_amount AS DECIMAL)) AS o`).Where("address = ?", address).Row().Scan(&totalTxNum, &totalInAmount, &totalOutAmount)
	if err != nil {
		return err
	}

	record := &WalletSummaryRecord{
		Address:          address,
		FirstTradeTime:   dayRecord.FirstTradeTime,
		TotalTxNum:       totalTxNum,
		TotalContractNum: contractNum,
		TotalTxAmount:    totalInAmount.Add(totalOutAmount),
		TotalTxInAmount:  totalInAmount,
		TotalTxOutAmount: totalOutAmount,
		Denomination:     DenominationNative, // TODO
		CreatedAt:        time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	}

	tableName := r.summaryTable(chainName)

	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{
				Name: "address",
			},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"address":             clause.Column{Table: "excluded", Name: "address"},
			"first_trade_time":    clause.Column{Table: "excluded", Name: "first_trade_time"},
			"total_tx_num":        clause.Column{Table: "excluded", Name: "total_tx_num"},
			"total_contract_num":  clause.Column{Table: "excluded", Name: "total_contract_num"},
			"total_tx_amount":     clause.Column{Table: "excluded", Name: "total_tx_amount"},
			"total_tx_in_amount":  clause.Column{Table: "excluded", Name: "total_tx_in_amount"},
			"total_tx_out_amount": clause.Column{Table: "excluded", Name: "total_tx_out_amount"},
			"denomination":        gorm.Expr("excluded.denomination"),
			"updated_at":          gorm.Expr("excluded.updated_at"),
		}),
	}
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(onConflict).Create(record)
	if ret.Error != nil {
		return ret.Error
	}
	return nil
}

func (r *walletRepoImpl) summaryTable(chainName string) string {
	return strings.ToLower(chainName) + "_wallet_summary"
}

func (r *walletRepoImpl) Count(ctx context.Context, chainName string, key string) (int64, error) {
	var result int64
	ret := r.db.WithContext(ctx).Table(r.summaryTable(chainName)).Where(key + " > 0").Count(&result)
	return result, ret.Error
}

func (r *walletRepoImpl) GetTopPercentRank(ctx context.Context, chainName string, count int64, key string, percent int) (decimal.Decimal, error) {
	if percent > 100 {
		return decimal.Zero, errors.New("percent overflow")
	}
	offset := int64(float64(count) * (float64(percent) / 100))
	db := r.db.WithContext(ctx).Table(r.summaryTable(chainName)).Select(key).Order(key + " DESC").Offset(int(offset)).Limit(1)
	if strings.HasSuffix(key, "_amount") {
		var v decimal.Decimal
		if ret := db.Scan(&v); ret.Error != nil {
			return v, ret.Error
		}
		return v, nil
	}
	var v int64
	if ret := db.Scan(&v); ret.Error != nil {
		return decimal.Zero, ret.Error
	}
	return decimal.NewFromInt(v), nil
}

func (r *walletRepoImpl) RangeDaySummary(ctx context.Context, chainName, address string, startTime int64, endTime int64) ([]*WalletDaySummaryRecord, error) {
	var results []*WalletDaySummaryRecord
	startTime = TimeSharding(startTime)
	endTime = TimeSharding(endTime)
	ret := r.db.WithContext(ctx).Table(r.daySummaryTable(chainName)).Where(
		"address = ? and sharding >= ? and sharding <= ?", address, startTime, endTime,
	).Order("sharding ASC").Find(&results)

	if ret.Error != nil {
		return nil, ret.Error
	}
	return results, nil
}

func (r *walletRepoImpl) AutoMigrate(ctx context.Context, chainName string) error {
	if err := r.db.WithContext(ctx).Table(r.contractTable(chainName)).AutoMigrate(&WalletContractRecord{}); err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Table(r.daySummaryTable(chainName)).AutoMigrate(&WalletDaySummaryRecord{}); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Table(r.summaryTable(chainName)).AutoMigrate(&WalletSummaryRecord{})
}

func (r *walletRepoImpl) LoadSummary(ctx context.Context, chainName, address string) (*WalletSummaryRecord, error) {
	var record *WalletSummaryRecord
	ret := r.db.WithContext(ctx).Table(r.summaryTable(chainName)).Where("address=?", address).Take(&record)
	return record, ret.Error
}

func (r *walletRepoImpl) LoadAllAddresses(ctx context.Context, chainName string, cursor *uint64, pageLimit int) ([]string, error) {
	var records []*WalletSummaryRecord
	ret := r.db.WithContext(ctx).Table(r.summaryTable(chainName)).Where(
		"id>?", *cursor).Order("id ASC").Limit(pageLimit).Find(&records)
	if ret.Error != nil {
		return nil, ret.Error
	}
	results := make([]string, 0, len(records))
	for _, item := range records {
		results = append(results, item.Address)
		*cursor = uint64(item.Id)
	}
	return results, nil
}

func (r *walletRepoImpl) AnalyzeDaySummary(ctx context.Context, chainName string) error {
	return r.db.Exec(fmt.Sprint("ANALYZE ", r.daySummaryTable(chainName))).Error
}
