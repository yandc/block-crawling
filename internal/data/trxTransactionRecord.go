package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

type TrxTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(42);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(42);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(42);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	NetUsage        string          `json:"netUsage" form:"netUsage" gorm:"type:character varying(30)"`
	FeeLimit        string          `json:"feeLimit" form:"feeLimit" gorm:"type:character varying(30)"`
	EnergyUsage     string          `json:"energyUsage" form:"energyUsage" gorm:"type:character varying(20)"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

// TrxTransactionRecordRepo is a Greater repo.
type TrxTransactionRecordRepo interface {
	Save(context.Context, string, *TrxTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*TrxTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*TrxTransactionRecord) (int64, error)
	Update(context.Context, string, *TrxTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*TrxTransactionRecord, error)
	FindByStatus(context.Context, string, string) ([]*TrxTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*TrxTransactionRecord, error)
	ListAll(context.Context, string) ([]*TrxTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*TrxTransactionRecord, error)
}

type TrxTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var TrxTransactionRecordRepoClient TrxTransactionRecordRepo

// NewTrxTransactionRecordRepo new a TrxTransactionRecord repo.
func NewTrxTransactionRecordRepo(gormDB *gorm.DB) TrxTransactionRecordRepo {
	TrxTransactionRecordRepoClient = &TrxTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return TrxTransactionRecordRepoClient
}

func (r *TrxTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, trxTransactionRecord *TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(trxTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(trxTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert trxTransactionRecord failed", err)
		} else {
			log.Errore("insert trxTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TrxTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, trxTransactionRecords []*TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).CreateInBatches(trxTransactionRecords, len(trxTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(trxTransactionRecords)), Data: 0}
			log.Warne("batch insert trxTransactionRecord failed", err)
		} else {
			log.Errore("batch insert trxTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TrxTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, trxTransactionRecords []*TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&trxTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update trxTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TrxTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, trxTransactionRecord *TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Model(&TrxTransactionRecord{}).Where("id = ?", trxTransactionRecord.Id).Updates(trxTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*TrxTransactionRecord, error) {
	var trxTransactionRecord *TrxTransactionRecord
	ret := r.gormDB.Table(tableName).First(&trxTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query trxTransactionRecord failed", err)
		}
		return nil, err
	}

	return trxTransactionRecord, nil
}

func (r *TrxTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Table(tableName).Where("id > ?", id).Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, status string) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = ?", status).Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Table(tableName).Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.Table(tableName).Delete(&TrxTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("block_number >= ?", blockNumber).Delete(&TrxTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*TrxTransactionRecord, error) {
	var trxTransactionRecord *TrxTransactionRecord
	ret := r.gormDB.Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&trxTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecord, nil
}
