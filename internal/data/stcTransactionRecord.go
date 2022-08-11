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

type StcTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
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
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(256);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	GasLimit        string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed         string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice        string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	Data            string          `json:"data" form:"data"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

// StcTransactionRecordRepo is a Greater repo.
type StcTransactionRecordRepo interface {
	Save(context.Context, string, *StcTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*StcTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*StcTransactionRecord) (int64, error)
	Update(context.Context, string, *StcTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*StcTransactionRecord, error)
	FindByStatus(context.Context, string, string) ([]*StcTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*StcTransactionRecord, error)
	ListAll(context.Context, string) ([]*StcTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*StcTransactionRecord, error)
}

type StcTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var StcTransactionRecordRepoClient StcTransactionRecordRepo

// NewStcTransactionRecordRepo new a StcTransactionRecord repo.
func NewStcTransactionRecordRepo(gormDB *gorm.DB) StcTransactionRecordRepo {
	StcTransactionRecordRepoClient = &StcTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return StcTransactionRecordRepoClient
}

func (r *StcTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, stcTransactionRecord *StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(stcTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(stcTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert stcTransactionRecord failed", err)
		} else {
			log.Errore("insert stcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *StcTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, stcTransactionRecords []*StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).CreateInBatches(stcTransactionRecords, len(stcTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(stcTransactionRecords)), Data: 0}
			log.Warne("batch insert stcTransactionRecord failed", err)
		} else {
			log.Errore("batch insert stcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *StcTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, stcTransactionRecords []*StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&stcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update stcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *StcTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, stcTransactionRecord *StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Model(&StcTransactionRecord{}).Where("id = ?", stcTransactionRecord.Id).Updates(stcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*StcTransactionRecord, error) {
	var stcTransactionRecord *StcTransactionRecord
	ret := r.gormDB.Table(tableName).First(&stcTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query stcTransactionRecord failed", err)
		}
		return nil, err
	}

	return stcTransactionRecord, nil
}

func (r *StcTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("id > ?", id).Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, status string) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = ?", status).Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Table(tableName).Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.Table(tableName).Delete(&StcTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("block_number >= ?", blockNumber).Delete(&StcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*StcTransactionRecord, error) {
	var stcTransactionRecord *StcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&stcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecord, nil
}
