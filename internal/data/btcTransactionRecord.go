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

// BtcTransactionRecord is a BtcTransactionRecord model.
type BtcTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(62);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(62);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ConfirmCount    int32           `json:"confirmCount" form:"confirmCount"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

// BtcTransactionRecordRepo is a Greater repo.
type BtcTransactionRecordRepo interface {
	Save(context.Context, string, *BtcTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*BtcTransactionRecord) (int64, error)
	Update(context.Context, string, *BtcTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*BtcTransactionRecord, error)
	FindByStatus(context.Context, string, string) ([]*BtcTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*BtcTransactionRecord, error)
	ListAll(context.Context, string) ([]*BtcTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*BtcTransactionRecord, error)
}

type BtcTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var BtcTransactionRecordRepoClient BtcTransactionRecordRepo

// NewBtcTransactionRecordRepo new a BtcTransactionRecord repo.
func NewBtcTransactionRecordRepo(gormDB *gorm.DB) BtcTransactionRecordRepo {
	BtcTransactionRecordRepoClient = &BtcTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return BtcTransactionRecordRepoClient
}

func (r *BtcTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(btcTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(btcTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert btcTransactionRecord failed", err)
		} else {
			log.Errore("insert btcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).CreateInBatches(btcTransactionRecords, len(btcTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(btcTransactionRecords)), Data: 0}
			log.Warne("batch insert btcTransactionRecord failed", err)
		} else {
			log.Errore("batch insert btcTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&btcTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update btcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Model(&BtcTransactionRecord{}).Where("id = ?", btcTransactionRecord.Id).Updates(btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.Table(tableName).First(&btcTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query btcTransactionRecord failed", err)
		}
		return nil, err
	}
	return btcTransactionRecord, nil
}

func (r *BtcTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, status string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = ?", status).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("id > ?", id).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Table(tableName).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.Table(tableName).Delete(&BtcTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("block_number >= ?", blockNumber).Delete(&BtcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecord, nil
}
