package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"strconv"
	"strings"
)

// BtcTransactionRecord is a BtcTransactionRecord model.
type BtcTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index"`
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
	Save(context.Context, *BtcTransactionRecord) (int64, error)
	BatchSave(context.Context, []*BtcTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, []*BtcTransactionRecord) (int64, error)
	Update(context.Context, *BtcTransactionRecord) (int64, error)
	FindByID(context.Context, int64) (*BtcTransactionRecord, error)
	FindByStatus(context.Context, string) ([]*BtcTransactionRecord, error)
	ListByID(context.Context, int64) ([]*BtcTransactionRecord, error)
	ListAll(context.Context) ([]*BtcTransactionRecord, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByBlockNumber(context.Context, int) (int64, error)
	FindLast(context.Context) (*BtcTransactionRecord, error)
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

func (r *BtcTransactionRecordRepoImpl) Save(ctx context.Context, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Create(btcTransactionRecord)
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

func (r *BtcTransactionRecordRepoImpl) BatchSave(ctx context.Context, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.CreateInBatches(btcTransactionRecords, len(btcTransactionRecords))
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

func (r *BtcTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, btcTransactionRecords []*BtcTransactionRecord) (int64, error) {
	sqlStr := "insert into public.btc_transaction_record (block_hash, block_number, transaction_hash, from_address, to_address, " +
		"from_uid, to_uid, fee_amount, amount, status, tx_time, confirm_count, dapp_data, client_data, created_at, updated_at) values "
	btcTransactionRecordsLen := len(btcTransactionRecords)
	for i := 0; i < btcTransactionRecordsLen; i++ {
		btc := btcTransactionRecords[i]
		sqlStr += "('" + btc.BlockHash + "', " + strconv.Itoa(btc.BlockNumber) + ", '" + btc.TransactionHash + "', '" + btc.FromAddress + "', '" +
			btc.ToAddress + "', '" + btc.FromUid + "', '" + btc.ToUid + "', " + btc.FeeAmount.String() + ", " + btc.Amount.String() + ", '" +
			btc.Status + "', " + strconv.Itoa(int(btc.TxTime)) + ", " + strconv.Itoa(int(btc.ConfirmCount)) + ", '" + btc.DappData + "', '" +
			btc.ClientData + "', " + strconv.Itoa(int(btc.CreatedAt)) + ", " + strconv.Itoa(int(btc.UpdatedAt)) + "),"
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	sqlStr += " on conflict (transaction_hash) do update set block_hash = excluded.block_hash, block_number = excluded.block_number, " +
		"transaction_hash = excluded.transaction_hash, from_address = excluded.from_address, to_address = excluded.to_address, " +
		"from_uid = excluded.from_uid, to_uid = excluded.to_uid, fee_amount = excluded.fee_amount, amount = excluded.amount, " +
		"status = excluded.status, tx_time = excluded.tx_time, confirm_count = excluded.confirm_count, dapp_data = excluded.dapp_data, " +
		"client_data = excluded.client_data, created_at = excluded.created_at, updated_at = excluded.updated_at"

	ret := r.gormDB.Exec(sqlStr)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update btcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *BtcTransactionRecordRepoImpl) Update(ctx context.Context, btcTransactionRecord *BtcTransactionRecord) (int64, error) {
	ret := r.gormDB.Model(&BtcTransactionRecord{}).Where("id = ?", btcTransactionRecord.Id).Updates(btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindByID(ctx context.Context, id int64) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.First(&btcTransactionRecord, id)
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

func (r *BtcTransactionRecordRepoImpl) FindByStatus(ctx context.Context, status string) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Where("status = ?", status).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListByID(ctx context.Context, id int64) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Where("id > ?", id).Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) ListAll(ctx context.Context) ([]*BtcTransactionRecord, error) {
	var btcTransactionRecordList []*BtcTransactionRecord
	ret := r.gormDB.Find(&btcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecordList, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.Delete(&BtcTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, blockNumber int) (int64, error) {
	ret := r.gormDB.Where("block_number >= ?", blockNumber).Delete(&BtcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete btcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *BtcTransactionRecordRepoImpl) FindLast(context.Context) (*BtcTransactionRecord, error) {
	var btcTransactionRecord *BtcTransactionRecord
	ret := r.gormDB.Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&btcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last btcTransactionRecord failed", err)
		return nil, err
	}
	return btcTransactionRecord, nil
}
