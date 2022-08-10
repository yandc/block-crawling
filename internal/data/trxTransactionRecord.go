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
	Save(context.Context, *TrxTransactionRecord) (int64, error)
	BatchSave(context.Context, []*TrxTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, []*TrxTransactionRecord) (int64, error)
	Update(context.Context, *TrxTransactionRecord) (int64, error)
	FindByID(context.Context, int64) (*TrxTransactionRecord, error)
	FindByStatus(context.Context, string) ([]*TrxTransactionRecord, error)
	ListByID(context.Context, int64) ([]*TrxTransactionRecord, error)
	ListAll(context.Context) ([]*TrxTransactionRecord, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByBlockNumber(context.Context, int) (int64, error)
	FindLast(context.Context) (*TrxTransactionRecord, error)
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

func (r *TrxTransactionRecordRepoImpl) Save(ctx context.Context, trxTransactionRecord *TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Create(trxTransactionRecord)
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

func (r *TrxTransactionRecordRepoImpl) BatchSave(ctx context.Context, trxTransactionRecords []*TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.CreateInBatches(trxTransactionRecords, len(trxTransactionRecords))
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

func (r *TrxTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, trxTransactionRecords []*TrxTransactionRecord) (int64, error) {
	sqlStr := "insert into public.trx_transaction_record (block_hash, block_number, transaction_hash, from_address, to_address, " +
		"from_uid, to_uid, fee_amount, amount, status, tx_time, contract_address, parse_data, net_usage, fee_limit, energy_usage, " +
		"transaction_type, dapp_data, client_data, created_at, updated_at) values "
	trxTransactionRecordsLen := len(trxTransactionRecords)
	for i := 0; i < trxTransactionRecordsLen; i++ {
		trx := trxTransactionRecords[i]
		sqlStr += "('" + trx.BlockHash + "', " + strconv.Itoa(trx.BlockNumber) + ", '" + trx.TransactionHash + "', '" +
			trx.FromAddress + "', '" + trx.ToAddress + "', '" + trx.FromUid + "', '" + trx.ToUid + "', " + trx.FeeAmount.String() + ", " +
			trx.Amount.String() + ", '" + trx.Status + "', " + strconv.Itoa(int(trx.TxTime)) + ", '" + trx.ContractAddress + "', '" +
			trx.ParseData + "', '" + trx.NetUsage + "', '" + trx.FeeLimit + "', '" + trx.EnergyUsage + "', '" + trx.TransactionType + "', '" +
			trx.DappData + "', '" + trx.ClientData + "', " + strconv.Itoa(int(trx.CreatedAt)) + ", " + strconv.Itoa(int(trx.UpdatedAt)) + "),"
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	sqlStr += " on conflict (transaction_hash) do update set block_hash = excluded.block_hash, block_number = excluded.block_number, " +
		"transaction_hash = excluded.transaction_hash, from_address = excluded.from_address, to_address = excluded.to_address, " +
		"from_uid = excluded.from_uid, to_uid = excluded.to_uid, fee_amount = excluded.fee_amount, amount = excluded.amount, " +
		"status = excluded.status, tx_time = excluded.tx_time, contract_address = excluded.contract_address, parse_data = excluded.parse_data, " +
		"net_usage = excluded.net_usage, fee_limit = excluded.fee_limit, energy_usage = excluded.energy_usage, " +
		"transaction_type = excluded.transaction_type, dapp_data = excluded.dapp_data, client_data = excluded.client_data, " +
		"created_at = excluded.created_at, updated_at = excluded.updated_at"

	ret := r.gormDB.Exec(sqlStr)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update trxTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TrxTransactionRecordRepoImpl) Update(ctx context.Context, trxTransactionRecord *TrxTransactionRecord) (int64, error) {
	ret := r.gormDB.Model(&TrxTransactionRecord{}).Where("id = ?", trxTransactionRecord.Id).Updates(trxTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) FindByID(ctx context.Context, id int64) (*TrxTransactionRecord, error) {
	var trxTransactionRecord *TrxTransactionRecord
	ret := r.gormDB.First(&trxTransactionRecord, id)
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

func (r *TrxTransactionRecordRepoImpl) ListByID(ctx context.Context, id int64) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Where("id > ?", id).Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) FindByStatus(ctx context.Context, status string) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Where("status = ?", status).Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) ListAll(ctx context.Context) ([]*TrxTransactionRecord, error) {
	var trxTransactionRecordList []*TrxTransactionRecord
	ret := r.gormDB.Find(&trxTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecordList, nil
}

func (r *TrxTransactionRecordRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.Delete(&TrxTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, blockNumber int) (int64, error) {
	ret := r.gormDB.Where("block_number >= ?", blockNumber).Delete(&TrxTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete trxTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TrxTransactionRecordRepoImpl) FindLast(context.Context) (*TrxTransactionRecord, error) {
	var trxTransactionRecord *TrxTransactionRecord
	ret := r.gormDB.Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&trxTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last trxTransactionRecord failed", err)
		return nil, err
	}
	return trxTransactionRecord, nil
}
