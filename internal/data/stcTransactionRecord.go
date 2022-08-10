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
	Save(context.Context, *StcTransactionRecord) (int64, error)
	BatchSave(context.Context, []*StcTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, []*StcTransactionRecord) (int64, error)
	Update(context.Context, *StcTransactionRecord) (int64, error)
	FindByID(context.Context, int64) (*StcTransactionRecord, error)
	FindByStatus(context.Context, string) ([]*StcTransactionRecord, error)
	ListByID(context.Context, int64) ([]*StcTransactionRecord, error)
	ListAll(context.Context) ([]*StcTransactionRecord, error)
	DeleteByID(context.Context, int64) (int64, error)
	DeleteByBlockNumber(context.Context, int) (int64, error)
	FindLast(context.Context) (*StcTransactionRecord, error)
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

func (r *StcTransactionRecordRepoImpl) Save(ctx context.Context, stcTransactionRecord *StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Create(stcTransactionRecord)
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

func (r *StcTransactionRecordRepoImpl) BatchSave(ctx context.Context, stcTransactionRecords []*StcTransactionRecord) (int64, error) {
	ret := r.gormDB.CreateInBatches(stcTransactionRecords, len(stcTransactionRecords))
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

func (r *StcTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, stcTransactionRecords []*StcTransactionRecord) (int64, error) {
	sqlStr := "insert into public.stc_transaction_record (block_hash, block_number, transaction_hash, from_address, to_address, " +
		"from_uid, to_uid, fee_amount, amount, status, tx_time, contract_address, parse_data, gas_limit, gas_used, gas_price, " +
		"data, transaction_type, dapp_data, client_data, created_at, updated_at) values "
	stcTransactionRecordsLen := len(stcTransactionRecords)
	for i := 0; i < stcTransactionRecordsLen; i++ {
		stc := stcTransactionRecords[i]
		sqlStr += "('" + stc.BlockHash + "', " + strconv.Itoa(stc.BlockNumber) + ", '" + stc.TransactionHash + "', '" +
			stc.FromAddress + "', '" + stc.ToAddress + "', '" + stc.FromUid + "', '" + stc.ToUid + "', " + stc.FeeAmount.String() + ", " +
			stc.Amount.String() + ", '" + stc.Status + "', " + strconv.Itoa(int(stc.TxTime)) + ", '" + stc.ContractAddress + "', '" +
			stc.ParseData + "', '" + stc.GasLimit + "', '" + stc.GasUsed + "', '" + stc.GasPrice + "', '" + stc.Data + "', '" +
			stc.TransactionType + "', '" + stc.DappData + "', '" + stc.ClientData + "', " +
			strconv.Itoa(int(stc.CreatedAt)) + ", " + strconv.Itoa(int(stc.UpdatedAt)) + "),"
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	sqlStr += " on conflict (transaction_hash) do update set block_hash = excluded.block_hash, block_number = excluded.block_number, " +
		"transaction_hash = excluded.transaction_hash, from_address = excluded.from_address, to_address = excluded.to_address, " +
		"from_uid = excluded.from_uid, to_uid = excluded.to_uid, fee_amount = excluded.fee_amount, amount = excluded.amount, " +
		"status = excluded.status, tx_time = excluded.tx_time, contract_address = excluded.contract_address, parse_data = excluded.parse_data, " +
		"gas_limit = excluded.gas_limit, gas_used = excluded.gas_used, gas_price = excluded.gas_price, data = excluded.data, " +
		"transaction_type = excluded.transaction_type, dapp_data = excluded.dapp_data, client_data = excluded.client_data, " +
		"created_at = excluded.created_at, updated_at = excluded.updated_at"

	ret := r.gormDB.Exec(sqlStr)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update stcTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *StcTransactionRecordRepoImpl) Update(ctx context.Context, stcTransactionRecord *StcTransactionRecord) (int64, error) {
	ret := r.gormDB.Model(&StcTransactionRecord{}).Where("id = ?", stcTransactionRecord.Id).Updates(stcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) FindByID(ctx context.Context, id int64) (*StcTransactionRecord, error) {
	var stcTransactionRecord *StcTransactionRecord
	ret := r.gormDB.First(&stcTransactionRecord, id)
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

func (r *StcTransactionRecordRepoImpl) ListByID(ctx context.Context, id int64) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Where("id > ?", id).Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) FindByStatus(ctx context.Context, status string) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Where("status = ?", status).Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) ListAll(ctx context.Context) ([]*StcTransactionRecord, error) {
	var stcTransactionRecordList []*StcTransactionRecord
	ret := r.gormDB.Find(&stcTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecordList, nil
}

func (r *StcTransactionRecordRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.Delete(&StcTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, blockNumber int) (int64, error) {
	ret := r.gormDB.Where("block_number >= ?", blockNumber).Delete(&StcTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete stcTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *StcTransactionRecordRepoImpl) FindLast(ctx context.Context) (*StcTransactionRecord, error) {
	var stcTransactionRecord *StcTransactionRecord
	ret := r.gormDB.Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&stcTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last stcTransactionRecord failed", err)
		return nil, err
	}
	return stcTransactionRecord, nil
}
