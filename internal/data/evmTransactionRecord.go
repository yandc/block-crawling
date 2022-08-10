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

// EvmTransactionRecord is a EvmTransactionRecord model.
type EvmTransactionRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	Nonce           int64           `json:"nonce" form:"nonce"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(42);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(42);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"` // gorm:"type:decimal.Decimal"
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(42);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	Type            string          `json:"type" form:"type" gorm:"type:character varying(2)"`
	GasLimit        string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed         string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice        string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	BaseFee         string          `json:"baseFee" form:"baseFee" gorm:"type:character varying(20)"`
	Data            string          `json:"data" form:"data"`
	EventLog        string          `json:"eventLog" form:"eventLog"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}


// EvmTransactionRecordRepo is a Greater repo.
type EvmTransactionRecordRepo interface {
	Save(context.Context, string, *EvmTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*EvmTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*EvmTransactionRecord) (int64, error)
	Update(context.Context, string, *EvmTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*EvmTransactionRecord, error)
	FindByStatus(context.Context, string, string) ([]*EvmTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*EvmTransactionRecord, error)
	ListAll(context.Context, string) ([]*EvmTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	FindLast(context.Context, string) (*EvmTransactionRecord, error)
}

type EvmTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var EvmTransactionRecordRepoClient EvmTransactionRecordRepo

// NewEvmTransactionRecordRepo new a EvmTransactionRecord repo.
func NewEvmTransactionRecordRepo(gormDB *gorm.DB) EvmTransactionRecordRepo {
	EvmTransactionRecordRepoClient = &EvmTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return EvmTransactionRecordRepoClient
}

func (r *EvmTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, evmTransactionRecord *EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Create(evmTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(evmTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert evmTransactionRecord failed", err)
		} else {
			log.Errore("insert "+tableName+" failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).CreateInBatches(evmTransactionRecords, len(evmTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(evmTransactionRecords)), Data: 0}
			log.Warne("batch insert "+tableName+" failed", err)
		} else {
			log.Errore("batch insert "+tableName+" failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, evmTransactionRecords []*EvmTransactionRecord) (int64, error) {
	sqlStr := "insert into public." + tableName + " (block_hash, block_number, nonce, transaction_hash, from_address, to_address, " +
		"from_uid, to_uid, fee_amount, amount, status, tx_time, contract_address, parse_data, type, gas_limit, gas_used, gas_price, base_fee, " +
		"data, transaction_type, dapp_data, client_data, created_at, updated_at,event_log) values "
	evmTransactionRecordsLen := len(evmTransactionRecords)
	for i := 0; i < evmTransactionRecordsLen; i++ {
		evm := evmTransactionRecords[i]
		sqlStr += "('" + evm.BlockHash + "', " + strconv.Itoa(evm.BlockNumber) + ", " + strconv.Itoa(int(evm.Nonce)) + ", '" + evm.TransactionHash + "', '" +
			evm.FromAddress + "', '" + evm.ToAddress + "', '" + evm.FromUid + "', '" + evm.ToUid + "', " + evm.FeeAmount.String() + ", " +
			evm.Amount.String() + ", '" + evm.Status + "', " + strconv.Itoa(int(evm.TxTime)) + ", '" + evm.ContractAddress + "', '" +
			evm.ParseData + "', '" + evm.Type + "', '" + evm.GasLimit + "', '" + evm.GasUsed + "', '" + evm.GasPrice + "', '" + evm.BaseFee + "', '" +
			evm.Data + "', '" + evm.TransactionType + "', '" + evm.DappData + "', '" + evm.ClientData + "', " +
			strconv.Itoa(int(evm.CreatedAt)) + ", " + strconv.Itoa(int(evm.UpdatedAt)) + ", " + evm.EventLog + "),"
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	sqlStr += " on conflict (transaction_hash) do update set block_hash = excluded.block_hash, block_number = excluded.block_number, " +
		"nonce = excluded.nonce, transaction_hash = excluded.transaction_hash, from_address = excluded.from_address, to_address = excluded.to_address, " +
		"from_uid = excluded.from_uid, to_uid = excluded.to_uid, fee_amount = excluded.fee_amount, amount = excluded.amount, status = excluded.status, " +
		"tx_time = excluded.tx_time, contract_address = excluded.contract_address, parse_data = excluded.parse_data, type = excluded.type, " +
		"gas_limit = excluded.gas_limit, gas_used = excluded.gas_used, gas_price = excluded.gas_price, base_fee = excluded.base_fee, " +
		"data = excluded.data, transaction_type = excluded.transaction_type, dapp_data = excluded.dapp_data, client_data = excluded.client_data, " +
		"created_at = excluded.created_at, updated_at = excluded.updated_at, event_log = excluded.event_log"

	ret := r.gormDB.Exec(sqlStr)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *EvmTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, evmTransactionRecord *EvmTransactionRecord) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("id = ?", evmTransactionRecord.Id).Updates(evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.Table(tableName).First(&evmTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query "+tableName+" failed", err)
		}
		return nil, err
	}

	return evmTransactionRecord, nil
}

func (r *EvmTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.Table(tableName).Where("id > ?", id).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, status string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.Table(tableName).Where("status = ?", status).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}

	//rows := ret.RowsAffected

	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*EvmTransactionRecord, error) {
	var evmTransactionRecordList []*EvmTransactionRecord
	ret := r.gormDB.Table(tableName).Find(&evmTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecordList, nil
}

func (r *EvmTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.Table(tableName).Delete(&EvmTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.Table(tableName).Where("block_number >= ?", blockNumber).Delete(&EvmTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *EvmTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*EvmTransactionRecord, error) {
	var evmTransactionRecord *EvmTransactionRecord
	ret := r.gormDB.Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&evmTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	}
	return evmTransactionRecord, nil
}
