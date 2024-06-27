package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// TransactionRecord is a TransactionRecord model.
type TransactionRecord struct {
	Id                   int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	SlotNumber           int             `json:"slotNumber" form:"slotNumber"`
	BlockHash            string          `json:"blockHash" form:"blockHash"  gorm:"type:character varying(66)"`
	BlockNumber          int             `json:"blockNumber" form:"blockNumber"`
	Nonce                int64           `json:"nonce" form:"nonce"`
	TransactionHash      string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	OriginalHash         string          `json:"originalHash" form:"originalHash" gorm:"type:character varying(80);default:null;index"`
	FromAddress          string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(42);index"`
	ToAddress            string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(46);index"`
	FromUid              string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(88);index"`
	ToUid                string          `json:"toUid" form:"toUid" gorm:"type:character varying(88);index"`
	FeeAmount            decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount               decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status               string          `json:"status" form:"status" gorm:"type:character varying(20);index"`
	TxTime               int64           `json:"txTime" form:"txTime"`
	ContractAddress      string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(42);index"`
	ParseData            string          `json:"parseData" form:"parseData"`
	Type                 string          `json:"type" form:"type" gorm:"type:character varying(2)"`
	GasLimit             string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(32)"`
	GasUsed              string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(32)"`
	GasPrice             string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(32)"`
	BaseFee              string          `json:"baseFee" form:"baseFee" gorm:"type:character varying(32)"`
	MaxFeePerGas         string          `json:"maxFeePerGas" form:"maxFeePerGas" gorm:"type:character varying(32)"`
	MaxPriorityFeePerGas string          `json:"maxPriorityFeePerGas" form:"maxPriorityFeePerGas" gorm:"type:character varying(32)"`
	FeeLimit             string          `json:"feeLimit" form:"feeLimit" gorm:"type:character varying(30)"`
	NetUsage             string          `json:"netUsage" form:"netUsage" gorm:"type:character varying(30)"`
	EnergyUsage          string          `json:"energyUsage" form:"energyUsage" gorm:"type:character varying(20)"`
	Data                 string          `json:"data" form:"data"`
	EventLog             string          `json:"eventLog" form:"eventLog"`
	LogAddress           datatypes.JSON  `json:"logAddress,omitempty" form:"logAddress" gorm:"type:jsonb"`
	TransactionType      string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	OperateType          string          `json:"operateType" form:"operateType" gorm:"type:character varying(8)"`
	DappData             string          `json:"dappData" form:"dappData"`
	ClientData           string          `json:"clientData" form:"clientData"`
	TokenInfo            string          `json:"tokenInfo" form:"tokenInfo"`
	SendTime             int64           `json:"sendTime" form:"sendTime"`
	SessionId            string          `json:"sessionId" form:"sessionId" gorm:"type:character varying(36);default:null;index:,unique"`
	ShortHost            string          `json:"shortHost" form:"shortHost" gorm:"type:character varying(200);default:null;"`
	TokenGasless         string          `json:"tokenGasless" form:"tokenGasless"`
	Memo                 string          `json:"memo" form:"memo"`
	CreatedAt            int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt            int64           `json:"updatedAt" form:"updatedAt"`
}

type TransactionRecordWrapper struct {
	TransactionRecord
	Total int64 `json:"total,omitempty"`
}

// TransactionRecordRepo is a Greater repo.
type TransactionRecordRepo interface {
	Save(context.Context, string, *TransactionRecord) (int64, error)
	SaveOrUpdateClient(context.Context, string, *TransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*TransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*TransactionRecord) (int64, error)
	BatchSaveOrIgnore(context.Context, string, []*TransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*TransactionRecord) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*TransactionRecord) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*TransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, string, []*TransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveByTransactionHash(context.Context, string, []*TransactionRecord, int) (int64, error)
	FindByID(context.Context, string, int64) (*TransactionRecord, error)
	ListByStatus(context.Context, string, ...string) ([]*TransactionRecord, error)
	ListAll(context.Context, string) ([]*TransactionRecord, error)
	PageListRecord(context.Context, string, *data.TransactionRequest) ([]*TransactionRecordWrapper, int64, error)
	PageList(context.Context, string, *data.TransactionRequest) ([]*TransactionRecord, int64, error)
	PageListAllCallBack(context.Context, string, *data.TransactionRequest, func(list []*TransactionRecord) error, ...time.Duration) error
	PageListAll(context.Context, string, *data.TransactionRequest, ...time.Duration) ([]*TransactionRecord, error)
	List(context.Context, string, *data.TransactionRequest) ([]*TransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	Delete(context.Context, string, *data.TransactionRequest) (int64, error)
	FindLast(context.Context, string) (*TransactionRecord, error)
	FindLastBlockNumberByAddress(context.Context, string, string) (*TransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*TransactionRecord, error)
	FindByTxHash(context.Context, string, string) (*TransactionRecord, error)
	SelectColumnByTxHash(context.Context, string, string, []string) (*TransactionRecord, error)
	ListIncompleteNft(context.Context, string, *data.TransactionRequest) ([]*TransactionRecord, error)
}

type TransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var TransactionRecordRepoClient TransactionRecordRepo

// NewTransactionRecordRepo new a TransactionRecord repo.
func NewTransactionRecordRepo(gormDB *gorm.DB) TransactionRecordRepo {
	TransactionRecordRepoClient = &TransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return TransactionRecordRepoClient
}

func (r *TransactionRecordRepoImpl) Save(ctx context.Context, chainName string, transactionRecord *TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecord *data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecord, &btcTransactionRecord)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.Save(ctx, tableName, btcTransactionRecord)
		}
	case EVM:
		var evmTransactionRecord *data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecord, &evmTransactionRecord)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.Save(ctx, tableName, evmTransactionRecord)
		}
	case APTOS:
		var aptTransactionRecord *data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecord, &aptTransactionRecord)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.Save(ctx, tableName, aptTransactionRecord)
		}
	case SUI:
		var suiTransactionRecord *data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecord, &suiTransactionRecord)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.Save(ctx, tableName, suiTransactionRecord)
		}
	case COSMOS:
		var atomTransactionRecord *data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecord, &atomTransactionRecord)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.Save(ctx, tableName, atomTransactionRecord)
		}
	case NERVOS:
		var ckbTransactionRecord *data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecord, &ckbTransactionRecord)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.Save(ctx, tableName, ckbTransactionRecord)
		}
	case CASPER:
		var csprTransactionRecord *data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecord, &csprTransactionRecord)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.Save(ctx, tableName, csprTransactionRecord)
		}
	case POLKADOT:
		var dotTransactionRecord *data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecord, &dotTransactionRecord)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.Save(ctx, tableName, dotTransactionRecord)
		}
	case SOLANA:
		var solTransactionRecord *data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecord, &solTransactionRecord)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.Save(ctx, tableName, solTransactionRecord)
		}
	case STC:
		var stcTransactionRecord *data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecord, &stcTransactionRecord)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.Save(ctx, tableName, stcTransactionRecord)
		}
	case TVM:
		var trxTransactionRecord *data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecord, &trxTransactionRecord)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.Save(ctx, tableName, trxTransactionRecord)
		}
	case KASPA:
		var kasTransactionRecord *data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecord, &kasTransactionRecord)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.Save(ctx, tableName, kasTransactionRecord)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) SaveOrUpdateClient(ctx context.Context, chainName string, transactionRecord *TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecord *data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecord, &btcTransactionRecord)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, btcTransactionRecord)
		}
	case EVM:
		var evmTransactionRecord *data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecord, &evmTransactionRecord)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, evmTransactionRecord)
		}
	case APTOS:
		var aptTransactionRecord *data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecord, &aptTransactionRecord)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, aptTransactionRecord)
		}
	case SUI:
		var suiTransactionRecord *data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecord, &suiTransactionRecord)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, suiTransactionRecord)
		}
	case COSMOS:
		var atomTransactionRecord *data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecord, &atomTransactionRecord)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, atomTransactionRecord)
		}
	case NERVOS:
		var ckbTransactionRecord *data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecord, &ckbTransactionRecord)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, ckbTransactionRecord)
		}
	case CASPER:
		var csprTransactionRecord *data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecord, &csprTransactionRecord)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, csprTransactionRecord)
		}
	case POLKADOT:
		var dotTransactionRecord *data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecord, &dotTransactionRecord)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, dotTransactionRecord)
		}
	case SOLANA:
		var solTransactionRecord *data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecord, &solTransactionRecord)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, solTransactionRecord)
		}
	case STC:
		var stcTransactionRecord *data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecord, &stcTransactionRecord)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, stcTransactionRecord)
		}
	case TVM:
		var trxTransactionRecord *data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecord, &trxTransactionRecord)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, trxTransactionRecord)
		}
	case KASPA:
		var kasTransactionRecord *data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecord, &kasTransactionRecord)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.SaveOrUpdateClient(ctx, tableName, kasTransactionRecord)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) BatchSave(ctx context.Context, chainName string, transactionRecords []*TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &btcTransactionRecords)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.BatchSave(ctx, tableName, btcTransactionRecords)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecords, &evmTransactionRecords)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.BatchSave(ctx, tableName, evmTransactionRecords)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecords, &aptTransactionRecords)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.BatchSave(ctx, tableName, aptTransactionRecords)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecords, &suiTransactionRecords)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.BatchSave(ctx, tableName, suiTransactionRecords)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecords, &atomTransactionRecords)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.BatchSave(ctx, tableName, atomTransactionRecords)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecords, &ckbTransactionRecords)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.BatchSave(ctx, tableName, ckbTransactionRecords)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecords, &csprTransactionRecords)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.BatchSave(ctx, tableName, csprTransactionRecords)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecords, &dotTransactionRecords)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.BatchSave(ctx, tableName, dotTransactionRecords)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecords, &solTransactionRecords)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.BatchSave(ctx, tableName, solTransactionRecords)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &stcTransactionRecords)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.BatchSave(ctx, tableName, stcTransactionRecords)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecords, &trxTransactionRecords)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.BatchSave(ctx, tableName, trxTransactionRecords)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecords, &kasTransactionRecords)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.BatchSave(ctx, tableName, kasTransactionRecords)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, chainName string, transactionRecords []*TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &btcTransactionRecords)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, btcTransactionRecords)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecords, &evmTransactionRecords)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, evmTransactionRecords)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecords, &aptTransactionRecords)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, aptTransactionRecords)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecords, &suiTransactionRecords)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, suiTransactionRecords)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecords, &atomTransactionRecords)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, atomTransactionRecords)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecords, &ckbTransactionRecords)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, ckbTransactionRecords)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecords, &csprTransactionRecords)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, csprTransactionRecords)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecords, &dotTransactionRecords)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, dotTransactionRecords)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecords, &solTransactionRecords)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, solTransactionRecords)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &stcTransactionRecords)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, stcTransactionRecords)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecords, &trxTransactionRecords)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, trxTransactionRecords)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecords, &kasTransactionRecords)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.BatchSaveOrUpdate(ctx, tableName, kasTransactionRecords)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, chainName string, transactionRecords []*TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &btcTransactionRecords)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, btcTransactionRecords)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecords, &evmTransactionRecords)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, evmTransactionRecords)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecords, &aptTransactionRecords)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, aptTransactionRecords)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecords, &suiTransactionRecords)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, suiTransactionRecords)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecords, &atomTransactionRecords)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, atomTransactionRecords)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecords, &ckbTransactionRecords)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, ckbTransactionRecords)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecords, &csprTransactionRecords)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, csprTransactionRecords)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecords, &dotTransactionRecords)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, dotTransactionRecords)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecords, &solTransactionRecords)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, solTransactionRecords)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &stcTransactionRecords)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, stcTransactionRecords)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecords, &trxTransactionRecords)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, trxTransactionRecords)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecords, &kasTransactionRecords)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.BatchSaveOrIgnore(ctx, tableName, kasTransactionRecords)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, chainName string, transactionRecords []*TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &btcTransactionRecords)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, btcTransactionRecords)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecords, &evmTransactionRecords)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, evmTransactionRecords)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecords, &aptTransactionRecords)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, aptTransactionRecords)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecords, &suiTransactionRecords)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, suiTransactionRecords)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecords, &atomTransactionRecords)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, atomTransactionRecords)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecords, &ckbTransactionRecords)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, ckbTransactionRecords)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecords, &csprTransactionRecords)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, csprTransactionRecords)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecords, &dotTransactionRecords)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, dotTransactionRecords)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecords, &solTransactionRecords)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, solTransactionRecords)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &stcTransactionRecords)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, stcTransactionRecords)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecords, &trxTransactionRecords)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, trxTransactionRecords)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecords, &kasTransactionRecords)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.BatchSaveOrUpdateSelective(ctx, tableName, kasTransactionRecords)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, chainName string, columns []string, transactionRecords []*TransactionRecord) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &btcTransactionRecords)
		if err == nil {
			affected, err = data.BtcTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, btcTransactionRecords)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		err = utils.CopyProperties(transactionRecords, &evmTransactionRecords)
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, evmTransactionRecords)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		err = utils.CopyProperties(transactionRecords, &aptTransactionRecords)
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, aptTransactionRecords)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		err = utils.CopyProperties(transactionRecords, &suiTransactionRecords)
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, suiTransactionRecords)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		err = utils.CopyProperties(transactionRecords, &atomTransactionRecords)
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, atomTransactionRecords)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		err = utils.CopyProperties(transactionRecords, &ckbTransactionRecords)
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, ckbTransactionRecords)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		err = utils.CopyProperties(transactionRecords, &csprTransactionRecords)
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, csprTransactionRecords)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		err = utils.CopyProperties(transactionRecords, &dotTransactionRecords)
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, dotTransactionRecords)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		err = utils.CopyProperties(transactionRecords, &solTransactionRecords)
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, solTransactionRecords)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		err = utils.CopyProperties(transactionRecords, &stcTransactionRecords)
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, stcTransactionRecords)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		err = utils.CopyProperties(transactionRecords, &trxTransactionRecords)
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, trxTransactionRecords)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		err = utils.CopyProperties(transactionRecords, &kasTransactionRecords)
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, kasTransactionRecords)
		}
	}
	return affected, err
}

func (r *TransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, chainName string, columns []string, txRecords []*TransactionRecord, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(txRecords)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdateSelectiveByColumns(ctx, chainName, columns, subTxRecords)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *TransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, chainName string, txRecords []*TransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, chainName, []string{"id"}, txRecords, pageSize)
}

func (r *TransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByTransactionHash(ctx context.Context, chainName string, txRecords []*TransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, chainName, []string{"transaction_hash"}, txRecords, pageSize)
}

func (r *TransactionRecordRepoImpl) FindByID(ctx context.Context, chainName string, id int64) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&transactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == data.POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query "+tableName+" failed", err)
		}
		return nil, err
	}

	return transactionRecord, nil
}

func (r *TransactionRecordRepoImpl) ListByStatus(ctx context.Context, chainName string, statusList ...string) ([]*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecordList []*TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", statusList).Find(&transactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return transactionRecordList, nil
}

func (r *TransactionRecordRepoImpl) ListAll(ctx context.Context, chainName string) ([]*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecordList []*TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Order("tx_time asc").Find(&transactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return transactionRecordList, nil
}

func (r *TransactionRecordRepoImpl) PageListRecord(ctx context.Context, chainName string, req *data.TransactionRequest) ([]*TransactionRecordWrapper, int64, error) {
	tableName := GetTableName(chainName)
	var transactionRecordList []*TransactionRecordWrapper
	var total int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecordWrapper
		if err == nil {
			evmTransactionRecords, total, err = data.EvmTransactionRecordRepoClient.PageListRecord(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(evmTransactionRecords, &transactionRecordList)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecordWrapper
		if err == nil {
			stcTransactionRecords, total, err = data.StcTransactionRecordRepoClient.PageListRecord(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(stcTransactionRecords, &transactionRecordList)
		}
	}
	return transactionRecordList, total, nil
}

func (r *TransactionRecordRepoImpl) PageList(ctx context.Context, chainName string, req *data.TransactionRequest) ([]*TransactionRecord, int64, error) {
	tableName := data.GetTableName(chainName)
	var transactionRecordList []*TransactionRecord
	var total int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case BTC:
		var btcTransactionRecords []*data.BtcTransactionRecord
		if err == nil {
			btcTransactionRecords, total, err = data.BtcTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(btcTransactionRecords, &transactionRecordList)
		}
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		if err == nil {
			evmTransactionRecords, total, err = data.EvmTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(evmTransactionRecords, &transactionRecordList)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		if err == nil {
			aptTransactionRecords, total, err = data.AptTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(aptTransactionRecords, &transactionRecordList)
		}
	case SUI:
		var suiTransactionRecords []*data.SuiTransactionRecord
		if err == nil {
			suiTransactionRecords, total, err = data.SuiTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(suiTransactionRecords, &transactionRecordList)
		}
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		if err == nil {
			atomTransactionRecords, total, err = data.AtomTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(atomTransactionRecords, &transactionRecordList)
		}
	case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		if err == nil {
			ckbTransactionRecords, total, err = data.CkbTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(ckbTransactionRecords, &transactionRecordList)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		if err == nil {
			csprTransactionRecords, total, err = data.CsprTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(csprTransactionRecords, &transactionRecordList)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		if err == nil {
			dotTransactionRecords, total, err = data.DotTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(dotTransactionRecords, &transactionRecordList)
		}
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		if err == nil {
			solTransactionRecords, total, err = data.SolTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(solTransactionRecords, &transactionRecordList)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		if err == nil {
			stcTransactionRecords, total, err = data.StcTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(stcTransactionRecords, &transactionRecordList)
		}
	case TVM:
		var trxTransactionRecords []*data.TrxTransactionRecord
		if err == nil {
			trxTransactionRecords, total, err = data.TrxTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(trxTransactionRecords, &transactionRecordList)
		}
	case KASPA:
		var kasTransactionRecords []*data.KasTransactionRecord
		if err == nil {
			kasTransactionRecords, total, err = data.KasTransactionRecordRepoClient.PageList(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(kasTransactionRecords, &transactionRecordList)
		}
	}
	return transactionRecordList, total, nil
}

func (r *TransactionRecordRepoImpl) PageListAllCallBack(ctx context.Context, chainName string, req *data.TransactionRequest, fn func(list []*TransactionRecord) error, timeDuration ...time.Duration) error {
	var timeout time.Duration
	if len(timeDuration) > 0 {
		timeout = timeDuration[0]
	} else {
		timeout = 1_000 * time.Millisecond
	}
	req.DataDirection = 2
	for {
		transactionRecords, _, err := r.PageList(ctx, chainName, req)
		if err != nil {
			return err
		}
		dataLen := int32(len(transactionRecords))
		if dataLen == 0 {
			break
		}

		err = fn(transactionRecords)
		if err != nil {
			return err
		}
		if dataLen < req.PageSize {
			break
		}
		req.StartIndex = transactionRecords[dataLen-1].Id
		time.Sleep(timeout)
	}
	return nil
}

func (r *TransactionRecordRepoImpl) PageListAll(ctx context.Context, chainName string, req *data.TransactionRequest, timeDuration ...time.Duration) ([]*TransactionRecord, error) {
	var transactionRecordList []*TransactionRecord
	err := r.PageListAllCallBack(nil, chainName, req, func(transactionRecords []*TransactionRecord) error {
		transactionRecordList = append(transactionRecordList, transactionRecords...)
		return nil
	}, timeDuration...)
	return transactionRecordList, err
}

func (r *TransactionRecordRepoImpl) List(ctx context.Context, chainName string, req *data.TransactionRequest) ([]*TransactionRecord, error) {
	tableName := data.GetTableName(chainName)
	var transactionRecordList []*TransactionRecord
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	/*case BTC:
	var btcTransactionRecords []*data.BtcTransactionRecord
	if err == nil {
		btcTransactionRecords, err = data.BtcTransactionRecordRepoClient.List(ctx, tableName, req)
	}
	if err == nil {
		err = utils.CopyProperties(btcTransactionRecords, &transactionRecordList)
	}*/
	case EVM:
		var evmTransactionRecords []*data.EvmTransactionRecord
		if err == nil {
			evmTransactionRecords, err = data.EvmTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(evmTransactionRecords, &transactionRecordList)
		}
	case APTOS:
		var aptTransactionRecords []*data.AptTransactionRecord
		if err == nil {
			aptTransactionRecords, err = data.AptTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(aptTransactionRecords, &transactionRecordList)
		}
	/*case SUI:
	var suiTransactionRecords []*data.SuiTransactionRecord
	if err == nil {
		suiTransactionRecords, err = data.SuiTransactionRecordRepoClient.List(ctx, tableName, req)
	}
	if err == nil {
		err = utils.CopyProperties(suiTransactionRecords, &transactionRecordList)
	}*/
	case COSMOS:
		var atomTransactionRecords []*data.AtomTransactionRecord
		if err == nil {
			atomTransactionRecords, err = data.AtomTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(atomTransactionRecords, &transactionRecordList)
		}
	/*case NERVOS:
		var ckbTransactionRecords []*data.CkbTransactionRecord
		if err == nil {
			ckbTransactionRecords, err = data.CkbTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(ckbTransactionRecords, &transactionRecordList)
		}
	case CASPER:
		var csprTransactionRecords []*data.CsprTransactionRecord
		if err == nil {
			csprTransactionRecords, err = data.CsprTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(csprTransactionRecords, &transactionRecordList)
		}
	case POLKADOT:
		var dotTransactionRecords []*data.DotTransactionRecord
		if err == nil {
			dotTransactionRecords, err = data.DotTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(dotTransactionRecords, &transactionRecordList)
		}*/
	case SOLANA:
		var solTransactionRecords []*data.SolTransactionRecord
		if err == nil {
			solTransactionRecords, err = data.SolTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(solTransactionRecords, &transactionRecordList)
		}
	case STC:
		var stcTransactionRecords []*data.StcTransactionRecord
		if err == nil {
			stcTransactionRecords, err = data.StcTransactionRecordRepoClient.List(ctx, tableName, req)
		}
		if err == nil {
			err = utils.CopyProperties(stcTransactionRecords, &transactionRecordList)
		}
		/*case TVM:
			var trxTransactionRecords []*data.TrxTransactionRecord
			if err == nil {
				trxTransactionRecords, err = data.TrxTransactionRecordRepoClient.List(ctx, tableName, req)
			}
			if err == nil {
				err = utils.CopyProperties(trxTransactionRecords, &transactionRecordList)
			}
		case KASPA:
			var kasTransactionRecords []*data.KasTransactionRecord
			if err == nil {
				kasTransactionRecords, err = data.KasTransactionRecordRepoClient.List(ctx, tableName, req)
			}
			if err == nil {
				err = utils.CopyProperties(kasTransactionRecords, &transactionRecordList)
			}*/
	}
	return transactionRecordList, nil
}

func (r *TransactionRecordRepoImpl) DeleteByID(ctx context.Context, chainName string, id int64) (int64, error) {
	tableName := GetTableName(chainName)
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&TransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, chainName string, blockNumber int) (int64, error) {
	tableName := GetTableName(chainName)
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number >= ?", blockNumber).Delete(&TransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TransactionRecordRepoImpl) Delete(ctx context.Context, chainName string, req *data.TransactionRequest) (int64, error) {
	tableName := GetTableName(chainName)
	var affected int64
	var err error
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	/*case BTC:
	if err == nil {
		affected, err = data.BtcTransactionRecordRepoClient.Delete(ctx, tableName, req)
	}*/
	case EVM:
		if err == nil {
			affected, err = data.EvmTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case APTOS:
		if err == nil {
			affected, err = data.AptTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	/*case SUI:
		if err == nil {
			affected, err = data.SuiTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case COSMOS:
		if err == nil {
			affected, err = data.AtomTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case NERVOS:
		if err == nil {
			affected, err = data.CkbTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case CASPER:
		if err == nil {
			affected, err = data.CsprTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case POLKADOT:
		if err == nil {
			affected, err = data.DotTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}*/
	case SOLANA:
		if err == nil {
			affected, err = data.SolTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case STC:
		if err == nil {
			affected, err = data.StcTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
	case TVM:
		if err == nil {
			affected, err = data.TrxTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}
		/*case KASPA:
		if err == nil {
			affected, err = data.KasTransactionRecordRepoClient.Delete(ctx, tableName, req)
		}*/
	}
	return affected, nil
}

func (r *TransactionRecordRepoImpl) FindLast(ctx context.Context, chainName string) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number is not null").Order("block_number desc").Limit(1).Find(&transactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return transactionRecord, nil
		}
	}
}

func (r *TransactionRecordRepoImpl) FindLastBlockNumberByAddress(ctx context.Context, chainName string, address string) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName)
	if address != "" {
		db = db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Where("block_number is not null").Order("block_number desc").Limit(1).Find(&transactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return transactionRecord, nil
		}
	}
}

func (r *TransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, chainName string, blockNumber int) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("block_number = ?", blockNumber).Limit(1).Find(&transactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one "+tableName+" by blockNumber failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return transactionRecord, nil
		}
	}
}

func (r *TransactionRecordRepoImpl) FindByTxHash(ctx context.Context, chainName string, txHash string) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Find(&transactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" by txHash failed", err)
		return nil, err
	}
	if transactionRecord.Id != 0 {
		return transactionRecord, nil
	} else {
		return nil, nil
	}
}

func (r *TransactionRecordRepoImpl) SelectColumnByTxHash(ctx context.Context, chainName string, txHash string, selectColumn []string) (*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecord *TransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Select(selectColumn).Find(&transactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" for column by txHash failed", err)
		return nil, err
	}
	return transactionRecord, nil
}

func (r *TransactionRecordRepoImpl) ListIncompleteNft(ctx context.Context, chainName string, req *data.TransactionRequest) ([]*TransactionRecord, error) {
	tableName := GetTableName(chainName)
	var transactionRecords []*TransactionRecord
	chainType, _ := GetChainNameType(chainName)

	switch chainType {
	case EVM:
		transactionRecordList, err := data.EvmTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}
	case APTOS:
		transactionRecordList, err := data.AptTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}
	case SUI:
		transactionRecordList, err := data.SuiTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}
	case COSMOS:
		transactionRecordList, err := data.AtomTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}
	case SOLANA:
		transactionRecordList, err := data.SolTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}
		/*case TVM:
		transactionRecordList, err := data.TrxTransactionRecordRepoClient.ListIncompleteNft(ctx, tableName, req)
		if err == nil {
			err = utils.CopyProperties(transactionRecordList, &transactionRecords)
		}*/
	}
	return transactionRecords, nil
}
