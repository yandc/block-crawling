package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	UtxoStatusUnSpend     = 1
	UtxoStatusSpent       = 2
	UtxoStatusAll         = 3
	UtxoStatusPending     = 4
	UtxoStatusCancelSpend = 5
)

type UtxoUnspentRecord struct {
	Id          int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName   string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_address_hash_n"` //联合索引
	Uid         string `json:"uid" form:"uid"  gorm:"type:character varying(66)"`
	Address     string `json:"address" form:"address" gorm:"type:character varying(72);index:,unique,composite:unique_chain_name_address_hash_n"` //联合索引
	Hash        string `json:"hash" form:"hash" gorm:"type:character varying(80);index:,unique,composite:unique_chain_name_address_hash_n"`
	N           int    `json:"n" form:"n" gorm:"type:int ;index:,unique,composite:unique_chain_name_address_hash_n"`
	Script      string `json:"script" form:"script" gorm:"type:character varying(300)"`
	Unspent     int32  `json:"unspent" form:"unspent" gorm:"type:bigint"` //1 未花费 2 已花费 联合索引 3 所有 4 pending 5 取消花费
	Amount      string `json:"amount" form:"amount" sql:"type:text"`
	TxTime      int64  `json:"txTime" form:"txTime"`
	SpentTxHash string `json:"spentTxHash" form:"spentTxHash"`
	CreatedAt   int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt   int64  `json:"updatedAt" form:"updatedAt"`
}

type UserUtxo struct {
	ChainName                   string
	Uid                         string
	UidList                     []string
	Address                     string
	AddressList                 []string
	Unspent                     int32
	UnspentNotEqual             int32
	ChainNameAddressUnspentList []*UserUtxo
	StartTime                   int64
	StopTime                    int64
	OrderBy                     string
	DataDirection               int32
	StartIndex                  int64
	PageNum                     int32
	PageSize                    int32
	Total                       bool
}

func (utxoUnspentRecord UtxoUnspentRecord) TableName() string {
	return "utxo_unspent_record"
}

type UtxoUnspentRecordRepo interface {
	SaveOrUpdate(context.Context, *UtxoUnspentRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, []*UtxoUnspentRecord) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*UtxoUnspentRecord, int) (int64, error)
	UpdateUnspentToPending(context.Context, string, string, int, string, string) (int64, error)
	FindByCondition(context.Context, *pb.UnspentReq) ([]*UtxoUnspentRecord, error)
	FindBySpentTxHash(ctx context.Context, hash string) ([]*UtxoUnspentRecord, error)
	DeleteByAddressWithNotPending(context.Context, string, string) (int64, error)
	DeleteByAddress(ctx context.Context, chainName string, address string) error
	Delete(context.Context, *UserUtxo) (int64, error)
	FindAddressGroup(ctx context.Context) ([]string, error)
	UpdateUidByAddress(context.Context, string, string) (int64, error)
	FindNotPendingByTxHash(ctx context.Context, chainName, address, spendTxHash string) ([]*UtxoUnspentRecord, error)
	Sum(ctx context.Context, chainName, address string) (decimal.Decimal, error)
}

type UtxoUnspentRecordRepoImpl struct {
	gormDB *gorm.DB
}

// Sum implements UtxoUnspentRecordRepo
func (r *UtxoUnspentRecordRepoImpl) Sum(ctx context.Context, chainName string, address string) (decimal.Decimal, error) {
	total := decimal.Zero
	err := r.gormDB.WithContext(ctx).Model(&UtxoUnspentRecord{}).Select("COALESCE(SUM(amount::DECIMAL), 0) AS t").Where("chain_name = ? AND address = ? AND unspent IN (1, 4)", chainName, address).Row().Scan(&total)
	return total, err
}

var UtxoUnspentRecordRepoClient UtxoUnspentRecordRepo

func NewUtxoUnspentRecordRepo(gormDB *gorm.DB) UtxoUnspentRecordRepo {
	UtxoUnspentRecordRepoClient = &UtxoUnspentRecordRepoImpl{
		gormDB: gormDB,
	}
	return UtxoUnspentRecordRepoClient
}

func (r *UtxoUnspentRecordRepoImpl) SaveOrUpdate(ctx context.Context, utxoUnspentRecord *UtxoUnspentRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "hash"}, {Name: "n"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"unspent", "updated_at"}),
	}).Create(&utxoUnspentRecord)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update userUTXO failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UtxoUnspentRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, utxoUnspentRecords []*UtxoUnspentRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "address"}, {Name: "hash"}, {Name: "n"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"unspent", "spent_tx_hash", "updated_at"}),
	}).Create(&utxoUnspentRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update userUTXO failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UtxoUnspentRecordRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, utxoUnspentRecords []*UtxoUnspentRecord, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(utxoUnspentRecords)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subUtxoUnspentRecordss := utxoUnspentRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subUtxoUnspentRecordss)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *UtxoUnspentRecordRepoImpl) FindByCondition(ctx context.Context, req *pb.UnspentReq) ([]*UtxoUnspentRecord, error) {
	var utxos []*UtxoUnspentRecord
	tx := r.gormDB

	if req.IsUnspent != strconv.Itoa(UtxoStatusAll) {
		unspent, err := strconv.Atoi(req.IsUnspent)
		if err == nil {
			tx = tx.Where("unspent = ?", unspent)
		}
	}

	//if req.Uid != "" {
	//	tx = tx.Where("uid = ?", req.Uid)
	//}
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if req.Address != "" {
		tx = tx.Where("address = ?", req.Address)
	}
	if req.TxHash != "" {
		tx = tx.Where("hash = ?", req.TxHash)
	}

	ret := tx.Find(&utxos)
	err := ret.Error
	if err != nil {
		log.Errore("page query utxoTransactionRecord failed", err)
		return nil, err
	}
	return utxos, nil
}

func (r *UtxoUnspentRecordRepoImpl) FindBySpentTxHash(ctx context.Context, hash string) ([]*UtxoUnspentRecord, error) {
	var utxos []*UtxoUnspentRecord

	ret := r.gormDB.Where("spent_tx_hash = ?", hash).Find(&utxos)
	err := ret.Error
	if err != nil {
		log.Errore("query utxoTransactionRecord by spent tx hash failed", err)
		return nil, err
	}
	return utxos, nil
}

func (r *UtxoUnspentRecordRepoImpl) FindNotPendingByTxHash(ctx context.Context, chainName, address, spendTxHash string) ([]*UtxoUnspentRecord, error) {
	var utxos []*UtxoUnspentRecord
	ret := r.gormDB.Where("chain_name = ? and address = ? and spent_tx_hash != ? and unspent = ?", chainName, address, spendTxHash, UtxoStatusPending).Find(&utxos)
	err := ret.Error
	if err != nil {
		log.Errore("query FindNotPendingByTxHash by spent tx hash failed", err)
		return nil, err
	}
	return utxos, nil
}

func (r *UtxoUnspentRecordRepoImpl) DeleteByAddressWithNotPending(ctx context.Context, chainName string, address string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Where("chain_name = ? and address = ? and unspent != ?", chainName, address, UtxoStatusPending).Delete(&UtxoUnspentRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+address+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UtxoUnspentRecordRepoImpl) DeleteByAddress(ctx context.Context, chainName string, address string) error {
	ret := r.gormDB.WithContext(ctx).Where("chain_name = ? and address = ?", chainName, address).Delete(&UtxoUnspentRecord{})
	if ret.Error != nil {
		log.Errore("delete "+address+" failed", ret.Error)
		return ret.Error
	}
	return nil
}

func (r *UtxoUnspentRecordRepoImpl) Delete(ctx context.Context, req *UserUtxo) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table("utxo_unspent_record")

	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Uid != "" {
		db = db.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if req.Unspent != 0 {
		db = db.Where("unspent = ?", req.Unspent)
	}
	if req.UnspentNotEqual != 0 {
		db = db.Where("unspent != ?", req.UnspentNotEqual)
	}
	if len(req.ChainNameAddressUnspentList) > 0 {
		chainNameAddressUnspent := "("
		for _, record := range req.ChainNameAddressUnspentList {
			chainNameAddressUnspent += "('" + record.ChainName + "','" + record.Address + "'," + strconv.Itoa(int(record.Unspent)) + "),"
		}
		chainNameAddressUnspent = chainNameAddressUnspent[:len(chainNameAddressUnspent)-1]
		chainNameAddressUnspent += ")"
		db = db.Where("(chain_name, address, unspent) in" + chainNameAddressUnspent)
	}

	ret := db.Delete(&UtxoUnspentRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete userUTXO failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *UtxoUnspentRecordRepoImpl) UpdateUnspentToPending(ctx context.Context, chainName string, address string, n int, txHash string, spentTxHash string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).
		Table("utxo_unspent_record").
		Where(" chain_name = ? and address = ? and n = ? and hash = ?", chainName, address, n, txHash).
		Update("unspent", UtxoStatusPending).
		Update("spent_tx_hash", spentTxHash).
		Update("updated_at", time.Now().Unix())
	err := ret.Error
	if err != nil {
		log.Errore("update "+address+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *UtxoUnspentRecordRepoImpl) FindAddressGroup(ctx context.Context) ([]string, error) {
	var ncr []string
	ret := r.gormDB.Table("utxo_unspent_record").Select("address").Where("address != '' ").Group("address").Find(&ncr)
	err := ret.Error
	if err != nil {
		return nil, err
	}
	return ncr, nil

}

func (r *UtxoUnspentRecordRepoImpl) UpdateUidByAddress(ctx context.Context, address string, uid string) (int64, error) {
	ret := r.gormDB.Table("utxo_unspent_record").Where("address = ?", address).Update("uid", uid)
	err := ret.Error
	if err != nil {
		log.Errore("update cell failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
