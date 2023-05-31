package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
)

type UtxoUnspentRecord struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_address_hash_n"` //联合索引
	Uid       string `json:"uid" form:"uid"  gorm:"type:character varying(66)"`
	Address   string `json:"address" form:"address" gorm:"type:character varying(72);index:,unique,composite:unique_chain_name_address_hash_n"` //联合索引
	Hash      string `json:"hash" form:"hash" gorm:"type:character varying(80);index:,unique,composite:unique_chain_name_address_hash_n"`
	N         int    `json:"n" form:"n" gorm:"type:int ;index:,unique,composite:unique_chain_name_address_hash_n"`
	Script    string `json:"script" form:"script" gorm:"type:character varying(300)"`
	Unspent   int32  `json:"unspent" form:"unspent" gorm:"type:bigint"` //1 未花费 2 已花费 联合索引 3 所有 4 pending 5 取消花费
	Amount    string `json:"amount" form:"amount" sql:"type:text"`
	TxTime    int64  `json:"txTime" form:"txTime"`
	CreatedAt int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt int64  `json:"updatedAt" form:"updatedAt"`
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
	UpdateUnspent(context.Context, string, string, string, int, string) (int64, error)
	FindByCondition(context.Context, *pb.UnspentReq) ([]*UtxoUnspentRecord, error)
	DeleteByUid(context.Context, string, string, string) (int64, error)
	Delete(context.Context, *UserUtxo) (int64, error)
	FindAddressGroup(ctx context.Context) ([]string, error)
	UpdateUidByAddress(context.Context, string, string) (int64, error)
}

type UtxoUnspentRecordRepoImpl struct {
	gormDB *gorm.DB
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
		DoUpdates: clause.AssignmentColumns([]string{"unspent", "updated_at"}),
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

	if req.IsUnspent != "3" {
		unspent, err := strconv.Atoi(req.IsUnspent)
		if err == nil {
			tx = tx.Where("unspent = ?", unspent)
		}
	}

	if req.Uid != "" {
		tx = tx.Where("uid = ?", req.Uid)
	}
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

func (r *UtxoUnspentRecordRepoImpl) DeleteByUid(ctx context.Context, uid string, chainName string, address string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Where("uid = ? and chain_name = ? and address = ? and unspent != ?", uid, chainName, address, "4").Delete(&UtxoUnspentRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete "+address+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
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

func (r *UtxoUnspentRecordRepoImpl) UpdateUnspent(ctx context.Context, uid string, chainName string, address string, n int, txhash string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table("utxo_unspent_record").Where("uid = ? and chain_name = ? and address = ? and n = ? and hash = ?", uid, chainName, address, n, txhash).Update("unspent", 4)
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
