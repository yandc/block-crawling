package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UtxoUnspentRecord struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid       string `json:"uid" form:"uid"  gorm:"type:character varying(66);index:,unique,composite:unique_uid_hash_n"` //联合索引
	Hash      string `json:"hash" form:"hash" gorm:"type:character varying(80);index:,unique,composite:unique_uid_hash_n"`
	N         int    `json:"n" form:"n" gorm:"type:int ;index:,unique,composite:unique_uid_hash_n"`
	ChainName string `json:"chainName" form:"chainName" gorm:"type:character varying(20)"` //联合索引
	Address   string `json:"address" form:"address" gorm:"type:character varying(64)"`     //联合索引
	Script    string `json:"script" form:"script" gorm:"type:character varying(300)"`
	Unspent   int32  `json:"unspent" form:"unspent" gorm:"type:bigint"` //1 未花费 2 已花费 联合索引
	Amount    string `json:"amount" form:"amount" sql:"type:text"`
	TxTime    int64  `json:"txTime" form:"txTime"`
	CreatedAt int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt int64  `json:"updatedAt" form:"updatedAt"`
}

func (utxoUnspentRecord UtxoUnspentRecord) TableName() string {
	return "utxo_unspent_record"
}

type UtxoUnspentRecordRepo interface {
	SaveOrUpdate(context.Context, *UtxoUnspentRecord) (int64, error)
	FindByCondition(context.Context, *pb.UnspentReq) ([]*UtxoUnspentRecord, error)
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
		Columns:   []clause.Column{{Name: "uid"}, {Name: "hash"}, {Name: "n"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"unspent", "updated_at"}),
	}).Create(&utxoUnspentRecord)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update utxo failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *UtxoUnspentRecordRepoImpl) FindByCondition(ctx context.Context, req *pb.UnspentReq) ([]*UtxoUnspentRecord, error) {

	var utxos []*UtxoUnspentRecord
	tx := r.gormDB

	if req.IsUnspent != "3" {
		tx = tx.Where("unspent = ?", req.IsUnspent)
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
