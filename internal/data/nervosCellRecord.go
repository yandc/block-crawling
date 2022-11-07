package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"fmt"

	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NervosCellRecord struct {
	Id                 int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid                string `json:"uid" form:"uid"  gorm:"type:character varying(66);index:,unique,composite:unique_uid_hash_n"` //联合索引
	Capacity           string `json:"capacity" form:"capacity" gorm:"type:character varying(66)"`
	Index              int    `json:"index" form:"index" gorm:"type:int ;index:,unique,composite:unique_uid_hash_n"` //联合索引
	TransactionHash    string `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique,composite:unique_uid_hash_n"`
	UseTransactionHash string `json:"useTransactionHash" form:"useTransactionHash" gorm:"type:character varying(80);index"`
	LockCodeHash       string `json:"lockCodeHash" form:"lockCodeHash"`
	LockHashType       string `json:"lockHashType" form:"lockHashType"`
	LockArgs           string `json:"lockArgs" form:"lockArgs"`
	Address            string `json:"address" form:"address" gorm:"type:character varying(516);index"`
	ContractAddress    string `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(256);index"`
	TypeCodeHash       string `json:"typeCodeHash" form:"typeCodeHash"`
	TypeHashType       string `json:"typeHashType" form:"typeHashType"`
	TypeArgs           string `json:"typeArgs" form:"typeArgs"`
	Data               string `json:"data" form:"data"`
	Status             string `json:"status" form:"status" gorm:"type:character varying(1);index"` // 1 未花费 2 已花费  3 全部状态 4 pending
	CreatedAt          int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt          int64  `json:"updatedAt" form:"updatedAt"`
}

type NervosCellRecordRepoImpl struct {
	gormDB *gorm.DB
}

type NervosCellRecordRepo interface {
	SaveOrUpdate(context.Context, *NervosCellRecord) (int64, error)
	FindByCondition(context.Context, *pb.UnspentReq) ([]*NervosCellRecord, error)
	UpdateStatusByUseTransactionHash(context.Context, string, string) (int64, error)
	Save(context.Context, *NervosCellRecord) (int64, error)
	LoadBatch(ctx context.Context, txHashs []string) ([]*NervosCellRecord, error)
}

var NervosCellRecordRepoClient NervosCellRecordRepo

func NewNervosCellRecordRepo(gormDB *gorm.DB) NervosCellRecordRepo {
	NervosCellRecordRepoClient = &NervosCellRecordRepoImpl{
		gormDB: gormDB,
	}
	return NervosCellRecordRepoClient
}

func (r *NervosCellRecordRepoImpl) LoadBatch(ctx context.Context, txHashs []string) ([]*NervosCellRecord, error) {
	var results []*NervosCellRecord
	ret := r.gormDB.WithContext(ctx).Where("transaction_hash in (?)", txHashs).Find(&results)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return results, nil
}

func (r *NervosCellRecordRepoImpl) SaveOrUpdate(ctx context.Context, utxoUnspentRecord *NervosCellRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}, {Name: "transaction_hash"}, {Name: "index"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"status", "updated_at", "use_transaction_hash"}),
	}).Create(&utxoUnspentRecord)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update utxo failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err

}

func (r *NervosCellRecordRepoImpl) FindByCondition(ctx context.Context, req *pb.UnspentReq) ([]*NervosCellRecord, error) {
	var ncr []*NervosCellRecord
	tx := r.gormDB
	if req.IsUnspent != "3" {
		tx = tx.Where("status = ?", req.IsUnspent)
	}
	if req.Uid != "" {
		tx = tx.Where("uid = ?", req.Uid)
	}
	if req.Address != "" {
		tx = tx.Where("address = ?", req.Address)
	}
	if req.TxHash != "" {
		tx = tx.Where("transaction_hash = ?", req.TxHash)
	}
	if req.Address != "" {
		tx = tx.Where("address = ?", req.Address)
	}
	tx = tx.Where("contract_address = ?",req.ContractAddress)

	ret := tx.Find(&ncr)
	err := ret.Error
	if err != nil {
		log.Errore("query "+req.ChainName+" utxoTransactionRecord failed", err)
		return nil, err
	}
	return ncr, nil

}

func (r *NervosCellRecordRepoImpl) UpdateStatusByUseTransactionHash(ctx context.Context, uth string, status string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Where("use_transaction_hash = ?", uth).Update("status", status)
	err := ret.Error
	if err != nil {
		log.Errore("update cell failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *NervosCellRecordRepoImpl) Save(ctx context.Context, nervosCellRecord *NervosCellRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(nervosCellRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			return 1, err
		} else {
			log.Errore("insert nervosCellRecord failed", err)
		}
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}
