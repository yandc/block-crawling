package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
)

type DappApproveRecord struct {
	Id         int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid        string `json:"uid" form:"uid"  gorm:"type:character varying(66)"`
	LastTxhash string `json:"lastTxhash" form:"lastTxhash" gorm:"type:character varying(80);"`
	Decimals   int64  `json:"decimals" form:"decimals"`
	ChainName  string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index"`
	Address    string `json:"address" form:"address" gorm:"type:character varying(42);index:,unique,composite:unique_dapp_address"`
	Token      string `json:"token" form:"token" gorm:"type:character varying(42);index:,unique,composite:unique_dapp_address"`         //代币合约地址
	ToAddress  string `json:"toAddress" form:"toAddress" gorm:"type:character varying(42);index:,unique,composite:unique_dapp_address"` //dapp 地址
	Amount     string `json:"amount" form:"amount" sql:"type:text;"`
	Original   string `json:"original" form:"original" sql:"type:text;"`
	Symbol     string `json:"symbol" form:"symbol" gorm:"type:character varying(32);"`
	TxTime     int64  `json:"txTime" form:"txTime"`
}

func (dappApproveRecord DappApproveRecord) TableName() string {
	return "dapp_approve_record"
}

type DappApproveRecordRepo interface {
	SaveOrUpdate(context.Context, *DappApproveRecord) (int64, error)
	UpdateAmout(context.Context, []*DappApproveRecord)
	ListByCondition(ctx context.Context, req *pb.DappListReq) ([]*DappApproveRecord, error)
	GetAmountList(ctx context.Context, req *pb.OpenAmountReq) ([]*DappApproveRecord, error)
	GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) ([]*DappApproveRecord, error)
	GetDappListPageCount(ctx context.Context, req *pb.DappPageListReq) int64
	GetDappListByToken(ctx context.Context, req *pb.DappPageListReq) ([]*DappApproveRecord, error)
}

type DappApproveRecordRepoImpl struct {
	gormDB *gorm.DB
}

var DappApproveRecordRepoClient DappApproveRecordRepo

func NewDappApproveRecordRepo(gormDB *gorm.DB) DappApproveRecordRepo {
	DappApproveRecordRepoClient = &DappApproveRecordRepoImpl{
		gormDB: gormDB,
	}
	return DappApproveRecordRepoClient
}

func (r *DappApproveRecordRepoImpl) SaveOrUpdate(ctx context.Context, dappApproveRecord *DappApproveRecord) (int64, error) {
	ret := r.gormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}, {Name: "token"}, {Name: "to_address"}},
		UpdateAll: true,
	}).Create(&dappApproveRecord)
	err := ret.Error
	if err != nil {
		log.Errore("dapp insert or update failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *DappApproveRecordRepoImpl) UpdateAmout(ctx context.Context, dappApproveRecords []*DappApproveRecord) {
	for _, dar := range dappApproveRecords {
		var dappApproveRecord *DappApproveRecord
		r.gormDB.Where(" address = ? and token = ? and to_address = ? ", dar.Address, dar.Token, dar.ToAddress).Find(&dappApproveRecord)
		if dappApproveRecord.Amount == "" || len(dappApproveRecord.Amount) > 40 {
			continue
		}
		//dappApproveRecord.Amount
		oldDecimal, err := decimal.NewFromString(dappApproveRecord.Amount)
		consumeDecimal, err1 := decimal.NewFromString(dar.Amount)
		if err != nil || err1 != nil {
			log.Errore("dapp 敞口金额转换出错", err)
		}
		newAmount := oldDecimal.Sub(consumeDecimal)
		dappApproveRecord.Amount = newAmount.String()
		dappApproveRecord.LastTxhash = dar.LastTxhash
		r.gormDB.Updates(dappApproveRecord)
	}
}

func (r *DappApproveRecordRepoImpl) ListByCondition(ctx context.Context, req *pb.DappListReq) ([]*DappApproveRecord, error) {
	var dars []*DappApproveRecord
	tx := r.gormDB
	if req.Uid != "" {
		tx = tx.Where("uid = ?", req.Uid)
	}
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if len(req.Addresses) > 0 {
		tx = tx.Where("address in (?)", req.Addresses)
	}
	if req.ContractAddress != "" {
		tx = tx.Where("to_address = ?", req.ContractAddress)
	}
	if req.IsCancelStatus == "" {
		if !req.IsCancel {
			tx = tx.Where("amount != '0' and amount !='' ")
		}
	}
	if req.IsCancelStatus == "1" {
		tx = tx.Where("amount != '0' and amount !='' ")
	} else if req.IsCancelStatus == "2" {
		tx = tx.Where("amount = '0' and amount !='' ")
	} else if req.IsCancelStatus == "0" {
		tx = tx.Where("amount !='' ")
	}

	ret := tx.Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("page query evmTransactionRecord failed", err)
		return nil, err
	}
	return dars, nil
}
func (r *DappApproveRecordRepoImpl) GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) ([]*DappApproveRecord, error) {
	var dars []*DappApproveRecord
	tx := r.gormDB.Where("amount != '0' ")
	if req.ContractAddress != "" {
		tx = tx.Where("token = ?", req.ContractAddress)
	}
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if req.FromAddress != "" {
		tx = tx.Where("address = ?", req.FromAddress)
	}
	if req.Fromuid != "" {
		tx = tx.Where("uid = ?", req.Fromuid)
	}
	if req.DataDirection > 0 {
		dataDirection := ">"
		if req.DataDirection == 1 {
			dataDirection = "<"
		}
		if req.OrderBy != "" {
			orderBys := strings.Split(req.OrderBy, " ")
			tx = tx.Where(orderBys[0]+" "+dataDirection+" ?", req.StartIndex)
		}
	}
	if req.DataDirection == 0 {
		tx = tx.Offset(int(req.Page-1) * int(req.Limit))
		tx = tx.Limit(int(req.Limit))
	}
	ret := tx.Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("page query dappRecord failed", err)
		return nil, err
	}
	return dars, nil
}
func (r *DappApproveRecordRepoImpl) GetDappListPageCount(ctx context.Context, req *pb.DappPageListReq) int64 {
	var count int64
	tx := r.gormDB.Model(&DappApproveRecord{}).Where("amount != '0' ")
	if req.ContractAddress != "" {
		tx = tx.Where("token = ?", req.ContractAddress)
	}
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if req.FromAddress != "" {
		tx = tx.Where("address = ?", req.FromAddress)
	}
	if req.Fromuid != "" {
		tx = tx.Where("uid = ?", req.Fromuid)
	}
	tx.Count(&count)
	return count
}

func (r *DappApproveRecordRepoImpl) GetDappListByToken(ctx context.Context, req *pb.DappPageListReq) ([]*DappApproveRecord, error) {
	var dars []*DappApproveRecord
	tx := r.gormDB.Where("amount != '0' ")
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	ret := tx.Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("page query evmTransactionRecord failed", err)
		return nil, err
	}
	return dars, nil
}

func (r *DappApproveRecordRepoImpl) GetAmountList(ctx context.Context, req *pb.OpenAmountReq) ([]*DappApproveRecord, error) {

	var dars []*DappApproveRecord
	tx := r.gormDB
	if req.Uid != "" {
		tx = tx.Where("amount != '0' and amount !='' and uid = ?", req.Uid)
	}
	if len(req.ChainName) > 0 {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if len(req.Address) > 0 {
		tx = tx.Where("address = ?", req.Address)
	}
	if req.ContractAddress != "" {
		tx = tx.Where("token = ?", req.ContractAddress)
	}
	ret := tx.Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("page query dappRecord failed", err)
		return nil, err
	}
	return dars, nil
}