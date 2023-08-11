package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/log"
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NftRecordHistory struct {
	Id              int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName       string `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_chain_name_from_address_to_address_contract_address_token_id"`
	BlockNumber     int    `json:"blockNumber" form:"blockNumber"`
	EventType       string `json:"eventType" form:"eventType" gorm:"type:character varying(128)"`
	TransactionHash string `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null"`
	FromAddress     string `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(66);index:,unique,composite:unique_chain_name_from_address_to_address_contract_address_token_id"`
	ToAddress       string `json:"toAddress" form:"toAddress" gorm:"type:character varying(66);index:,unique,composite:unique_chain_name_from_address_to_address_contract_address_token_id"`
	FromUid         string `json:"fromUid" form:"fromUid" gorm:"type:character varying(36)"`
	ToUid           string `json:"toUid" form:"toUid" gorm:"type:character varying(36)"`
	TxTime          string `json:"txTime" form:"txTime"`
	Quantity        string `json:"quantity" form:"quantity"`
	ContractAddress string `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_chain_name_from_address_to_address_contract_address_token_id"`
	TokenId         string `json:"tokenId" form:"tokenId" gorm:"type:character varying(128);index:,unique,composite:unique_chain_name_from_address_to_address_contract_address_token_id"`
	CreatedAt       int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt       int64  `json:"updatedAt" form:"updatedAt"`
}

func (nftRecordHistory NftRecordHistory) TableName() string {
	return "nft_record_history"
}

type NftRecordHistoryRepo interface {
	ListByCondition(context.Context, *pb.NftRecordReq) ([]*NftRecordHistory, error)
	SaveOrUpdate(context.Context, []*NftRecordHistory) (int64, error)
	FindAddressGroup(ctx context.Context, column string) ([]string, error)
	UpdateUidByAddress(context.Context, string, string, string, string) (int64, error)
}

type NftRecordHistoryRepoImpl struct {
	gormDB *gorm.DB
}

var NftRecordHistoryRepoClient NftRecordHistoryRepo

func NewNftRecordHistoryRepo(gormDB *gorm.DB) NftRecordHistoryRepo {
	NftRecordHistoryRepoClient = &NftRecordHistoryRepoImpl{
		gormDB: gormDB,
	}
	return NftRecordHistoryRepoClient
}

func (r *NftRecordHistoryRepoImpl) ListByCondition(ctx context.Context, req *pb.NftRecordReq) ([]*NftRecordHistory, error) {
	var dars []*NftRecordHistory
	tx := r.gormDB
	if req.ChainName != "" {
		tx = tx.Where("chain_name = ?", req.ChainName)
	}
	if req.ContractAddress != "" {
		tx = tx.Where("contract_address = ? ", req.ContractAddress)
	}
	if req.TokenId != "" {
		tx = tx.Where("token_id = ?", req.TokenId)
	}

	ret := tx.Order("tx_time DESC").Find(&dars)
	err := ret.Error
	if err != nil {
		log.Errore("query nft history failed", err)
		return nil, err
	}
	return dars, nil
}
func (r *NftRecordHistoryRepoImpl) SaveOrUpdate(ctx context.Context, nftRecordHistorys []*NftRecordHistory) (int64, error) {
	ret := r.gormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "contract_address"}, {Name: "token_id"}},
		UpdateAll: true,
	}).Create(&nftRecordHistorys)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update nftHistory failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}
func (r *NftRecordHistoryRepoImpl) FindAddressGroup(ctx context.Context, column string) ([]string, error) {
	var ncr []string
	ret := r.gormDB.Table("nft_record_history").Select(column).Where(column + " != '' ").Group(column).Find(&ncr)
	err := ret.Error
	if err != nil {
		return nil, err
	}
	return ncr, nil

}

func (r *NftRecordHistoryRepoImpl) UpdateUidByAddress(ctx context.Context, address string, addressColumn string, uid string, uidColumn string) (int64, error) {
	ret := r.gormDB.Table("nft_record_history").Where(addressColumn+" = ?", address).Update(uidColumn, uid)
	err := ret.Error
	if err != nil {
		log.Errore("update cell failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}
