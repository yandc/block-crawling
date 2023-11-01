package data

import (
	pb "block-crawling/api/bfstation/v1"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BSTxType string

const (
	BSTxTypeMint            BSTxType = "mint"
	BSTxRedeem              BSTxType = "redeem"
	BSTxTypeSwap            BSTxType = "swap"
	BSTxTypeAddLiquidity    BSTxType = "add"
	BSTxTypeRemoveLiquidity BSTxType = "remove"
)

type BFCStationRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(71)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	WalletAddress   string          `json:"walletAddress" form:"walletAddress" gorm:"type:character varying(71);index"`
	Type            BSTxType        `json:"type" form:"type" gorm:"type:character varying(20);index"`
	Vault           string          `json:"vault" form:"vault" gorm:"type:character varying(1024);index"`
	TokenAmountIn   decimal.Decimal `json:"tokenAmountIn" form:"tokenAmountIn" sql:"type:decimal(128,0);"`
	TokenAmountOut  decimal.Decimal `json:"tokenAmountOut" form:"tokenAmountOut" sql:"type:decimal(128,0);"`
	CoinTypeIn      string          `json:"coinTypeIn" form:"coinTypeIn" gorm:"type:character varying(1024)"`
	CoinTypeOut     string          `json:"coinTypeOut" form:"coinTypeOut" gorm:"type:character varying(1024)"`
	ParsedJson      string          `json:"parsedJson" form:"parsedJson"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

func GetBFCStationTable(chainName string) string {
	return fmt.Sprintf("%s_bfc_station_txns", strings.ToLower(chainName))
}

type BFCStationRepo interface {
	BatchSave(ctx context.Context, chainName string, records []BFCStationRecord) error
	PageList(ctx context.Context, chainName string, req *pb.PageListTxnsRequest) ([]BFCStationRecord, int64, error)
}

type bfcStationRepoImpl struct {
	db *gorm.DB
}

// PageList implements BFCStationRepo
func (r *bfcStationRepoImpl) PageList(ctx context.Context, chainName string, req *pb.PageListTxnsRequest) ([]BFCStationRecord, int64, error) {
	tableName := GetBFCStationTable(chainName)
	var records []BFCStationRecord
	var total int64
	db := r.db.WithContext(ctx).Table(tableName)

	if req.WalletAddress != "" {
		db = db.Where("wallet_address = ?", req.WalletAddress)
	}
	if req.Type != "" {
		db = db.Where("type = ?", req.Type)
	}

	// 统计总记录数
	db.Count(&total)

	db = db.Order("id desc")
	db = db.Offset(int(req.PageNum-1) * int(req.PageSize))
	db = db.Limit(int(req.PageSize))

	ret := db.Find(&records)
	err := ret.Error
	if err != nil {
		log.Errore("page query suiTransactionRecord failed", err)
		return nil, 0, err
	}
	return records, total, nil
}

// BatchSave implements BFCStationRepo
func (r *bfcStationRepoImpl) BatchSave(ctx context.Context, chainName string, records []BFCStationRecord) error {
	tableName := GetBFCStationTable(chainName)

	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":       clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":     clause.Column{Table: "excluded", Name: "block_number"},
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"wallet_address":   clause.Column{Table: "excluded", Name: "wallet_address"},
			"type":             clause.Column{Table: "excluded", Name: "type"},
			"vault":            clause.Column{Table: "excluded", Name: "vault"},
			"token_amount_in":  clause.Column{Table: "excluded", Name: "token_amount_in"},
			"token_amount_out": clause.Column{Table: "excluded", Name: "token_amount_out"},
			"coin_type_in":     clause.Column{Table: "excluded", Name: "coin_type_in"},
			"coin_type_out":    clause.Column{Table: "excluded", Name: "coin_type_out"},
			"parsed_json":      clause.Column{Table: "excluded", Name: "parsed_json"},
			"status":           clause.Column{Table: "excluded", Name: "status"},
			"updated_at":       clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective bfcStationRecord failed", err)
		return err
	}

	return err
}

var BFCStationRepoIns BFCStationRepo

func NewBFCStationRepo(db *gorm.DB) BFCStationRepo {
	BFCStationRepoIns = &bfcStationRepoImpl{
		db: db,
	}
	return BFCStationRepoIns
}
