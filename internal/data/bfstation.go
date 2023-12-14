package data

import (
	pb "block-crawling/api/bfstation/v1"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"strings"
	"time"

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
	BSTxTypeCollectFee      BSTxType = "collect"
)

const (
	bfStationTableRecord   = "bfc_station_txns"
	bfStationTableToken    = "bfc_station_tokens"
	bfStationTablePool     = "bfc_station_pools"
	bfStationTableAccToken = "bfc_station_account_tokens"
	bfStationTableAccPool  = "bfc_station_account_pools"
	bfStationTableCollFee  = "bfc_station_collect_fees"
)

const (
	bfStationSouceUser = "station"
	bfStationSourceMgt = "mgt"
)

type BFCStationRecord struct {
	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(71)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	TxTime          int64           `json:"txTime" form:"txTime" gorm:"type:bigint;index"`
	WalletAddress   string          `json:"walletAddress" form:"walletAddress" gorm:"type:character varying(71);index"`
	WalletUID       string          `json:"walletUid" form:"walletUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	GasLimit        string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed         string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	Type            BSTxType        `json:"type" form:"type" gorm:"type:character varying(20);index"`
	PoolID          string          `json:"poolId" form:"poolId" gorm:"type:character varying(1024);index"`
	Vault           string          `json:"vault" form:"vault" gorm:"type:character varying(1024);index"`
	TokenAmountIn   decimal.Decimal `json:"tokenAmountIn" form:"tokenAmountIn" sql:"type:decimal(128,0);"`
	TokenAmountOut  decimal.Decimal `json:"tokenAmountOut" form:"tokenAmountOut" sql:"type:decimal(128,0);"`
	CoinTypeIn      string          `json:"coinTypeIn" form:"coinTypeIn" gorm:"type:character varying(1024)"`
	CoinTypeOut     string          `json:"coinTypeOut" form:"coinTypeOut" gorm:"type:character varying(1024)"`
	CoinInfoIn      string          `json:"coinInfoIn" form:"coinInfoIn"`
	CoinInfoOut     string          `json:"coinInfoOut" form:"coinInfoOut"`
	ParsedJson      string          `json:"parsedJson" form:"parsedJson"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

type BFStationToken struct {
	Id          int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	CoinType    string `json:"coinType" form:"coinType" gorm:"type:character varying(1024);index:,unique"`
	MintTxHash  string `json:"mintTxHash" form:"mintTxHash" gorm:"type:character varying(80);default:null;index"`
	MintAddress string `json:"mintAddress" form:"mintAddress" gorm:"type:character varying(512);"`
	MintedAt    int64  `json:"mintedAt" form:"mintedAt"`
	CreatedAt   int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt   int64  `json:"updatedAt" form:"updatedAt"`
}

type BFStationPool struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ObjectID  string `json:"objectId" form:"objectId" gorm:"type:character varying(1024);index:,unique"`
	TxHash    string `json:"txHash" form:"txHash" gorm:"type:character varying(80);default:null;index"`
	CoinTypeA string `json:"coinTypeA" form:"coinTypeA" gorm:"type:character varying(1024)"`
	CoinTypeB string `json:"coinTypeB" form:"coinTypeB" gorm:"type:character varying(1024)"`
	CreatedAt int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt int64  `json:"updatedAt" form:"updatedAt"`
}

type BFStationAccountToken struct {
	Id                int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid               string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address           string          `json:"address" form:"address" gorm:"type:character varying(512);index:,unique,composite:unique_address_coin_type"`
	TokenCoinType     string          `json:"tokenCoinType" form:"tokenCoinType" gorm:"type:character varying(1024);index:,unique,composite:unique_address_coin_type"`
	Balance           decimal.Decimal `json:"balance" form:"balance" sql:"type:decimal(128,0);index"`
	BalanceChangeHash string          `json:"balanceChangeHash" form:"balanceChangeHash" gorm:"type:character varying(80);default:null;index"`
	FirstBlockNumber  uint64          `json:"firstBlockNumber" form:"firstBlockNumber"`
	LastBlockNumber   uint64          `json:"lastBlockNumber" form:"lastBlockNumber"`
	FirstTxHash       string          `json:"firstTxHash" form:"firstTxHash" gorm:"type:character varying(80);default:null;index"`
	LastTxHash        string          `json:"lastTxHash" form:"lastTxHash" gorm:"type:character varying(80);default:null;index"`
	CreatedAt         int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt         int64           `json:"updatedAt" form:"updatedAt"`
}

type BFStationAccountPool struct {
	Id                int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid               string `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address           string `json:"address" form:"address" gorm:"type:character varying(512);"`
	Position          string `json:"position" form:"position" gorm:"type:character varying(1024);index:,unique"`
	PoolObjectID      string `json:"poolObjectId" form:"poolObjectId" gorm:"type:character varying(1024);index:,composite:index_pool_object_id_type"`
	ObjectChangeType  string `json:"objectChangeType" form:"objectChangeType" gorm:"type:character varying(100);index,composite:index_pool_object_id_type"`
	ObjectChangeHash  string `json:"objectChangeHash" form:"objectChangeHash" gorm:"type:character varying(80);default:null"`
	ObjectChangeBlock uint64 `json:"objectChangeBLock" form:"objectChangeBlock"`
	CreatedAt         int64  `json:"createdAt" form:"createdAt"`
	UpdatedAt         int64  `json:"updatedAt" form:"updatedAt"`
}

type BFStationCollectFee struct {
	Id        int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	TxHash    string          `json:"txHash" form:"txHash" gorm:"type:character varying(80);index:,unique"`
	PoolID    string          `json:"poolId" form:"poolId" gorm:"type:character varying(1024);index"`
	Uid       string          `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	Address   string          `json:"address" form:"address" gorm:"type:character varying(512);"`
	CoinTypeA string          `json:"coinTypeA" form:"coinTypeA" gorm:"type:character varying(1024)"`
	CoinTypeB string          `json:"coinTypeB" form:"coinTypeB" gorm:"type:character varying(1024)"`
	AmountA   decimal.Decimal `json:"amountA" form:"amountA" sql:"type:decimal(128,0)"`
	AmountB   decimal.Decimal `json:"amountB" form:"amountB" sql:"type:decimal(128,0)"`
	TxTime    int64           `json:"txTime" form:"txTime" gorm:"type:bigint;index"`
	Position  string          `json:"position" form:"position" gorm:"type:character varying(1024);"`
	CreatedAt int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt int64           `json:"updatedAt" form:"updatedAt"`
}

func GetBFCStationTable(chainName string, optTables ...string) string {
	table := bfStationTableRecord
	if len(optTables) > 0 {
		table = optTables[0]
	}
	return fmt.Sprintf("%s_%s", strings.ToLower(chainName), table)
}

type BFCStationRepo interface {
	BatchSave(ctx context.Context, chainName string, records []BFCStationRecord) error
	PageList(ctx context.Context, chainName string, req *pb.PageListTxnsRequest) ([]BFCStationRecord, int64, error)
	LoadTimeoutRecords(ctx context.Context, chainName string, timeout time.Duration) ([]BFCStationRecord, error)
	BatchSaveTokens(ctx context.Context, chainName string, records []BFStationToken) error
	BatchSavePools(ctx context.Context, chainName string, records []BFStationPool) error
	GetPool(ctx context.Context, chainName string, poolID string) (*BFStationPool, error)
	ListAllAccountTokens(ctx context.Context, chainName string) ([]BFStationAccountToken, error)
	BatchSaveAccountTokens(ctx context.Context, chainName string, records []BFStationAccountToken) error
	SaveAccountToken(ctx context.Context, chainName string, record *BFStationAccountToken) error
	BatchSaveAccountPools(ctx context.Context, chainName string, records []BFStationAccountPool) error
	CountTokenHolders(ctx context.Context, chainName string, coinType string) (int64, error)
	CountPoolHolders(ctx context.Context, chainName string, poolID string) (int64, error)
	BatchSaveCollectFees(ctx context.Context, chainName string, records []BFStationCollectFee) error
	PageListCollectFees(ctx context.Context, chainName string, req *pb.PageListFeesRequest) ([]BFStationCollectFee, int64, error)
}

type bfcStationRepoImpl struct {
	db *gorm.DB
}

func (r *bfcStationRepoImpl) PageListCollectFees(ctx context.Context, chainName string, req *pb.PageListFeesRequest) ([]BFStationCollectFee, int64, error) {
	tableName := GetBFCStationTable(chainName, bfStationTableCollFee)
	var total int64
	var records []BFStationCollectFee
	db := r.db.WithContext(ctx).Table(tableName)
	if req.PoolId != "" {
		db = db.Where("pool_id = ?", req.PoolId)
	}

	if req.StartTime > 0 {
		db = db.Where("tx_time >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("tx_time <= ?", req.StopTime)
	}

	db = db.Order("id desc")
	if req.PageSize > 0 {
		// 统计总记录数
		db.Count(&total)

		db = db.Offset(int(req.PageNum-1) * int(req.PageSize))
		db = db.Limit(int(req.PageSize))
	}

	ret := db.Find(&records)
	err := ret.Error
	if err != nil {
		return nil, 0, err
	}
	if req.PageSize <= 0 {
		total = int64(len(records))
	}
	return records, total, nil
}

func (r *bfcStationRepoImpl) BatchSaveCollectFees(ctx context.Context, chainName string, records []BFStationCollectFee) error {
	tableName := GetBFCStationTable(chainName, bfStationTableCollFee)
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"address":     clause.Column{Table: "excluded", Name: "address"},
			"uid":         clause.Column{Table: "excluded", Name: "uid"},
			"position":    clause.Column{Table: "excluded", Name: "position"},
			"pool_id":     clause.Column{Table: "excluded", Name: "pool_id"},
			"amount_a":    clause.Column{Table: "excluded", Name: "amount_a"},
			"amount_b":    clause.Column{Table: "excluded", Name: "amount_b"},
			"coin_type_a": clause.Column{Table: "excluded", Name: "coin_type_a"},
			"coin_type_b": clause.Column{Table: "excluded", Name: "coin_type_b"},
			"updated_at":  clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	return ret.Error
}

// CountPoolHolders implements BFCStationRepo
func (r *bfcStationRepoImpl) CountPoolHolders(ctx context.Context, chainName string, poolID string) (int64, error) {
	var total int64
	ret := r.db.WithContext(ctx).Table(
		GetBFCStationTable(chainName, bfStationTableAccPool),
	).Where("pool_object_id=? and (object_change_type='created' OR object_change_type='mutated')", poolID).Count(&total)
	return total, ret.Error
}

// CountTokenHolders implements BFCStationRepo
func (r *bfcStationRepoImpl) CountTokenHolders(ctx context.Context, chainName string, coinType string) (int64, error) {
	var total int64
	ret := r.db.WithContext(ctx).Table(
		GetBFCStationTable(chainName, bfStationTableAccToken),
	).Where("token_coin_type=? and balance::decimal > 0", coinType).Count(&total)
	return total, ret.Error
}

// BatchSavePools implements BFCStationRepo
func (r *bfcStationRepoImpl) BatchSavePools(ctx context.Context, chainName string, records []BFStationPool) error {
	tableName := GetBFCStationTable(chainName, bfStationTablePool)
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "object_id"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"updated_at": clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		return err
	}

	return err
}

func (r *bfcStationRepoImpl) GetPool(ctx context.Context, chainName string, poolID string) (*BFStationPool, error) {
	tableName := GetBFCStationTable(chainName, bfStationTablePool)
	var result *BFStationPool
	ret := r.db.WithContext(ctx).Table(tableName).Where("object_id=?", poolID).Take(&result)
	return result, ret.Error
}

// BatchSaveTokens implements BFCStationRepo
func (r *bfcStationRepoImpl) BatchSaveTokens(ctx context.Context, chainName string, records []BFStationToken) error {
	tableName := GetBFCStationTable(chainName, bfStationTableToken)
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "coin_type"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"coin_type":  clause.Column{Table: "excluded", Name: "coin_type"},
			"updated_at": clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		return err
	}

	return err
}

// BatchSaveAccountPools implements BFCStationRepo
func (r *bfcStationRepoImpl) BatchSaveAccountPools(ctx context.Context, chainName string, records []BFStationAccountPool) error {
	tableName := GetBFCStationTable(chainName, bfStationTableAccPool)
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "position"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"uid":                 clause.Column{Table: "excluded", Name: "uid"},
			"object_change_type":  clause.Column{Table: "excluded", Name: "object_change_type"},
			"object_change_hash":  clause.Column{Table: "excluded", Name: "object_change_hash"},
			"object_change_block": clause.Column{Table: "excluded", Name: "object_change_block"},
			"updated_at":          clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		return err
	}

	return err
}

func (r *bfcStationRepoImpl) ListAllAccountTokens(ctx context.Context, chainName string) ([]BFStationAccountToken, error) {
	tableName := GetBFCStationTable(chainName, bfStationTableAccToken)
	var records []BFStationAccountToken
	ret := r.db.WithContext(ctx).Table(tableName).Find(&records)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return records, nil
}
func (r *bfcStationRepoImpl) SaveAccountToken(ctx context.Context, chainName string, record *BFStationAccountToken) error {
	tableName := GetBFCStationTable(chainName, bfStationTableAccToken)
	ret := r.db.WithContext(ctx).Table(tableName).Save(record)
	return ret.Error
}

// BatchSaveAccountTokens implements BFCStationRepo
func (r *bfcStationRepoImpl) BatchSaveAccountTokens(ctx context.Context, chainName string, records []BFStationAccountToken) error {
	tableName := GetBFCStationTable(chainName, bfStationTableAccToken)
	ret := r.db.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}, {Name: "token_coin_type"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"uid":                 clause.Column{Table: "excluded", Name: "uid"},
			"last_tx_hash":        clause.Column{Table: "excluded", Name: "last_tx_hash"},
			"last_block_number":   clause.Column{Table: "excluded", Name: "last_block_number"},
			"balance":             clause.Column{Table: "excluded", Name: "balance"},
			"balance_change_hash": clause.Column{Table: "excluded", Name: "balance_change_hash"},
			"updated_at":          clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		return err
	}

	return err
}

// LoadTimeoutRecords implements BFCStationRepo
func (r *bfcStationRepoImpl) LoadTimeoutRecords(ctx context.Context, chainName string, timeout time.Duration) ([]BFCStationRecord, error) {
	since := time.Now().Unix() - int64(timeout.Seconds())
	tableName := GetBFCStationTable(chainName)
	var records []BFCStationRecord
	ret := r.db.WithContext(ctx).Table(tableName).Where("tx_time < ? and status = ?", since, "pending").Find(&records)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return records, nil
}

// PageList implements BFCStationRepo
func (r *bfcStationRepoImpl) PageList(ctx context.Context, chainName string, req *pb.PageListTxnsRequest) ([]BFCStationRecord, int64, error) {
	tableName := GetBFCStationTable(chainName)
	var records []BFCStationRecord
	var total int64
	db := r.db.WithContext(ctx).Table(tableName)

	if req.WalletAddress != "" {
		db = db.Where("wallet_address = ?", utils.UnifyBFCAddress(chainName, req.WalletAddress))
	}
	if req.Type != "" {
		db = db.Where("type = ?", req.Type)
	}
	if req.Source != bfStationSourceMgt {
		db = db.Where("type != ?", BSTxTypeCollectFee)
	}
	if req.WalletAddressOrUid != "" {
		db = db.Where("(wallet_uid = ? OR wallet_address = ?)", req.WalletAddressOrUid, req.WalletAddressOrUid)
	}
	if req.FromOBWallet {
		db = db.Where("(wallet_uid != ''")

	}
	if req.Status != "" {
		db = db.Where("status = ?", req.Status)
	}
	if req.StartTime > 0 {
		db = db.Where("tx_time >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("tx_time <= ?", req.StopTime)
	}

	// 统计总记录数
	db.Count(&total)

	db = db.Order("id desc")
	db = db.Offset(int(req.PageNum-1) * int(req.PageSize))
	db = db.Limit(int(req.PageSize))

	ret := db.Find(&records)
	err := ret.Error
	if err != nil {
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
			"wallet_uid":       clause.Column{Table: "excluded", Name: "wallet_uid"},
			"fee_amount":       clause.Column{Table: "excluded", Name: "fee_amount"},
			"gas_limit":        clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":         clause.Column{Table: "excluded", Name: "gas_used"},
			"type":             clause.Column{Table: "excluded", Name: "type"},
			"vault":            clause.Column{Table: "excluded", Name: "vault"},
			"token_amount_in":  clause.Column{Table: "excluded", Name: "token_amount_in"},
			"token_amount_out": clause.Column{Table: "excluded", Name: "token_amount_out"},
			"coin_type_in":     clause.Column{Table: "excluded", Name: "coin_type_in"},
			"coin_type_out":    clause.Column{Table: "excluded", Name: "coin_type_out"},
			"coin_info_in":     clause.Column{Table: "excluded", Name: "coin_info_in"},
			"coin_info_out":    clause.Column{Table: "excluded", Name: "coin_info_out"},
			"parsed_json":      clause.Column{Table: "excluded", Name: "parsed_json"},
			"status":           clause.Column{Table: "excluded", Name: "status"},
			"updated_at":       clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
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

func AutoMigrateBFStationTables(db *gorm.DB, chainName string) {
	db.Table(GetBFCStationTable(chainName)).AutoMigrate(&BFCStationRecord{})

	db.Table(GetBFCStationTable(chainName, bfStationTableToken)).AutoMigrate(&BFStationToken{})
	db.Table(GetBFCStationTable(chainName, bfStationTablePool)).AutoMigrate(&BFStationPool{})

	db.Table(GetBFCStationTable(chainName, bfStationTableAccToken)).AutoMigrate(&BFStationAccountToken{})
	db.Table(GetBFCStationTable(chainName, bfStationTableAccPool)).AutoMigrate(&BFStationAccountPool{})

	db.Table(GetBFCStationTable(chainName, bfStationTableCollFee)).AutoMigrate(&BFStationCollectFee{})
}
