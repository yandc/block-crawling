package data

import (
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"strings"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TonTransactionRecord struct {
	DefaultVersionMarkerIn

	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(80);default:null;index:,unique"`
	MessageHash     string          `json:"messageHash" form:"messageHash" gorm:"type:character varying(80);index:"`
	AccountTxHash   string          `json:"accountTxHash" form:"accountTxHash" gorm:"type:character varying(80);index:"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(64);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(64);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(20);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(256);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	GasLimit        string          `json:"gasLimit" form:"gasLimit" gorm:"type:character varying(30)"`
	GasUsed         string          `json:"gasUsed" form:"gasUsed" gorm:"type:character varying(10)"`
	GasPrice        string          `json:"gasPrice" form:"gasPrice" gorm:"type:character varying(20)"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	EventLog        string          `json:"eventLog" form:"eventLog"`
	TokenInfo       string          `json:"tokenInfo" form:"tokenInfo"`
	SendTime        int64           `json:"sendTime" form:"sendTime"`
	SessionId       string          `json:"sessionId" form:"sessionId" gorm:"type:character varying(36);default:null;index:,unique"`
	ShortHost       string          `json:"shortHost" form:"shortHost" gorm:"type:character varying(200);default:null;"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

type TonTransactionRecordRepo interface {
	FindLast(ctx context.Context, tableName string) (*TonTransactionRecord, error)
	FindByStatus(ctx context.Context, tableName string, statuses []string) ([]*TonTransactionRecord, error)
	BatchSaveOrUpdateSelective(ctx context.Context, tableName string, records []*TonTransactionRecord) (int64, error)
	BatchSaveOrIgnore(ctx context.Context, tableName string, records []*TonTransactionRecord) (int64, error)
	SelectColumnByTxHash(ctx context.Context, tableName string, txHash string, columns []string) (*TonTransactionRecord, error)
	FindByTxHash(ctx context.Context, tableName string, txHash string) (*TonTransactionRecord, error)
	SaveOrUpdateClient(ctx context.Context, tableName string, record *TonTransactionRecord) (int64, error)
	PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*TonTransactionRecord, int64, error)
	PendingByAddress(ctx context.Context, tableName string, address string) ([]*TonTransactionRecord, error)
}

var TonTransactionRecordClient TonTransactionRecordRepo

type tonTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

// BatchSaveOrIgnore implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, tableName string, records []*TonTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		DoNothing: true,
	}).Create(&records)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or ignore "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

// BatchSaveOrUpdateSelective implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, records []*TonTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"block_hash":       clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":     clause.Column{Table: "excluded", Name: "block_number"},
			"account_tx_hash":  clause.Column{Table: "excluded", Name: "account_tx_hash"},
			"message_hash":     clause.Column{Table: "excluded", Name: "message_hash"},
			"transaction_hash": clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":     clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":       gorm.Expr("case when '" + tableName + "' = 'ronin_transaction_record' and " + tableName + ".to_address like 'ronin:%' and excluded.to_address != '' then 'ronin:' || substring (excluded.to_address, 3) else excluded.to_address end"),
			"from_uid":         clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":           clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":       clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":           clause.Column{Table: "excluded", Name: "amount"},
			"status":           gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') or (" + tableName + ".status in('success', 'fail') and excluded.status = 'dropped_replaced') then " + tableName + ".status else excluded.status end"),
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address": clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":       gorm.Expr("case when excluded.status = 'success' or excluded.parse_data not like '%\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}' or " + tableName + ".parse_data = '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"gas_limit":        clause.Column{Table: "excluded", Name: "gas_limit"},
			"gas_used":         clause.Column{Table: "excluded", Name: "gas_used"},
			"gas_price":        clause.Column{Table: "excluded", Name: "gas_price"},
			"event_log":        clause.Column{Table: "excluded", Name: "event_log"},
			"transaction_type": gorm.Expr("case when " + tableName + ".transaction_type in('mint', 'swap', 'addLiquidity') and excluded.transaction_type not in('mint', 'swap', 'addLiquidity') then " + tableName + ".transaction_type else excluded.transaction_type end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"token_info":       gorm.Expr("case when excluded.status = 'success' or excluded.token_info != '{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}' or " + tableName + ".token_info = '' then excluded.token_info else " + tableName + ".token_info end"),
			"send_time":        gorm.Expr("case when excluded.send_time != 0 then excluded.send_time else " + tableName + ".send_time end"),
			"session_id":       gorm.Expr("case when excluded.session_id != '' then excluded.session_id else " + tableName + ".session_id end"),
			"short_host":       gorm.Expr("case when excluded.short_host != '' then excluded.short_host else " + tableName + ".short_host end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&records)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

// FindByStatus implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, statuses []string) ([]*TonTransactionRecord, error) {
	var tonTransactionRecordList []*TonTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", statuses).Find(&tonTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return tonTransactionRecordList, nil
}

// FindByTxHash implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) FindByTxHash(ctx context.Context, tableName string, txHash string) (*TonTransactionRecord, error) {
	var tonTransactionRecord *TonTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Find(&tonTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" failed", err)
		return nil, err
	}
	return tonTransactionRecord, nil
}

// FindLast implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*TonTransactionRecord, error) {
	var tonTransactionRecord *TonTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("BLOCK_NUMBER IS NOT NULL").Order("BLOCK_NUMBER DESC").Limit(1).Find(&tonTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last "+tableName+" failed", err)
		return nil, err
	} else {
		if ret.RowsAffected == 0 {
			return nil, nil
		} else {
			return tonTransactionRecord, nil
		}
	}
}

// PageList implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*TonTransactionRecord, int64, error) {
	var tonTransactionRecordList []*TonTransactionRecord
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid, req.Uid)
	}
	if req.FromAddress != "" {
		db = db.Where("from_address = ?", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("to_address = ?", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		db = db.Where("from_address in (?)", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		db = db.Where("to_address in (?)", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("from_uid = ? OR to_uid = ?", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("from_address = ? OR to_address = ?", req.Address, req.Address)
	}
	if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}
	if len(req.ContractAddressList) > 0 {
		db = db.Where("contract_address in(?)", req.ContractAddressList)
	}
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	if len(req.StatusNotInList) > 0 {
		db = db.Where("status not in(?)", req.StatusNotInList)
	}
	if req.TransactionType != "" {
		db = db.Where("transaction_type = ?", req.TransactionType)
	}
	if req.TransactionTypeNotEquals != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEquals)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
	}
	if req.OperateTypeEmpty {
		db = db.Where("(operate_type is null or operate_type = '')")
	}
	if req.TransactionHash != "" {
		db = db.Where("transaction_hash = ?", req.TransactionHash)
	}
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
	}
	if len(req.TransactionHashNotInList) > 0 {
		db = db.Where("transaction_hash not in(?)", req.TransactionHashNotInList)
	}
	if req.TransactionHashLike != "" {
		db = db.Where("transaction_hash like ?", req.TransactionHashLike+"%")
	}
	if req.DappDataEmpty {
		db = db.Where("(dapp_data is null or dapp_data = '')")
	}
	if req.ClientDataNotEmpty {
		db = db.Where("client_data is not null and client_data != ''")
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
	}
	if req.TokenAddress != "" && req.TokenAddress != MAIN_ADDRESS_PARAM {
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint', 'addLiquidity') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint', 'addLiquidity') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'addLiquidity', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint', 'addLiquidity') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint', 'addLiquidity') and event_log like '%\"token_type\":\"%'))")
		}
	}

	for _, req := range req.OrParamList {
		orSql := ""
		if req.TransactionHash != "" {
			orSql += " or transaction_hash = '" + req.TransactionHash + "'"
		}
		if len(req.TransactionHashList) > 0 {
			transactionHashList := strings.ReplaceAll(utils.ListToString(req.TransactionHashList), "\"", "'")
			orSql += " or transaction_hash in(" + transactionHashList + ")"
		}
		if len(req.TransactionHashNotInList) > 0 {
			transactionHashNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionHashNotInList), "\"", "'")
			orSql += " or transaction_hash not in(" + transactionHashNotInList + ")"
		}
		if req.TransactionHashLike != "" {
			orSql += " or transaction_hash like '" + req.TransactionHashLike + "%'"
		}
		if len(req.OriginalHashList) > 0 {
			originalHashList := strings.ReplaceAll(utils.ListToString(req.OriginalHashList), "\"", "'")
			orSql += " or original_hash in(" + originalHashList + ")"
		}
		if orSql != "" {
			orSql = "(" + orSql[4:len(orSql)] + ")"
			db = db.Where(orSql)
		}
	}

	if req.Total {
		// 统计总记录数
		db.Count(&total)
	}

	if req.DataDirection > 0 {
		dataDirection := ">"
		if req.DataDirection == 1 {
			dataDirection = "<"
		}
		if req.OrderBy == "" {
			db = db.Where("id "+dataDirection+" ?", req.StartIndex)
		} else {
			orderBys := strings.Split(req.OrderBy, " ")
			db = db.Where(orderBys[0]+" "+dataDirection+" ?", req.StartIndex)
		}
	}

	db = db.Order(req.OrderBy)

	if req.DataDirection == 0 {
		if req.PageNum > 0 {
			db = db.Offset(int(req.PageNum-1) * int(req.PageSize))
		} else {
			db = db.Offset(0)
		}
	}
	db = db.Limit(int(req.PageSize))

	ret := db.Find(&tonTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query "+tableName+" failed", err)
		return nil, 0, err
	}
	return tonTransactionRecordList, total, nil
}

// PendingByAddress implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*TonTransactionRecord, error) {
	var tonTransactionRecordList []*TonTransactionRecord
	db := r.gormDB.Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&tonTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query tonTransactionRecord failed", err)
		return nil, err
	}
	return tonTransactionRecordList, nil
}

// SaveOrUpdateClient implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) SaveOrUpdateClient(ctx context.Context, tableName string, record *TonTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"dapp_data":   gorm.Expr("excluded.dapp_data"),
			"client_data": gorm.Expr("excluded.client_data"),
			"send_time":   gorm.Expr("excluded.send_time"),
			"session_id":  gorm.Expr("excluded.session_id"),
			"short_host":  gorm.Expr("excluded.short_host"),
			"updated_at":  gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&record)
	err := ret.Error
	if err != nil {
		log.Errore("更新ton "+tableName+" failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, err
}

// SelectColumnByTxHash implements TonTransactionRecordRepo
func (r *tonTransactionRecordRepoImpl) SelectColumnByTxHash(ctx context.Context, tableName string, txHash string, columns []string) (*TonTransactionRecord, error) {
	var tonTransactionRecord *TonTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txHash).Select(columns).Find(&tonTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query "+tableName+" for column by txHash failed", err)
		return nil, err
	}
	return tonTransactionRecord, nil
}

// NewTonTransactionRecordRepo new a TonTransactionRecord repo.
func NewTonTransactionRecordRepo(gormDB *gorm.DB) TonTransactionRecordRepo {
	TonTransactionRecordClient = &tonTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return TonTransactionRecordClient
}
