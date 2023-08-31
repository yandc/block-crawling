package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/datatypes"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SolTransactionRecord struct {
	DefaultVersionMarkerIn

	Id              int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	SlotNumber      int             `json:"slotNumber" form:"slotNumber"`
	BlockHash       string          `json:"blockHash" form:"blockHash" gorm:"type:character varying(66)"`
	BlockNumber     int             `json:"blockNumber" form:"blockNumber"`
	TransactionHash string          `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(98);default:null;index:,unique"`
	FromAddress     string          `json:"fromAddress" form:"fromAddress" gorm:"type:character varying(66);index"`
	ToAddress       string          `json:"toAddress" form:"toAddress" gorm:"type:character varying(66);index"`
	FromUid         string          `json:"fromUid" form:"fromUid" gorm:"type:character varying(36);index"`
	ToUid           string          `json:"toUid" form:"toUid" gorm:"type:character varying(36);index"`
	FeeAmount       decimal.Decimal `json:"feeAmount" form:"feeAmount" sql:"type:decimal(128,0);"`
	Amount          decimal.Decimal `json:"amount" form:"amount" sql:"type:decimal(128,0);"`
	Status          string          `json:"status" form:"status" gorm:"type:character varying(12);index"`
	TxTime          int64           `json:"txTime" form:"txTime"`
	ContractAddress string          `json:"contractAddress" form:"contractAddress" gorm:"type:character varying(1024);index"`
	ParseData       string          `json:"parseData" form:"parseData"`
	Data            string          `json:"data" form:"data"`
	EventLog        string          `json:"eventLog" form:"eventLog"`
	LogAddress      datatypes.JSON  `json:"logAddress" form:"logAddress" gorm:"type:jsonb"`
	TransactionType string          `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	DappData        string          `json:"dappData" form:"dappData"`
	ClientData      string          `json:"clientData" form:"clientData"`
	CreatedAt       int64           `json:"createdAt" form:"createdAt" gorm:"type:bigint;index"`
	UpdatedAt       int64           `json:"updatedAt" form:"updatedAt"`
}

// SolTransactionRecordRepo is a Greater repo.
type SolTransactionRecordRepo interface {
	OutTxCounter

	Save(context.Context, string, *SolTransactionRecord) (int64, error)
	BatchSave(context.Context, string, []*SolTransactionRecord) (int64, error)
	BatchSaveOrUpdate(context.Context, string, []*SolTransactionRecord) (int64, error)
	BatchSaveOrIgnore(context.Context, string, []*SolTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelective(context.Context, string, []*SolTransactionRecord) (int64, error)
	BatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*SolTransactionRecord) (int64, error)
	PageBatchSaveOrUpdateSelectiveByColumns(context.Context, string, []string, []*SolTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveById(context.Context, string, []*SolTransactionRecord, int) (int64, error)
	PageBatchSaveOrUpdateSelectiveByTransactionHash(context.Context, string, []*SolTransactionRecord, int) (int64, error)
	Update(context.Context, string, *SolTransactionRecord) (int64, error)
	FindByID(context.Context, string, int64) (*SolTransactionRecord, error)
	FindByStatus(context.Context, string, string, string) ([]*SolTransactionRecord, error)
	ListByID(context.Context, string, int64) ([]*SolTransactionRecord, error)
	ListAll(context.Context, string) ([]*SolTransactionRecord, error)
	PageList(context.Context, string, *TransactionRequest) ([]*SolTransactionRecord, int64, error)
	PendingByAddress(context.Context, string, string) ([]*SolTransactionRecord, error)
	List(context.Context, string, *TransactionRequest) ([]*SolTransactionRecord, error)
	DeleteByID(context.Context, string, int64) (int64, error)
	DeleteByBlockNumber(context.Context, string, int) (int64, error)
	Delete(context.Context, string, *TransactionRequest) (int64, error)
	FindLast(context.Context, string) (*SolTransactionRecord, error)
	FindOneByBlockNumber(context.Context, string, int) (*SolTransactionRecord, error)
	GetAmount(context.Context, string, *pb.AmountRequest, string) (string, error)
	FindByTxhash(context.Context, string, string) (*SolTransactionRecord, error)
	ListIncompleteNft(context.Context, string, *TransactionRequest) ([]*SolTransactionRecord, error)
}

type SolTransactionRecordRepoImpl struct {
	gormDB *gorm.DB
}

var SolTransactionRecordRepoClient SolTransactionRecordRepo

// NewSolTransactionRecordRepo new a SolTransactionRecord repo.
func NewSolTransactionRecordRepo(gormDB *gorm.DB) SolTransactionRecordRepo {
	SolTransactionRecordRepoClient = &SolTransactionRecordRepoImpl{
		gormDB: gormDB,
	}
	return SolTransactionRecordRepoClient
}

func (r *SolTransactionRecordRepoImpl) Save(ctx context.Context, tableName string, solTransactionRecord *SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Create(solTransactionRecord)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(solTransactionRecord.Id, 10), Data: 0}
			log.Warne("insert solTransactionRecord failed", err)
		} else {
			log.Errore("insert solTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) BatchSave(ctx context.Context, tableName string, solTransactionRecords []*SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).CreateInBatches(solTransactionRecords, len(solTransactionRecords))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(solTransactionRecords)), Data: 0}
			log.Warne("insert solTransactionRecord failed", err)
		} else {
			log.Errore("insert solTransactionRecord failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) BatchSaveOrUpdate(ctx context.Context, tableName string, solTransactionRecords []*SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: true,
	}).Create(&solTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("insert solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) BatchSaveOrIgnore(ctx context.Context, tableName string, solTransactionRecords []*SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		DoNothing: true,
	}).Create(&solTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or ignore "+tableName+" failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) BatchSaveOrUpdateSelective(ctx context.Context, tableName string, solTransactionRecords []*SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"slot_number":      clause.Column{Table: "excluded", Name: "slot_number"},
			"block_hash":       clause.Column{Table: "excluded", Name: "block_hash"},
			"block_number":     clause.Column{Table: "excluded", Name: "block_number"},
			"transaction_hash": clause.Column{Table: "excluded", Name: "transaction_hash"},
			"from_address":     clause.Column{Table: "excluded", Name: "from_address"},
			"to_address":       clause.Column{Table: "excluded", Name: "to_address"},
			"from_uid":         clause.Column{Table: "excluded", Name: "from_uid"},
			"to_uid":           clause.Column{Table: "excluded", Name: "to_uid"},
			"fee_amount":       clause.Column{Table: "excluded", Name: "fee_amount"},
			"amount":           gorm.Expr("case when excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":           gorm.Expr("case when (" + tableName + ".status in('success', 'fail', 'dropped_replaced', 'dropped') and excluded.status = 'no_status') or (" + tableName + ".status in('success', 'fail', 'dropped_replaced') and excluded.status = 'dropped') or (" + tableName + ".status in('success', 'fail') and excluded.status = 'dropped_replaced') then " + tableName + ".status else excluded.status end"),
			"tx_time":          clause.Column{Table: "excluded", Name: "tx_time"},
			"contract_address": clause.Column{Table: "excluded", Name: "contract_address"},
			"parse_data":       gorm.Expr("case when excluded.status = 'success' or excluded.parse_data not like '%\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}' or " + tableName + ".parse_data = '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"data":             clause.Column{Table: "excluded", Name: "data"},
			"event_log":        clause.Column{Table: "excluded", Name: "event_log"},
			"log_address":      clause.Column{Table: "excluded", Name: "log_address"},
			"transaction_type": gorm.Expr("case when " + tableName + ".transaction_type in('mint', 'swap') and excluded.transaction_type not in('mint', 'swap') then " + tableName + ".transaction_type else excluded.transaction_type end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&solTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) BatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, solTransactionRecords []*SolTransactionRecord) (int64, error) {
	var columnList []clause.Column
	for _, column := range columns {
		columnList = append(columnList, clause.Column{Name: column})
	}
	ret := r.gormDB.WithContext(ctx).Table(tableName).Clauses(clause.OnConflict{
		Columns:   columnList,
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"slot_number":      gorm.Expr("case when excluded.slot_number != 0 then excluded.slot_number else " + tableName + ".slot_number end"),
			"block_hash":       gorm.Expr("case when excluded.block_hash != '' then excluded.block_hash else " + tableName + ".block_hash end"),
			"block_number":     gorm.Expr("case when excluded.block_number != 0 then excluded.block_number else " + tableName + ".block_number end"),
			"transaction_hash": gorm.Expr("case when excluded.transaction_hash != '' then excluded.transaction_hash else " + tableName + ".transaction_hash end"),
			"from_address":     gorm.Expr("case when excluded.from_address != '' then excluded.from_address else " + tableName + ".from_address end"),
			"to_address":       gorm.Expr("case when excluded.to_address != '' then excluded.to_address else " + tableName + ".to_address end"),
			"from_uid":         gorm.Expr("case when excluded.from_uid != '' then excluded.from_uid else " + tableName + ".from_uid end"),
			"to_uid":           gorm.Expr("case when excluded.to_uid != '' then excluded.to_uid else " + tableName + ".to_uid end"),
			"fee_amount":       gorm.Expr("case when excluded.fee_amount != '' and excluded.fee_amount != '0' then excluded.fee_amount else " + tableName + ".fee_amount end"),
			"amount":           gorm.Expr("case when excluded.amount != '' and excluded.amount != '0' then excluded.amount else " + tableName + ".amount end"),
			"status":           gorm.Expr("case when excluded.status != '' then excluded.status else " + tableName + ".status end"),
			"tx_time":          gorm.Expr("case when excluded.tx_time != 0 then excluded.tx_time else " + tableName + ".tx_time end"),
			"contract_address": gorm.Expr("case when excluded.contract_address != '' then excluded.contract_address else " + tableName + ".contract_address end"),
			"parse_data":       gorm.Expr("case when excluded.parse_data != '' then excluded.parse_data else " + tableName + ".parse_data end"),
			"data":             gorm.Expr("case when excluded.data != '' then excluded.data else " + tableName + ".data end"),
			"event_log":        gorm.Expr("case when excluded.event_log != '' then excluded.event_log else " + tableName + ".event_log end"),
			"log_address":      gorm.Expr("case when excluded.log_address is not null then excluded.log_address else " + tableName + ".log_address end"),
			"transaction_type": gorm.Expr("case when excluded.transaction_type != '' then excluded.transaction_type else " + tableName + ".transaction_type end"),
			"dapp_data":        gorm.Expr("case when excluded.dapp_data != '' then excluded.dapp_data else " + tableName + ".dapp_data end"),
			"client_data":      gorm.Expr("case when excluded.client_data != '' then excluded.client_data else " + tableName + ".client_data end"),
			"updated_at":       gorm.Expr("excluded.updated_at"),
		}),
	}).Create(&solTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update selective solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *SolTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByColumns(ctx context.Context, tableName string, columns []string, txRecords []*SolTransactionRecord, pageSize int) (int64, error) {
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

		affected, err := r.BatchSaveOrUpdateSelectiveByColumns(ctx, tableName, columns, subTxRecords)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *SolTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveById(ctx context.Context, tableName string, txRecords []*SolTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"id"}, txRecords, pageSize)
}

func (r *SolTransactionRecordRepoImpl) PageBatchSaveOrUpdateSelectiveByTransactionHash(ctx context.Context, tableName string, txRecords []*SolTransactionRecord, pageSize int) (int64, error) {
	return r.PageBatchSaveOrUpdateSelectiveByColumns(ctx, tableName, []string{"transaction_hash"}, txRecords, pageSize)
}

func (r *SolTransactionRecordRepoImpl) Update(ctx context.Context, tableName string, solTransactionRecord *SolTransactionRecord) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Model(&SolTransactionRecord{}).Where("id = ?", solTransactionRecord.Id).Updates(solTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("update solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SolTransactionRecordRepoImpl) FindByID(ctx context.Context, tableName string, id int64) (*SolTransactionRecord, error) {
	var solTransactionRecord *SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).First(&solTransactionRecord, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query solTransactionRecord failed", err)
		}
		return nil, err
	}

	return solTransactionRecord, nil
}

func (r *SolTransactionRecordRepoImpl) ListByID(ctx context.Context, tableName string, id int64) ([]*SolTransactionRecord, error) {
	var solTransactionRecordList []*SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("id > ?", id).Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query solTransactionRecord failed", err)
		return nil, err
	}

	return solTransactionRecordList, nil
}

func (r *SolTransactionRecordRepoImpl) FindByStatus(ctx context.Context, tableName string, s1 string, s2 string) ([]*SolTransactionRecord, error) {
	var solTransactionRecordList []*SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("status in (?)", []string{s1, s2}).Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query solTransactionRecord failed", err)
		return nil, err
	}

	return solTransactionRecordList, nil
}

func (r *SolTransactionRecordRepoImpl) ListAll(ctx context.Context, tableName string) ([]*SolTransactionRecord, error) {
	var solTransactionRecordList []*SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("query solTransactionRecord failed", err)
		return nil, err
	}

	return solTransactionRecordList, nil
}

func (r *SolTransactionRecordRepoImpl) PageList(ctx context.Context, tableName string, req *TransactionRequest) ([]*SolTransactionRecord, int64, error) {
	var solTransactionRecordList []*SolTransactionRecord
	var total int64
	db := r.gormDB.WithContext(ctx).Table(tableName)
	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
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
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
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
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
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

	ret := db.Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query solTransactionRecord failed", err)
		return nil, 0, err
	}
	return solTransactionRecordList, total, nil
}

func (r *SolTransactionRecordRepoImpl) List(ctx context.Context, tableName string, req *TransactionRequest) ([]*SolTransactionRecord, error) {
	var solTransactionRecordList []*SolTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
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
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
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
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
		}
	}

	db = db.Order(req.OrderBy)

	ret := db.Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query solTransactionRecord failed", err)
		return nil, err
	}
	return solTransactionRecordList, nil
}

func (r *SolTransactionRecordRepoImpl) DeleteByID(ctx context.Context, tableName string, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Delete(&SolTransactionRecord{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SolTransactionRecordRepoImpl) DeleteByBlockNumber(ctx context.Context, tableName string, blockNumber int) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("slot_number >= ?", blockNumber).Delete(&SolTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete solTransactionRecord failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, nil
}

func (r *SolTransactionRecordRepoImpl) Delete(ctx context.Context, tableName string, req *TransactionRequest) (int64, error) {
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if req.FromAddress != "" {
		db = db.Where("(from_address = ? or (log_address is not null and log_address->0 ? '"+req.FromAddress+"'))", req.FromAddress)
	}
	if req.ToAddress != "" {
		db = db.Where("(to_address = ? or (log_address is not null and log_address->1 ? '"+req.ToAddress+"'))", req.ToAddress)
	}
	if len(req.FromAddressList) > 0 {
		fromAddressList := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
		db = db.Where("(from_address in(?) or (log_address is not null and log_address->0 ?| array["+fromAddressList+"]))", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		toAddressList := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
		db = db.Where("(to_address in(?) or (log_address is not null and log_address->1 ?| array["+toAddressList+"]))", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ? or (log_address is not null and (log_address->0 ? '"+req.Address+"' or log_address->1 ? '"+req.Address+"')))",
			req.Address, req.Address)
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
	if req.TransactionTypeNotEqual != "" {
		db = db.Where("transaction_type != ?", req.TransactionTypeNotEqual)
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}
	if len(req.TransactionTypeNotInList) > 0 {
		db = db.Where("transaction_type not in(?)", req.TransactionTypeNotInList)
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
	if req.TokenAddress != "" {
		if req.TokenAddress == MAIN_ADDRESS_PARAM {
			req.TokenAddress = ""
		}
		tokenAddressLike := "'%\"address\":\"" + req.TokenAddress + "\"%'"
		db = db.Where("((transaction_type not in('contract', 'swap', 'mint') and (contract_address = '" + req.TokenAddress + "' or parse_data like " + tokenAddressLike + ")) or (transaction_type in('contract', 'swap', 'mint') and event_log like " + tokenAddressLike + "))")
	}
	if req.AssetType != "" {
		if req.AssetType == FT {
			db = db.Where("(transaction_type not in('contract', 'swap', 'mint', 'approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and ((amount != '' and amount != '0') or array_length(regexp_split_to_array(event_log, '\"token\"'), 1) != array_length(regexp_split_to_array(event_log, '\"token_type\"'), 1))))")
		} else if req.AssetType == NFT {
			db = db.Where("(transaction_type in('approveNFT', 'transferNFT') or (transaction_type in('contract', 'swap', 'mint') and event_log like '%\"token_type\":\"%'))")
		}
	}

	ret := db.Delete(&SolTransactionRecord{})
	err := ret.Error
	if err != nil {
		log.Errore("delete solTransactionRecord failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *SolTransactionRecordRepoImpl) FindLast(ctx context.Context, tableName string) (*SolTransactionRecord, error) {
	var solTransactionRecord *SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("slot_number IS NOT NULL").Order("slot_number DESC").Limit(1).Find(&solTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query last solTransactionRecord failed", err)
		return nil, err
	}
	if ret.RowsAffected == 0 {
		return nil, nil
	} else {
		return solTransactionRecord, nil
	}
}

func (r *SolTransactionRecordRepoImpl) FindOneByBlockNumber(ctx context.Context, tableName string, blockNumber int) (*SolTransactionRecord, error) {
	var solTransactionRecord *SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("slot_number = ?", blockNumber).Limit(1).Find(&solTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query one solTransactionRecord by blockNumber failed", err)
		return nil, err
	}
	if ret.RowsAffected == 0 {
		return nil, nil
	} else {
		return solTransactionRecord, nil
	}
}

func (r *SolTransactionRecordRepoImpl) GetAmount(ctx context.Context, tableName string, req *pb.AmountRequest, status string) (string, error) {
	var amount string
	db := r.gormDB.WithContext(ctx).Table(tableName)

	if req.FromUid != "" || len(req.FromAddressList) > 0 || req.ToUid != "" || len(req.ToAddressList) > 0 {
		if req.FromUid == "" && len(req.FromAddressList) == 0 {
			if req.ToUid != "" {
				db = db.Where("to_uid = ?", req.ToUid)
			}
			if len(req.ToAddressList) > 0 {
				db = db.Where("to_address in(?)", req.ToAddressList)
			}
		} else if req.ToUid == "" && len(req.ToAddressList) == 0 {
			if req.FromUid != "" {
				db = db.Where("from_uid = ?", req.FromUid)
			}
			if len(req.FromAddressList) > 0 {
				db = db.Where("from_address in(?)", req.FromAddressList)
			}
		} else {
			fromToSql := "(("

			if req.FromUid != "" && len(req.FromAddressList) > 0 {
				fromToSql += "from_uid = '" + req.FromUid + "'"
				addressLists := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
				fromToSql += " and from_address in(" + addressLists + ")"
			} else if req.FromUid != "" {
				fromToSql += "from_uid = '" + req.FromUid + "'"
			} else if len(req.FromAddressList) > 0 {
				addressLists := strings.ReplaceAll(utils.ListToString(req.FromAddressList), "\"", "'")
				fromToSql += "from_address in(" + addressLists + ")"
			}

			fromToSql += ") or ("

			if req.ToUid != "" && len(req.ToAddressList) > 0 {
				fromToSql += "to_uid = '" + req.ToUid + "'"
				addressLists := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
				fromToSql += " and to_address in(" + addressLists + ")"
			} else if req.ToUid != "" {
				fromToSql += "to_uid = '" + req.ToUid + "'"
			} else if len(req.ToAddressList) > 0 {
				addressLists := strings.ReplaceAll(utils.ListToString(req.ToAddressList), "\"", "'")
				fromToSql += "to_address in(" + addressLists + ")"
			}

			fromToSql += "))"
			db = db.Where(fromToSql)
		}
	}
	db = db.Where("status = ?", status)
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}

	ret := db.Pluck("coalesce(sum(cast(amount as numeric)), 0)", &amount)
	err := ret.Error
	if err != nil {
		log.Errore("query amount failed", err)
		return "", err
	}
	return amount, nil
}

func (r *SolTransactionRecordRepoImpl) FindByTxhash(ctx context.Context, tableName string, txhash string) (*SolTransactionRecord, error) {
	var solTransactionRecord *SolTransactionRecord
	ret := r.gormDB.WithContext(ctx).Table(tableName).Where("transaction_hash = ?", txhash).Find(&solTransactionRecord)
	err := ret.Error
	if err != nil {
		log.Errore("query solTransactionRecord by txHash failed", err)
		return nil, err
	}
	if ret.RowsAffected == 0 {
		return nil, nil
	} else {
		return solTransactionRecord, nil
	}
}

func (r *SolTransactionRecordRepoImpl) PendingByAddress(ctx context.Context, tableName string, address string) ([]*SolTransactionRecord, error) {
	var solTransactionRecordList []*SolTransactionRecord
	db := r.gormDB.WithContext(ctx).Table(tableName).Where(" status in  ('pending','no_status')")
	if address != "" {
		db.Where("(from_address = ? or to_address = ?)", address, address)
	}
	ret := db.Find(&solTransactionRecordList)
	err := ret.Error
	if err != nil {
		log.Errore("page query CsprTransactionRecord failed", err)
		return nil, err
	}
	return solTransactionRecordList, nil
}

func (r *SolTransactionRecordRepoImpl) ListIncompleteNft(ctx context.Context, tableName string, req *TransactionRequest) ([]*SolTransactionRecord, error) {
	var solTransactionRecords []*SolTransactionRecord

	sqlStr := "select transaction_type, transaction_hash, amount, parse_data, event_log from " + tableName +
		" where 1=1 " +
		"and (" +
		"(" +
		"(" +
		"(parse_data not like '%\"collection_name\":\"%' and parse_data not like '%\"item_name\":%') " +
		"or (parse_data like '%\"collection_name\":\"\"%' and parse_data like '%\"item_name\":\"\"%')" +
		") and (" +
		"parse_data like '%\"token_type\":\"SolanaNFT\"%' " +
		")" +
		") or (" +
		"(" +
		"(event_log not like '%\"collection_name\":\"%' and event_log not like '%\"item_name\":%') " +
		"or (event_log like '%\"collection_name\":\"\"%' and event_log like '%\"item_name\":\"\"%')" +
		") and (" +
		"event_log like '%\"token_type\":\"SolanaNFT\"%'" +
		")" +
		")" +
		")"

	if len(req.StatusNotInList) > 0 {
		statusNotInList := strings.ReplaceAll(utils.ListToString(req.StatusNotInList), "\"", "'")
		sqlStr += " and status not in (" + statusNotInList + ")"
	}
	if len(req.TransactionTypeNotInList) > 0 {
		transactionTypeNotInList := strings.ReplaceAll(utils.ListToString(req.TransactionTypeNotInList), "\"", "'")
		sqlStr += " and transaction_type not in (" + transactionTypeNotInList + ")"
	}

	ret := r.gormDB.WithContext(ctx).Table(tableName).Raw(sqlStr).Find(&solTransactionRecords)
	err := ret.Error
	if err != nil {
		log.Errore("list query solTransactionRecord failed", err)
		return nil, err
	}
	return solTransactionRecords, nil
}

// CountOut implements AtomTransactionRecordRepo
func (r *SolTransactionRecordRepoImpl) CountOut(ctx context.Context, tableName string, address string, toAddress string) (int64, error) {
	return countOutTx(r.gormDB, ctx, tableName, address, toAddress)
}
