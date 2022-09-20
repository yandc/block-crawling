package data

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

// TransactionStatistic is a TransactionStatistic model.
type TransactionStatistic struct {
	Id                  int64           `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName           string          `json:"chainName" form:"chainName" gorm:"type:character varying(20);index:,unique,composite:unique_dt_chain_name_token_address_fund_direction_fund_type"`
	TokenAddress        string          `json:"tokenAddress" form:"tokenAddress" gorm:"type:character varying(1024);index:,unique,composite:unique_dt_chain_name_token_address_fund_direction_fund_type"`
	FundDirection       int16           `json:"fundDirection" form:"fundDirection" gorm:"type:smallint;index:,unique,composite:unique_dt_chain_name_token_address_fund_direction_fund_type"`
	FundType            int16           `json:"fundType" form:"fundType" gorm:"type:smallint;index:,unique,composite:unique_dt_chain_name_token_address_fund_direction_fund_type"`
	TransactionQuantity int64           `json:"transactionQuantity" form:"transactionQuantity"`
	Amount              decimal.Decimal `json:"amount" form:"amount" gorm:"type:decimal(256,0);"`
	CnyAmount           decimal.Decimal `json:"cnyAmount" form:"cnyAmount" gorm:"type:decimal(256,2);"`
	UsdAmount           decimal.Decimal `json:"usdAmount" form:"usdAmount" gorm:"type:decimal(256,2);"`
	Dt                  int64           `json:"dt" form:"dt" gorm:"type:bigint;index:,unique,composite:unique_dt_chain_name_token_address_fund_direction_fund_type"`
	CreatedAt           int64           `json:"createdAt" form:"createdAt"`
	UpdatedAt           int64           `json:"updatedAt" form:"updatedAt"`
}

func (transactionStatistic TransactionStatistic) TableName() string {
	return "transaction_statistic"
}

// TransactionStatisticRepo is a Greater repo.
type TransactionStatisticRepo interface {
	Save(context.Context, *TransactionStatistic) (int64, error)
	BatchSave(context.Context, []*TransactionStatistic) (int64, error)
	SaveOrUpdate(context.Context, *TransactionStatistic) (int64, error)
	BatchSaveOrUpdate(context.Context, []*TransactionStatistic) (int64, error)
	PageBatchSaveOrUpdate(context.Context, []*TransactionStatistic, int) (int64, error)
	IncrementBatchSaveOrUpdate(context.Context, []*TransactionStatistic) (int64, error)
	PageIncrementBatchSaveOrUpdate(context.Context, []*TransactionStatistic, int) (int64, error)
	Update(context.Context, *TransactionStatistic) (int64, error)
	FindByID(context.Context, int64) (*TransactionStatistic, error)
	ListByID(context.Context, int64) ([]*TransactionStatistic, error)
	ListAll(context.Context) ([]*TransactionStatistic, error)
	PageList(context.Context, *pb.PageListRequest) ([]*TransactionStatistic, int64, error)
	DeleteByID(context.Context, int64) (int64, error)
	StatisticFundAmount(context.Context, *pb.StatisticFundRequest) ([]*pb.FundAmountResponse, error)
	StatisticFundRate(context.Context, *pb.StatisticFundRequest) ([]*pb.FundRateResponse, error)
}

type TransactionStatisticRepoImpl struct {
	gormDB *gorm.DB
}

var TransactionStatisticRepoClient TransactionStatisticRepo

// NewTransactionStatisticRepo new a TransactionStatistic repo.
func NewTransactionStatisticRepo(gormDB *gorm.DB) TransactionStatisticRepo {
	TransactionStatisticRepoClient = &TransactionStatisticRepoImpl{
		gormDB: gormDB,
	}
	return TransactionStatisticRepoClient
}

func (r *TransactionStatisticRepoImpl) Save(ctx context.Context, transactionStatistic *TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Create(transactionStatistic)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(transactionStatistic.Id, 10), Data: 0}
			log.Warne("insert transactionStatistic failed", err)
		} else {
			log.Errore("insert transactionStatistic failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionStatisticRepoImpl) BatchSave(ctx context.Context, transactionStatistics []*TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).CreateInBatches(transactionStatistics, len(transactionStatistics))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(transactionStatistics)), Data: 0}
			log.Warne("batch insert transactionStatistic failed", err)
		} else {
			log.Errore("batch insert transactionStatistic failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionStatisticRepoImpl) SaveOrUpdate(ctx context.Context, transactionStatistic *TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "token_address"}, {Name: "fund_direction"}, {Name: "fund_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"transaction_quantity", "amount", "cny_amount", "usd_amount", "updated_at"}),
	}).Create(&transactionStatistic)
	err := ret.Error
	if err != nil {
		log.Errore("insert or update transactionStatistic failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionStatisticRepoImpl) BatchSaveOrUpdate(ctx context.Context, transactionStatistics []*TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "token_address"}, {Name: "fund_direction"}, {Name: "fund_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.AssignmentColumns([]string{"transaction_quantity", "amount", "cny_amount", "usd_amount", "updated_at"}),
	}).Create(&transactionStatistics)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update transactionStatistic failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionStatisticRepoImpl) PageBatchSaveOrUpdate(ctx context.Context, transactionStatistics []*TransactionStatistic, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(transactionStatistics)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTransactionStatistics := transactionStatistics[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.BatchSaveOrUpdate(ctx, subTransactionStatistics)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *TransactionStatisticRepoImpl) IncrementBatchSaveOrUpdate(ctx context.Context, transactionStatistics []*TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_name"}, {Name: "token_address"}, {Name: "fund_direction"}, {Name: "fund_type"}, {Name: "dt"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"transaction_quantity": gorm.Expr("excluded.transaction_quantity + transaction_statistic.transaction_quantity"),
			"amount":               gorm.Expr("excluded.amount + transaction_statistic.amount"),
			"cny_amount":           gorm.Expr("excluded.cny_amount + transaction_statistic.cny_amount"),
			"usd_amount":           gorm.Expr("excluded.usd_amount + transaction_statistic.usd_amount"),
			"updated_at":           clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}).Create(&transactionStatistics)
	err := ret.Error
	if err != nil {
		log.Errore("batch insert or update transactionStatistic failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *TransactionStatisticRepoImpl) PageIncrementBatchSaveOrUpdate(ctx context.Context, transactionStatistics []*TransactionStatistic, pageSize int) (int64, error) {
	var totalAffected int64 = 0
	total := len(transactionStatistics)
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTransactionStatistics := transactionStatistics[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		affected, err := r.IncrementBatchSaveOrUpdate(ctx, subTransactionStatistics)
		if err != nil {
			return totalAffected, err
		} else {
			totalAffected += affected
		}
	}
	return totalAffected, nil
}

func (r *TransactionStatisticRepoImpl) Update(ctx context.Context, transactionStatistic *TransactionStatistic) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&TransactionStatistic{}).Where("id = ?", transactionStatistic.Id).Updates(transactionStatistic)
	err := ret.Error
	if err != nil {
		log.Errore("update transactionStatistic failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *TransactionStatisticRepoImpl) FindByID(ctx context.Context, id int64) (*TransactionStatistic, error) {
	var transactionStatistic *TransactionStatistic
	ret := r.gormDB.WithContext(ctx).First(&transactionStatistic, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			log.Errore("query transactionStatistic failed", err)
		}
		return nil, err
	}
	return transactionStatistic, nil
}

func (r *TransactionStatisticRepoImpl) ListByID(ctx context.Context, id int64) ([]*TransactionStatistic, error) {
	var transactionStatisticList []*TransactionStatistic
	ret := r.gormDB.WithContext(ctx).Where("id > ?", id).Find(&transactionStatisticList)
	err := ret.Error
	if err != nil {
		log.Errore("query transactionStatistic failed", err)
		return nil, err
	}
	return transactionStatisticList, nil
}

func (r *TransactionStatisticRepoImpl) ListAll(ctx context.Context) ([]*TransactionStatistic, error) {
	var transactionStatisticList []*TransactionStatistic
	ret := r.gormDB.WithContext(ctx).Find(&transactionStatisticList)
	err := ret.Error
	if err != nil {
		log.Errore("query transactionStatistic failed", err)
		return nil, err
	}
	return transactionStatisticList, nil
}

func (r *TransactionStatisticRepoImpl) PageList(ctx context.Context, req *pb.PageListRequest) ([]*TransactionStatistic, int64, error) {
	var transactionStatisticList []*TransactionStatistic
	var total int64
	db := r.gormDB.WithContext(ctx)

	/*if req.FromUid != "" || len(req.FromAddressList) > 0 || req.ToUid != "" || len(req.ToAddressList) > 0 {
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
	}*/
	if req.FromUid != "" {
		db = db.Where("from_uid = ?", req.FromUid)
	}
	if req.ToUid != "" {
		db = db.Where("to_uid = ?", req.ToUid)
	}
	if len(req.FromAddressList) > 0 {
		db = db.Where("from_address in(?)", req.FromAddressList)
	}
	if len(req.ToAddressList) > 0 {
		db = db.Where("to_address in(?)", req.ToAddressList)
	}
	if req.Uid != "" {
		db = db.Where("(from_uid = ? or to_uid = ?)", req.Uid, req.Uid)
	}
	if req.Address != "" {
		db = db.Where("(from_address = ? or to_address = ?)", req.Address, req.Address)
	}
	/*if req.ContractAddress != "" {
		db = db.Where("contract_address = ?", req.ContractAddress)
	}*/
	if len(req.StatusList) > 0 {
		db = db.Where("status in(?)", req.StatusList)
	}
	/*if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in(?)", req.TransactionTypeList)
	}*/
	if len(req.TransactionHashList) > 0 {
		db = db.Where("transaction_hash in(?)", req.TransactionHashList)
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

	if req.Total {
		// 统计总记录数
		db.Count(&total)
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

	ret := db.Find(&transactionStatisticList)
	err := ret.Error
	if err != nil {
		log.Errore("page query transactionStatistic failed", err)
		return nil, 0, err
	}
	return transactionStatisticList, total, nil
}

func (r *TransactionStatisticRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Delete(&TransactionStatistic{}, id)
	err := ret.Error
	if err != nil {
		log.Errore("delete transactionStatistic failed", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

//维度：资金流向 + 日期
func (r *TransactionStatisticRepoImpl) StatisticFundAmount(ctx context.Context, req *pb.StatisticFundRequest) ([]*pb.FundAmountResponse, error) {
	var fundAmountResponseList []*pb.FundAmountResponse

	sqlStr := "select sum(amount) as amount, sum(cny_amount) as cny_amount, sum(usd_amount) as usd_amount, " +
		"sum(transaction_quantity) as transaction_quantity, fund_direction, dt " +
		"from transaction_statistic " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if len(req.FundDirectionList) > 0 {
		fundDirectionList := utils.ListToString(req.FundDirectionList)
		sqlStr += " and fund_direction in (" + fundDirectionList + ")"
	}
	sqlStr += " and dt >= " + strconv.Itoa(int(req.StartTime))
	sqlStr += " and dt < " + strconv.Itoa(int(req.StopTime))
	sqlStr += " and amount > 0"
	sqlStr += " and cny_amount > 0"
	sqlStr += " group by fund_direction, dt"
	sqlStr += " order by fund_direction asc, dt asc"

	ret := r.gormDB.WithContext(ctx).Table("transaction_statistic").Raw(sqlStr).Find(&fundAmountResponseList)
	err := ret.Error
	if err != nil {
		log.Errore("statistic fund amount failed", err)
		return nil, err
	}

	return fundAmountResponseList, nil
}

//维度：资金流向 + 资金类型
func (r *TransactionStatisticRepoImpl) StatisticFundRate(ctx context.Context, req *pb.StatisticFundRequest) ([]*pb.FundRateResponse, error) {
	var fundRateResponseList []*pb.FundRateResponse

	sqlStr := "select sum(amount) as amount, sum(cny_amount) as cny_amount, sum(usd_amount) as usd_amount, " +
		"sum(transaction_quantity) as transaction_quantity, fund_direction, fund_type " +
		"from transaction_statistic " +
		"where 1=1 "
	if req.ChainName != "" {
		sqlStr += " and chain_name = '" + req.ChainName + "'"
	}
	if len(req.FundDirectionList) > 0 {
		fundDirectionList := utils.ListToString(req.FundDirectionList)
		sqlStr += " and fund_direction in (" + fundDirectionList + ")"
	}
	sqlStr += " and dt >= " + strconv.Itoa(int(req.StartTime))
	sqlStr += " and dt < " + strconv.Itoa(int(req.StopTime))
	sqlStr += " and amount > 0"
	sqlStr += " and cny_amount > 0"
	sqlStr += " group by fund_direction, fund_type"
	sqlStr += " order by fund_direction asc, fund_type asc"

	ret := r.gormDB.WithContext(ctx).Table("transaction_statistic").Raw(sqlStr).Find(&fundRateResponseList)
	err := ret.Error
	if err != nil {
		log.Errore("statistic fund rate failed", err)
		return nil, err
	}

	if len(fundRateResponseList) > 0 {
		var fundRateMap = make(map[int32]decimal.Decimal)
		for _, fundRateResponse := range fundRateResponseList {
			cnyAmount := fundRateResponse.GetCnyAmount()
			if cnyAmount != "" && cnyAmount != "0" {
				cnyAmountDecimal, err := decimal.NewFromString(cnyAmount)
				if err == nil {
					fundDirection := fundRateResponse.FundDirection
					totalCnyAmount, ok := fundRateMap[fundDirection]
					if !ok {
						fundRateMap[fundDirection] = cnyAmountDecimal
					} else {
						fundRateMap[fundDirection] = totalCnyAmount.Add(cnyAmountDecimal)
					}
				}
			}
		}

		for _, fundRateResponse := range fundRateResponseList {
			cnyAmount := fundRateResponse.GetCnyAmount()
			if cnyAmount != "" && cnyAmount != "0" {
				cnyAmountDecimal, err := decimal.NewFromString(cnyAmount)
				if err == nil {
					fundDirection := fundRateResponse.FundDirection
					totalCnyAmount, ok := fundRateMap[fundDirection]
					if ok {
						fundRateResponse.Rate = cnyAmountDecimal.DivRound(totalCnyAmount, 4).String()
					}
				}
			}
		}
	}

	return fundRateResponseList, nil
}
