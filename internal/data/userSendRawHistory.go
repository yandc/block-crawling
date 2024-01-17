package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm/clause"

	"gorm.io/gorm"
)

type UserSendRawHistory struct {
	Id              int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Uid             string `json:"uid" form:"uid" gorm:"type:character varying(36);index"`
	SessionId       string `json:"sessionId" form:"sessionId" gorm:"type:character varying(96);default:null;index:,unique"`
	UserName        string `json:"userName" form:"userName" gorm:"type:character varying(128)"`
	Address         string `json:"address" form:"address" gorm:"type:character varying(550)"`
	TxInput         string `json:"txInput" form:"txInput"`
	BaseTxInput     string `json:"baseTxInput" form:"baseTxInput"`
	ErrMsg          string `json:"errMsg" form:"errMsg"`
	ChainName       string `json:"chainName"  form:"chainName" gorm:"type:character varying(20)"`
	DeviceId        string `json:"deviceId" form:"deviceId" gorm:"type: character varying(36)"`
	UserAgent       string `json:"userAgent" form:"userAgent" gorm:"type: character varying(200)"`
	Nonce           int64  `json:"nonce" form:"nonce"`
	TransactionHash string `json:"transactionHash" form:"transactionHash" gorm:"type:character varying(1024);default:null;index:"`
	TransactionType string `json:"transactionType" form:"transactionType" gorm:"type:character varying(42)"`
	SignStatus      string `json:"signStatus" form:"signStatus" gorm:"type:character varying(20)"`
	SignType        string `json:"signType" form:"signType" gorm:"type:character varying(20)"`
	CreatedAt       int64  `json:"createdAt" form:"createdAt" gorm:"type:bigint"`
	UpdatedAt       int64  `json:"updatedAt" form:"updatedAt" gorm:"type:bigint"`
	TxTime          int64  `json:"txTime" form:"txTime"`
}

type SignRequest struct {
	Address             string
	AddressList         []string
	ChainName           string
	SignType            string
	SignStatus          []string
	TransactionTypeList []string
	StartTime           int64
	StopTime            int64
	SelectColumn        string
	GroupBy             string
	OrderBy             string
	DataDirection       int32
	StartIndex          int64
	PageNum             int32
	PageSize            int32
	Total               bool
}

func (userSendRawHistory UserSendRawHistory) TableName() string {
	return "user_sendraw_history"
}

var UserSendRawHistoryRepoInst UserSendRawHistoryRepo

type UserSendRawHistoryRepo interface {
	Save(context.Context, *UserSendRawHistory) (int64, error)
	SaveOrUpdate(context.Context, []*UserSendRawHistory) (int64, error)
	UpdateAddressListByAddress(ctx context.Context, address string, addressList []string) (int64, error)
	PageList(context.Context, *SignRequest) ([]*UserSendRawHistory, int64, error)
	PageListAllCallBack(context.Context, *SignRequest, func(list []*UserSendRawHistory) error, ...time.Duration) error
	PageListAll(context.Context, *SignRequest, ...time.Duration) ([]*UserSendRawHistory, error)
	GetLatestOneBySessionId(ctx context.Context, sessionID string) (*UserSendRawHistory, error)
	SelectSignStatus(ctx context.Context, signStatus []string) ([]*UserSendRawHistory, error)
	SelectBySessionIds(ctx context.Context, sessionIDs []string) ([]*UserSendRawHistory, error)
	SelectByTxHash(ctx context.Context, txHash string) (*UserSendRawHistory, error)
	CursorListAll(ctx context.Context, cursor *int64, pageSize int) ([]*UserSendRawHistory, error)
}

type userSendRawHistoryRepoImpl struct {
	gormDB *gorm.DB
}

// CursorListAll implements UserSendRawHistoryRepo
func (r *userSendRawHistoryRepoImpl) CursorListAll(ctx context.Context, cursor *int64, pageSize int) ([]*UserSendRawHistory, error) {
	var results []*UserSendRawHistory
	db := r.gormDB.WithContext(ctx)
	if cursor != nil {
		db = db.Where("id > ?", *cursor)
	}
	ret := db.Order("id ASC").Limit(pageSize).Find(&results)
	if ret.Error != nil {
		return nil, ret.Error
	}
	if len(results) > 0 {
		*cursor = results[len(results)-1].Id
	}
	return results, nil
}

func NewUserSendRawHistoryRepo(gormDB *gorm.DB) UserSendRawHistoryRepo {
	UserSendRawHistoryRepoInst = &userSendRawHistoryRepoImpl{
		gormDB: gormDB,
	}
	return UserSendRawHistoryRepoInst
}

func (r *userSendRawHistoryRepoImpl) Save(ctx context.Context, userSendRawHistory *UserSendRawHistory) (int64, error) {
	ret := r.gormDB.Create(userSendRawHistory)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(userSendRawHistory.Id, 10), Data: 0}
			log.Warne("insert UserSendRawHistory failed", err)
		} else {
			log.Errore("insert UserSendRawHistory failed", err)
		}
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *userSendRawHistoryRepoImpl) GetLatestOneBySessionId(ctx context.Context, sessionID string) (*UserSendRawHistory, error) {
	var result *UserSendRawHistory
	ret := r.gormDB.Where("session_id=? AND address != ''", sessionID).Last(&result)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return result, nil
}

func (r *userSendRawHistoryRepoImpl) SelectByTxHash(ctx context.Context, txHash string) (*UserSendRawHistory, error) {
	var result *UserSendRawHistory
	ret := r.gormDB.Where("transaction_hash like ?", "%"+txHash+"%").Last(&result)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return result, nil
}

func (r *userSendRawHistoryRepoImpl) SaveOrUpdate(ctx context.Context, userSendRawHistory []*UserSendRawHistory) (int64, error) {
	ret := r.gormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "session_id"}},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"transaction_type": gorm.Expr("case when excluded.transaction_type != '' then excluded.transaction_type else user_sendraw_history.transaction_type end"),
			"transaction_hash": gorm.Expr("case when excluded.transaction_hash != '' then excluded.transaction_hash else user_sendraw_history.transaction_hash end"),
			"user_name":        gorm.Expr("case when excluded.user_name != '' then excluded.user_name else user_sendraw_history.user_name end"),
			"sign_status":      gorm.Expr("case when excluded.sign_status != '' then excluded.sign_status else user_sendraw_history.sign_status end"),
			"sign_type":        gorm.Expr("case when excluded.sign_type != '' then excluded.sign_type else user_sendraw_history.sign_type end"),
			"address":          gorm.Expr("case when excluded.address != '' then excluded.address else user_sendraw_history.address end"),
			"tx_input":         gorm.Expr("case when excluded.tx_input != '' then excluded.tx_input else user_sendraw_history.tx_input end"),
			"base_tx_input":    gorm.Expr("case when excluded.base_tx_input != '' then excluded.base_tx_input else user_sendraw_history.base_tx_input end"),
			"tx_time":          gorm.Expr("case when excluded.tx_time > 0 then excluded.tx_time else user_sendraw_history.tx_time end"),
			"nonce":            gorm.Expr("case when excluded.nonce > 0 then excluded.nonce else user_sendraw_history.nonce end"),
			"err_msg":          gorm.Expr("case when excluded.err_msg != '' then excluded.err_msg else user_sendraw_history.err_msg end"),
			"chain_name":       gorm.Expr("case when excluded.chain_name != '' then excluded.chain_name else user_sendraw_history.chain_name end"),
			"device_id":        gorm.Expr("case when excluded.device_id != '' then excluded.device_id else user_sendraw_history.device_id end"),
			"user_agent":       gorm.Expr("case when excluded.user_agent != '' then excluded.user_agent else user_sendraw_history.user_agent end"),
			"updated_at":       gorm.Expr("case when excluded.updated_at >= 0 then excluded.updated_at else user_sendraw_history.updated_at end"),
		}),
	}).Create(&userSendRawHistory)
	err := ret.Error
	if err != nil {
		log.Errore(" insert or update user_sendraw_history failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	return affected, err
}

func (r *userSendRawHistoryRepoImpl) UpdateAddressListByAddress(ctx context.Context, address string, addressList []string) (int64, error) {
	ret := r.gormDB.WithContext(ctx).Model(&UserSendRawHistory{}).Where("address in(?)", addressList).Update("address", address)
	err := ret.Error
	if err != nil {
		log.Errore("update address by addressList", err)
		return 0, err
	}
	affected := ret.RowsAffected
	return affected, nil
}

func (r *userSendRawHistoryRepoImpl) SelectSignStatus(ctx context.Context, signStatus []string) ([]*UserSendRawHistory, error) {
	var userSendRawHistoryList []*UserSendRawHistory
	ret := r.gormDB.Table("user_sendraw_history").Where("sign_status in (?)", signStatus).Find(&userSendRawHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("SelectSignStatus failed", err)
		return nil, err
	}
	return userSendRawHistoryList, nil
}

func (r *userSendRawHistoryRepoImpl) SelectBySessionIds(ctx context.Context, sessionIDs []string) ([]*UserSendRawHistory, error) {
	var userSendRawHistoryList []*UserSendRawHistory
	ret := r.gormDB.Table("user_sendraw_history").Where("session_id in (?)", sessionIDs).Find(&userSendRawHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("SelectBySessionIds failed", err)
		return nil, err
	}
	return userSendRawHistoryList, nil
}

func (r *userSendRawHistoryRepoImpl) PageList(ctx context.Context, req *SignRequest) ([]*UserSendRawHistory, int64, error) {
	var userSendRawHistoryList []*UserSendRawHistory
	var total int64

	db := r.gormDB.Table("user_sendraw_history")
	if req.ChainName != "" {
		db = db.Where("chain_name = ?", req.ChainName)
	}
	if req.Address != "" {
		db = db.Where("address = ?", req.Address)
	}
	if len(req.AddressList) > 0 {
		db = db.Where("address in(?)", req.AddressList)
	}
	if req.SignType != "" {
		db = db.Where("sign_type = ?", req.SignType)
	} else {
		db = db.Where("(sign_type = '1' or (sign_type= '2' and sign_status = '2'))")
	}
	if req.SignType == "1" {
		db = db.Where(" transaction_hash != '' ")
	}
	if req.SignType == "2" {
		db = db.Where(" sign_status = '2' ")
	}
	if len(req.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in ?", req.TransactionTypeList)
	}
	if len(req.SignStatus) > 0 {
		db = db.Where("sign_status in ?", req.SignStatus)
	}
	if req.StartTime > 0 {
		db = db.Where("created_at >= ?", req.StartTime)
	}
	if req.StopTime > 0 {
		db = db.Where("created_at < ?", req.StopTime)
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

	if req.SelectColumn != "" {
		db = db.Select(req.SelectColumn)
	}
	ret := db.Find(&userSendRawHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query userSendRawHistory failed", err)
		return nil, 0, err
	}
	return userSendRawHistoryList, total, nil
}

func (r *userSendRawHistoryRepoImpl) PageListAllCallBack(ctx context.Context, req *SignRequest, fn func(list []*UserSendRawHistory) error, timeDuration ...time.Duration) error {
	var timeout time.Duration
	if len(timeDuration) > 0 {
		timeout = timeDuration[0]
	} else {
		timeout = 1_000 * time.Millisecond
	}
	req.DataDirection = 2
	for {
		userAssets, _, err := r.PageList(ctx, req)
		if err != nil {
			return err
		}
		dataLen := int32(len(userAssets))
		if dataLen == 0 {
			break
		}

		err = fn(userAssets)
		if err != nil {
			return err
		}
		if dataLen < req.PageSize {
			break
		}
		req.StartIndex = userAssets[dataLen-1].Id
		time.Sleep(timeout)
	}
	return nil
}

func (r *userSendRawHistoryRepoImpl) PageListAll(ctx context.Context, req *SignRequest, timeDuration ...time.Duration) ([]*UserSendRawHistory, error) {
	var userSendRawHistoryList []*UserSendRawHistory
	err := r.PageListAllCallBack(nil, req, func(userAssets []*UserSendRawHistory) error {
		userSendRawHistoryList = append(userSendRawHistoryList, userAssets...)
		return nil
	}, timeDuration...)
	return userSendRawHistoryList, err
}
