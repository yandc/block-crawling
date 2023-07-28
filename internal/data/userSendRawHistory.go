package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

const (
	userSendRawHistoryTable = "user_sendraw_history"
)

type UserSendRawHistory struct {
	Id              int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	SessionId       string `json:"sessionId" form:"sessionId" gorm:"type:character varying(96);default:null;index:,unique"`
	UserName        string `json:"userName" form:"userName" gorm:"type:character varying(128)"`
	Address         string `json:"address" form:"address" gorm:"type:character varying(550)"`
	TxInput         string `json:"txInput" form:"txInput"`
	BaseTxInput     string `json:"baseTxInput" form:"baseTxInput"`
	ErrMsg          string `json:"errMsg" form:"errMsg"`
	ChainName       string `json:"chainName"  form:"chainName" gorm:"type:character varying(16)"`
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

func (userSendRawHistory UserSendRawHistory) TableName() string {
	return userSendRawHistoryTable
}

var UserSendRawHistoryRepoInst UserSendRawHistoryRepo

type UserSendRawHistoryRepo interface {
	Save(context.Context, *UserSendRawHistory) (int64, error)
	SaveOrUpdate(context.Context, []*UserSendRawHistory) (int64, error)
	PageList(context.Context, SignReqPage) ([]*UserSendRawHistory, int64, error)
	GetLatestOneBySessionId(ctx context.Context, sessionID string) (*UserSendRawHistory, error)
	SelectSignStatus(ctx context.Context, signStatus []string) ([]*UserSendRawHistory, error)
	//UpdateSignStatusByTxHash(context.Context, string, map[string]interface{}, int64, string) (int64, error)
	//UpdateSignStatusLikeTxHash(context.Context, string, map[string]interface{}, int64, string) (int64, error)
	SelectByTxHash(ctx context.Context, txhash string) (*UserSendRawHistory, error)
}

type userSendRawHistoryRepoImpl struct {
	gormDB *gorm.DB
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
func (r *userSendRawHistoryRepoImpl) SelectByTxHash(ctx context.Context, txhash string) (*UserSendRawHistory, error) {
	var result *UserSendRawHistory
	ret := r.gormDB.Where("transaction_hash like ?", "%"+txhash+"%").Last(&result)
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

func (r *userSendRawHistoryRepoImpl) PageList(ctx context.Context, signReqPage SignReqPage) ([]*UserSendRawHistory, int64, error) {
	var userSendRawHistoryList []*UserSendRawHistory
	var total int64

	db := r.gormDB.Table("user_sendraw_history")
	if signReqPage.Address != "" {
		db = db.Where("address = ?", signReqPage.Address)
	}
	if signReqPage.ChainName != "" {
		db = db.Where("chain_name = ?", signReqPage.ChainName)
	}
	if signReqPage.SignType != "" {
		db = db.Where("sign_type = ?", signReqPage.SignType)
	} else {
		db = db.Where("sign_type in ('1','2')")
	}
	if signReqPage.SignType == "1"{
		db = db.Where(" transaction_hash != '' ")
	}

	if len(signReqPage.TransactionTypeList) > 0 {
		db = db.Where("transaction_type in ?", signReqPage.TransactionTypeList)
	}
	if len(signReqPage.SignStatus) > 0 {
		db = db.Where("sign_status in ?", signReqPage.SignStatus)
	}
	db.Count(&total)
	if signReqPage.PageSize != -1 && signReqPage.PageNum != -1 {
		db = db.Offset((signReqPage.PageNum - 1) * signReqPage.PageSize)
	}
	if signReqPage.TradeTime == 1 {
		db.Order("updated_at desc")
	}
	if signReqPage.TradeTime == 2 {
		db.Order("updated_at asc")
	}

	ret := db.Find(&userSendRawHistoryList)
	err := ret.Error
	if err != nil {
		log.Errore("page query user_sendraw_history failed", err)
		return nil, 0, err
	}
	return userSendRawHistoryList, total, nil
}

//func (r *userSendRawHistoryRepoImpl) UpdateSignStatusByTxHash(ctx context.Context, transactionHash string, ufiles map[string]interface{}, nonce int64, address string) (int64, error) {
//	db := r.gormDB.WithContext(ctx).Table("user_sendraw_history").Where("sign_status in ('1','6')")
//	if transactionHash != "" {
//		db = db.Where("transaction_hash = ?", transactionHash)
//	}
//	if address != "" {
//		db = db.Where("address = ?", address)
//	}
//	if nonce >= 0 {
//		db = db.Where("nonce >=0 and nonce <= ? ", nonce)
//	}
//
//	ret := db.UpdateColumns(ufiles)
//	err := ret.Error
//	if err != nil {
//		log.Errore("UpdateSignStatusByTxHash failed", err)
//		return 0, err
//	}
//	return ret.RowsAffected, nil
//}
//func (r *userSendRawHistoryRepoImpl) UpdateSignStatusLikeTxHash(ctx context.Context, transactionHash string, ufiles map[string]interface{}, nonce int64, address string) (int64, error) {
//	db := r.gormDB.WithContext(ctx).Table("user_sendraw_history").Where("sign_status in ('1','6')")
//	if transactionHash != "" {
//		db = db.Where("transaction_hash like ?", "%"+transactionHash+"%")
//	}
//	if address != "" {
//		db = db.Where("address = ?", address)
//	}
//	if nonce >= 0 {
//		db = db.Where("nonce >=0 and nonce <= ? ", nonce)
//	}
//
//	ret := db.UpdateColumns(ufiles)
//	err := ret.Error
//	if err != nil {
//		log.Errore("UpdateSignStatusByTxHash failed", err)
//		return 0, err
//	}
//	return ret.RowsAffected, nil
//}
