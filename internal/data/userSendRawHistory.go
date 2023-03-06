package data

import (
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"gorm.io/gorm"
	"strconv"
	"strings"
)

const (
	userSendRawHistoryTable = "user_sendraw_history"
)

type UserSendRawHistory struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	SessionId string `json:"sessionId" form:"sessionId" gorm:"type:character varying(96)"`
	UserName  string `json:"userName" form:"userName" gorm:"type:character varying(128)"`
	Address   string `json:"address" form:"address" gorm:"type:character varying(550)"`
	TxInput   string `json:"txInput" form:"txInput"`
	ErrMsg    string `json:"errMsg" form:"errMsg"`
	ChainName string `json:"chainName"  form:"chainName" gorm:"type:character varying(16)"`
	CreatedAt int64  `json:"createdAt" form:"createdAt" gorm:"type:bigint"`
}

func (userSendRawHistory UserSendRawHistory) TableName() string {
	return userSendRawHistoryTable
}

var UserSendRawHistoryRepoInst UserSendRawHistoryRepo

type UserSendRawHistoryRepo interface {
	Save(context.Context, *UserSendRawHistory) (int64, error)
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
	ret := r.gormDB.WithContext(ctx).Create(userSendRawHistory)
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
