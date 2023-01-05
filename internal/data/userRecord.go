package data

import (
	"context"

	"gorm.io/gorm"
)

const (
	userTable = "tb_user"
)

type UserRecord struct {
	UUID  string `json:"uuid" form:"id" gorm:"type:character varing(32);index:,unique"`
	Phone string `json:"phone" form:"phone" gorm:"type:character varing(32)"`
	Email string `json:"email" form:"email" gorm:"type:character varing(128)"`
}

var UserRecordRepoInst UserRecordRepo

type UserRecordRepo interface {
	LoadByUUIDs(ctx context.Context, uuids []string) (map[string]UserRecord, error)
}

type userRecordRepoImpl struct {
	gormDB *gorm.DB
}

func NewUserRecordRepo(gormDB UserGormDB) UserRecordRepo {
	UserRecordRepoInst = &userRecordRepoImpl{
		gormDB: gormDB,
	}
	return UserRecordRepoInst
}

// LoadByUUIDs implements UserRecordRepo
func (repo *userRecordRepoImpl) LoadByUUIDs(ctx context.Context, uuids []string) (map[string]UserRecord, error) {
	var records []UserRecord
	ret := repo.gormDB.WithContext(ctx).Table(userTable).Where("uuid in (?)", uuids).Find(&records)
	if ret.Error != nil {
		return nil, ret.Error
	}
	results := make(map[string]UserRecord)
	for _, r := range records {
		results[r.UUID] = r
	}
	return results, nil
}
