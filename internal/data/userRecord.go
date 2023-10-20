package data

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

const (
	userTable         = "tb_user"
	chainAddressTable = "tb_chain_address"
)

type UserRecord struct {
	UUID     string `json:"uuid" form:"id" gorm:"type:character varing(32);index:,unique"`
	Username string `json:"username" form:"username"`
	Phone    string `json:"phone" form:"phone" gorm:"type:character varing(32)"`
	Email    string `json:"email" form:"email" gorm:"type:character varing(128)"`
	Category string `json:"category" form:"category"`
}

func (u *UserRecord) String() string {
	return fmt.Sprintf("%s<%s>(%s)", u.Username, u.Email, u.Category)
}

type ChainAddress struct {
	UUID      string
	ChainName string
	Nickname  string
	Address   string
	UserUUID  string
}

var UserRecordRepoInst UserRecordRepo

type UserRecordRepo interface {
	LoadByUUIDs(ctx context.Context, uuids []string) (map[string]UserRecord, error)
	FindOne(ctx context.Context, uuid string) (*UserRecord, error)
	FindAddress(ctx context.Context, uuid, chainName string) (string, error)
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

func (repo *userRecordRepoImpl) FindOne(ctx context.Context, uuid string) (*UserRecord, error) {
	var record *UserRecord
	ret := repo.gormDB.WithContext(ctx).Table(userTable).Where("uuid = ?", uuid).Take(&record)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return record, nil
}

// FindAddress implements UserRecordRepo
func (repo *userRecordRepoImpl) FindAddress(ctx context.Context, uuid string, chainName string) (string, error) {
	var record *ChainAddress
	ret := repo.gormDB.WithContext(ctx).Table(chainAddressTable).Where("user_uuid = ?", uuid).Take(&record)
	if ret.Error != nil {
		return "", ret.Error
	}
	return record.Address, nil
}
