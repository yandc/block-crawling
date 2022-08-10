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

// Greeter is a Greeter model.
type Greeter struct {
	Id         int64  `json:"id" form:"id"`
	Username   string `json:"username" form:"username"`
	Password   string `json:"password" form:"password"`
	Remark     string `json:"remark,omitempty" form:"remark,omitempty"`
	CreateTime int64  `json:"createTime" form:"createTime"`
	UpdateTime int64  `json:"updateTime" form:"updateTime"`
}

func (user Greeter) TableName() string {
	return "t_user"
}

// GreeterRepo is a Greater repo.
type GreeterRepo interface {
	Save(context.Context, *Greeter) (int64, error)
	BatchSave(context.Context, []*Greeter) (int64, error)
	BatchSaveOrUpdate(context.Context, []*Greeter) (int64, error)
	Update(context.Context, *Greeter) (int64, error)
	FindByID(context.Context, int64) (*Greeter, error)
	ListByID(context.Context, int64) ([]*Greeter, error)
	ListAll(context.Context) ([]*Greeter, error)
	DeleteByID(context.Context, int64) (int64, error)
}

type GreeterRepoImpl struct {
	gormDB *gorm.DB
}

// NewGreeterRepo new a greeter repo.
func NewGreeterRepo(gormDB *gorm.DB) GreeterRepo {
	return &GreeterRepoImpl{
		gormDB: gormDB,
	}
}

func (r *GreeterRepoImpl) Save(ctx context.Context, user *Greeter) (int64, error) {
	ret := r.gormDB.Create(user)
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), "duplicate key value") {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, id:" + strconv.FormatInt(user.Id, 10), Data: 0}
			log.Warne("insert user failed", err)
		} else {
			fmt.Printf("insert user failed, err: %v\n", err)
			log.Errore("insert user failed", err)
		}
		return 0, err
	}
	fmt.Printf("insert user success, the id is %d.\n", user.Id)

	affected := ret.RowsAffected
	fmt.Printf("insert user success, the affected row is %d.\n", affected)
	return affected, err
}

func (r *GreeterRepoImpl) BatchSave(ctx context.Context, users []*Greeter) (int64, error) {
	ret := r.gormDB.CreateInBatches(users, len(users))
	err := ret.Error
	if err != nil {
		if strings.Contains(fmt.Sprintf("%s", err), POSTGRES_DUPLICATE_KEY) {
			err = &common.ApiResponse{Code: 200, Status: false,
				Msg: "duplicate key value, size:" + strconv.Itoa(len(users)), Data: 0}
			log.Warne("batch insert user failed", err)
		} else {
			fmt.Printf("batch insert user failed, err: %v\n", err)
			log.Errore("batch insert user failed", err)
		}
		return 0, err
	}
	fmt.Printf("batch insert user success, size is %d.\n", len(users))

	affected := ret.RowsAffected
	fmt.Printf("batch insert user success, the affected row is %d.\n", affected)
	return affected, err
}

func (r *GreeterRepoImpl) BatchSaveOrUpdate(ctx context.Context, users []*Greeter) (int64, error) {
	sqlStr := "insert into public.t_user (username, password, remark, create_time, update_time) values "
	usersLen := len(users)
	for i := 0; i < usersLen; i++ {
		user := users[i]
		sqlStr += "('" + user.Username + "', '" + user.Password + "', '" + user.Remark + "', " +
			strconv.Itoa(int(user.CreateTime)) + ", " + strconv.Itoa(int(user.UpdateTime)) + "),"
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	sqlStr += " on conflict (username) do update set username = excluded.username, password = excluded.password, " +
		"remark = excluded.remark, update_time = excluded.update_time"

	ret := r.gormDB.Exec(sqlStr)
	err := ret.Error
	if err != nil {
		fmt.Printf("batch insert or update user failed, err: %v\n", err)
		log.Errore("batch insert or update user failed", err)
		return 0, err
	}
	fmt.Printf("batch insert or update user success, size is %d.\n", len(users))

	affected := ret.RowsAffected
	fmt.Printf("batch insert or update user success, the affected row is %d.\n", affected)
	return affected, err
}

func (r *GreeterRepoImpl) Update(ctx context.Context, user *Greeter) (int64, error) {
	ret := r.gormDB.Model(&Greeter{}).Where("id = ?", user.Id).Updates(user)
	err := ret.Error
	if err != nil {
		fmt.Printf("update user failed, err:%v\n", err)
		log.Errore("update user failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	fmt.Printf("update user success, the affected row is %d.\n", affected)
	return affected, nil
}

func (r *GreeterRepoImpl) FindByID(ctx context.Context, id int64) (*Greeter, error) {
	var user *Greeter
	ret := r.gormDB.First(&user, id)
	err := ret.Error
	if err != nil {
		if fmt.Sprintf("%s", err) == POSTGRES_NOT_FOUND {
			err = nil
		} else {
			fmt.Printf("query user failed, err:%v\n", err)
			log.Errore("query user failed", err)
		}
		return nil, err
	}
	fmt.Printf("user:%v\n", user)

	return user, nil
}

func (r *GreeterRepoImpl) ListByID(ctx context.Context, id int64) ([]*Greeter, error) {
	var userList []*Greeter
	ret := r.gormDB.Where("id > ?", id).Find(&userList)
	err := ret.Error
	if err != nil {
		fmt.Printf("query user failed, err:%v\n", err)
		log.Errore("query user failed", err)
		return nil, err
	}

	rows := ret.RowsAffected
	fmt.Printf("rows:%v, userList:%v\n", rows, userList)

	return userList, nil
}

func (r *GreeterRepoImpl) ListAll(context.Context) ([]*Greeter, error) {
	var userList []*Greeter
	ret := r.gormDB.Find(&userList)
	err := ret.Error
	if err != nil {
		fmt.Printf("query user failed, err:%v\n", err)
		log.Errore("query user failed", err)
		return nil, err
	}

	rows := ret.RowsAffected
	fmt.Printf("rows:%v, userList:%v\n", rows, userList)

	return userList, nil
}

func (r *GreeterRepoImpl) DeleteByID(ctx context.Context, id int64) (int64, error) {
	ret := r.gormDB.Delete(&Greeter{}, id)
	err := ret.Error
	if err != nil {
		fmt.Printf("delete user failed, err:%v\n", err)
		log.Errore("delete user failed", err)
		return 0, err
	}

	affected := ret.RowsAffected
	fmt.Printf("delete user success, the affected row is %d.\n", affected)
	return affected, nil
}
