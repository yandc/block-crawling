package biz

import (
	v1 "block-crawling/api/helloworld/v1"
	"block-crawling/internal/data"
	"context"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"strconv"
	"time"
)

var (
	// ErrUserNotFound is user not found.
	ErrUserNotFound = errors.NotFound(v1.ErrorReason_USER_NOT_FOUND.String(), "user not found")
)

// GreeterUsecase is a Greeter usecase.
type GreeterUsecase struct {
	repo data.GreeterRepo
	lark *Lark
	log  *log.Helper
}

// NewGreeterUsecase new a Greeter usecase.
func NewGreeterUsecase(repo data.GreeterRepo, lark *Lark, logger log.Logger) *GreeterUsecase {
	return &GreeterUsecase{repo: repo, lark: lark, log: log.NewHelper(logger)}
}

// CreateGreeter create a Greeter, and returns the affected rows.
func (uc *GreeterUsecase) CreateGreeter(ctx context.Context, g *data.Greeter) (int64, error) {
	uc.log.WithContext(ctx).Infof("CreateGreeter: %v", g)
	return uc.repo.Save(ctx, g)
}

// CreateGreeters create sum Greeter, and returns the affected rows.
func (uc *GreeterUsecase) CreateGreeters(ctx context.Context, gs []*data.Greeter) (int64, error) {
	uc.log.WithContext(ctx).Infof("CreateGreeters: %v", gs)
	return uc.repo.BatchSaveOrUpdate(ctx, gs)
}

// UpdateGreeter update a Greeter, and returns the affected rows.
func (uc *GreeterUsecase) UpdateGreeter(ctx context.Context, g *data.Greeter) (int64, error) {
	uc.log.WithContext(ctx).Infof("UpdateGreeter: %v", g)
	return uc.repo.Update(ctx, g)
}

// FindByID find a Greeter, and returns the Greeter.
func (uc *GreeterUsecase) FindByID(ctx context.Context, id int64) (*data.Greeter, error) {
	uc.log.WithContext(ctx).Infof("FindGreeter: %v", id)
	greeterJson, _ := data.RedisClient.Get(strconv.FormatInt(id, 10)).Result()
	if greeterJson != "" {
		uc.log.WithContext(ctx).Infof("FindByID greeterJson: %v", greeterJson)
	} else {
		data.RedisClient.Set(strconv.FormatInt(id, 10), "1234567890", 60*time.Second)
	}
	uc.log.WithContext(ctx).Infof("LarkHost: %v", uc.lark.conf.LarkHost)
	timestamp, _ := GetAlarmTimestamp(strconv.FormatInt(id, 10))
	uc.log.WithContext(ctx).Infof("GetAlarmTimestamp: %v", timestamp)
	return uc.repo.FindByID(ctx, id)
}

// ListByID list Greeter, and returns the Greeters.
func (uc *GreeterUsecase) ListByID(ctx context.Context, id int64) ([]*data.Greeter, error) {
	uc.log.WithContext(ctx).Infof("ListGreeter: %v", id)
	return uc.repo.ListByID(ctx, id)
}

// ListAll list all Greeter, and returns the Greeters.
func (uc *GreeterUsecase) ListAll(ctx context.Context) ([]*data.Greeter, error) {
	uc.log.WithContext(ctx).Infof("ListAllGreeter")
	return uc.repo.ListAll(ctx)
}

// DeleteByID delete a Greeter, and returns the affected rows.
func (uc *GreeterUsecase) DeleteByID(ctx context.Context, id int64) (int64, error) {
	uc.log.WithContext(ctx).Infof("DeleteGreeter: %v", id)
	return uc.repo.DeleteByID(ctx, id)
}
