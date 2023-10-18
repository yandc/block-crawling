package data

import (
	"block-crawling/internal/log"
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SwapContract struct {
	Id        int64  `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ChainName string `json:"chainName" gorm:"type:character varying(100);index:,unique,composite:unique_chain_name_contract"`
	Contract  string `json:"contract" gorm:"type:character varying(512);index:,unique,composite:unique_chain_name_contract"`
	DEX       string `json:"dex" gorm:"type:character varying(100)"`
	TxHash    string `json:"txHash" gorm:"type:character varying(512)"`
	CreatedAt int64  `json:"createdAt"`
}

var SwapContractRepoClient SwapContractRepo
var swapContractMemCache sync.Map

type SwapContractRepo interface {
	BatchSave(ctx context.Context, chainName string, records []*SwapContract) error
	FindOne(ctx context.Context, chainName, contract string) (*SwapContract, error)
	Is(chainName, contract string) bool
}

type swapContractRepoImpl struct {
	db *gorm.DB
}

// FindOne implements SwapContractRepo
func (r *swapContractRepoImpl) FindOne(ctx context.Context, chainName string, contract string) (*SwapContract, error) {
	var result *SwapContract
	dbRet := r.db.WithContext(ctx).Where("chain_name=? AND contract=?", chainName, contract).First(&result)

	if dbRet.Error == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if dbRet.Error != nil {
		return nil, dbRet.Error
	}
	return result, nil
}

// Save implements SwapContractRepo
func (r *swapContractRepoImpl) BatchSave(ctx context.Context, chainName string, records []*SwapContract) error {
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "chain_name"},
			{Name: "contract"},
		},
		DoNothing: true,
	}).Create(&records).Error
	r.setMemCache(records)
	return err
}

// init load all contracts from database to the memeory to avoid put too much
// pressure on database.(Have tried redis, also too much pressure to it.)
func (r *swapContractRepoImpl) init(ctx context.Context) error {
	var cursor int64

	for {
		query := r.db.WithContext(ctx).Where("id > ?", cursor)
		var records []*SwapContract
		query = query.Order("id ASC").Limit(100).Find(&records)
		if query.Error != nil {
			return query.Error
		}
		if len(records) == 0 {
			break
		}
		r.setMemCache(records)
		log.Info("[SWAP] INITIALIZED IN-MEMORY CACHE", zap.Int64("cursor", cursor), zap.Int("num", len(records)))
		cursor = records[len(records)-1].Id
	}
	return nil
}

func (r *swapContractRepoImpl) setMemCache(records []*SwapContract) {
	for _, record := range records {
		swapContractMemCache.Store(r.key(record.ChainName, record.Contract), true)
	}
}

// Is implements SwapContractRepo
func (r *swapContractRepoImpl) Is(chainName string, contract string) bool {
	_, ok := swapContractMemCache.Load(r.key(chainName, contract))
	return ok
}

func (r *swapContractRepoImpl) key(chainName, contract string) string {
	return fmt.Sprintf(chainName, ":", contract)
}

func NewSwapContractRepo(db *gorm.DB) SwapContractRepo {
	repo := &swapContractRepoImpl{
		db: db,
	}
	if err := repo.init(context.Background()); err != nil {
		log.Error("[SWAP] INIT IN-MEMORY CACHED FAILED", zap.Error(err))
	}
	SwapContractRepoClient = repo
	return SwapContractRepoClient
}
