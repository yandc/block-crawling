package kanban

import (
	"context"
	"strings"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TrendingRecord struct {
	Id         int64           `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	Key        string          `json:"key" gorm:"type:character varying(100);index:,unique,composite:key_top_percent"`
	TopPercent int             `json:"topPercent" gorm:"type:int;index:,unique,composite:key_top_percent"`
	Rank       decimal.Decimal `json:"rank" gorm:"type:decimal(256,0)"`
	CreatedAt  int64           `json:"createdAt" gorm:"type:bigint"`
	UpdatedAt  int64           `json:"updatedAt" gorm:"type:bigint"`
}

type TrendingRepo interface {
	AutoMigrate(ctx context.Context, chainName string) error
	Save(ctx context.Context, chainName string, record *TrendingRecord) error
	Load(ctx context.Context, chainName, key string) ([]*TrendingRecord, error)
}

func NewTrendingRepo(db KanbanGormDB) TrendingRepo {
	return &trendingRepoImpl{
		db: db,
	}
}

type trendingRepoImpl struct {
	db *gorm.DB
}

// AutoMigrate implements TrendingRepo
func (r *trendingRepoImpl) AutoMigrate(ctx context.Context, chainName string) error {
	return r.db.Table(r.table(chainName)).AutoMigrate(&TrendingRecord{})
}

// Save implements TrendingRepo
func (r *trendingRepoImpl) Save(ctx context.Context, chainName string, record *TrendingRecord) error {
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{
				Name: "key",
			},
			{
				Name: "top_percent",
			},
		},
		UpdateAll: false,
		DoUpdates: clause.Assignments(map[string]interface{}{
			"key":         clause.Column{Table: "excluded", Name: "key"},
			"top_percent": clause.Column{Table: "excluded", Name: "top_percent"},
			"rank":        clause.Column{Table: "excluded", Name: "rank"},
			"updated_at":  clause.Column{Table: "excluded", Name: "updated_at"},
		}),
	}
	ret := r.db.WithContext(ctx).Table(r.table(chainName)).Clauses(onConflict).Create(record)
	if ret.Error != nil {
		return ret.Error
	}
	return nil
}

func (r *trendingRepoImpl) table(chainName string) string {
	return strings.ToLower(chainName) + "_trending"
}

func (r *trendingRepoImpl) Load(ctx context.Context, chainName string, key string) ([]*TrendingRecord, error) {
	var records []*TrendingRecord
	ret := r.db.WithContext(ctx).Table(r.table(chainName)).Where("key = ?", key).Order("top_percent DESC").Find(&records)
	if ret.Error != nil {
		return nil, ret.Error
	}
	return records, nil
}
