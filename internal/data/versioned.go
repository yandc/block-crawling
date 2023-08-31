package data

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// VersionMarker provides a Version method to indicate the table's current version.
// Which is useful when we do migration to help us prevent unnecessary opeartion.
type VersionMarker interface {
	Version() string
}

type DefaultVersionMarkerIn struct {
}

func (*DefaultVersionMarkerIn) Version() string {
	return "20230831"
}

type Migration struct {
	Id        uint64 `gorm:"primary_key;AUTO_INCREMENT"`
	Table     string `gorm:"type:character varying(512);index:,unique,composite:unique_table_version"`
	Version   string `gorm:"type:character varying(100);index:,unique,composite:unique_table_version"`
	CreatedAt int64  `gorm:"type:bigint"`
}

type MigrationRepo interface {
	FindOne(ctx context.Context, table, version string) (*Migration, error)
	Create(ctx context.Context, m *Migration) error
}

type migrationRepoImpl struct {
	db *gorm.DB
}

// Create implements MigrationRepo
func (r *migrationRepoImpl) Create(ctx context.Context, m *Migration) error {
	return r.db.WithContext(ctx).Create(m).Error
}

// FindOne implements MigrationRepo
func (r *migrationRepoImpl) FindOne(ctx context.Context, table string, version string) (*Migration, error) {
	var result *Migration
	ret := r.db.WithContext(ctx).Where("\"table\" = ? AND version = ?", table, version).Take(&result)
	if errors.Is(ret.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return result, ret.Error
}

func NewMigrationRepo(db *gorm.DB) MigrationRepo {
	return &migrationRepoImpl{
		db: db,
	}
}
