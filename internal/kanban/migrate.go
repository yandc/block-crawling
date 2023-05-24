package kanban

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"context"
	"time"

	"gorm.io/gorm"
)

func iterChains(bc *conf.Bootstrap) []*conf.PlatInfo {
	chains := make([]*conf.PlatInfo, 0, 4)
	chainNames := make(map[string]bool)
	for _, item := range bc.Platform {
		if _, ok := chainNames[item.Chain]; ok {
			continue
		}
		chainNames[item.Chain] = true

		if item.GetEnableKanban() {
			chains = append(chains, item)
		}
	}
	for _, item := range bc.PlatformTest {

		if _, ok := chainNames[item.Chain]; ok {
			continue
		}
		chainNames[item.Chain] = true

		if item.GetEnableKanban() {
			chains = append(chains, item)
		}
	}
	return chains
}

func NewMigrateScheduler(bc *conf.Bootstrap, bundle *kanban.Bundle, s *biz.TransactionUsecase, db kanban.KanbanGormDB) *MigrateScheduler {
	return &MigrateScheduler{
		chains: iterChains(bc),
		db:     db,
	}
}

type MigrateScheduler struct {
	chains []*conf.PlatInfo
	db     *gorm.DB
}

func (s *MigrateScheduler) Start(ctx context.Context) error {
	for _, platInfo := range s.chains {
		tableName := biz.GetTableName(platInfo.Chain)
		today := time.Now().Unix()
		tomorrow := today + 3600*24
		biz.DynamicCreateTable(s.db, kanban.GetShardingTable(tableName, today), platInfo.Type)
		biz.DynamicCreateTable(s.db, kanban.GetShardingTable(tableName, tomorrow), platInfo.Type)
	}
	return nil
}

func (s *MigrateScheduler) Stop(ctx context.Context) error {
	return nil
}

type RecordSync struct {
	chains       []*conf.PlatInfo
	dataBundle   *data.Bundle
	kanbanBundle *kanban.Bundle
	options      *Options
}

func NewRecordSync(bc *conf.Bootstrap, dataBundle *data.Bundle, kanbanBundle *kanban.Bundle, option *Options) *RecordSync {
	return &RecordSync{
		chains:       iterChains(bc),
		dataBundle:   dataBundle,
		kanbanBundle: kanbanBundle,
		options:      option,
	}
}

func (s *RecordSync) Start(ctx context.Context) error {
	defer s.options.Cancel()
	pageLimit := 1_000
	var cursor int64
	for _, platInfo := range s.chains {
		tableName := data.GetTableName(platInfo.Chain)
		switch platInfo.Type {
		case biz.EVM:
			dataEVM := s.dataBundle.EVM
			kanbanEVM := s.kanbanBundle.EVM
			for {
				records, err := dataEVM.CursorListAll(ctx, tableName, &cursor, pageLimit)
				if err != nil {
					return err
				}

				for _, item := range records {
					item.Id = 0
				}
				nrows, err := kanbanEVM.BatchSaveOrUpdateSelective(ctx, tableName, records)
				if err != nil {
					return err
				}
				println("Sync", nrows, "items for", tableName)

				if len(records) < pageLimit {
					break
				}
			}
		}
	}
	return nil
}

func (s *RecordSync) Stop(ctx context.Context) error {
	return nil
}
