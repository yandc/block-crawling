package kanban

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
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

type MigrateScheduler struct {
	chains  []*conf.PlatInfo
	db      *gorm.DB
	options *Options
	m       data.MigrationRepo
}

func NewMigrateScheduler(m data.MigrationRepo, bc *conf.Bootstrap, bundle *kanban.Bundle, s *biz.TransactionUsecase, db kanban.KanbanGormDB, options *Options) *MigrateScheduler {
	return &MigrateScheduler{
		chains:  iterChains(bc),
		db:      db,
		m:       m,
		options: options,
	}
}
func (s *MigrateScheduler) Start(ctx context.Context) error {
	if err := s.schudule(ctx); err != nil {
		return err
	}
	if s.options.RunPeriodically {
		channel := "migrate-scheduler"
		go func() {
			for {
				sleepUntilTomorrow(s.options.ChainName, channel)
				if err := s.schudule(ctx); err != nil {
					log.Errore("MIGRATE", err)
					alarmMsg := fmt.Sprintf("请注意：%s链创建看板分表失败", s.options.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					alarmOpts = biz.WithAlarmChannel("kanban")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				} else {
					setJobsDone(s.options.ChainName, channel)
				}
			}
		}()
	}
	return nil
}

func (s *MigrateScheduler) schudule(ctx context.Context) error {
	for _, platInfo := range s.chains {
		tableName := biz.GetTableName(platInfo.Chain)
		today := time.Now().Unix()
		tomorrow := today + 3600*24
		biz.DynamicCreateTable(s.m, s.db, kanban.GetShardingTable(tableName, today), platInfo.Type)
		biz.DynamicCreateTable(s.m, s.db, kanban.GetShardingTable(tableName, tomorrow), platInfo.Type)
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
			agger := newChainCursorAggerator(platInfo.Chain, s.kanbanBundle, nil)

			txRecords := make([]*kanban.TxRecord, 0, pageLimit)
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

				for _, item := range records {
					txRecords = append(txRecords, kanban.EVMRecordIntoTxRecord(&kanban.KanbanEvmTransactionRecord{
						EvmTransactionRecord: *item,
					}))
				}
				if err := agger.parser.Parse(txRecords); err != nil {
					return err
				}

				txRecords = txRecords[:0]
				if len(records) < pageLimit {
					break
				}
			}

			if err := agger.parser.Save(ctx, platInfo.Chain, tableName); err != nil {
				return err
			}
			if err := agger.AccumulateAddresses(ctx); err != nil {
				return err
			}
			if err := agger.GenerateTrending(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RecordSync) Stop(ctx context.Context) error {
	return nil
}

func sleepUntilTomorrow(chainName, channel string) int {
	for {
		now := time.Now().Unix()
		dayStart := kanban.TimeSharding(now)
		if now-dayStart >= 3600 {
			key := redisKey(chainName, channel)
			exists, err := data.RedisClient.Exists(key).Result()
			log.Info(
				"KANBAN JOBS ARE READY TO START",
				zap.String("chainName", chainName),
				zap.String("channel", channel),
				zap.String("redisKey", key),
				zap.Int64("redisRet", exists),
			)
			if err != nil {
				log.Error(
					"KANBAN JOBS REDIS ERROR",
					zap.Error(err),
					zap.String("chainName", chainName),
					zap.String("channel", channel),
				)
			} else if exists != 1 {
				return int(dayStart)
			}
		}
		sleepSces := 3600 - now%3600
		log.Info(
			"KANBAN JOBS MIGHT DONE",
			zap.String("chainName", chainName),
			zap.String("channel", channel),
			zap.Int64("sleepSecs", sleepSces),
		)
		time.Sleep(time.Second * time.Duration(sleepSces+1))
	}
}

func redisKey(chainName, channel string) string {
	now := time.Now().Unix()
	dayStart := kanban.TimeSharding(now)
	key := fmt.Sprintf("kanban:done:%s:%s:%d", chainName, channel, dayStart)
	return key
}

func setJobsDone(chainName, channel string) error {
	return data.RedisClient.Set(redisKey(chainName, channel), "1", time.Hour*24).Err()
}
