package kanban

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data/kanban"
	"block-crawling/internal/log"
	"context"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Aggerator struct {
	bc      *conf.Bootstrap
	bundle  *kanban.Bundle
	options *Options
}

func NewAggerator(bc *conf.Bootstrap, bundle *kanban.Bundle, options *Options) *Aggerator {
	return &Aggerator{
		bc:      bc,
		bundle:  bundle,
		options: options,
	}
}

func (a *Aggerator) Start(ctx context.Context) error {
	if a.options.RunPeriodically {
		go func() {
			for {
				sleepUntilTomorrow()
				yesterday := time.Now().Unix() - (24 * 3600)
				yesterdayStart := yesterday - yesterday%(24*3600)
				a.options.AggerateTime = yesterdayStart
				if err := a.run(ctx); err != nil {
					log.Errore("KANBAN AGGERATING", err)
				}
			}
		}()
		return nil
	} else {
		defer a.options.Cancel()
		return a.run(ctx)
	}

}
func (a *Aggerator) run(ctx context.Context) error {
	for _, platInfo := range iterChains(a.bc) {
		agger := createChainAggerator(platInfo, a.bundle)

		if a.options.AggerateOnly == "day" || a.options.AggerateOnly == "" {
			if a.options.AggerateRange == nil {
				if err := aggerateOneDayOneChain(ctx, platInfo, a.bundle, agger, a.options.AggerateTime); err != nil {
					return err
				}
			} else {
				start := a.options.AggerateRange[0]
				stop := a.options.AggerateRange[1]
				for start < stop {
					println(time.Unix(start, 0).Format("2006-01-02"))
					if err := aggerateOneDayOneChain(ctx, platInfo, a.bundle, agger, start); err != nil {
						return err
					}
					start += 24 * 3600
				}
			}
		}
		if a.options.AggerateOnly == "accumulating" || a.options.AggerateOnly == "" {
			if a.options.AggerateOnly == "accumulating" {
				if err := agger.LoadAllAddresses(ctx); err != nil {
					return err
				}
			}

			if err := agger.AccumulateAddresses(ctx); err != nil {
				return err
			}
		}
		if a.options.AggerateOnly == "trending" || a.options.AggerateOnly == "" {
			if err := agger.GenerateTrending(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Aggerator) Stop(ctx context.Context) error {
	return nil
}

func createChainAggerator(platInfo *conf.PlatInfo, bundle *kanban.Bundle) *chainCursorAggerator {
	switch platInfo.Type {
	case biz.EVM:
		return newChainCursorAggerator(platInfo.Chain, bundle, bundle.EVM)
	}
	return nil
}

func aggerateOneDayOneChain(ctx context.Context, platInfo *conf.PlatInfo, bundle *kanban.Bundle, agger *chainCursorAggerator, txTime int64) error {
	log.Info(
		"AGGERATING",
		zap.String("chainName", platInfo.Chain),
		zap.String("sharding", kanban.Sharding(txTime)),
	)
	bundle.Wallet.AutoMigrate(ctx, platInfo.Chain)
	bundle.Trending.AutoMigrate(ctx, platInfo.Chain)
	return agger.AggerateDaySummary(ctx, txTime)
}

type chainCursorAggerator struct {
	chainName string
	bundle    *kanban.Bundle
	cursor    kanban.TxRecordCursor
	addresses map[string]bool
}

func newChainCursorAggerator(chainName string, bundle *kanban.Bundle, cursor kanban.TxRecordCursor) *chainCursorAggerator {
	return &chainCursorAggerator{
		chainName: chainName,
		cursor:    cursor,
		bundle:    bundle,
		addresses: make(map[string]bool),
	}
}

func (a *chainCursorAggerator) AggerateDaySummary(ctx context.Context, txTime int64) error {
	var nextCursor uint64
	pageLimit := 1_000

	sharding := kanban.GetShardingTable(a.chainName, txTime)

	contracts := make(map[string]*kanban.WalletContractRecord)
	summaries := make(map[string]*kanban.WalletDaySummaryRecord)

	for {
		txRecords, err := a.cursor.CursorList(ctx, txTime, a.chainName, pageLimit, &nextCursor)
		if err != nil {
			return err
		}
		log.Info(
			"LOAD TX RECORDS",
			zap.String("chainName", a.chainName),
			zap.Uint64("cursor", nextCursor),
			zap.Int("pageLimit", pageLimit),
			zap.Int("len", len(txRecords)),
			zap.String("sharding", sharding),
		)
		for _, item := range txRecords {
			if item.Status != biz.SUCCESS {
				continue
			}

			for _, c := range item.IntoContracts() {
				if _, ok := contracts[c.Key()]; ok {
					contracts[c.Key()].Merge(c)
				} else {
					contracts[c.Key()] = c
				}
				a.addresses[c.Address] = true
			}

			for _, s := range item.IntoDaySummary() {
				if _, ok := summaries[s.Key()]; ok {
					summaries[s.Key()].Merge(s)
				} else {
					summaries[s.Key()] = s
				}
				a.addresses[s.Address] = true
			}
		}
		if len(txRecords) < pageLimit {
			break
		}
	}

	batchSize := 100
	counterBatch := make([]*kanban.WalletContractRecord, 0, batchSize)
	saved := 0
	for _, c := range contracts {
		counterBatch = append(counterBatch, c)
		if len(counterBatch) >= batchSize {

			if err := a.bundle.Wallet.BatchSaveContract(ctx, a.chainName, counterBatch); err != nil {
				log.Error("SAVE CONTRACT FAILED WITH ERROR", zap.Error(err), zap.Any("record", c), zap.String("chainName", a.chainName))
				return err
			}
			saved += len(counterBatch)

			if saved%10000 == 0 {
				log.Info(
					"BATCH SAVE CONTRACTS",
					zap.String("chainName", a.chainName),
					zap.Int("totalSize", len(contracts)),
					zap.Int("batchSize", batchSize),
					zap.String("sharding", sharding),
					zap.Int("saved", saved),
				)
			}

			counterBatch = counterBatch[:0]
		}
	}

	if len(counterBatch) > 0 {
		if err := a.bundle.Wallet.BatchSaveContract(ctx, a.chainName, counterBatch); err != nil {
			log.Error("SAVE CONTRACT FAILED WITH ERROR", zap.Error(err), zap.String("chainName", a.chainName))
			return err
		}
		saved += len(counterBatch)

		log.Info(
			"BATCH SAVE CONTRACTS",
			zap.String("chainName", a.chainName),
			zap.Int("totalSize", len(contracts)),
			zap.String("sharding", sharding),
			zap.Int("batchSize", batchSize),
			zap.Int("saved", saved),
		)

		counterBatch = counterBatch[:0]
	}

	saved = 0
	summaryBatch := make([]*kanban.WalletDaySummaryRecord, 0, batchSize)
	for _, s := range summaries {
		summaryBatch = append(summaryBatch, s)
		if len(summaryBatch) >= batchSize {
			if err := a.bundle.Wallet.BatchSaveDaySummary(ctx, a.chainName, summaryBatch); err != nil {
				log.Error("SAVE DAY SUMMARY FAILED WITH ERROR", zap.Error(err), zap.String("chainName", a.chainName))
				return err
			}
			saved += len(summaryBatch)
			if saved%10000 == 0 {
				log.Info(
					"BATCH SAVE DAY SUMMARY",
					zap.String("chainName", a.chainName),
					zap.Int("totalSize", len(summaries)),
					zap.String("sharding", sharding),
					zap.Int("batchSize", batchSize),
					zap.Int("saved", saved),
				)
			}
			summaryBatch = summaryBatch[:0]
		}
	}

	if len(summaryBatch) > 0 {
		if err := a.bundle.Wallet.BatchSaveDaySummary(ctx, a.chainName, summaryBatch); err != nil {
			log.Error("SAVE DAY SUMMARY FAILED WITH ERROR", zap.Error(err), zap.String("chainName", a.chainName))
			return err
		}
		saved += len(summaryBatch)
		log.Info(
			"BATCH SAVE DAY SUMMARY",
			zap.String("chainName", a.chainName),
			zap.String("sharding", sharding),
			zap.Int("totalSize", len(summaries)),
			zap.Int("batchSize", batchSize),
			zap.Int("saved", saved),
		)
		summaryBatch = summaryBatch[:0]
	}
	return nil
}

func (a *chainCursorAggerator) LoadAllAddresses(ctx context.Context) error {
	var cursor uint64
	pageLimit := 1_000
	for {
		addresses, err := a.bundle.Wallet.LoadAllAddresses(ctx, a.chainName, &cursor, pageLimit)
		if err != nil {
			return err
		}

		for _, addr := range addresses {
			a.addresses[addr] = true
		}

		if len(addresses) < pageLimit {
			return nil
		}
	}
}

func (a *chainCursorAggerator) AccumulateAddresses(ctx context.Context) error {
	accNum := 0
	for addr := range a.addresses {
		if addr == "" {
			continue
		}

		if err := a.bundle.Wallet.Accumulate(ctx, a.chainName, addr); err != nil {
			if err == gorm.ErrRecordNotFound {
				continue
			}

			log.Error("ACCUMULATE FAILED WITH ERROR", zap.Error(err), zap.Any("address", addr), zap.String("chainName", a.chainName))
			return err
		}
		accNum++
		if accNum%1000 == 0 {
			log.Info("ACCUMULATING", zap.Int("total", len(a.addresses)), zap.Int("accumulated", accNum))
		}
	}
	return nil
}

func (a *chainCursorAggerator) GenerateTrending(ctx context.Context) error {
	var trendingKeys = []string{
		"total_tx_num",
		"total_tx_amount",
		"total_tx_in_amount",
		"total_contract_num",
	}
	for _, key := range trendingKeys {
		count, err := a.bundle.Wallet.Count(ctx, a.chainName, key)
		if err != nil {
			log.Error("COUNT ADDRESS FAILED WITH ERROR", zap.Error(err), zap.String("chainName", a.chainName))
			return err
		}
		for percent := 1; percent < 100; percent++ {
			rank, err := a.bundle.Wallet.GetTopPercentRank(ctx, a.chainName, count, key, percent)
			if err != nil {
				log.Error(
					"GET TOP PERCENT RANK FAILED WITH ERROR",
					zap.Error(err),
					zap.String("chainName", a.chainName),
					zap.String("key", key),
					zap.Int("percent", percent),
					zap.Int64("count", count),
				)
				return err
			}
			record := &kanban.TrendingRecord{
				Key:        key,
				TopPercent: percent,
				Rank:       rank,
				CreatedAt:  time.Now().Unix(),
				UpdatedAt:  time.Now().Unix(),
			}
			if err := a.bundle.Trending.Save(ctx, a.chainName, record); err != nil {
				log.Error(
					"SAVE RANK FAILED WITH ERROR",
					zap.Error(err),
					zap.String("chainName", a.chainName),
					zap.Any("record", record),
				)
			}
		}
	}
	return nil
}
