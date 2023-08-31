package kanban

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"block-crawling/internal/log"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/common"
	"context"
	"sync"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type TimeMachine struct {
	bc      *conf.Bootstrap
	db      *gorm.DB
	bundle  *kanban.Bundle
	plats   []*timeMachineBootstrap
	options *Options
	mr      data.MigrationRepo
}

func NewTimeMachine(mr data.MigrationRepo, bc *conf.Bootstrap, db kanban.KanbanGormDB, s platform.Server, bundle *kanban.Bundle, options *Options) *TimeMachine {
	return &TimeMachine{
		bc:      bc,
		db:      db,
		bundle:  bundle,
		plats:   make([]*timeMachineBootstrap, 0, 4),
		options: options,
		mr:      mr,
	}
}

func (tm *TimeMachine) Start(ctx context.Context) error {
	defer tm.options.Cancel()
	for _, item := range iterChains(tm.bc) {
		if item.Chain != tm.options.ChainName {
			continue
		}

		log.Info("START TIMEMACHINE", zap.String("chainName", item.Chain))
		p := newTimeMachineBootstrap(tm.mr, tm.db, 1, item, tm.bundle, tm.options)
		tm.plats = append(tm.plats, p)
		p.Start(ctx)

	}
	return nil
}

func (tm *TimeMachine) Stop(ctx context.Context) error {
	return nil
}

func newTimeMachineBootstrap(mr data.MigrationRepo, db *gorm.DB, startHeight int64, conf *conf.PlatInfo, bundle *kanban.Bundle, options *Options) *timeMachineBootstrap {
	plat := biz.PlatformMap[conf.Chain]
	tp := &timeMachinePlatform{
		bundle:      bundle,
		conf:        conf,
		Platform:    plat,
		db:          db,
		chainName:   conf.Chain,
		chainType:   conf.Type,
		startHeight: uint64(startHeight),
		stop:        options.Cancel,
		mr:          mr,
	}
	return &timeMachineBootstrap{
		platform: tp,
		conf:     conf,
		ctx:      options.Context,
		cancel:   options.Cancel,
	}
}

type timeMachineBootstrap struct {
	platform biz.Platform
	conf     *conf.PlatInfo
	ctx      context.Context
	cancel   func()
}

func (b *timeMachineBootstrap) Start(ctx context.Context) error {
	nodeURL := b.conf.GetKanbanFullRPCURL()

	if len(nodeURL) == 0 {
		nodeURL = b.conf.RpcURL
	}

	value := b.conf

	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		clients = append(clients, b.platform.CreateClient(url))
	}
	spider := chain.NewBlockSpider(b.platform.CreateStateStore(), clients...)

	spider.StartIndexBlockWithContext(
		b.ctx,
		b.platform.CreateBlockHandler(time.Minute),
		int(value.GetSafelyConcurrentBlockDelta()),
		int(b.conf.GetMaxConcurrency()),
	)
	return nil
}

func (b *timeMachineBootstrap) Stop(ctx context.Context) error {
	b.cancel()
	return nil
}

type timeMachinePlatform struct {
	biz.Platform

	conf        *conf.PlatInfo
	bundle      *kanban.Bundle
	db          *gorm.DB
	chainName   string
	chainType   string
	startHeight uint64
	stopHeight  uint64
	stop        func()
	mr          data.MigrationRepo
}

func (tp *timeMachinePlatform) CreateStateStore() chain.StateStore {
	bstore := common.NewStateStore(tp.chainName, nil)
	if curHeight, err := bstore.LoadHeight(); err != nil {
		log.Error("LOAD CUR HEIGHT FAILED WITH ERROR", zap.Error(err), zap.String("chainName", tp.chainName))
	} else {
		log.Info("LOAD CUR HEIGHT TO STOP", zap.Uint64("stopHeight", curHeight), zap.String("chainName", tp.chainName))
		tp.stopHeight = curHeight
	}
	store := common.NewStateStore(tp.chainName+":kanban", nil)
	// store.StoreHeight(tp.startHeight)
	return store
}

func (tp *timeMachinePlatform) CreateBlockHandler(liveInterval time.Duration) chain.BlockHandler {
	return &timeMachineHandler{
		BlockHandler: tp.Platform.CreateBlockHandler(liveInterval),
		conf:         tp.conf,
		db:           tp.db,
		bundle:       tp.bundle,
		chainName:    tp.chainName,
		chainType:    tp.chainType,
		stopHeight:   tp.stopHeight,
		stop:         tp.stop,
		lock:         &sync.Mutex{},
		mr:           tp.mr,
		migrated:     make(map[string]bool),
		agger:        createChainAggerator(tp.conf, tp.bundle),
	}
}

type timeMachineHandler struct {
	chain.BlockHandler

	conf       *conf.PlatInfo
	db         *gorm.DB
	bundle     *kanban.Bundle
	chainName  string
	chainType  string
	stopHeight uint64
	stop       func()
	lock       *sync.Mutex
	migrated   map[string]bool
	agger      *chainCursorAggerator
	mr         data.MigrationRepo
}

// OnNewBlock implements chain.BlockHandler
func (h *timeMachineHandler) OnNewBlock(client chain.Clienter, chainHeight uint64, block *chain.Block) (chain.TxHandler, error) {
	if block.Number >= h.stopHeight {
		if err := h.agger.AccumulateAddresses(context.Background()); err != nil {
			return nil, err
		}
		h.stop()
	}
	table := kanban.GetShardingTable(biz.GetTableName(h.chainName), block.Time)

	h.lock.Lock()
	if _, ok := h.migrated[table]; !ok {
		yesterday := block.Time - (24 * 3600)
		lastTable := kanban.GetShardingTable(biz.GetTableName(h.chainName), yesterday)
		if _, ok := h.migrated[lastTable]; ok {
			go aggerateOneDayOneChain(context.Background(), h.conf, h.bundle, h.agger, yesterday)
		}

		h.migrated[table] = true
		biz.DynamicCreateTable(h.mr, h.db, table, h.chainType)
	}
	h.lock.Unlock()

	return h.BlockHandler.OnNewBlock(client, chainHeight, block)
}
