package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/scheduling"
	"block-crawling/internal/utils"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/transport"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Bootstrap struct {
	ChainName string
	Conf      *conf.PlatInfo
	Platform  biz.Platform
	Spider    *chain.BlockSpider
	cancel    func()
	ctx       context.Context

	pCtx    context.Context
	pCancel func()

	db         *gorm.DB
	mr         data.MigrationRepo
	available  int32
	stateStore chain.StateStore
}

var startChainMap = &sync.Map{}
var startCustomChainMap = &sync.Map{}
var chainLock sync.RWMutex

func NewBootstrap(p biz.Platform, value *conf.PlatInfo, db *gorm.DB, mr data.MigrationRepo) *Bootstrap {
	ctx, cancel := context.WithCancel(context.Background())
	pCtx, pCancel := context.WithCancel(context.Background())
	nodeURL := value.RpcURL
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		clients = append(clients, p.CreateClient(url))
	}
	bootstrap := &Bootstrap{
		ChainName:  value.Chain,
		Platform:   p,
		Conf:       value,
		cancel:     cancel,
		ctx:        ctx,
		db:         db,
		pCtx:       pCtx,
		pCancel:    pCancel,
		available:  1,
		stateStore: p.CreateStateStore(),
		mr:         mr,
	}

	bootstrap.Spider = chain.NewBlockSpider(bootstrap.stateStore, clients...)
	if len(value.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(value.StandbyRPCURL))
		for _, url := range value.StandbyRPCURL {
			c := p.CreateClient(url)
			standby = append(standby, c)
		}
		bootstrap.Spider.AddStandby(standby...)
	}
	bootstrap.Spider.Watch(common.NewDectorZapWatcher(value.Chain, bootstrap.onAvailablityChanged))

	if value.GetRoundRobinConcurrent() {
		bootstrap.Spider.EnableRoundRobin()
	}
	p.SetBlockSpider(bootstrap.Spider)
	bootstrap.Spider.SetHandlingTxsConcurrency(int(value.GetHandlingTxConcurrency()))
	return bootstrap
}

func (b *Bootstrap) Stop() {
	b.cancel()
	b.pCancel()
}

func (b *Bootstrap) Start() {
	b.setAvailablity(true)
	defer func() {
		if err := recover(); err != nil {
			log.Error("main panic:", zap.Any("", err))
		}
	}()

	table := strings.ToLower(b.Conf.Chain) + biz.TABLE_POSTFIX
	biz.DynamicCreateTable(b.mr, b.db, table, b.Conf.Type)
	if biz.IsBenfenNet(b.Conf.Chain) {
		data.AutoMigrateBFStationTables(b.db, b.Conf.Chain)
	}

	go func() {
		if !b.shouldStartIndexing() {
			return
		}
		log.Info("我启动啦", zap.Any(b.ChainName, b))
		b.GetTransactions()
	}()

	// get inner memerypool
	go func() {
		time.Sleep(time.Duration(utils.RandInt32(0, 300)) * time.Second)
		log.Info("start main BACKGROUND PENDING AND MONITORING", zap.Any("platform", b))
		defer log.Info("stop main BACKGROUND PENDING AND MONITORING", zap.Any("platform", b))
		// get result
		go b.GetTransactionResultByTxhash()

		resultPlan := time.NewTicker(time.Duration(5) * time.Minute)
		for {
			select {
			case <-resultPlan.C:
				go b.GetTransactionResultByTxhash()
				if b.isAvailable() && b.shouldStartIndexing() {
					go b.Platform.MonitorHeight(b.onAvailablityChanged, b.liveInterval())
				}
			case <-b.pCtx.Done():
				resultPlan.Stop()
				return
			}
		}
	}()
}

func (b *Bootstrap) shouldStartIndexing() bool {
	if b.ChainName == "Solana" || b.ChainName == "SUITEST" || b.ChainName == "Kaspa" || b.ChainName == "SeiTEST" ||
		b.ChainName == "CelestiaMochaTEST" || b.ChainName == "STC" || b.ChainName == "Polkadot" || b.ChainName == "Cosmos" {
		return false
	}
	return true
}

func (b *Bootstrap) onAvailablityChanged(available bool) {
	key := b.Conf.Type + b.Conf.ChainId
	if _, loaded := startCustomChainMap.Load(key); !loaded {
		log.WarnS(
			"IGNORE AVAILABLITY CHANGED",
			zap.String("chainName", b.ChainName),
			zap.Bool("available", available),
		)
		return
	}

	current := atomic.LoadInt32(&b.available) == 1
	if current == available {
		log.InfoS(
			"AVAILABLITY NOT CHANGED",
			zap.String("chainName", b.ChainName),
			zap.Bool("available", available),
		)
	}

	// Setting is not affected, somewhere else may already be affected.
	if !b.setAvailablity(available) {
		return
	}

	log.Info(
		"AVAILABLITY CHANGED",
		zap.Bool("current", current),
		zap.Bool("new", available),
		zap.String("chainName", b.ChainName),
	)
	if available {
		var height uint64
		var err error
		err = b.Platform.GetBlockSpider().WithRetry(func(client chain.Clienter) error {
			height, err = client.GetBlockHeight()
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Error(
				"AVAILABLITY CHANGED: RESUME BLOCKS CRAWL TO THE NEWEST BLOCK FAILED",
				zap.String("chainName", b.ChainName),
				zap.Error(err),
			)
			return
		}
		if err := b.stateStore.StoreHeight(height); err != nil {
			log.Error(
				"AVAILABLITY CHANGED: RESUME BLOCKS CRAWL TO THE NEWEST BLOCK FAILED",
				zap.String("chainName", b.ChainName),
				zap.Error(err),
			)
			return
		}

		log.Info(
			"AVAILABLITY CHANGED: RESUME BLOCKS CRAWL TO THE NEWEST BLOCK",
			zap.String("chainName", b.ChainName),
			zap.Uint64("nodeHeight", height),
		)
		go b.GetTransactions()
	} else {
		log.Warn(
			"AVAILABLITY CHANGED: SUSPEND BLOCKS CRAWL ONLY KEEP THE PENDING TXS HANDLING",
			zap.String("chainName", b.ChainName),
		)
		b.cancel()
		b.ctx, b.cancel = context.WithCancel(context.Background())
	}
}

func (b *Bootstrap) isAvailable() bool {
	return atomic.LoadInt32(&b.available) == 1
}

func (b *Bootstrap) setAvailablity(val bool) bool {
	if val {
		return atomic.CompareAndSwapInt32(&b.available, 0, 1)
	} else {
		return atomic.CompareAndSwapInt32(&b.available, 1, 0)
	}
}

func (b *Bootstrap) GetTransactions() {
	liveInterval := b.liveInterval()
	log.Info(
		"GetTransactions starting, chainName:"+b.ChainName,
		zap.String("liveInterval", liveInterval.String()),
		zap.Bool("roundRobinConcurrent", b.Conf.GetRoundRobinConcurrent()),
	)
	b.Spider.StartIndexBlockWithContext(
		b.ctx,
		b.Platform.CreateBlockHandler(liveInterval),
		int(b.Conf.GetSafelyConcurrentBlockDelta()),
		int(b.Conf.GetMaxConcurrency()),
	)
}

func (b *Bootstrap) GetTransactionResultByTxhash() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactionsResult error, chainName:"+b.ChainName, e)
			} else {
				log.Errore("GetTransactionsResult panic, chainName:"+b.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", b.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	liveInterval := b.liveInterval()
	n := b.Spider.SealPendingTransactions(b.Platform.CreateBlockHandler(liveInterval))
	if n > 0 {
		b.onAvailablityChanged(true)
	}
}

func (b *Bootstrap) liveInterval() time.Duration {
	return biz.LiveInterval(b.Platform)
}

type Server interface {
	transport.Server

	Find(string) *Bootstrap
}

type serverImpl struct {
	inner map[string]*Bootstrap

	customProvider CustomConfigProvider
	mr             data.MigrationRepo
}

func newServer(customProvider CustomConfigProvider, mr data.MigrationRepo) *serverImpl {
	return &serverImpl{
		inner:          make(map[string]*Bootstrap),
		customProvider: customProvider,
	}
}

func (bs *serverImpl) Find(chainName string) *Bootstrap {
	return bs.inner[chainName]
}

func (bs *serverImpl) Start(ctx context.Context) error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("main panic:", zap.Any("", err))
		}
	}()

	innerquit := make(chan int)

	//本地配置 链的爬取
	log.Info("BOOTSTRAP CHAINS", zap.String("stage", "before"))
	var wg sync.WaitGroup
	for _, b := range bs.inner {
		wg.Add(1)
		go func(b *Bootstrap) {
			defer wg.Done()
			b.Start()
			//记录 已启动chain
			ci := b.Conf.Type + b.Conf.ChainId
			startChainMap.Store(ci, b)
			log.Info("本地配置启动bnb", zap.Any("", b))
		}(b)
	}
	wg.Wait()
	log.Info("BOOTSTRAP CHAINS", zap.String("stage", "after"))

	go func() {
		platforms := InnerPlatforms
		platformsLen := len(platforms)
		for i := 0; i < platformsLen; i++ {
			p := platforms[i]
			if p == nil {
				continue
			}
			go func(p biz.Platform) {
				if btc, ok := p.(*bitcoin.Platform); ok {
					log.Info("start inner main", zap.Any("platform", btc))
					signal := make(chan bool)
					go runGetPendingTransactionsByInnerNode(signal, btc)
					signalGetPendingTransactionsByInnerNode(signal, btc)
					liveInterval := p.Coin().LiveInterval
					pendingTransactions := time.NewTicker(time.Duration(liveInterval) * time.Millisecond)
					for true {
						select {
						case <-pendingTransactions.C:
							signalGetPendingTransactionsByInnerNode(signal, btc)
						case <-innerquit:
							close(signal) // close signal to make background goroutine to exit.
							pendingTransactions.Stop()
							return
						}
					}
				}
			}(p)
		}
	}()

	for i := 0; i < 8; i++ {
		go customChainRun(bs.customProvider, bs.mr)
	}

	//添加定时任务（每天23:55:00）执行
	_, err := scheduling.Task.AddTask("55 23 * * *", scheduling.NewAssetEvmTask())
	if err != nil {
		log.Error("add AssetEvmTask error", zap.Any("", err))
		return err
	}

	//添加定时任务（每天23:58:00）执行
	_, err = scheduling.Task.AddTask("58 23 * * *", scheduling.NewStatisticUserAssetTask())
	if err != nil {
		log.Error("add statisticUserAssetTask error", zap.Any("", err))
		return err
	}

	//添加定时任务（每10分钟）执行
	_, err = scheduling.Task.AddTask("0/10 * * * *", scheduling.NewFixNftInfoTask())
	if err != nil {
		log.Error("add fixNftInfoTask error", zap.Any("", err))
		return err
	}
	// 添加定时任务每天执行
	_, err = scheduling.Task.AddTask("0 9 * * *", scheduling.NewSwapIndexerTask())
	if err != nil {
		log.Error("add swapIndexerTask error", zap.Any("", err))
		return err
	}

	// 添加定时任务每天执行
	_, err = scheduling.Task.AddTask("0 0 * * *", scheduling.NewUserWalletAssetHistoryTask())
	if err != nil {
		log.Error("add WalletAssetHistoryTask error", zap.Any("", err))
		return err
	}

	_, err = scheduling.Task.AddTask("0 0 * * *", scheduling.NewUserDeFiAssetHistoryTask())
	if err != nil {
		log.Error("add DeFiAssetHistoryTask error", zap.Any("", err))
		return err
	}

	_, err = scheduling.Task.AddTask("58 8 * * *", &scheduling.ReportFailureTxnsTask{})
	if err != nil {
		log.Error("add report failure txns tasks error", zap.Error(err))
		return err
	}
	//启动定时任务
	scheduling.Task.Start()

	return nil
}

func customChainRun(provider CustomConfigProvider, mr data.MigrationRepo) {
	for cp := range provider.Updated() {
		if len(cp.RpcURL) == 0 {
			continue
		}

		// 已启动 本地爬块
		k := cp.Type + cp.ChainId
		if v, loaded := startChainMap.Load(k); loaded && v != nil {
			continue
		}
		if sl, ok := startCustomChainMap.LoadOrStore(k, customBootStrap(cp, mr)); ok {
			sccm := sl.(*Bootstrap)
			chainLock.Lock()
			//校验块高差 1000  停止爬块 更新块高
			nodeRedisHeight, _ := data.RedisClient.Get(biz.BLOCK_NODE_HEIGHT_KEY + cp.Chain).Result()
			redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + cp.Chain).Result()

			oldHeight, _ := strconv.Atoi(redisHeight)
			height, _ := strconv.Atoi(nodeRedisHeight)

			ret := height - oldHeight
			if ret > 1000 {
				sccm.Stop()
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+cp.Chain, height, 0).Err()
				sccm.ctx, sccm.cancel = context.WithCancel(context.Background())
				sccm.pCtx, sccm.pCancel = context.WithCancel(context.Background())
				log.Info("CUSTOM-CHAIN: 拉取配置启动bnb 重启", zap.Any("", sccm))

				sccm.Start()
			}
			chainLock.Unlock()
			sccm.Conf.RpcURL = cp.RpcURL
			nodeURL := cp.RpcURL
			cs := make([]chain.Clienter, 0, len(nodeURL))
			for _, url := range nodeURL {
				cs = append(cs, sccm.Platform.CreateClient(url))
			}
			sccm.Spider.ReplaceClients(cs...)
			startCustomChainMap.Store(k, sccm)
		} else {
			sccm := sl.(*Bootstrap)
			sccm.Start()
			log.Info("CUSTOM-CHAIN: 拉取配置启动bnb", zap.Any("", sccm))
		}
	}
}

func customBootStrap(cp *conf.PlatInfo, mr data.MigrationRepo) *Bootstrap {
	if v, ok := customBootstrapMap.Load(cp.Chain); ok {
		return v.(*Bootstrap)
	}
	platform, _ := biz.GetChainPlatform(cp.Chain)
	return NewBootstrap(platform, cp, data.BlockCreawlingDB, mr)
}
func (bs *serverImpl) Stop(ctx context.Context) error {
	return nil
}

func signalGetPendingTransactionsByInnerNode(signal chan<- bool, btc *bitcoin.Platform) {
	select {
	case signal <- true:
		log.Info("SIGNALED TO BACKGROUND GOROUTINE", zap.Any("platform", btc))
	default:
		log.Info("SIGNAL FAILED AS BACKGROUND GOROUTINE IS BUSY", zap.Any("platform", btc))
	}
}

func runGetPendingTransactionsByInnerNode(signal <-chan bool, btc *bitcoin.Platform) {
	log.Info("inner main started and wait signal", zap.Any("platform", btc))
	for range signal {
		btc.GetPendingTransactionsByInnerNode()
	}
}
