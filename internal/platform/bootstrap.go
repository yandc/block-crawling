package platform

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/scheduling"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	available  int32
	stateStore chain.StateStore
}

var startChainMap = &sync.Map{}
var startCustomChainMap = &sync.Map{}
var chainLock sync.RWMutex

func NewBootstrap(p biz.Platform, value *conf.PlatInfo, db *gorm.DB) *Bootstrap {
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
	biz.DynamicCreateTable(b.db, table, b.Conf.Type)

	go func() {
		if b.ChainName == "Osmosis" || b.ChainName == "SUITEST" || b.ChainName == "Kaspa" || b.ChainName == "SeiTEST" {
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
				if b.isAvailable() {
					go b.Platform.MonitorHeight(b.onAvailablityChanged)
				}
			case <-b.pCtx.Done():
				resultPlan.Stop()
				return
			}
		}
	}()
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

func FixNftInfo() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("FixNftInfo error", e)
			} else {
				log.Errore("FixNftInfo panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：定时更新缺失的NFT信息失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum", "Avalanche", "Optimism", "Klaytn", "ArbitrumNova", "Conflux", "Aptos", "SUI", "SUITEST", "Solana", "ScrollL2TEST", "zkSync", "Ronin", "Sei", "SeiTEST", "Fantom", "xDai", "Cronos"}
	for _, chainName := range chainNames {
		tableName := biz.GetTableName(chainName)
		chainType := biz.ChainNameType[chainName]
		transactionRequest := &data.TransactionRequest{
			StatusNotInList:          []string{biz.PENDING},
			TransactionTypeNotInList: []string{biz.CONTRACT},
		}
		switch chainType {
		case biz.EVM:
			incompleteNfts, err := data.EvmTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					var token types.TokenInfo
					if tokenId != "" {
						token, err = biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					} else {
						token, err = biz.GetCollectionInfoDirectlyRetryAlert(nil, chainName, tokenAddress)
					}
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.EvmTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.EvmTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.APTOS:
			incompleteNfts, err := data.AptTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.AptTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.AptTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.AptTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.SUI:
			incompleteNfts, err := data.SuiTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.SuiTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.SuiTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.SuiTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.SOLANA:
			incompleteNfts, err := data.SolTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.SolTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.SolTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.SolTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		case biz.COSMOS:
			incompleteNfts, err := data.AtomTransactionRecordRepoClient.ListIncompleteNft(nil, tableName, transactionRequest)
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，从数据库中查询交易记录信息失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"定时修复NFT信息，从数据库中查询交易记录信息失败", zap.Any("error", err))
				return
			}
			txHashMap := make(map[string]string)
			for _, incompleteNft := range incompleteNfts {
				parseData := incompleteNft.ParseData
				tokenInfo, _ := biz.ParseTokenInfo(parseData)
				tokenAddress := tokenInfo.Address
				tokenId := tokenInfo.TokenId

				txHash := incompleteNft.TransactionHash
				txHash = strings.Split(txHash, "#")[0]
				if _, ok := txHashMap[txHash]; !ok {
					token, err := biz.GetNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(chainName+"定时修复NFT信息，从nodeProxy中获取NFT信息失败", zap.Any("txHash", txHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						continue
					}
					if token.Address == "" {
						continue
					}
					txHashMap[txHash] = ""
				}
			}

			var txRecords []*data.AtomTransactionRecord
			for txHash, _ := range txHashMap {
				txRecords = append(txRecords, &data.AtomTransactionRecord{TransactionHash: txHash, Status: biz.PENDING})
			}
			if len(txRecords) > 0 {
				_, err := data.AtomTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveByTransactionHash(nil, tableName, txRecords, biz.PAGE_SIZE)
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s定时修复NFT信息，将数据更新到数据库中失败", chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(chainName+"定时修复NFT信息，将数据更新到数据库中失败", zap.Any("error", err))
					return
				}
			}
		}
	}
}

func (b *Bootstrap) liveInterval() time.Duration {
	return time.Duration(b.Platform.Coin().LiveInterval) * time.Millisecond
}

type Server map[string]*Bootstrap

func (bs Server) Start(ctx context.Context) error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("main panic:", zap.Any("", err))
		}
	}()

	innerquit := make(chan int)
	quit := make(chan int)

	//本地配置 链的爬取
	log.Info("BOOTSTRAP CHAINS", zap.String("stage", "before"))
	var wg sync.WaitGroup
	for _, b := range bs {
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

	go func() {
		go FixNftInfo()
		resultPlan := time.NewTicker(time.Duration(10) * time.Minute)
		for true {
			select {
			case <-resultPlan.C:
				go FixNftInfo()
			case <-quit:
				resultPlan.Stop()
				return
			}
		}
	}()

	//定时  已启动的 ==  拉取内存 配置
	InitCustomePlan()

	//添加定时任务（每天23:55:00）执行
	_, err1 := scheduling.Task.AddTask("55 23 * * *", scheduling.NewAssetEvmTask())
	if err1 != nil {
		log.Error("add AssetEvmTask error", zap.Any("", err1))
		return err1
	}

	//添加定时任务（每天23:58:00）执行
	_, err := scheduling.Task.AddTask("58 23 * * *", scheduling.NewStatisticUserAssetTask())
	if err != nil {
		log.Error("add statisticUserAssetTask error", zap.Any("", err))
		return err
	}
	//启动定时任务
	scheduling.Task.Start()

	return nil
}

func InitCustomePlan() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("main panic:", zap.Any("", err))
		}
	}()

	go func() {
		customChainPlan := time.NewTicker(time.Duration(10) * time.Second)
		for true {
			select {
			case <-customChainPlan.C:
				customChainRun()
			}
		}
	}()
}

func customChainRun() {
	//定是拉取 调用 grpc node-proxy
	//la := []string{"BSC","ETH"}
	chainNodeInUsedList, err := biz.GetCustomChainList(nil)

	if err != nil {
		log.Error("获取自定义链信息失败", zap.Error(err))
		return
	}

	for _, chainInfo := range chainNodeInUsedList.Data {
		if chainInfo != nil && len(chainInfo.Urls) == 0 {
			continue
		}

		//已启动 本地爬块
		k := chainInfo.Type + chainInfo.ChainId
		if v, loaded := startChainMap.Load(k); loaded && v != nil {
			continue
		}
		if sl, ok := startCustomChainMap.LoadOrStore(k, GetBootStrap(chainInfo)); ok {
			sccm := sl.(*Bootstrap)
			chainLock.Lock()
			//校验块高差 1000  停止爬块 更新块高
			nodeRedisHeight, _ := data.RedisClient.Get(biz.BLOCK_NODE_HEIGHT_KEY + chainInfo.Chain).Result()
			redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + chainInfo.Chain).Result()

			oldHeight, _ := strconv.Atoi(redisHeight)
			height, _ := strconv.Atoi(nodeRedisHeight)

			ret := height - oldHeight
			if ret > 1000 {
				sccm.Stop()
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+chainInfo.Chain, height, 0).Err()
				sccm.ctx, sccm.cancel = context.WithCancel(context.Background())
				sccm.pCtx, sccm.pCancel = context.WithCancel(context.Background())
				log.Info("拉取配置启动bnb 重启", zap.Any("", sccm))

				sccm.Start()
			}
			chainLock.Unlock()
			if !reflect.DeepEqual(sccm.Conf.RpcURL, chainInfo.Urls) {
				//sccm.Stop()
				sccm.Conf.RpcURL = chainInfo.Urls
				//sccm.ctx, sccm.cancel = context.WithCancel(context.Background())
				//sccm.Start()
				nodeURL := chainInfo.Urls
				cs := make([]chain.Clienter, 0, len(nodeURL))
				for _, url := range nodeURL {
					cs = append(cs, sccm.Platform.CreateClient(url))
				}
				sccm.Spider.ReplaceClients(cs...)
				startCustomChainMap.Store(k, sccm)
			}
		} else {
			sccm := sl.(*Bootstrap)
			sccm.Start()
			log.Info("拉取配置启动bnb", zap.Any("", sccm))

		}
	}
}

func GetBootStrap(chainInfo *v1.GetChainNodeInUsedListResp_Data) *Bootstrap {
	var mhat int32 = 1000
	var mc int32 = 1
	var scbd int32 = 500
	cp := &conf.PlatInfo{
		Chain:                      chainInfo.Chain,
		Type:                       chainInfo.Type,
		RpcURL:                     chainInfo.Urls,
		ChainId:                    chainInfo.ChainId,
		Decimal:                    int32(chainInfo.Decimals),
		NativeCurrency:             chainInfo.CurrencyName,
		Source:                     biz.SOURCE_REMOTE,
		MonitorHeightAlarmThr:      &mhat,
		MaxConcurrency:             &mc,
		SafelyConcurrentBlockDelta: &scbd,
		Handler:                    chainInfo.Chain,
	}
	var PlatInfos []*conf.PlatInfo
	PlatInfos = append(PlatInfos, cp)
	platform := GetPlatform(cp)
	bt := NewBootstrap(platform, cp, data.BlockCreawlingDB)

	biz.PlatInfos = PlatInfos
	biz.ChainNameType[cp.Chain] = cp.Type
	biz.PlatformMap[cp.Chain] = platform
	biz.PlatInfoMap[cp.Chain] = cp
	//bt.Start()
	return bt
}
func (bs Server) Stop(ctx context.Context) error {
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
