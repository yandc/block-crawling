package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Bootstrap struct {
	ChainName string
	Conf      *conf.PlatInfo
	Platform  biz.Platform
	Spider    *chain.BlockSpider
}

func NewBootstrap(p biz.Platform, value *conf.PlatInfo) *Bootstrap {
	nodeURL := value.RpcURL
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		clients = append(clients, p.CreateClient(url))
	}
	spider := chain.NewBlockSpider(p.CreateStateStore(), clients...)
	if len(value.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(value.StandbyRPCURL))
		for _, url := range value.StandbyRPCURL {
			c := p.CreateClient(url)
			standby = append(standby, c)
		}
		spider.AddStandby(standby...)
	}
	spider.Watch(common.NewDectorZapWatcher(value.Chain))

	if value.GetRoundRobinConcurrent() {
		spider.EnableRoundRobin()
	}
	p.SetBlockSpider(spider)
	spider.SetHandlingTxsConcurrency(int(value.GetHandlingTxConcurrency()))
	return &Bootstrap{
		ChainName: value.Chain,
		Platform:  p,
		Spider:    spider,
		Conf:      value,
	}
}

func (b *Bootstrap) GetTransactions() {
	liveInterval := b.liveInterval()
	log.Info(
		"GetTransactions starting, chainName:"+b.ChainName,
		zap.String("liveInterval", liveInterval.String()),
		zap.Bool("roundRobinConcurrent", b.Conf.GetRoundRobinConcurrent()),
	)
	b.Spider.StartIndexBlock(
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
	b.Spider.SealPendingTransactions(b.Platform.CreateBlockHandler(liveInterval))
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

	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum", "Optimism", "Klaytn", "Aptos", "SUITEST"}
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
	quit := make(chan int)
	innerquit := make(chan int)

	go func() {
		for _, p := range bs {
			if p.ChainName == "Osmosis" || p.ChainName == "SUITEST" {
				continue
			}
			go p.GetTransactions()
		}
	}()

	// get inner memerypool
	go func() {
		for _, b := range bs {
			go func(p *Bootstrap) {
				log.Info("start main", zap.Any("platform", p))
				// get result
				go p.GetTransactionResultByTxhash()
				resultPlan := time.NewTicker(time.Duration(5) * time.Minute)
				for true {
					select {
					case <-resultPlan.C:
						go p.GetTransactionResultByTxhash()
						go p.Platform.MonitorHeight()
					case <-quit:
						resultPlan.Stop()
						return
					}
				}
			}(b)
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
	return nil
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
