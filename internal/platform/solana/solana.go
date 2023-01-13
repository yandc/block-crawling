package solana

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/subhandle"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
	ncommon "gitlab.bixin.com/mili/node-driver/common"
)

type Platform struct {
	subhandle.CommPlatform
	NodeURL   string
	CoinIndex uint
	UrlList   []string
	spider    *chain.BlockSpider
	conf      *conf.PlatInfo
}

type Config struct {
	ProjectId []string
}

type KVPair struct {
	Key string
	Val int
}

func Init(handler string, c *conf.PlatInfo, nodeURL []string, height int) *Platform {
	log.Info(c.Chain+"链初始化", zap.Any("nodeURLs", nodeURL))
	chainType := c.Handler
	chainName := c.Chain

	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		client := NewClient(url, chainName)
		clients = append(clients, client)
	}
	spider := chain.NewBlockSpider(newStateStore(chainName), clients...)
	if len(c.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(c.StandbyRPCURL))
		for _, url := range c.StandbyRPCURL {
			cl := NewClient(url, chainName)
			standby = append(standby, cl)
		}
		spider.AddStandby(standby...)
	}
	spider.Watch(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		NodeURL:   nodeURL[0],
		CommPlatform: subhandle.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
		},
		UrlList: nodeURL,
		spider:  spider,
		conf:    c,
	}
}

func (p *Platform) SetNodeURL(nodeURL string) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.NodeURL = nodeURL
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	// Solana node is slow that can't meet our requirements to index block.
	// So only keep the logics to handle pending tx.
	// Added at 2023-01-10
	return

	log.Info("GetTransactions starting, chainName:"+p.ChainName, zap.Bool("roundRobinConcurrent", p.conf.GetRoundRobinConcurrent()))
	if p.conf.GetRoundRobinConcurrent() {
		p.spider.EnableRoundRobin()
	}

	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond
	p.spider.StartIndexBlock(
		newHandler(p.ChainName, liveInterval),
		int(p.conf.GetSafelyConcurrentBlockDelta()),
		int(p.conf.GetMaxConcurrency()),
	)
}

func (p *Platform) GetTransactionResultByTxhash() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactionsResult error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactionsResult panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond
	p.spider.SealPendingTransactions(newHandler(p.ChainName, liveInterval))
}

func (p *Platform) UpdateUserAsset(ctx context.Context, assets []*data.UserAsset) {
	done := make(chan struct{})
	uids := make(map[string]struct{})
	for _, a := range assets {
		uids[fmt.Sprintf("%s:%s", a.Uid, a.Symbol)] = struct{}{}
	}

	go p.doUpdateUserAsset(assets, done)
	select {
	case <-done:
		log.Info("UPDATE SOL USER ASSETS SUCCESS", zap.Any("assets", uids))
	case <-ctx.Done():
		log.Info("UPDATE SOL USER ASSETS TIMEOUT", zap.Any("assets", uids))
	}
}

func (p *Platform) doUpdateUserAsset(assets []*data.UserAsset, done chan<- struct{}) {
	now := time.Now().Unix()
	updatedAssets := make([]*data.UserAsset, 0, len(assets))

	for _, asset := range assets {
		if now-asset.UpdatedAt < 120 {
			continue
		}

		if asset.ChainName != p.ChainName {
			continue
		}
		if asset.Uid == "" || asset.Address == "" {
			log.Info("USER ASSET INCOMPELETE", zap.Any("asset", asset))
			continue
		}

		err := p.spider.WithRetry(func(c chain.Clienter) error {
			client := c.(*Client)
			newAsset, err := doHandleUserAsset(
				p.ChainName, *client, "", asset.Uid, asset.Address, asset.TokenAddress,
				asset.Decimals, asset.Symbol, now,
			)
			if err != nil {
				return ncommon.Retry(err)
			}
			if newAsset == nil {
				return nil
			}

			if newAsset.Balance != asset.Balance {
				asset.Balance = newAsset.Balance
				updatedAssets = append(updatedAssets, asset)
			}
			return nil
		})
		log.Error("QUERY ASSET FAILED", zap.Any("asset", asset), zap.Error(err))
	}

	if len(updatedAssets) > 0 {
		_, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, updatedAssets, biz.PAGE_SIZE)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, updatedAssets, biz.PAGE_SIZE)
		}
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s 链更新用户资产数据失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"更新用户资产，将数据插入到数据库中失败", zap.Any("error", err))
		}
	}
	done <- struct{}{}
}

func BatchSaveOrUpdate(txRecords []*data.SolTransactionRecord, tableName string) error {
	total := len(txRecords)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		_, err := data.SolTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.SolTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}

var monitorHeightSeq uint64

func (p *Platform) MonitorHeight() {
	// Disabled at 2023-01-10
	return

	// 测试环境每 1 小时监控一次，生产环境每 6 小时监控一次。
	seq := atomic.AddUint64(&monitorHeightSeq, 1)
	if seq == 60 {
		p.CommPlatform.MonitorHeight()
		atomic.StoreUint64(&monitorHeightSeq, 0)
	}
}
