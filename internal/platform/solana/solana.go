package solana

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/subhandle"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
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
	spider.WatchDetector(common.NewDectorZapWatcher(chainName))

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
	log.Info("GetTransactions starting, chainName:" + p.ChainName)

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