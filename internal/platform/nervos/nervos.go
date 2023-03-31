package nervos

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/types"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

const (
	nervosDecimals = 8
)

type Platform struct {
	biz.CommPlatform
	NodeURL   string
	CoinIndex uint
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
		client, err := NewClient(chainName, url)
		if err != nil {
			panic(err)
		}
		clients = append(clients, client)
	}
	spider := chain.NewBlockSpider(newStateStore(chainName), clients...)
	if len(c.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(c.StandbyRPCURL))
		for _, url := range c.StandbyRPCURL {
			c, err := NewClient(chainName, url)
			if err != nil {
				panic(err)
			}
			standby = append(standby, c)
		}
		spider.AddStandby(standby...)
	}
	spider.Watch(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		NodeURL:   nodeURL[0],
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
		},
		spider: spider,
		conf:   c,
	}
}

func (p *Platform) SetNodeURL(nodeURL string) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.NodeURL = nodeURL
}

func (p *Platform) GetSpider() *chain.BlockSpider {
	return p.spider
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

func (p *Platform) GetBlockSpider() *chain.BlockSpider {
	return p.spider
}

func (p *Platform) GetUTXOByHash(txHash string) (tx *types.TransactionWithStatus, err error) {
	p.spider.WithRetry(func(client chain.Clienter) error {
		tx, err = client.(*Client).GetUTXOByHash(txHash)
		return err
	})
	return
}

func BatchSaveOrUpdate(txRecords []*data.CkbTransactionRecord, tableName string) error {
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

		_, err := data.CkbTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.CkbTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
