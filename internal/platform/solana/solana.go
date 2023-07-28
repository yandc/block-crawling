package solana

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type Platform struct {
	biz.CommPlatform
	NodeURL   string
	CoinIndex uint
	spider    *chain.BlockSpider
}

type Config struct {
	ProjectId []string
}

type KVPair struct {
	Key string
	Val int
}

const SOL_CODE = "11111111111111111111111111111111"

func Init(handler string, c *conf.PlatInfo, nodeURL []string, height int) *Platform {
	log.Info(c.Chain+"链初始化", zap.Any("nodeURLs", nodeURL))
	chainType := c.Handler
	chainName := c.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		NodeURL:   nodeURL[0],
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
		},
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

func (p *Platform) CreateStateStore() chain.StateStore {
	return newStateStore(p.ChainName)
}

func (p *Platform) CreateClient(url string) chain.Clienter {
	c := NewClient(url, p.ChainName)
	return c
}

func (p *Platform) CreateBlockHandler(liveInterval time.Duration) chain.BlockHandler {
	return newHandler(p.ChainName, liveInterval)
}

func (p *Platform) GetBlockSpider() *chain.BlockSpider {
	return p.spider
}

func (p *Platform) SetBlockSpider(blockSpider *chain.BlockSpider) {
	p.spider = blockSpider
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
	var ssr []biz.SignStatusRequest
	for _ , r := range txRecords {
		ssr = append(ssr,biz.SignStatusRequest{
			TransactionHash :r.TransactionHash,
			Status          :r.Status,
			TransactionType :r.TransactionType,
			TxTime          :r.TxTime,
		})
	}
	go biz.SyncStatus(ssr)
	return nil
}

var monitorHeightSeq uint64

func (p *Platform) MonitorHeight() {
	// 测试环境每 1 小时监控一次，生产环境每 6 小时监控一次。
	seq := atomic.AddUint64(&monitorHeightSeq, 1)
	if seq == 60 {
		p.CommPlatform.MonitorHeight()
		atomic.StoreUint64(&monitorHeightSeq, 0)
	}
}
