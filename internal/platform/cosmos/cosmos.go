package cosmos

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
	CoinIndex uint
	conf      *conf.PlatInfo
	spider    *chain.BlockSpider
}

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	log.Info(value.Chain+"链初始化", zap.Any("nodeURLs", nodeURL))
	chainType, chainName := value.Handler, value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		conf:      value,
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(value.GetMonitorHeightAlarmThr()),
		},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) CreateStateStore() chain.StateStore {
	return newStateStore(p.ChainName)
}

func (p *Platform) CreateClient(url string) chain.Clienter {
	c := NewClient(p.ChainName, url, p.conf.GetEnableProxy())
	return &c
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

func BatchSaveOrUpdate(txRecords []*data.AtomTransactionRecord, tableName string) error {
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

		_, err := data.AtomTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.AtomTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	var ssr []biz.SignStatusRequest
	for _, r := range txRecords {
		ssr = append(ssr, biz.SignStatusRequest{
			TransactionHash: r.TransactionHash,
			Status:          r.Status,
			TransactionType: r.TransactionType,
			Nonce:           r.Nonce,
			TxTime:          r.TxTime,
		})
	}
	go biz.SyncStatus(ssr)
	return nil
}

var monitorHeightSeq uint64

func (p *Platform) MonitorHeight() {
	if p.ChainName == "Cosmos" {
		p.CommPlatform.MonitorHeight()
	} else {
		// 每 6 小时监控一次
		seq := atomic.AddUint64(&monitorHeightSeq, 1)
		if seq == 360 {
			p.CommPlatform.MonitorHeight()
			atomic.StoreUint64(&monitorHeightSeq, 0)
		}
	}
}
