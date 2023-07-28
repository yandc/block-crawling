package nervos

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
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

func Init(handler string, c *conf.PlatInfo, nodeURL []string, height int) *Platform {
	log.Info(c.Chain+"链初始化", zap.Any("nodeURLs", nodeURL))
	chainType := c.Handler
	chainName := c.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
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
	c, err := NewClient(p.ChainName, url)
	if err != nil {
		panic(err)
	}
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

func (p *Platform) GetUTXOByHash(txHash string) (tx *types.TransactionWithStatus, err error) {
	result, err := ExecuteRetry(p.ChainName, func(client Client) (interface{}, error) {
		return client.GetUTXOByHash(txHash)
	})
	if err != nil {
		return nil, err
	}
	tx = result.(*types.TransactionWithStatus)
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
