package kaspa

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/types"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type Platform struct {
	biz.CommPlatform
	client    Client
	CoinIndex uint
	spider    *chain.BlockSpider
}

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chainType := value.Handler
	chainName := value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(nodeURL[0], chainName),
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
	c := NewClient(p.ChainName, url)
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

func (p *Platform) GetUTXOByHash(txHash string) (tx *types.KaspaTransactionInfo, err error) {
	result, err := ExecuteRetry(p.ChainName, func(client Client) (interface{}, error) {
		return client.GetTransactionByHash(txHash)
	})
	if err != nil {
		return nil, err
	}
	tx = result.(*types.KaspaTransactionInfo)
	return
}

func BatchSaveOrUpdate(txRecords []*data.KasTransactionRecord, table string) error {
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

		_, err := data.KasTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.KasTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
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
			TxTime          :r.TxTime,
		})
	}
	go biz.SyncStatus(ssr)
	return nil
}
