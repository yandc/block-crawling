package sui

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type Platform struct {
	biz.CommPlatform
	CoinIndex uint
	spider    *chain.BlockSpider
}

const SUI_CODE = "0x2::sui::SUI"
const TYPE_PREFIX = "0x2::coin::Coin"

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chainType := value.Handler
	chainName := value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
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
	c := NewClient(url, p.ChainName)
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

func BatchSaveOrUpdate(txRecords []*data.SuiTransactionRecord, tableName string) error {
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

		_, err := data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
