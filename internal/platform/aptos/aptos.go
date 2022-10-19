package aptos

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
	client    Client
	CoinIndex uint
	conf      *conf.PlatInfo
	spider    *chain.BlockSpider
}

const APT_CODE = "0x1::aptos_coin::AptosCoin"
const APT_CREATE_ACCOUNT = "0x1::aptos_account::create_account"
const APT_REGISTER = "0x1::coins::register"

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	log.Info(value.Chain+"链初始化", zap.Any("nodeURLs", nodeURL))
	chainType, chainName := value.Handler, value.Chain

	nodes := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c := NewClient(chainName, url)
		nodes = append(nodes, &c)
	}
	spider := chain.NewBlockSpider(newStateStore(chainName), nodes...)
	spider.WatchDetector(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(chainName, nodeURL[0]),
		conf:      value,
		spider:    spider,
		CommPlatform: subhandle.CommPlatform{
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

func (p *Platform) GetTransactions() {
	log.Info("GetTransactions starting, chainName:" + p.ChainName)
	p.spider.StartIndexBlock(
		newHandler(p.ChainName, time.Duration(p.Coin().LiveInterval)*time.Millisecond),
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
	p.spider.SealPendingTransactions(newHandler(p.ChainName, time.Duration(p.Coin().LiveInterval)*time.Millisecond))
}

func BatchSaveOrUpdate(txRecords []*data.AptTransactionRecord, tableName string) error {
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

		_, err := data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
