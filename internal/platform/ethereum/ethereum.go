package ethereum

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
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

// ERC20 or ERC721
const APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

// ERC721 or ERC1155
const APPROVALFORALL_TOPIC = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31"

// ERC20 or ERC721
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ERC1155
const TRANSFERSINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

const WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
const DEPOSIT_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
const BLOCK_NO_TRANSCATION = "server returned empty transaction list but block header indicates transactions"
const BLOCK_NONAL_TRANSCATION = "server returned non-empty transaction list but block header indicates no transactions"
const TOO_MANY_REQUESTS = "429 Too Many Requests"
const POLYGON_CODE = "0x0000000000000000000000000000000000001010"

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
	chainType := c.Handler // ethereum
	chainName := c.Chain   // ETH

	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c, err := NewClient(url, chainName)
		if err != nil {
			panic(err)
		}
		clients = append(clients, c)
	}
	spider := chain.NewBlockSpider(NewStateStore(chainName), clients...)
	if len(c.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(c.StandbyRPCURL))
		for _, url := range c.StandbyRPCURL {
			c, err := NewClient(url, chainName)
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
	log.Info(
		"GetTransactions starting, chainName:"+p.ChainName,
		zap.Bool("roundRobinConcurrent", p.conf.GetRoundRobinConcurrent()),
	)

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

func BatchSaveOrUpdate(txRecords []*data.EvmTransactionRecord, tableName string) error {
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

		_, err := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}

// nonstandardEVM returns true if the block hash should be retrieved from tx receipt.
func isNonstandardEVM(chainName string) bool {
	return chainName == "OEC" || chainName == "Optimism" || chainName == "Cronos" || chainName == "Polygon" ||
		chainName == "Fantom" || chainName == "Avalanche" || chainName == "Klaytn" || chainName == "xDai" ||
		chainName == "OECTEST" || chainName == "OptimismTEST" || chainName == "CronosTEST" || chainName == "PolygonTEST" ||
		chainName == "FantomTEST" || chainName == "AvalancheTEST" || chainName == "KlaytnTEST" || chainName == "xDaiTEST"
}
