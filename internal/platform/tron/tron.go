package tron

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

const ADDRESS_PREFIX = "41"
const TRC10TYPE = "TransferAssetContract"

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint

	spider *chain.BlockSpider
	conf   *conf.PlatInfo
}

func Init(handler string, value *conf.PlatInfo, nodeUrl []string, height int) *Platform {
	chainType := value.Handler
	chainName := value.Chain

	clients := make([]chain.Clienter, 0, len(nodeUrl))
	for _, url := range nodeUrl {
		c := NewClient(url, chainName)
		clients = append(clients, &c)
	}
	spider := chain.NewBlockSpider(newStateStore(chainName), clients...)
	if len(value.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(value.StandbyRPCURL))
		for _, url := range value.StandbyRPCURL {
			c := NewClient(url, chainName)
			standby = append(standby, &c)
		}
		spider.AddStandby(standby...)
	}
	spider.WatchDetector(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(nodeUrl[0], chainName),
		CommPlatform: subhandle.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(value.GetMonitorHeightAlarmThr()),
		},
		spider: spider,
		conf:   value,
	}
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

func GetTokenInfo(chainName, token string) (types.TokenInfo, error) {
	var url string
	if strings.Contains(chainName, "TEST") {
		url = "https://shastapi.tronscan.org/api/contract"
	} else {
		url = "https://apilist.tronscan.org/api/contract"
	}
	params := map[string]string{
		"contract": token,
	}
	out := &types.TronTokenInfo{}
	err := httpclient.HttpsForm(url, http.MethodGet, params, nil, out)
	if err != nil {
		return types.TokenInfo{}, err
	}
	if len(out.Data) == 0 {
		return types.TokenInfo{}, nil
	}
	return types.TokenInfo{
		Decimals: out.Data[0].TokenInfo.TokenDecimal,
		Symbol:   out.Data[0].TokenInfo.TokenName,
		Address:  token,
	}, nil
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

func BatchSaveOrUpdate(txRecords []*data.TrxTransactionRecord, table string) error {
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

		_, err := data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
