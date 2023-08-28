package tron

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const ADDRESS_PREFIX = "41"
const TRC10TYPE = "TransferAssetContract"
const TRXSTAKE2 = "FreezeBalanceV2Contract"
const TRXUNSTAKE2 = "UnfreezeBalanceV2Contract"
const DELEGATERESOURCES = "DelegateResourceContract"
const RECLAIMRESOURCES = "UnDelegateResourceContract"

var TronBridgeWhiteAddressList = []string{
	"TEorZTZ5MHx8SrvsYs1R3Ds5WvY1pVoMSA",
}

type Platform struct {
	biz.CommPlatform
	CoinIndex uint
	spider    *chain.BlockSpider
}

func Init(handler string, value *conf.PlatInfo, nodeUrl []string) *Platform {
	chainType := value.Handler
	chainName := value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		CommPlatform: biz.CommPlatform{
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

func GetTokenInfo(chainName, token string) (types.TokenInfo, error) {
	var url string
	if biz.IsTestNet(chainName) {
		url = "https://shastapi.tronscan.org/api/contract"
	} else {
		url = "https://apilist.tronscan.org/api/contract"
	}
	params := map[string]string{
		"contract": token,
	}
	out := &types.TronTokenInfo{}
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, params, out, &timeoutMS)
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

func (p *Platform) GetBlockSpider() *chain.BlockSpider {
	return p.spider
}

func (p *Platform) SetBlockSpider(blockSpider *chain.BlockSpider) {
	p.spider = blockSpider
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
	var ssr []biz.SignStatusRequest
	for _, r := range txRecords {
		ssr = append(ssr, biz.SignStatusRequest{
			TransactionHash: r.TransactionHash,
			Status:          r.Status,
			TransactionType: r.TransactionType,
			TxTime:          r.TxTime,
		})
	}
	go biz.SyncStatus(ssr)
	return nil
}
