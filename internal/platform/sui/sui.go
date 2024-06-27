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

const (
	SUI_CODE  = "0x2::sui::SUI"
	SUI_CODE1 = "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI"
	BFC_CODE  = "0x2::bfc::BFC"
	BFC_CODE1 = "0x0000000000000000000000000000000000000000000000000000000000000002::bfc::BFC"
	BFC_CODE2 = "BFC000000000000000000000000000000000000000000000000000000000000000268e4::bfc::BFC"
)

func IsNative(coinType string) bool {
	switch coinType {
	case SUI_CODE, SUI_CODE1, BFC_CODE, BFC_CODE1, BFC_CODE2:
		return true
	default:
		return false
	}
}

// UnwrapTokenIDFromCoinType unwrap 0x2::sui::SUI from 0x2::coin::Coin<0x2::sui::SUI>
func UnwrapTokenIDFromCoinType(ty string) string {
	prefix := "0x2::coin::Coin<"
	if strings.HasPrefix(ty, prefix) && strings.HasSuffix(ty, ">") {
		return ty[len(prefix) : len(ty)-1]
	}
	return ty
}

func IsNativeContains(objectType string) bool {
	return strings.Contains(objectType, SUI_CODE) || strings.Contains(objectType, SUI_CODE1) ||
		strings.Contains(objectType, BFC_CODE) || strings.Contains(objectType, BFC_CODE1)
}

func IsNativePrefixs(objectType string) bool {
	return strings.HasPrefix(objectType, "0x2::") || strings.HasPrefix(objectType, "0x3::") ||
		strings.HasPrefix(objectType, "0x0000000000000000000000000000000000000000000000000000000000000002::") ||
		strings.HasPrefix(objectType, "0x0000000000000000000000000000000000000000000000000000000000000003::") ||
		strings.HasPrefix(objectType, "BFC00000000000000000000000000000000000000000000000000000000000002e7e9::")
}

func IsNativeStakedBfc(objectType string) bool {
	return strings.HasPrefix(objectType, "0x3::stable_pool::StakedStable<") && strings.HasSuffix(objectType, ">")
}

func Init(handler string, value *conf.PlatInfo, nodeURL []string) *Platform {
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
