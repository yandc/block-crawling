package sui

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/utils"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type Platform struct {
	biz.CommPlatform
	CoinIndex uint
	spider    *chain.BlockSpider
}

const (
	SUI_CODE             = "0x2::sui::SUI"
	SUI_CODE1            = "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI"
	BFC_CODE             = "0x2::bfc::BFC"
	BFC_CODE1            = "0x0000000000000000000000000000000000000000000000000000000000000002::bfc::BFC"
	BFC_CODE2            = "BFC000000000000000000000000000000000000000000000000000000000000000268e4::bfc::BFC"
	PAY_CHAINGE_CATEGORY = 1
	PAY_REFUND_CATEGORY  = 11
	PAY_ASSEM_CATEGORY   = 2
	PAY_TRANS_CATEGORY   = 3
	EVENT_TYPE_CHARGE    = "charge"
	EVENT_TYPE_RETRIEVEL = "retrieval"
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
		strings.HasPrefix(objectType, "0xc8") ||
		strings.HasPrefix(objectType, "0x0000000000000000000000000000000000000000000000000000000000000002::") ||
		strings.HasPrefix(objectType, "0x0000000000000000000000000000000000000000000000000000000000000003::") ||
		strings.HasPrefix(objectType, "0x00000000000000000000000000000000000000000000000000000000000000c8::") ||
		strings.HasPrefix(objectType, "BFC00000000000000000000000000000000000000000000000000000000000002e7e9::") ||
		strings.HasPrefix(objectType, "BFC00000000000000000000000000000000000000000000000000000000000000c8e30a::")
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

type pushSUIPayCardResq struct {
	*data.SuiTransactionRecord
	Chain           string          `json:"chain"`
	CardUUID        string          `json:"cardUUID"`
	EventType       string          `json:"eventType"`
	DepositAmount   decimal.Decimal `json:"depositAmount"`
	AvailableAmount decimal.Decimal `json:"availableAmount"`
	WithDrawAmount  decimal.Decimal `json:"withDrawAmount"`
}

//PushSUIPayCardCMQ
/**
category=1
在to  推到主题  obcard_charge_onchain_result  topic-bZv2d5DoSmRo3YDHToGjTG
在from  推到主题  obcard_refund_onchain_result  topic-9nHdeiiRvSck7afzCkBDTr
category=2
在to 推到主题  obcard_assem_onchain_result   topic-UwQWsxsWuq8dzNGdDZGGns
category=3
在from 推到主题 obpay_trans_onchain_result    topic-DfBim9dBKXRqEHLpqAFhcf
*/
func PushSUIPayCardCMQ(category int, data pushSUIPayCardResq) {
	log.Info("PushSUIPayCardCMQ start", zap.Any("chainName", data.Chain), zap.Any("cardUUID", data.CardUUID), zap.Any("eventType", data.EventType))
	topicId := getPayCardTopicId(category)
	if data.SuiTransactionRecord != nil && topicId != "" {
		//resq := pushSUIPayCardResq{SuiTransactionRecord: data, Chain: chainName, CardUUID: cardUuid,EventType: eventType}
		rawMsg, _ := utils.JsonEncode(data)
		biz.PushTopicCMQ(data.Chain, topicId, rawMsg, biz.AppConfig.Cmq.Endpoint.TopicURL)

	}
}

func getPayCardTopicId(category int) string {
	switch category {
	case PAY_CHAINGE_CATEGORY:
		return biz.AppConfig.Cmq.Topic.PayCardCharge.Id
	case PAY_ASSEM_CATEGORY:
		return biz.AppConfig.Cmq.Topic.PayCardAssem.Id
	case PAY_TRANS_CATEGORY:
		return biz.AppConfig.Cmq.Topic.PayCardTrans.Id
	case PAY_REFUND_CATEGORY: //from category = 1
		return biz.AppConfig.Cmq.Topic.PayCardRefund.Id
	}
	return ""
}

func CheckContractCard(chainName string, transactionInfo *stypes.TransactionInfo, data *data.SuiTransactionRecord) {
	if transactionInfo == nil {
		return
	}
	//get events
	events, err := transactionInfo.Events()
	if err != nil {
		log.Error("CheckContractCard get events error ", zap.Error(err))
		return
	}
	for _, event := range events {
		if !strings.Contains(event.Type, "::") {
			log.Error("CheckContractCard events type is error ", zap.Any("eventsType", event.Type))
			return
		}
		eventTypeKey := strings.SplitN(event.Type, "::", 2)[1]
		if eventType, _ := biz.GetBenfenCardEvent(eventTypeKey); eventType != "" {
			parseJson := &stypes.FundsParseJson{}
			if err := event.ParseJson(parseJson); err != nil {
				log.Error("CheckContractCard get parseJson error ", zap.Error(err))
			}
			suiResq := pushSUIPayCardResq{SuiTransactionRecord: data, Chain: chainName, CardUUID: parseJson.CardUuid,
				DepositAmount: decimal.Zero, AvailableAmount: decimal.Zero, WithDrawAmount: decimal.Zero}
			switch eventType {
			//充值
			case "DepositEvent":
				suiResq.EventType = EVENT_TYPE_CHARGE
				if parseJson.AvailableAmount != "" {
					suiResq.AvailableAmount, err = decimal.NewFromString(parseJson.AvailableAmount)
					if err != nil {
						log.Error("CheckContractCard  NewFromString AvailableAmount error ", zap.Error(err))
					}
				}
				if parseJson.DepositAmount != "" {
					suiResq.DepositAmount, err = decimal.NewFromString(parseJson.DepositAmount)
					if err != nil {
						log.Error("CheckContractCard  NewFromString DepositAmount error ", zap.Error(err))
					}
				}
				//提取
			case "WithdrawEvent":
				suiResq.EventType = EVENT_TYPE_RETRIEVEL
				if parseJson.WithdrawAmount != "" {
					suiResq.WithDrawAmount, err = decimal.NewFromString(parseJson.WithdrawAmount)
					if err != nil {
						log.Error("CheckContractCard  NewFromString WithdrawAmount error ", zap.Error(err))
					}
				}
			default:
				log.Error("CheckContractCard dont support eventType.", zap.Any("eventType", eventType))
				return

			}
			go PushSUIPayCardCMQ(PAY_CHAINGE_CATEGORY, suiResq)
		}
	}
}
