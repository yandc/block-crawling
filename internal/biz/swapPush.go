package biz

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"strconv"

	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

type SwapPair struct {
	TxTime       int          `json:"txTime"`
	BlockNumber  int          `json:"blockNumber"`
	FromAddress  string       `json:"fromAddress"`
	Chain        string       `json:"chain"`
	TxHash       string       `json:"txHash"`
	Dex          string       `json:"dex"`
	DexContract  string       `json:"dexContract"`
	PairContract string       `json:"pairContract"`
	Input        SwapPairItem `json:"input"`
	Output       SwapPairItem `json:"output"`
}

type SwapPairItem struct {
	Address string `json:"address"`
	Amount  string `json:"amount"`
}

type TransMq struct {
	Timestamp     uint32      `json:"timestamp"`
	BlockNumber   int64       `json:"block_number"`
	WalletAddress string      `json:"wallet_address"`
	Chain         string      `json:"chain"`
	DexID         string      `json:"dex_id"`
	PairAddress   string      `json:"pair_address"`
	TxHash        string      `json:"tx_hash"`
	TokenIn       *TransToken `json:"token_in"`
	TokenOut      *TransToken `json:"token_out"`
}

type TransToken struct {
	Address string  `json:"address"`
	Amount  float64 `json:"amount"`
}

func BulkPushSwapPairs(chainName string, pairs []*SwapPair) error {
	for _, item := range pairs {
		amountIn, err := rawAmountToFloat(chainName, item.Input.Address, item.Input.Amount)
		if err != nil {
			log.Error("[SWAP] PARSE AMOUNT TO FLOAT FAILED", zap.Any("input", item.Input), zap.String("chainName", chainName), zap.Error(err))
			return err
		}
		amountOut, err := rawAmountToFloat(chainName, item.Output.Address, item.Output.Amount)
		if err != nil {
			log.Error("[SWAP] PARSE AMOUNT TO FLOAT FAILED", zap.Any("output", item.Output), zap.String("chainName", chainName), zap.Error(err))
			return err
		}

		if IsCustomChain(item.Chain) && !IsCustomChainFeatured(chainName) {
			continue
		}
		if IsTestNet(item.Chain) {
			continue
		}
		if real, ok := AppConfig.FeaturedCustomChain[item.Chain]; ok {
			item.Chain = real
		}

		msg := &TransMq{
			Timestamp:     uint32(item.TxTime),
			BlockNumber:   int64(item.BlockNumber),
			WalletAddress: item.FromAddress,
			Chain:         item.Chain,
			DexID:         item.Dex,
			PairAddress:   item.PairContract,
			TxHash:        item.TxHash,
			TokenIn: &TransToken{
				Address: item.Input.Address,
				Amount:  amountIn,
			},
			TokenOut: &TransToken{
				Address: item.Output.Address,
				Amount:  amountOut,
			},
		}
		if err := pushTransMq(chainName, msg); err != nil {
			log.Error("[SWAP] PUSH PAIR TO QUEUE FAILED", zap.Any("msg", msg), zap.Error(err))
			return err
		}
	}
	return nil
}

func rawAmountToFloat(chainName string, tokenAddress string, amount string) (float64, error) {
	tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), chainName, tokenAddress)
	if err != nil {
		return 0, err
	}
	amount = utils.StringDecimals(amount, int(tokenInfo.Decimals))
	v, err := strconv.ParseFloat(amount, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

type eventOut struct {
	EventID string `json:"eventId"`
	Data    struct {
		MsgID string `json:"msgId"`
		Count int    `json:"count"`
	} `json:"data"`
}

func pushTransMq(chainName string, s *TransMq) error {
	rawMsg, _ := utils.JsonEncode(s)
	eventId := uuid.NewV1().String()
	var out *eventOut
	err := httpclient.HttpPostJson(
		AppConfig.Cmq.Endpoint,
		map[string]interface{}{
			"eventId": eventId,
			"queueId": AppConfig.Cmq.Queues.SwapPairs.Id,
			"msg":     rawMsg,
		},
		&out, nil,
	)

	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链推送pairs信息，推送pairs信息到 CMQ 中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("kanban")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("推送pairs信息，推送pairs信息到 CMQ 中失败", zap.Any("chainName", chainName), zap.Any("error", err), zap.Any("msg", rawMsg))
		return err
	}
	log.Info("推送pairs信息，推送pairs信息到 CMQ 中成功", zap.Any("chainName", chainName), zap.Any("msg", rawMsg), zap.Any("out", out))
	return nil
}
