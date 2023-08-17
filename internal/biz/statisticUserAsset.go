package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strconv"
	"time"
)

type UserAssetStatistic struct {
	ChainName    string          `json:"chainName"`
	FromUid      string          `json:"fromUid"`
	ToUid        string          `json:"toUid"`
	Amount       decimal.Decimal `json:"amount"`
	TokenAddress string          `json:"tokenAddress"`
	Decimals     int32           `json:"decimals"`
}

func HandleUserAssetStatistic(chainName string, txRecords []UserAssetStatistic) {
	tm := time.Now()
	nowTime := tm.Unix()
	var dt = utils.GetDayTime(&tm)
	//资金流向: 1:充值, 2:提现, 3:内部转账

	//资金类型: 1:小单提现, 2:次中单提现, 3:中单提现, 4:大单提现, 5:超大单提现
	//小单：金额<1K
	//次中单：1K=<金额<1W
	//中单：1W=<金额<10W
	//大单：10W=<金额<100W
	//超大单：100W=<金额

	var transactionStatisticMap = make(map[string]*data.TransactionStatistic)
	var transactionStatisticList []*data.TransactionStatistic
	for _, record := range txRecords {
		var fundDirection int16
		var fundType int16
		var amount = record.Amount
		var tokenAddress = record.TokenAddress
		var decimals = record.Decimals
		var cnyAmount decimal.Decimal
		var usdAmount decimal.Decimal

		if record.FromUid == "" && record.ToUid == "" {
			continue
		} else if record.FromUid == "" {
			fundDirection = 1
		} else if record.ToUid == "" {
			fundDirection = 2
		} else {
			fundDirection = 3
		}

		cnyPrice, err := GetTokenPriceRetryAlert(nil, chainName, CNY, tokenAddress)
		if err != nil {
			log.Error("交易记录统计，从nodeProxy中获取代币价格失败", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
			continue
		}
		usdPrice, err := GetTokenPriceRetryAlert(nil, chainName, USD, tokenAddress)
		if err != nil {
			log.Error("交易记录统计，从nodeProxy中获取代币价格失败", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
			continue
		}

		cnyPrices, _ := decimal.NewFromString(cnyPrice)
		usdPrices, _ := decimal.NewFromString(usdPrice)
		balance := utils.StringDecimals(amount.String(), int(decimals))
		balances, _ := decimal.NewFromString(balance)
		cnyAmount = cnyPrices.Mul(balances)
		usdAmount = usdPrices.Mul(balances)
		if cnyAmount.LessThan(decimal.NewFromInt(1000)) {
			fundType = 1
		} else if cnyAmount.LessThan(decimal.NewFromInt(10000)) {
			fundType = 2
		} else if cnyAmount.LessThan(decimal.NewFromInt(100000)) {
			fundType = 3
		} else if cnyAmount.LessThan(decimal.NewFromInt(100000)) {
			fundType = 4
		} else {
			fundType = 5
		}

		key := chainName + strconv.Itoa(int(fundDirection)) + strconv.Itoa(int(fundType))
		if statistic, ok := transactionStatisticMap[key]; ok {
			statistic.TransactionQuantity += 1
			statistic.Amount = statistic.Amount.Add(amount)
			statistic.CnyAmount = statistic.CnyAmount.Add(cnyAmount)
			statistic.UsdAmount = statistic.UsdAmount.Add(usdAmount)
		} else {
			var transactionStatistic = &data.TransactionStatistic{
				ChainName:           chainName,
				TokenAddress:        tokenAddress,
				FundDirection:       fundDirection,
				FundType:            fundType,
				TransactionQuantity: 1,
				Amount:              amount,
				CnyAmount:           cnyAmount,
				UsdAmount:           usdAmount,
				Dt:                  dt,
				CreatedAt:           nowTime,
				UpdatedAt:           nowTime,
			}

			transactionStatisticMap[key] = transactionStatistic
		}
	}

	if len(transactionStatisticMap) == 0 {
		return
	}
	for _, transactionStatistic := range transactionStatisticMap {
		transactionStatisticList = append(transactionStatisticList, transactionStatistic)
	}
	_, err := data.TransactionStatisticRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionStatisticList, PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.TransactionStatisticRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionStatisticList, PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s交易记录统计，将数据插入到数据库中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("交易记录统计，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
	}
}
