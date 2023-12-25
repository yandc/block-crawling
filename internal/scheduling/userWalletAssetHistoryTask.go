package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strings"
	"time"
)

type UserWalletAssetHistoryTask struct {
}

func NewUserWalletAssetHistoryTask() *UserWalletAssetHistoryTask {
	task := UserWalletAssetHistoryTask{}
	return &task
}

func (task *UserWalletAssetHistoryTask) Run() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UserWalletAssetHistoryTask error", e)
			} else {
				log.Errore("UserWalletAssetHistoryTask panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：每日用户资产快照失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	yesterdayTimestamp := date.Unix() - 3600*24
	log.Info("UserWalletAssetHistoryTask start")

	//分页查询所有uid
	ctx := context.Background()
	var uids []string
	var offset, limit = 0, 50
	var err error

	//分页查询uid
	for {
		uids, err = data.UserAssetRepoClient.FindDistinctUidByOffset(ctx, offset, limit)
		offset += limit
		if err != nil {
			log.Error("FindDistinctUidByOffset error", zap.String("error", err.Error()))
			continue
		}

		if len(uids) == 0 {
			return
		}

		//批量查询用户资产
		userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, uids)
		if err != nil {
			log.Error("FindByUids error", zap.String("error", err.Error()))
			continue
		}

		if len(userAssets) == 0 {
			continue
		}
		//获取所有币价
		tokenPriceMap, err := biz.GetAssetsPrice(userAssets)
		if err != nil {
			continue
		}

		//获取 美元/人民币 换算价格
		cnyRate, err := biz.GetCnyRate()
		if err != nil {
			continue
		}

		//资产分组求和、资产昨日差值
		userAmountMap := map[string]decimal.Decimal{}    //统计用户总资产
		userAmountChange := map[string]decimal.Decimal{} //统计用户净流入流出资产
		for _, asset := range userAssets {

			platInfo, _ := biz.GetChainPlatInfo(asset.ChainName)
			if platInfo == nil {
				continue
			}

			//过滤测试网
			if platInfo.NetType != biz.MAIN_NET_TYPE {
				continue
			}

			var key string
			if asset.TokenAddress == "" {
				key = platInfo.GetPriceKey
			} else {
				key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
			}
			price := decimal.NewFromFloat(tokenPriceMap[key].Price)

			//查询该币昨日的交易金额
			if asset.UpdatedAt > yesterdayTimestamp {
				assetChange := task.getYesterdayAssetChange(asset, date)
				userAmountChange[asset.Uid] = userAmountChange[asset.Uid].Add(assetChange.Mul(price))
				if _, ok := userAmountMap[asset.Uid]; !ok {
					userAmountMap[asset.Uid] = decimal.Decimal{} //有交易记录时初始化资产，防止资产为 0 时未打快照
				}
			}

			if asset.Balance == "" || asset.Balance == "0" {
				continue
			}

			balanceDecimal, err := decimal.NewFromString(asset.Balance)
			if err != nil {
				continue
			}

			amount := balanceDecimal.Mul(price)
			if amount.LessThan(decimal.NewFromFloat(10)) {
				continue
			}
			userAmountMap[asset.Uid] = userAmountMap[asset.Uid].Add(amount)
		}

		//批量插入用户资产历史
		var userWalletAssetHistories []*data.UserWalletAssetHistory
		for uid, amount := range userAmountMap {

			userWalletAssetHistories = append(userWalletAssetHistories, &data.UserWalletAssetHistory{
				Uid:       uid,
				UsdAmount: amount,
				UsdChange: userAmountChange[uid].Round(2),
				BtcPrice:  decimal.NewFromFloat(tokenPriceMap[biz.PriceKeyBTC].Price),
				UsdtPrice: decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(biz.ETH_USDT_ADDRESS))].Price),
				CnyPrice:  decimal.NewFromFloat(cnyRate.Rate),
				Dt:        date.Unix(),
				CreatedAt: now.Unix(),
			})
		}
		err = data.UserWalletAssetHistoryRepoClient.SaveBatch(ctx, userWalletAssetHistories)
		if err != nil {
			log.Error(err.Error())
			continue
		}
	}
}

func (task *UserWalletAssetHistoryTask) getYesterdayAssetChange(asset *data.UserAsset, date time.Time) decimal.Decimal {
	yesterdayTimestamp := date.Unix() - 3600*24
	request := data.TransactionRequest{
		Address:      asset.Address,
		TokenAddress: asset.TokenAddress,
		StatusList:   []string{biz.SUCCESS},
		StartTime:    yesterdayTimestamp,
		StopTime:     date.Unix(),
		Nonce:        -1, //负数代表没有此筛选条件
	}

	if asset.TokenAddress != "" {
		request.TransactionTypeList = []string{biz.EVENTLOG, biz.TRANSFER}
	} else {
		request.TransactionTypeList = []string{biz.NATIVE, biz.MINT, biz.SWAP, biz.CONTRACT, biz.ADDLIQUIDITY}
	}
	records, _, err := biz.TransactionRecordRepoClient.PageList(nil, asset.ChainName, &request)

	if err != nil || len(records) == 0 {
		return decimal.Decimal{}
	}

	//获取精度
	var decimals int
	if asset.TokenAddress == "" {
		platInfo, _ := biz.GetChainPlatInfo(asset.ChainName)
		decimals = int(platInfo.Decimal)
	} else {
		info, err := biz.ParseGetTokenInfo(asset.ChainName, records[0].ParseData)
		if err != nil || info.Decimals == 0 {
			return decimal.Decimal{}
		}
		decimals = int(info.Decimals)
	}

	var assetChange decimal.Decimal
	for _, record := range records {
		if record.FromAddress == asset.Address { //转出
			assetChange = assetChange.Sub(record.Amount)
		} else if record.ToAddress == asset.Address { //转入
			assetChange = assetChange.Add(record.Amount)
		}
	}

	//返回乘以精度之后的值
	return biz.Pow10(assetChange, -decimals)
}
