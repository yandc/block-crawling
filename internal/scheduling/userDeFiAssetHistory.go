package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/defi"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type UserDeFiAssetHistoryTask struct {
}

func NewUserDeFiAssetHistoryTask() *UserDeFiAssetHistoryTask {
	task := UserDeFiAssetHistoryTask{}
	return &task
}

func (task *UserDeFiAssetHistoryTask) Run() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UserDeFiAssetHistoryTask error", e)
			} else {
				log.Errore("UserDeFiAssetHistoryTask panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：每日用户 DeFi 资产快照失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	log.Info("UserDeFiAssetHistoryTask start")

	//分页查询所有uid
	ctx := context.Background()
	var uids []string
	var cursor, limit = 0, 50
	var err error

	processedUids := make(map[string]bool)
	//分页查询uid
	for {
		uids, err = data.DeFiAssetRepoInst.CursorListUids(ctx, &cursor, limit)
		if err != nil {
			log.Error("FindDistinctUidByOffset error", zap.String("error", err.Error()))
			continue
		}

		if len(uids) == 0 {
			return
		}

		//获取 美元/人民币 换算价格
		cnyRate, err := biz.GetCnyRate()
		if err != nil {
			return
		}

		for _, uid := range uids {
			if _, ok := processedUids[uid]; ok {
				continue
			}
			processedUids[uid] = true

			//批量查询用户资产
			userDeFiAssets, err := data.DeFiAssetRepoInst.FindByUid(ctx, uid)
			if err != nil {
				log.Error("FindByUids error", zap.String("error", err.Error()))
				continue
			}

			if len(userDeFiAssets) == 0 {
				continue
			}

			defiAmountsByPlat := make(map[int64]decimal.Decimal) // 按平台统计资产
			defiAmountsByTyp := make(map[string]decimal.Decimal) // 按类型统计资产
			updatedDeFiAssets := make([]*data.UserDeFiAsset, 0, 4)
			userAssets := make([]*data.UserAsset, 0, 4)
			tokenPriceMap := make(map[string]biz.MarketPrice)
			for _, userDeFiAsset := range userDeFiAssets {
				req := &data.DeFiOpenPostionReq{
					ChainName:    userDeFiAsset.ChainName,
					PlatformID:   userDeFiAsset.PlatformID,
					Address:      userDeFiAsset.Address,
					AssetAddress: userDeFiAsset.AssetAddress,
					TxTime:       userDeFiAsset.OpenedAt,
				}
				histories, err := data.DeFiAssetRepoInst.LoadAssetHistories(context.Background(), req, userDeFiAsset.Type)
				if err != nil {
					log.Error("DEFI: LOAD ASSET HISTORIES FAILED", zap.Any("req", req), zap.Error(err))
				}
				for _, item := range histories {
					userAssets = append(userAssets, &data.UserAsset{
						ChainName:    item.ChainName,
						Uid:          uid,
						Address:      item.Address,
						TokenAddress: item.TokenAddress,
						TokenUri:     item.TokenUri,
						Balance:      item.Amount,
						Decimals:     item.Decimals,
						Symbol:       item.Symbol,
						CostPrice:    item.UsdPrice.Mul(item.RawAmount).String(),
					})
				}

				//获取所有币价
				tokenPrices, err := biz.GetAssetsPrice(userAssets)
				if err != nil {
					continue
				}
				for k, v := range tokenPrices {
					tokenPriceMap[k] = v
				}

				for _, item := range histories {
					key := biz.GetAssetPriceKey(item.ChainName, item.TokenAddress)
					item.UsdPrice = decimal.NewFromFloat(tokenPrices[key].Price)
				}
				valueUsd := data.DeFiAssetRepoInst.GetValueUsd(histories)
				userDeFiAsset.ValueUsd = valueUsd
				userDeFiAsset.ProfitUsd = defi.ComputeProfit(userDeFiAsset, histories)
				userDeFiAsset.CostUsd = valueUsd // 更新零值
				updatedDeFiAssets = append(updatedDeFiAssets, userDeFiAsset)
				if valueUsd.Round(2).IsZero() {
					continue
				}
				defiAmountsByPlat[histories[0].PlatformID] = defiAmountsByPlat[histories[0].PlatformID].Add(valueUsd)
				defiAmountsByPlat[0] = defiAmountsByPlat[0].Add(valueUsd)
				defiAmountsByTyp[histories[0].Type] = defiAmountsByTyp[histories[0].Type].Add(valueUsd)
			}
			if err := data.DeFiAssetRepoInst.SaveDeFiAsset(context.TODO(), updatedDeFiAssets); err != nil {
				log.Info("DEFI: UPDATE DeFi Asset failed", zap.Any("err", err.Error()), zap.Any("assets", updatedDeFiAssets))
			}

			//批量插入用户 DeFi 资产历史
			var userWalletAssetHistories []*data.UserWalletDeFiAssetHistory
			for platformID, amount := range defiAmountsByPlat {
				userWalletAssetHistories = append(userWalletAssetHistories, &data.UserWalletDeFiAssetHistory{
					Uid:        uid,
					PlatformID: platformID,
					Type:       data.DeFiAssetTypeAll,
					UsdAmount:  amount,
					UsdChange:  decimal.Zero, // TODO(wanghui)
					BtcPrice:   decimal.NewFromFloat(tokenPriceMap[biz.PriceKeyBTC].Price),
					UsdtPrice:  decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(biz.ETH_USDT_ADDRESS))].Price),
					CnyPrice:   decimal.NewFromFloat(cnyRate.Rate),
					Dt:         date.Unix(),
					CreatedAt:  now.Unix(),
				})
			}
			for typ, amount := range defiAmountsByTyp {
				userWalletAssetHistories = append(userWalletAssetHistories, &data.UserWalletDeFiAssetHistory{
					Uid:        uid,
					PlatformID: 0,
					Type:       typ,
					UsdAmount:  amount,
					UsdChange:  decimal.Zero, // TODO(wanghui)
					BtcPrice:   decimal.NewFromFloat(tokenPriceMap[biz.PriceKeyBTC].Price),
					UsdtPrice:  decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(biz.ETH_USDT_ADDRESS))].Price),
					CnyPrice:   decimal.NewFromFloat(cnyRate.Rate),
					Dt:         date.Unix(),
					CreatedAt:  now.Unix(),
				})
			}
			if len(userWalletAssetHistories) > 0 {
				err = data.DeFiAssetRepoInst.SaveBatch(ctx, userWalletAssetHistories)
				if err != nil {
					log.Error(err.Error())
					continue
				}
			}
		}
	}
}
