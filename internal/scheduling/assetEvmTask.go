package scheduling

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strconv"
	"time"
)

type AssetEvmTask struct {
}

func NewAssetEvmTask() *AssetEvmTask {
	assetEvmTask := &AssetEvmTask{}
	return assetEvmTask
}

func (task *AssetEvmTask) Run() {
	AssetEvm()

}

func AssetEvm() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("AssetEvm error", e)
			} else {
				log.Errore("AssetEvm panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：定时查询币价信息失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	tm := time.Now()
	var dt = utils.GetDayTime(&tm)
	log.Info("统计四条链资产金额，处理指标数据开始", zap.Any("dt", dt), zap.Any("nowTime", tm))
	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum"}
	var userAssetsTotal []*data.UserAsset
	var priceTokens []*v1.DescribePriceByCoinAddressReply
	var coinPriceMap = make(map[string]*v1.Currency)
	var tokenPriceMap = make(map[string]*v1.Currency)
	var chainAndAddressMap = make(map[string]*data.UserAssetHistory)
	var userAssetHistorys = make([]*data.UserAssetHistory, 0)
	tokens := make([]*v1.Tokens, 0)
	//多链循环数据
	for _, chainName := range chainNames {
		var request = &data.AssetRequest{
			ChainName:     chainName,
			SelectColumn:  "id, chain_name, token_address, balance, address , uid",
			OrderBy:       "id asc",
			StartIndex:    0,
			DataDirection: 2,
			PageSize:      data.MAX_PAGE_SIZE,
			Total:         false,
		}
		var err error
		var total int64
		for {
			var userAssets []*data.UserAsset
			userAssets, _, err = data.UserAssetRepoClient.PageList(nil, request)
			if err != nil {
				break
			}
			dataLen := int32(len(userAssets))
			if dataLen == 0 {
				break
			}

			total += int64(dataLen)
			for _, userAsset := range userAssets {
				if userAsset.TokenAddress != "" {
					tokens = append(tokens, &v1.Tokens{
						Chain:   chainName,
						Address: userAsset.TokenAddress,
					})
				}
				userAssetsTotal = append(userAssetsTotal, userAsset)
			}
			if dataLen < request.PageSize {
				break
			}
			request.StartIndex = userAssets[dataLen-1].Id
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	if userAssetsTotal == nil {
		log.Info("统计四条链资产金额查出0条！")
		return
	} else {
		log.Info("统计四条链资产金额，处理指标数据中。。。。", zap.Any("需要处理", len(userAssetsTotal)))
	}
	ethCoinId := "ethereum"
	bscCoinId := "binancecoin"
	polygonCoinId := "matic-network"
	coinIds := []string{ethCoinId, bscCoinId, polygonCoinId}
	total := len(tokens)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTokenIds := tokens[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}
		nowPrice, err := biz.GetPriceFromMarket(subTokenIds, coinIds)
		if err != nil {
			log.Info("查询币价失败", zap.Any("error", err))
			return
		}
		for _, nativeSymbol := range nowPrice.Coins {
			coinPriceMap[nativeSymbol.CoinID] = nativeSymbol.Price
		}
		for _, tokenSymbol := range nowPrice.Tokens {
			key := tokenSymbol.Chain + tokenSymbol.Address
			tokenPriceMap[key] = tokenSymbol.Price
		}
		priceTokens = append(priceTokens, nowPrice)
		log.Info("调用用户中心", zap.Any("request-tokenAddress", subTokenIds), zap.Any("request-coin", coinIds), zap.Any("result", nowPrice), zap.Error(err))
	}

	for _, userAssert := range userAssetsTotal {
		caaMapKey := userAssert.ChainName + userAssert.Address
		if userAssert.TokenAddress != "" {
			key := userAssert.ChainName + userAssert.TokenAddress
			tp := tokenPriceMap[key]
			if tp != nil {
				cnyPrice := strconv.FormatFloat(tp.Cny, 'f', 2, 64)
				cpd, _ := decimal.NewFromString(cnyPrice)
				usdPrice := strconv.FormatFloat(tp.Usd, 'f', 2, 64)
				upd, _ := decimal.NewFromString(usdPrice)
				balance, _ := decimal.NewFromString(userAssert.Balance)
				if oldUserAsset, ok := chainAndAddressMap[caaMapKey]; ok {
					oldUserAsset.CnyAmount = oldUserAsset.CnyAmount.Add(balance.Mul(cpd))
					oldUserAsset.UsdAmount = oldUserAsset.UsdAmount.Add(balance.Mul(upd))
				} else {
					ua := &data.UserAssetHistory{
						ChainName: userAssert.ChainName,
						Uid:       userAssert.Uid,
						Address:   userAssert.Address,
						Dt:        dt,
						CreatedAt: tm.Unix(),
						UpdatedAt: tm.Unix(),
					}
					ua.CnyAmount = balance.Mul(cpd)
					ua.UsdAmount = balance.Mul(upd)
					chainAndAddressMap[caaMapKey] = ua
				}

			}
		} else {
			nativeSymbol := biz.PlatInfoMap[userAssert.ChainName].GetPriceKey
			cp := coinPriceMap[nativeSymbol]
			if cp != nil {
				cnyPrice := strconv.FormatFloat(cp.Cny, 'f', 2, 64)
				cpd, _ := decimal.NewFromString(cnyPrice)
				usdPrice := strconv.FormatFloat(cp.Usd, 'f', 2, 64)
				upd, _ := decimal.NewFromString(usdPrice)
				balance, _ := decimal.NewFromString(userAssert.Balance)

				if oldUserAsset, ok := chainAndAddressMap[caaMapKey]; ok {
					oldUserAsset.CnyAmount = oldUserAsset.CnyAmount.Add(balance.Mul(cpd))
					oldUserAsset.UsdAmount = oldUserAsset.UsdAmount.Add(balance.Mul(upd))
				} else {
					ua := &data.UserAssetHistory{
						ChainName: userAssert.ChainName,
						Uid:       userAssert.Uid,
						Address:   userAssert.Address,
						Dt:        dt,
						CreatedAt: tm.Unix(),
						UpdatedAt: tm.Unix(),
					}
					ua.CnyAmount = balance.Mul(cpd)
					ua.UsdAmount = balance.Mul(upd)
					chainAndAddressMap[caaMapKey] = ua
				}
			}
		}
	}
	for _,ua := range chainAndAddressMap {
		userAssetHistorys = append(userAssetHistorys, ua)
	}
	t, _ := data.UserAssetHistoryRepoClient.PageBatchSaveOrUpdate(nil, userAssetHistorys, 200)
	log.Info("统计四条链资产金额，处理指标数据结束", zap.Any("dt", dt), zap.Any("nowTime", tm), zap.Any("共处理", t))

}
