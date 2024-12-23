package biz

import (
	pb "block-crawling/api/userWalletAsset/v1"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

type UserWalletAssetUsecase struct {
	gormDB *gorm.DB
	lark   Larker
}

func NewUserWalletAssetUsecase(grom *gorm.DB, lark Larker) *UserWalletAssetUsecase {
	return &UserWalletAssetUsecase{
		gormDB: grom,
		lark:   lark,
	}
}

func (uc UserWalletAssetUsecase) UserWalletAssetTotal(ctx context.Context, req *pb.UserWalletAssetTotalReq) (*pb.UserWalletAssetTotalResp, error) {
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	//获取用户所有资产
	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserWalletAssetTotalResp{
			Cny:              &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{Amount: "0", Income: "0"},
			Usd:              &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{Amount: "0", Income: "0"},
			Usdt:             &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{Amount: "0", Income: "0"},
			Btc:              &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{Amount: "0", Income: "0"},
			IncomePercentage: "0",
		}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	if req.Platform == "web" || req.Platform == "" {
		assets, err := data.DeFiAssetRepoInst.FindByUids(ctx, req.Uids)
		if err != nil {
			return nil, err
		}
		for _, asset := range assets {
			userAssets = append(userAssets, &data.UserAsset{
				ChainName:    asset.ChainName,
				Uid:          asset.Uid,
				Address:      asset.Address,
				Balance:      asset.ValueUsd.String(),
				TokenAddress: "DEFI",
			})
		}
	}

	//资产求和
	var assetTotal decimal.Decimal
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)
		amount := decimal.Zero
		if asset.TokenAddress == "DEFI" {
			amount = balanceDecimal
		} else {
			var key string
			if asset.TokenAddress == "" {
				key = platInfo.GetPriceKey
			} else {
				key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
			}
			price := decimal.NewFromFloat(tokenPriceMap[key].Price)
			amount = balanceDecimal.Mul(price).Round(2)
		}
		if amount.IsZero() { //过滤保留两位小数四舍五入之后的资产
			continue
		}
		assetTotal = assetTotal.Add(amount)
	}

	if assetTotal.IsZero() {
		return &pb.UserWalletAssetTotalResp{IncomePercentage: "0"}, nil
	}
	assetTotal = assetTotal.Round(2)
	cnyAmount := assetTotal.Mul(decimal.NewFromFloat(cnyRate.Rate))
	usdtAmount := assetTotal.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	btcAmount := assetTotal.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

	//昨日净划入、昨日总资产、今日总资产
	var todayAssetTotal, yesterdayAssetTotal, changeTotal decimal.Decimal
	todayTimestamp := date.Unix()                     //今日 0 时时间戳
	yesterdayTimestamp := todayTimestamp - DAY_SECOND //昨日 0 时时间戳
	histories, err := data.UserWalletAssetHistoryRepoClient.FindByUidsAndDts(ctx, req.Uids, []int64{todayTimestamp, yesterdayTimestamp})
	if req.Platform == "web" || req.Platform == "" {
		defiHistories, _ := data.DeFiAssetRepoInst.FindByUidsAndDts(ctx, req.Uids, []int64{todayTimestamp, yesterdayTimestamp})
		histories = append(histories, defiHistories...)
	}
	for _, history := range histories {
		if history.Dt == todayTimestamp { //昨日净划入统计在今日快照中
			changeTotal = changeTotal.Add(history.UsdChange)
			todayAssetTotal = todayAssetTotal.Add(history.UsdAmount)
		} else if history.Dt == yesterdayTimestamp {
			yesterdayAssetTotal = yesterdayAssetTotal.Add(history.UsdAmount)
		}
	}

	//收益 = 期末资产(今日0时) - 期初资产(昨日0时) - 净划入资产(昨日净划入)
	income := todayAssetTotal.Sub(yesterdayAssetTotal).Sub(changeTotal)

	//今日收益
	cnyIncome := income.Mul(decimal.NewFromFloat(cnyRate.Rate))
	usdtIncome := income.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	btcIncome := income.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

	//收益率 收益/昨日初始资产
	var incomePercentage decimal.Decimal
	if !income.IsZero() && !yesterdayAssetTotal.IsZero() {
		incomePercentage = income.Div(yesterdayAssetTotal)
	}

	result := &pb.UserWalletAssetTotalResp{
		Cny: &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{
			Amount: cnyAmount.Round(2).String(),
			Income: cnyIncome.Round(2).String(),
		},
		Usd: &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{
			Amount: assetTotal.Round(2).String(),
			Income: income.Round(2).String(),
		},
		Usdt: &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{
			Amount: usdtAmount.Round(2).String(),
			Income: usdtIncome.Round(2).String(),
		},
		Btc: &pb.UserWalletAssetTotalResp_UserWalletAssetTotal{
			Amount: btcAmount.String(),
			Income: btcIncome.String(),
		},
		IncomePercentage: incomePercentage.Round(4).String(),
	}
	return result, nil
}

func (uc UserWalletAssetUsecase) getTodayAssetChange(asset *data.UserAsset) decimal.Decimal {
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	records, _, err := TransactionRecordRepoClient.PageList(nil, asset.ChainName, &data.TransactionRequest{
		Address:                  asset.Address,
		TokenAddress:             asset.TokenAddress,
		StatusList:               []string{SUCCESS},
		StartTime:                date.Unix(),
		StopTime:                 now.Unix(),
		TransactionTypeNotEquals: APPROVE,
	})

	if err != nil || len(records) == 0 {
		return decimal.Decimal{}
	}

	//获取精度
	var decimals int
	if asset.TokenAddress == "" {
		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		decimals = int(platInfo.Decimal)
	} else {
		info, err := ParseGetTokenInfo(asset.ChainName, records[0].ParseData)
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
	return Pow10(assetChange, -decimals)
}

func (uc UserWalletAssetUsecase) UserWalletAssetHistory(ctx context.Context, req *pb.UserWalletAssetHistoryReq) (*pb.UserWalletAssetHistoryResp, error) {
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	end := date.Unix()
	var start int64
	switch req.Range {
	case "7d":
		start = end - DAY_SECOND*7
	case "30d":
		start = end - DAY_SECOND*30
	case "90d":
		start = end - DAY_SECOND*90
	case "180d":
		start = end - DAY_SECOND*180
	}
	var findByUidsAndDTRange func(context.Context, []string, int64, int64) ([]*data.UserWalletAssetHistory, error)
	if req.IsDefi {
		findByUidsAndDTRange = data.DeFiAssetRepoInst.FindByUidsAndDTRange
	} else {
		findByUidsAndDTRange = data.UserWalletAssetHistoryRepoClient.FindByUidsAndDTRange
	}
	histories, err := findByUidsAndDTRange(ctx, req.Uids, start, end)
	if err != nil {
		return nil, err
	}

	if len(histories) == 0 {
		if req.Platform == IOS || req.Platform == ANDROID { //若无数据，安卓和 iOS 需要返回起始和终止的两个 0 数据
			return &pb.UserWalletAssetHistoryResp{
				Amount: &pb.Currency{Cny: "0", Usd: "0", Usdt: "0", Btc: "0"},
				Histories: []*pb.UserWalletAssetHistoryResp_UserWalletAssetHistory{
					{Time: start - 1, CnyAmount: "0", UsdAmount: "0", UsdtAmount: "0", BtcAmount: "0"},
					{Time: end - 1, CnyAmount: "0", UsdAmount: "0", UsdtAmount: "0", BtcAmount: "0"}},
			}, nil
		} else {
			return &pb.UserWalletAssetHistoryResp{
				Amount:    &pb.Currency{Cny: "0", Usd: "0", Usdt: "0", Btc: "0"},
				Histories: nil,
			}, nil
		}
	}

	btcPrice, usdtPrice, err := uc.getBTCAndUsdtPrice()
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//按照时间分组求和
	minDT := histories[0].Dt
	historyMap := map[int64]decimal.Decimal{}
	for _, history := range histories {
		historyMap[history.Dt] = historyMap[history.Dt].Add(history.UsdAmount)

		if history.Dt < minDT {
			minDT = history.Dt
		}
	}

	//缺失数据补充 0
	dt := minDT + DAY_SECOND
	for dt <= end {
		if _, ok := historyMap[dt]; !ok {
			historyMap[dt] = decimal.Decimal{}
		}
		dt += DAY_SECOND
	}
	var historiesList []*pb.UserWalletAssetHistoryResp_UserWalletAssetHistory
	for dt, amount := range historyMap {
		historiesList = append(historiesList, &pb.UserWalletAssetHistoryResp_UserWalletAssetHistory{
			Time:       dt - 1, //快照时间为0点，-1为了将时间显示为前一天
			CnyAmount:  amount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			UsdAmount:  amount.Round(2).String(),
			UsdtAmount: amount.Div(decimal.NewFromFloat(usdtPrice)).Round(2).String(),
			BtcAmount:  amount.Div(decimal.NewFromFloat(btcPrice)).String(),
		})

	}
	slices.SortFunc(historiesList, func(a, b *pb.UserWalletAssetHistoryResp_UserWalletAssetHistory) bool {
		return a.Time < b.Time
	})

	var firstDayUsdAmount *decimal.Decimal
	var lastDayUsdAmount *decimal.Decimal

	for _, h := range historiesList {
		camt, _ := decimal.NewFromString(h.UsdAmount)
		if firstDayUsdAmount == nil {
			firstDayUsdAmount = &camt
		}
		lastDayUsdAmount = &camt
		changeAmount := camt.Sub(*firstDayUsdAmount)

		h.ChangeAmount = uc.usdToCurrency(changeAmount, cnyRate, btcPrice, usdtPrice)
		h.ChangePercentage = changeAmount.Div(*firstDayUsdAmount).Round(4).String()
	}

	changeAmount := lastDayUsdAmount.Sub(*firstDayUsdAmount)
	result := &pb.UserWalletAssetHistoryResp{
		Amount: &pb.Currency{
			Cny:  historiesList[len(historiesList)-1].CnyAmount,
			Usd:  historiesList[len(historiesList)-1].UsdAmount,
			Usdt: historiesList[len(historiesList)-1].UsdtAmount,
			Btc:  historiesList[len(historiesList)-1].BtcAmount,
		},
		Histories:        historiesList,
		ChangeAmount:     uc.usdToCurrency(changeAmount, cnyRate, btcPrice, usdtPrice),
		ChangePercentage: changeAmount.Div(*firstDayUsdAmount).Round(4).String(),
	}
	return result, nil
}

func (uc UserWalletAssetUsecase) UserWalletIncomeHistory(ctx context.Context, req *pb.UserWalletIncomeHistoryReq) (*pb.UserWalletIncomeHistoryResp, error) {
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	end := date.Unix()
	var start int64
	switch req.Range {
	case "7d":
		start = end - DAY_SECOND*7
	case "30d":
		start = end - DAY_SECOND*30
	case "90d":
		start = end - DAY_SECOND*90
	case "180d":
		start = end - DAY_SECOND*180
	}

	var findByUidsAndDTRange func(context.Context, []string, int64, int64) ([]*data.UserWalletAssetHistory, error)
	if req.IsDefi {
		findByUidsAndDTRange = data.DeFiAssetRepoInst.FindByUidsAndDTRange
	} else {
		findByUidsAndDTRange = data.UserWalletAssetHistoryRepoClient.FindByUidsAndDTRange
	}
	histories, err := findByUidsAndDTRange(ctx, req.Uids, start, end)
	if err != nil {
		return nil, err
	}

	if len(histories) == 0 {
		if req.Platform == IOS || req.Platform == ANDROID { //若无数据，安卓和 iOS 需要返回起始和终止的两个 0 数据
			return &pb.UserWalletIncomeHistoryResp{
				Amount: &pb.Currency{Cny: "0", Usd: "0", Usdt: "0", Btc: "0"},
				Histories: []*pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory{
					{Time: start - 1, CnyAmount: "0", UsdAmount: "0", UsdtAmount: "0", BtcAmount: "0"},
					{Time: end - 1, CnyAmount: "0", UsdAmount: "0", UsdtAmount: "0", BtcAmount: "0"},
				},
			}, nil
		} else {
			return &pb.UserWalletIncomeHistoryResp{Amount: &pb.Currency{Cny: "0", Usd: "0", Usdt: "0", Btc: "0"}}, nil
		}
	}

	btcPrice, usdtPrice, err := uc.getBTCAndUsdtPrice()
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//按照 dt 升序排序
	slices.SortFunc(histories, func(a, b *data.UserWalletAssetHistory) bool {
		return a.Dt < b.Dt
	})

	minDT := histories[0].Dt

	//当日资产按照 dt 分组求和
	//净划入资产分组求和 dt当天净划入资产为前几日净划入之和
	totalMap := map[int64]decimal.Decimal{}
	changeMap := map[int64]decimal.Decimal{}
	for _, history := range histories {
		//资产求和
		totalMap[history.Dt] = totalMap[history.Dt].Add(history.UsdAmount)
		//净划入求和
		if _, ok := changeMap[history.Dt]; !ok {
			changeMap[history.Dt] = changeMap[history.Dt-DAY_SECOND] //初始化为前一天净划入总额
		}
		changeMap[history.Dt] = changeMap[history.Dt].Add(history.UsdChange)
	}

	startTimestamp := histories[0].Dt //期初时间戳
	historyMap := map[int64]*pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory{}
	for dt, amount := range totalMap {
		//收益 = 期末资产(dt) - 期初资产( min(dt) ) - 净划入资产( min(dt)~dt change之和 )
		income := amount.Sub(totalMap[startTimestamp]).Sub(changeMap[dt]).Round(2)
		historyMap[dt] = &pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory{
			Time:       dt,
			CnyAmount:  income.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			UsdAmount:  income.String(),
			UsdtAmount: income.Div(decimal.NewFromFloat(usdtPrice)).Round(2).String(),
			BtcAmount:  income.Div(decimal.NewFromFloat(btcPrice)).String(),
		}
	}

	//填充缺失数据,将缺失的数据填充为前一日数据
	dt := minDT + DAY_SECOND
	for dt <= end {
		if _, ok := historyMap[dt]; !ok {
			preHistory := historyMap[dt-DAY_SECOND]
			if preHistory == nil {
				historyMap[dt] = &pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory{
					Time: dt, CnyAmount: "0", UsdAmount: "0", UsdtAmount: "0", BtcAmount: "0",
				}
			} else {
				historyMap[dt] = &pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory{
					Time: dt, CnyAmount: preHistory.CnyAmount, UsdAmount: preHistory.UsdAmount, UsdtAmount: preHistory.UsdtAmount, BtcAmount: preHistory.BtcAmount,
				}
			}
		}
		dt += DAY_SECOND
	}

	var historiesList []*pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory
	for _, history := range historyMap {
		//快照时间为0点，-1为了将时间显示为前一天
		history.Time -= 1
		historiesList = append(historiesList, history)
	}

	slices.SortFunc(historiesList, func(a, b *pb.UserWalletIncomeHistoryResp_UserWalletIncomeHistory) bool {
		return a.Time < b.Time
	})

	lastHistory := historiesList[len(historiesList)-1]
	result := &pb.UserWalletIncomeHistoryResp{
		Amount: &pb.Currency{
			Cny:  lastHistory.CnyAmount,
			Usd:  lastHistory.UsdAmount,
			Usdt: lastHistory.UsdtAmount,
			Btc:  lastHistory.BtcAmount},
		Histories: historiesList,
	}
	return result, nil
}

func (uc UserWalletAssetUsecase) UserWallet(ctx context.Context, req *pb.UserWalletReq) (*pb.UserWalletResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserWalletResp{UserWalletList: nil}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//资产分组求和
	assetTotalMap := map[string]decimal.Decimal{}
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		if amount.IsZero() { //过滤四舍五入保留两位小数为 0 的资产
			continue
		}
		assetTotalMap[asset.Uid] = assetTotalMap[asset.Uid].Add(amount)
	}

	var userWalletList []*pb.UserWalletResp_UserWallet
	for uid, amount := range assetTotalMap {
		if amount.IsZero() {
			continue
		}

		cnyAmount := amount.Mul(decimal.NewFromFloat(cnyRate.Rate))
		usdtAmount := amount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
		btcAmount := amount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

		userWallet := &pb.UserWalletResp_UserWallet{
			Uid: uid,
			Amount: &pb.Currency{
				Cny:  cnyAmount.Round(2).String(),
				Usd:  amount.Round(2).String(),
				Usdt: usdtAmount.Round(2).String(),
				Btc:  btcAmount.String(),
			},
		}

		userWalletList = append(userWalletList, userWallet)
	}
	if len(userWalletList) == 0 {
		return &pb.UserWalletResp{}, nil
	}

	return &pb.UserWalletResp{UserWalletList: userWalletList}, nil
}

func (uc UserWalletAssetUsecase) UserChain(ctx context.Context, req *pb.UserChainReq) (*pb.UserChainResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserChainResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	var assets []*data.UserAsset
	for _, userAsset := range userAssets {
		platInfo, _ := GetChainPlatInfo(userAsset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(userAsset.Balance)

		var key string
		if userAsset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", userAsset.ChainName, strings.ToLower(userAsset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		//过滤保留两位小数四舍五入后为 0 的资产
		if amount.IsZero() {
			continue
		}

		assets = append(assets, userAsset)
	}

	//去重
	chainNameSet := map[string]struct{}{}
	for _, asset := range assets {
		chainNameSet[asset.ChainName] = struct{}{}
	}

	var userChains []*pb.UserChainResp_UserChain
	for chainName, _ := range chainNameSet {
		platInfo, _ := GetChainPlatInfo(chainName)
		userChains = append(userChains, &pb.UserChainResp_UserChain{ChainName: chainName, ChainLogo: platInfo.Icon})
	}

	return &pb.UserChainResp{Chains: userChains}, nil
}

func (uc UserWalletAssetUsecase) UserChainAmount(ctx context.Context, req *pb.UserChainAmountReq) (*pb.UserChainAmountResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUidsAndChainNamesWithNotZero(ctx, req.Uids, req.ChainNames)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserChainAmountResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//获取主网资产
	var assets []*data.UserAsset
	chainAmountMap := map[string]decimal.Decimal{}
	for _, userAsset := range userAssets {
		platInfo, _ := GetChainPlatInfo(userAsset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(userAsset.Balance)

		var key string
		if userAsset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", userAsset.ChainName, strings.ToLower(userAsset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price)

		assets = append(assets, userAsset)
		chainAmountMap[userAsset.ChainName] = chainAmountMap[userAsset.ChainName].Add(amount)
	}

	//去重
	chainNameSet := map[string]struct{}{}
	for _, asset := range assets {
		chainNameSet[asset.ChainName] = struct{}{}
	}

	var userChains []*pb.UserChainAmountResp_UserChainAmount
	for chainName, _ := range chainNameSet {
		platInfo, _ := GetChainPlatInfo(chainName)
		amount := chainAmountMap[chainName]
		cnyAmount := amount.Mul(decimal.NewFromFloat(cnyRate.Rate))
		usdtAmount := amount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
		btcAmount := amount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))
		userChains = append(userChains, &pb.UserChainAmountResp_UserChainAmount{
			ChainName: chainName,
			ChainLogo: platInfo.Icon,
			Amount: &pb.Currency{
				Cny:  cnyAmount.Round(2).String(),
				Usd:  amount.Round(2).String(),
				Usdt: usdtAmount.Round(2).String(),
				Btc:  btcAmount.String(),
			},
		})
	}

	return &pb.UserChainAmountResp{
		UserChainAmountList: userChains,
	}, nil
}

func (uc UserWalletAssetUsecase) UserToken(ctx context.Context, req *pb.UserTokenReq) (*pb.UserTokenResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUidsAndChainNamesWithNotZero(ctx, req.Uids, req.ChainNames)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserTokenResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//去重
	userTokenMap := map[string]*data.UserAsset{}
	for _, asset := range userAssets {
		key := fmt.Sprintf("%s_%s", asset.ChainName, asset.TokenAddress)
		userTokenMap[key] = asset
	}

	var userTokens []*pb.UserTokenResp_UserToken
	for _, asset := range userTokenMap {

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)

		//过滤保留两位小数四舍五入后为 0 的资产
		if amount.IsZero() {
			continue
		}

		//主币的 logo 为链的 logo
		var tokenLogo string
		if asset.TokenAddress == "" {
			if platInfo.NativeCurrencyIcon != "" {
				tokenLogo = platInfo.NativeCurrencyIcon
			} else {
				tokenLogo = platInfo.Icon
			}
		} else {
			tokenLogo = asset.TokenUri
		}

		userTokens = append(userTokens, &pb.UserTokenResp_UserToken{
			ChainName:    asset.ChainName,
			ChainLogo:    platInfo.Icon,
			TokenAddress: asset.TokenAddress,
			TokenSymbol:  asset.Symbol,
			TokenLogo:    tokenLogo,
		})
	}

	return &pb.UserTokenResp{Tokens: userTokens}, nil
}

func (uc UserWalletAssetUsecase) UserAssetList(ctx context.Context, req *pb.UserAssetListReq) (*pb.UserAssetListResp, error) {

	//因为需要对资产 实时价格 进行排序，无法实现数据库翻页，所以需要查询用户资产之后，计算出价格再进行排序、分页
	//查询用户资产，并过滤筛选条件
	userAssets, err := data.UserAssetRepoClient.FindByUidsAndAddressesAndChainNamesWithNotZero(ctx, req.Uids, req.Addresses, req.ChainNames)
	if err != nil {
		return nil, err
	}

	//如果查询资产不存在，则填充查询的资产信息
	if req.FillNotExitAsset && len(req.TokenAddresses) != 0 {
		for _, chanAndAddress := range req.TokenAddresses {
			chainName, address, found := strings.Cut(chanAndAddress, "_")
			if !found {
				continue
			}

			if !slices.ContainsFunc(userAssets, func(asset *data.UserAsset) bool {
				return asset.ChainName == chainName && strings.ToLower(asset.TokenAddress) == strings.ToLower(address)
			}) {
				userAssets = append(userAssets, &data.UserAsset{
					ChainName:    chainName,
					TokenAddress: address,
					Balance:      "0",
				})
			}
		}
	}

	if len(userAssets) == 0 {
		return &pb.UserAssetListResp{Total: 0, TotalAmount: nil, UserAssetList: nil}, err
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//对查询出来的结果，进行筛选条件过滤
	userAssetList := uc.userAssetListFilter(userAssets, req, tokenPriceMap)

	//资产求和
	var assetTotal decimal.Decimal
	for _, asset := range userAssetList {
		//过滤已在 userAssetListFilter 中实现，这里不需要过滤
		platInfo, _ := GetChainPlatInfo(asset.ChainName)

		//填充价格等字段
		fillValue(asset, tokenPriceMap, cnyRate, req.ShowTest)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		balanceDecimal, _ := decimal.NewFromString(asset.Amount)

		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)

		assetTotal = assetTotal.Add(amount)
	}

	//资产总额换算
	cnyAmount := assetTotal.Mul(decimal.NewFromFloat(cnyRate.Rate))
	usdtAmount := assetTotal.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	btcAmount := assetTotal.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

	//排序
	slices.SortFunc(userAssetList, func(a, b *pb.UserAssetListResp_UserAsset) bool {

		if req.Order == "asc" {
			if a.CurrencyAmount == nil {
				return true
			}

			if b.CurrencyAmount == nil {
				return false
			}

			aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
			bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)

			if aAmount.Equal(bAmount) {
				aBalance, _ := decimal.NewFromString(a.Amount)
				bBalance, _ := decimal.NewFromString(b.Amount)
				return aBalance.LessThan(bBalance)
			}

			return aAmount.LessThan(bAmount)
		} else {
			if a.CurrencyAmount == nil {
				return false
			}

			if b.CurrencyAmount == nil {
				return true
			}

			aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
			bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)

			if aAmount.Equal(bAmount) {
				aBalance, _ := decimal.NewFromString(a.Amount)
				bBalance, _ := decimal.NewFromString(b.Amount)
				return aBalance.GreaterThan(bBalance)
			}

			return aAmount.GreaterThan(bAmount)
		}
	})

	//分页
	start := (req.PageNumber - 1) * req.PageSize
	end := req.PageNumber * req.PageSize

	if int(start) > len(userAssetList) {
		return &pb.UserAssetListResp{
			Total: int64(len(userAssetList)),
			TotalAmount: &pb.Currency{
				Cny:  cnyAmount.Round(2).String(),
				Usd:  assetTotal.Round(2).String(),
				Usdt: usdtAmount.Round(2).String(),
				Btc:  btcAmount.String(),
			},
			UserAssetList: nil,
		}, nil
	}

	if int(end) > len(userAssetList) {
		end = int64(len(userAssetList))
	}

	result := &pb.UserAssetListResp{
		Total: int64(len(userAssetList)),
		TotalAmount: &pb.Currency{
			Cny:  cnyAmount.Round(2).String(),
			Usd:  assetTotal.Round(2).String(),
			Usdt: usdtAmount.Round(2).String(),
			Btc:  btcAmount.String(),
		},
		UserAssetList: userAssetList[start:end],
	}

	return result, nil
}

func fillValue(asset *pb.UserAssetListResp_UserAsset, tokenPriceMap map[string]MarketPrice, cnyRate *v1.DescribeRateReply, showTest bool) {
	var key string
	if asset.TokenAddress == "" {
		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			return
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE && !showTest {
			return
		}
		key = platInfo.GetPriceKey
	} else {
		key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
	}

	//最新价
	usdPrice := decimal.NewFromFloat(tokenPriceMap[key].Price)
	cnyPrice := usdPrice.Mul(decimal.NewFromFloat(cnyRate.Rate))
	usdtPrice := usdPrice.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	btcPrice := usdPrice.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))
	asset.Price = &pb.Currency{
		Cny:  cnyPrice.Round(2).String(),
		Usd:  usdPrice.Round(2).String(),
		Usdt: usdtPrice.Round(2).String(),
		Btc:  btcPrice.String(),
	}

	//最新价24小时涨跌幅
	asset.PriceDelta24H = decimal.NewFromFloat(tokenPriceMap[key].Delta24H).Div(decimal.NewFromFloat(100)).Round(4).String()

	//成本价
	costPriceUsd, _ := decimal.NewFromString(asset.CostPrice.Usd)

	costPriceCny := costPriceUsd.Mul(decimal.NewFromFloat(cnyRate.Rate))
	costPriceUsdt := costPriceUsd.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	costPriceBtc := costPriceUsd.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

	asset.CostPrice.Usd = costPriceUsd.Round(2).String()
	asset.CostPrice.Cny = costPriceCny.Round(2).String()
	asset.CostPrice.Usdt = costPriceUsdt.Round(2).String()
	asset.CostPrice.Btc = costPriceBtc.String()

	//总价值
	balance, _ := decimal.NewFromString(asset.Amount)
	usdAmount := balance.Mul(usdPrice) // XXX：这里先不进行 Round(2) 防止后面 CNY 数量偏离过多.
	cnyAmount := usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate))
	usdtAmount := usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price))
	btcAmount := usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price))

	asset.CurrencyAmount = &pb.Currency{
		Cny:  cnyAmount.Round(2).String(),
		Usd:  usdAmount.Round(2).String(),
		Usdt: usdtAmount.Round(2).String(),
		Btc:  btcAmount.String(),
	}

	//收益
	asset.Income = &pb.Currency{
		Cny:  cnyPrice.Sub(costPriceCny).Mul(balance).Round(2).String(),
		Usd:  usdPrice.Sub(costPriceUsd).Mul(balance).Round(2).String(),
		Usdt: usdtPrice.Sub(costPriceUsdt).Mul(balance).Round(2).String(),
		Btc:  btcPrice.Sub(costPriceBtc).Mul(balance).String(),
	}

	//收益率
	if costPriceUsd.IsZero() {
		asset.IncomePercentage = "0"
	} else {
		asset.IncomePercentage = usdPrice.Sub(costPriceUsd).Div(costPriceUsd).Round(4).String()
	}
}

func (uc UserWalletAssetUsecase) userAssetListFilter(userAssets []*data.UserAsset, req *pb.UserAssetListReq, tokenPriceMap map[string]MarketPrice) []*pb.UserAssetListResp_UserAsset {
	var userAssetList []*pb.UserAssetListResp_UserAsset
	for _, userAsset := range userAssets {

		//tokenAddress 筛选
		contains := slices.ContainsFunc(req.TokenAddresses, func(tokenAddress string) bool {
			chainName, address, _ := strings.Cut(tokenAddress, "_")
			if userAsset.ChainName == chainName && strings.ToLower(userAsset.TokenAddress) == strings.ToLower(address) {
				return true
			}
			return false
		})

		//如果没有 tokenAddress 筛选条件，则不过滤
		if len(req.TokenAddresses) == 0 || (len(req.TokenAddresses) == 1 && req.TokenAddresses[0] == "") {
			contains = true
		}

		if !contains {
			continue
		}

		// 过滤 0 资产
		if !req.FillNotExitAsset && (userAsset.Balance == "" || userAsset.Balance == "0") {
			continue
		}

		platInfo, _ := GetChainPlatInfo(userAsset.ChainName)
		if platInfo == nil {
			continue
		}

		// 过滤测试网
		if !req.ShowTest && platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(userAsset.Balance)

		//过滤资产保留两位小数后等于 0 的资产
		var key string
		if userAsset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", userAsset.ChainName, strings.ToLower(userAsset.TokenAddress))
		}

		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		if !req.ShowZeroValue && amount.IsZero() {
			continue
		}

		//主币的 logo 为链的 logo
		var tokenLogo string
		if userAsset.TokenAddress == "" {
			if platInfo.NativeCurrencyIcon != "" {
				tokenLogo = platInfo.NativeCurrencyIcon
			} else {
				tokenLogo = platInfo.Icon
			}
		} else {
			tokenLogo = userAsset.TokenUri
		}

		userAssetList = append(userAssetList, &pb.UserAssetListResp_UserAsset{
			TokenAddress: userAsset.TokenAddress,
			TokenSymbol:  userAsset.Symbol,
			TokenLogo:    tokenLogo,
			ChainName:    userAsset.ChainName,
			Uid:          userAsset.Uid,
			Amount:       userAsset.Balance,
			CostPrice:    &pb.Currency{Usd: userAsset.CostPrice},
		})
	}

	return userAssetList
}

func (uc UserWalletAssetUsecase) UserAssetDistribution(ctx context.Context, req *pb.UserAssetDistributionReq) (*pb.UserAssetDistributionResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserAssetDistributionResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//资产按照 symbol 分组求和
	symbolAmountMap := map[string]decimal.Decimal{}
	symbolBalanceMap := map[string]decimal.Decimal{}
	var totalAmount decimal.Decimal
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		if amount.IsZero() { //过滤保留两位小数之后四舍五入为 0 的资产
			continue
		}

		symbolAmountMap[asset.Symbol] = symbolAmountMap[asset.Symbol].Add(amount)
		symbolBalanceMap[asset.Symbol] = symbolBalanceMap[asset.Symbol].Add(balanceDecimal)
		totalAmount = totalAmount.Add(amount)
	}

	var userAssetList []*pb.UserAssetDistributionResp_UserAsset
	for symbol, usdAmount := range symbolAmountMap {
		percentage := usdAmount.Div(totalAmount)
		currency := &pb.Currency{
			Cny:  usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			Usd:  usdAmount.Round(2).String(),
			Usdt: usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price)).Round(2).String(),
			Btc:  usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price)).String(),
		}

		asset := &pb.UserAssetDistributionResp_UserAsset{
			TokenSymbol:    symbol,
			Amount:         symbolBalanceMap[symbol].String(),
			CurrencyAmount: currency,
			Percentage:     percentage.Round(4).String(),
		}

		userAssetList = append(userAssetList, asset)
	}

	//排序
	slices.SortFunc(userAssetList, func(a, b *pb.UserAssetDistributionResp_UserAsset) bool {
		aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
		bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)
		return aAmount.GreaterThanOrEqual(bAmount)
	})

	return &pb.UserAssetDistributionResp{UserAssetList: userAssetList}, nil
}

func (uc UserWalletAssetUsecase) UserChainAssetDistribution(ctx context.Context, req *pb.UserChainAssetDistributionReq) (*pb.UserChainAssetDistributionResp, error) {
	userAssets, err := data.UserAssetRepoClient.FindByUidsAndChainNameWithNotZero(ctx, req.ChainName, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserChainAssetDistributionResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//资产按照 symbol 分组求和
	symbolAmountMap := map[string]decimal.Decimal{}
	symbolBalanceMap := map[string]decimal.Decimal{}
	var totalAmount decimal.Decimal
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price)

		symbolAmountMap[asset.Symbol] = symbolAmountMap[asset.Symbol].Add(amount)
		symbolBalanceMap[asset.Symbol] = symbolBalanceMap[asset.Symbol].Add(balanceDecimal)
		totalAmount = totalAmount.Add(amount)
	}

	var userAssetList []*pb.UserChainAssetDistributionResp_UserAsset
	for symbol, usdAmount := range symbolAmountMap {

		percentage := usdAmount.Div(totalAmount)

		currency := &pb.Currency{
			Cny:  usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			Usd:  usdAmount.Round(2).String(),
			Usdt: usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price)).Round(2).String(),
			Btc:  usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price)).String(),
		}

		asset := &pb.UserChainAssetDistributionResp_UserAsset{
			TokenSymbol:    symbol,
			Amount:         symbolBalanceMap[symbol].String(),
			CurrencyAmount: currency,
			Percentage:     percentage.Round(4).String(),
		}

		userAssetList = append(userAssetList, asset)
	}

	//排序
	slices.SortFunc(userAssetList, func(a, b *pb.UserChainAssetDistributionResp_UserAsset) bool {
		if a.CurrencyAmount == nil {
			return false
		}

		if b.CurrencyAmount == nil {
			return true
		}

		aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
		bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)

		return aAmount.GreaterThan(bAmount)
	})

	//聚合 top3 以下的资产
	aggAmount := decimal.Decimal{}
	if len(userAssetList) > 3 {
		for i, asset := range userAssetList {
			if i > 2 {
				amount, _ := decimal.NewFromString(asset.CurrencyAmount.Usd)
				aggAmount = aggAmount.Add(amount)
			}
		}

		aggPercentage := aggAmount.Div(totalAmount)
		userAssetList = []*pb.UserChainAssetDistributionResp_UserAsset{
			userAssetList[0],
			userAssetList[1],
			userAssetList[2],
			{
				TokenSymbol: "others",
				CurrencyAmount: &pb.Currency{
					Cny:  aggAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
					Usd:  aggAmount.Round(2).String(),
					Usdt: aggAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price)).Round(2).String(),
					Btc:  aggAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price)).String(),
				},
				Percentage: aggPercentage.Round(4).String(),
			}}
	}

	return &pb.UserChainAssetDistributionResp{
		UserAssetList: userAssetList,
	}, nil
}

func (uc UserWalletAssetUsecase) UserChainDistribution(ctx context.Context, req *pb.UserChainDistributionReq) (*pb.UserChainDistributionResp, error) {

	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserChainDistributionResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//资产按照 chainName 分组求和
	chainAmountMap := map[string]decimal.Decimal{}
	var totalAmount decimal.Decimal
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		if amount.IsZero() { //过滤保留两位小数之后四舍五入为 0 的资产
			continue
		}

		chainAmountMap[asset.ChainName] = chainAmountMap[asset.ChainName].Add(amount)
		totalAmount = totalAmount.Add(amount)
	}

	var userAssetList []*pb.UserChainDistributionResp_UserChain
	for chainName, usdAmount := range chainAmountMap {

		//过滤保留两位小数之后四舍五入为 0 的资产
		if usdAmount.Round(2).IsZero() {
			continue
		}

		percentage := usdAmount.Div(totalAmount)

		currency := &pb.Currency{
			Cny:  usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			Usd:  usdAmount.Round(2).String(),
			Usdt: usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price)).Round(2).String(),
			Btc:  usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price)).String(),
		}

		asset := &pb.UserChainDistributionResp_UserChain{
			ChainName:      chainName,
			CurrencyAmount: currency,
			Percentage:     percentage.Round(4).String(),
		}

		userAssetList = append(userAssetList, asset)
	}

	//排序
	slices.SortFunc(userAssetList, func(a, b *pb.UserChainDistributionResp_UserChain) bool {
		aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
		bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)
		return aAmount.GreaterThanOrEqual(bAmount)
	})

	return &pb.UserChainDistributionResp{UserChainList: userAssetList}, nil
}

func (uc UserWalletAssetUsecase) UserWalletDistribution(ctx context.Context, req *pb.UserWalletDistributionReq) (*pb.UserWalletDistributionResp, error) {

	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		return &pb.UserWalletDistributionResp{}, nil
	}

	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	//资产按照 uid 分组求和
	uidAmountMap := map[string]decimal.Decimal{}
	var totalAmount decimal.Decimal
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		if amount.IsZero() { //过滤保留两位小数之后四舍五入为 0 的资产
			continue
		}
		uidAmountMap[asset.Uid] = uidAmountMap[asset.Uid].Add(amount)
		totalAmount = totalAmount.Add(amount)
	}

	var userAssetList []*pb.UserWalletDistributionResp_UserWallet
	for uid, usdAmount := range uidAmountMap {

		percentage := usdAmount.Div(totalAmount)

		currency := &pb.Currency{
			Cny:  usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
			Usd:  usdAmount.Round(2).String(),
			Usdt: usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[fmt.Sprintf("%s_%s", "ETH", strings.ToLower(ETH_USDT_ADDRESS))].Price)).Round(2).String(),
			Btc:  usdAmount.Div(decimal.NewFromFloat(tokenPriceMap[PriceKeyBTC].Price)).String(),
		}

		asset := &pb.UserWalletDistributionResp_UserWallet{
			Uid:            uid,
			CurrencyAmount: currency,
			Percentage:     percentage.Round(4).String(),
		}

		userAssetList = append(userAssetList, asset)
	}

	//排序
	slices.SortFunc(userAssetList, func(a, b *pb.UserWalletDistributionResp_UserWallet) bool {
		aAmount, _ := decimal.NewFromString(a.CurrencyAmount.Usd)
		bAmount, _ := decimal.NewFromString(b.CurrencyAmount.Usd)
		return aAmount.GreaterThanOrEqual(bAmount)
	})

	return &pb.UserWalletDistributionResp{UserWalletList: userAssetList}, nil
}

func (uc UserWalletAssetUsecase) UserChainAssetFilter(ctx context.Context, req *pb.UserChainAssetFilterReq) (*pb.UserChainAssetFilterResp, error) {
	chainNames := []string{"ETH", "BSC", "Arbitrum", "Polygon"}
	userAssets, err := data.UserAssetRepoClient.FindByUidsAndChainNamesWithNotZero(ctx, req.Uids, chainNames)
	if err != nil {
		return nil, err
	}

	if len(userAssets) == 0 {
		//如果用户无资产，则返回四条链回去，用户中心补充用户地址
		return &pb.UserChainAssetFilterResp{}, nil
	}

	userAddressSet := map[string]struct{}{}
	userChainAssetListMap := map[string][]*pb.UserChainAssetFilterResp_UserChainAsset{}
	for _, asset := range userAssets {

		//去重
		userAddressKey := fmt.Sprintf("%s_%s", asset.ChainName, asset.Address)
		_, ok := userAddressSet[userAddressKey]
		if ok {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		//标记已存在
		userAddressSet[userAddressKey] = struct{}{}

		userChainAssetListMap[asset.ChainName] = append(userChainAssetListMap[asset.ChainName], &pb.UserChainAssetFilterResp_UserChainAsset{
			Uid:     asset.Uid,
			Address: asset.Address,
		})
	}

	var userChainList []*pb.UserChainAssetFilterResp_UserChain
	for chainName, assets := range userChainAssetListMap {
		userChainList = append(userChainList, &pb.UserChainAssetFilterResp_UserChain{
			ChainName:          chainName,
			UserChainAssetList: assets,
		})
	}

	result := &pb.UserChainAssetFilterResp{
		UserChainList: userChainList,
	}
	return result, nil
}

func (uc UserWalletAssetUsecase) getAssetsPrice(assets []*data.UserAsset) (map[string]float64, error) {
	if len(assets) == 0 {
		return make(map[string]float64), nil
	}

	coinIdSet := map[string]struct{}{}
	tokenSet := map[string]*v1.Tokens{}

	//去重
	for _, userAsset := range assets {
		//coin 去重
		platInfo, _ := GetChainPlatInfo(userAsset.ChainName)
		if platInfo == nil || userAsset.TokenAddress == "" {
			continue
		}
		coinIdSet[platInfo.GetPriceKey] = struct{}{}
		//token 去重
		key := fmt.Sprintf("%s_%s", userAsset.ChainName, strings.ToLower(userAsset.TokenAddress))
		tokenSet[key] = &v1.Tokens{
			Chain:   userAsset.ChainName,
			Address: userAsset.TokenAddress,
		}
	}

	var coinIds []string
	for k, _ := range coinIdSet {
		coinIds = append(coinIds, k)
	}
	//默认添加 BTC 价格
	if !slices.Contains(coinIds, PriceKeyBTC) {
		coinIds = append(coinIds, PriceKeyBTC)
	}

	tokens := make([]*v1.Tokens, 0)
	for _, v := range tokenSet {
		tokens = append(tokens, v)
	}
	//默认添加 USDT 价格
	tokens = append(tokens, &v1.Tokens{
		Chain:   "ETH",
		Address: ETH_USDT_ADDRESS,
	})

	tokenPrices, err := GetPriceFromMarket(tokens, coinIds)
	if err != nil {
		return nil, err
	}

	tokenPriceMap := map[string]float64{}
	for _, coin := range tokenPrices.Coins {
		tokenPriceMap[coin.CoinID] = coin.Price.Usd
	}

	for _, token := range tokenPrices.Tokens {
		key := fmt.Sprintf("%s_%s", token.Chain, strings.ToLower(token.Address))
		tokenPriceMap[key] = token.Price.Usd
	}

	return tokenPriceMap, err
}

func (uc UserWalletAssetUsecase) getBTCAndUsdtPrice() (float64, float64, error) {

	var coinIds = []string{PriceKeyBTC}
	var tokens = []*v1.Tokens{{Chain: "ETH", Address: ETH_USDT_ADDRESS}}

	tokenPrices, err := GetPriceFromMarket(tokens, coinIds)
	if err != nil {
		return 0, 0, err
	}

	btcPrice := tokenPrices.Coins[0].Price
	usdtPrice := tokenPrices.Tokens[0].Price

	return btcPrice.Usd, usdtPrice.Usd, err
}

func (uc UserWalletAssetUsecase) UserWalletDeFiPlatforms(ctx context.Context, req *pb.UserWalletDeFiPlatformRequest) (*pb.UserWalletDeFiPlatformResp, error) {
	results, err := data.DeFiAssetRepoInst.ListUserPlatforms(ctx, req.Uids, req.Type)
	if err != nil {
		return nil, err
	}
	resp := &pb.UserWalletDeFiPlatformResp{
		List: []*pb.DeFiPlatform{},
	}
	for _, item := range results {
		resp.List = append(resp.List, &pb.DeFiPlatform{
			Id:       fmt.Sprint(item.Id),
			Origin:   item.URL,
			Icon:     item.Icon,
			DappName: item.Name,
			Type:     req.Type,
		})
	}
	return resp, nil
}

func (uc UserWalletAssetUsecase) UserWalletDeFiAssets(ctx context.Context, req *pb.UserWalletDeFiAssetRequest) (*pb.UserWalletDeFiAssetResp, error) {
	total, totalAmount, results, err := data.DeFiAssetRepoInst.ListUserDeFiAssets(ctx, req)
	if err != nil {
		return nil, err
	}
	historiesUniq := make(map[string][]*data.UserDeFiAssetTxnHistory)
	for _, item := range results {
		req := &data.DeFiOpenPostionReq{
			ChainName:    item.ChainName,
			PlatformID:   item.PlatformID,
			Address:      item.Address,
			AssetAddress: item.AssetAddress,
			TxTime:       item.OpenedAt,
		}
		histories, err := data.DeFiAssetRepoInst.LoadAssetHistories(ctx, req, item.Type)
		if err != nil {
			return nil, err
		}
		historiesUniq[item.TransactionHash] = histories
	}
	platformsById, err := data.DeFiAssetRepoInst.LoadPlatformsMap(ctx, results)
	if err != nil {
		return nil, err
	}

	btcPrice, usdtPrice, err := uc.getBTCAndUsdtPrice()
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	resp := &pb.UserWalletDeFiAssetResp{
		Total:       total,
		List:        []*pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset{},
		TotalAmount: uc.usdToCurrency(totalAmount, cnyRate, btcPrice, usdtPrice),
	}

	for _, item := range results {
		rawPlat := platformsById[item.PlatformID]
		plat := &pb.DeFiPlatform{
			Id:       fmt.Sprintf("%d", rawPlat.Id),
			Origin:   rawPlat.URL,
			Icon:     rawPlat.Icon,
			DappName: rawPlat.Name,
			Type:     req.Type,
		}
		switch req.Type {
		case data.DeFiAssetTypeDebt:
			token, amount := getDeFiDebt(historiesUniq[item.TransactionHash])
			resp.List = append(resp.List, &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset{
				Value: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_Debt{
					Debt: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_UserWalletDeFiAssetDebt{
						Token:        uc.fullfillNativeToken(item.ChainName, token),
						BorrowAmount: amount,
						Platform:     plat,
						BorrowedAt:   int32(item.OpenedAt),
						RepayAmount:  amount,
						Value:        uc.usdToCurrency(item.ValueUsd, cnyRate, btcPrice, usdtPrice),
						Uid:          item.Uid,
						ChainName:    item.ChainName,
					},
				},
			})
		case data.DeFiAssetTypeDeposits:
			token, amount := getDeFiDeposit(historiesUniq[item.TransactionHash])
			resp.List = append(resp.List, &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset{
				Value: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_Deposit{
					Deposit: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_UserWalletDeFiDeposit{
						Token:         uc.fullfillNativeToken(item.ChainName, token),
						DepositAmount: amount,
						Platform:      plat,
						DepositedAt:   int32(item.OpenedAt),
						Profit:        uc.usdToCurrency(item.ProfitUsd, cnyRate, btcPrice, usdtPrice),
						Value:         uc.usdToCurrency(item.ValueUsd, cnyRate, btcPrice, usdtPrice),
						Uid:           item.Uid,
						ProfitRate:    uc.percentage(item.ProfitUsd, item.ValueUsd),
						ChainName:     item.ChainName,
					},
				},
			})
		case data.DeFiAssetTypeStake:
			token, amount := getDeFiStake(historiesUniq[item.TransactionHash])
			resp.List = append(resp.List, &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset{
				Value: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_Staked{
					Staked: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_UserWalletDeFiAssetStaked{
						Token:        uc.fullfillNativeToken(item.ChainName, token),
						StakedAmount: amount,
						Platform:     plat,
						StakedAt:     int32(item.OpenedAt),
						Profit:       uc.usdToCurrency(item.ProfitUsd, cnyRate, btcPrice, usdtPrice),
						Value:        uc.usdToCurrency(item.ValueUsd, cnyRate, btcPrice, usdtPrice),
						Uid:          item.Uid,
						ProfitRate:   uc.percentage(item.ProfitUsd, item.ValueUsd),
						ChainName:    item.ChainName,
					},
				},
			})
		case data.DeFiAssetTypeLP:
			lp, token0, token1, amount0, amount1 := getDeFiLP(historiesUniq[item.TransactionHash])
			resp.List = append(resp.List, &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset{
				Value: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_Lp{
					Lp: &pb.UserWalletDeFiAssetResp_UserWalletDeFiAsset_UserWalletDeFiAssetLP{
						TokenA:     uc.fullfillNativeToken(item.ChainName, token0),
						TokenB:     uc.fullfillNativeToken(item.ChainName, token1),
						LpToken:    lp,
						Platform:   plat,
						CreatedAt:  int32(item.OpenedAt),
						Profit:     uc.usdToCurrency(item.ProfitUsd, cnyRate, btcPrice, usdtPrice),
						Value:      uc.usdToCurrency(item.ValueUsd, cnyRate, btcPrice, usdtPrice),
						Uid:        item.Uid,
						AmountA:    amount0,
						AmountB:    amount1,
						ProfitRate: uc.percentage(item.ProfitUsd, item.ValueUsd),
						ChainName:  item.ChainName,
					},
				},
			})
		}
	}
	return resp, nil
}

func getDeFiDebt(b []*data.UserDeFiAssetTxnHistory) (*pb.UserWalletAssetToken, string) {
	totalBorrowed := decimal.Zero
	totalRepayed := decimal.Zero
	var token *pb.UserWalletAssetToken
	for _, h := range b {
		amount, _ := decimal.NewFromString(h.Amount)
		switch h.Action {
		case data.DeFiActionDebtBorrow:
			if h.AssetDirection == data.DeFiAssetTypeDirRecv {
				totalBorrowed = totalBorrowed.Add(amount)
				token = &pb.UserWalletAssetToken{
					TokenAddress: h.TokenAddress,
					TokenSymbol:  h.Symbol,
					TokenLogo:    h.TokenUri,
				}
			}
		case data.DeFiActionDebtRepay:
			if h.AssetDirection == data.DeFiAssetTypeDirSend {
				totalRepayed = totalRepayed.Add(amount)
			}
		}
	}
	return token, totalBorrowed.Sub(totalRepayed).String()
}

func getDeFiDeposit(b []*data.UserDeFiAssetTxnHistory) (*pb.UserWalletAssetToken, string) {
	totalSupplied := decimal.Zero
	totalWithdrawn := decimal.Zero
	var token *pb.UserWalletAssetToken
	for _, h := range b {
		amount, _ := decimal.NewFromString(h.Amount)
		switch h.Action {
		case data.DeFiActionDepositSupply:
			if h.AssetDirection == data.DeFiAssetTypeDirSend {
				totalSupplied = totalSupplied.Add(amount)
				token = &pb.UserWalletAssetToken{
					TokenAddress: h.TokenAddress,
					TokenSymbol:  h.Symbol,
					TokenLogo:    h.TokenUri,
				}
			}
		case data.DeFiActionDepositWithdraw:
			if h.AssetDirection == data.DeFiAssetTypeDirRecv {
				totalWithdrawn = totalWithdrawn.Add(amount)
			}
		}
	}
	return token, totalSupplied.Sub(totalWithdrawn).String()
}

func getDeFiStake(b []*data.UserDeFiAssetTxnHistory) (*pb.UserWalletAssetToken, string) {
	totalStaked := decimal.Zero
	totalUnstaked := decimal.Zero
	var token *pb.UserWalletAssetToken
	for _, h := range b {
		amount, _ := decimal.NewFromString(h.Amount)
		switch h.Action {
		case data.DeFiActionStakedStake:
			if h.AssetDirection == data.DeFiAssetTypeDirSend {
				totalStaked = totalStaked.Add(amount)
				token = &pb.UserWalletAssetToken{
					TokenAddress: h.TokenAddress,
					TokenSymbol:  h.Symbol,
					TokenLogo:    h.TokenUri,
				}
			}
		case data.DeFiActionStakedUnstake:
			if h.AssetDirection == data.DeFiAssetTypeDirRecv {
				totalUnstaked = totalUnstaked.Add(amount)
			}
		}
	}
	return token, totalStaked.Sub(totalUnstaked).String()
}

func getDeFiLP(b []*data.UserDeFiAssetTxnHistory) (*pb.UserWalletAssetToken, *pb.UserWalletAssetToken, *pb.UserWalletAssetToken, string, string) {
	token0TotalAdded := decimal.Zero
	token1TotalAdded := decimal.Zero
	var lp *pb.UserWalletAssetToken
	var token0 *pb.UserWalletAssetToken
	var token1 *pb.UserWalletAssetToken
	for _, h := range b {
		amount, _ := decimal.NewFromString(h.Amount)
		switch h.Action {
		case data.DeFiActionLPAdd:
			if h.AssetDirection == data.DeFiAssetTypeDirSend {
				if token0 == nil || h.TokenAddress == token0.TokenAddress {
					token0TotalAdded = token0TotalAdded.Add(amount)
					token0 = &pb.UserWalletAssetToken{
						TokenAddress: h.TokenAddress,
						TokenSymbol:  h.Symbol,
						TokenLogo:    h.TokenUri,
					}
				} else if token1 == nil || h.TokenAddress == token1.TokenAddress {
					token1TotalAdded = token1TotalAdded.Add(amount)
					token1 = &pb.UserWalletAssetToken{
						TokenAddress: h.TokenAddress,
						TokenSymbol:  h.Symbol,
						TokenLogo:    h.TokenUri,
					}
				}
			} else {
				lp = &pb.UserWalletAssetToken{
					TokenAddress: h.TokenAddress,
					TokenSymbol:  h.Symbol,
					TokenLogo:    h.TokenUri,
				}
			}
		case data.DeFiActionLPRemove:
			// TODO
		}
	}
	return lp, token0, token1, token0TotalAdded.String(), token1TotalAdded.String()
}

func (uc UserWalletAssetUsecase) percentage(v1 decimal.Decimal, v2 decimal.Decimal) string {
	if v2.IsZero() {
		return decimal.Zero.Round(4).String()
	}
	return v1.Div(v2).Round(4).String()
}

func (uc UserWalletAssetUsecase) fullfillNativeToken(chainName string, token *pb.UserWalletAssetToken) *pb.UserWalletAssetToken {
	platInfo, _ := GetChainPlatInfo(chainName)
	if platInfo == nil {
		return token
	}
	if token.TokenAddress == "" {
		if platInfo.NativeCurrencyIcon != "" {
			token.TokenLogo = platInfo.NativeCurrencyIcon
		} else {
			token.TokenLogo = platInfo.Icon
		}
		token.TokenSymbol = platInfo.NativeCurrency
	}
	return token
}

func (uc UserWalletAssetUsecase) UserWalletDeFiDistribution(ctx context.Context, req *pb.UserWalletRequest) (*pb.UserWalletDeFiDistributionResp, error) {
	assets, err := data.DeFiAssetRepoInst.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}
	resp := &pb.UserWalletDeFiDistributionResp{
		List: []*pb.UserWalletDeFiDistributionResp_UserWalletDeFiDistribution{},
	}
	assetsByPlatforms := make(map[int64][]*data.UserDeFiAsset)
	totalAmountUSD := decimal.Zero
	for _, item := range assets {
		arr := assetsByPlatforms[item.PlatformID]
		arr = append(arr, item)
		assetsByPlatforms[item.PlatformID] = arr
		totalAmountUSD = totalAmountUSD.Add(item.ValueUsd)
	}

	platformsById, err := data.DeFiAssetRepoInst.LoadPlatformsMap(ctx, assets)
	if err != nil {
		return nil, err
	}
	btcPrice, usdtPrice, err := uc.getBTCAndUsdtPrice()
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	for platformID, assets := range assetsByPlatforms {
		usdAmount := decimal.Zero
		for _, item := range assets {
			usdAmount = usdAmount.Add(item.ValueUsd)
		}
		if usdAmount.IsZero() {
			continue
		}
		percentage := usdAmount.Div(totalAmountUSD).Round(4)
		value := &pb.UserWalletDeFiDistributionResp_UserWalletDeFiDistribution{
			Percentage: percentage.String(),
			Amount:     uc.usdToCurrency(usdAmount, cnyRate, btcPrice, usdtPrice),
			Platform: &pb.DeFiPlatform{
				Id:       fmt.Sprintf("%d", platformID),
				Origin:   platformsById[platformID].URL,
				Icon:     platformsById[platformID].Icon,
				DappName: platformsById[platformID].Name,
				Type:     assets[0].Type,
			},
		}
		resp.List = append(resp.List, value)
	}

	sort.Slice(resp.List, func(i, j int) bool {
		a, _ := decimal.NewFromString(resp.List[i].Amount.Usd)
		b, _ := decimal.NewFromString(resp.List[j].Amount.Usd)
		return a.GreaterThan(b)
	})
	return resp, nil
}

func (uc UserWalletAssetUsecase) usdToCurrency(usdAmount decimal.Decimal, cnyRate *v1.DescribeRateReply, btcPrice, usdtPrice float64) *pb.Currency {
	return &pb.Currency{
		Cny:  usdAmount.Mul(decimal.NewFromFloat(cnyRate.Rate)).Round(2).String(),
		Usd:  usdAmount.Round(2).String(),
		Usdt: usdAmount.Div(decimal.NewFromFloat(usdtPrice)).Round(2).String(),
		Btc:  usdAmount.Div(decimal.NewFromFloat(btcPrice)).String(),
	}
}

func (uc UserWalletAssetUsecase) UserWalletAssetTypeDistribution(ctx context.Context, req *pb.UserWalletRequest) (*pb.UserWalletAssetTypeDistributionResp, error) {
	assets, err := data.DeFiAssetRepoInst.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}

	//获取用户所有资产
	userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, req.Uids)
	if err != nil {
		return nil, err
	}
	for _, ty := range []string{PERSON, COMPANY, "staked", "debt", "lp", "deposit"} {
		assets = append(assets, &data.UserDeFiAsset{
			Type:     ty,
			ValueUsd: decimal.Zero,
		})
	}
	//获取所有币价
	tokenPriceMap, err := GetAssetsPrice(userAssets)
	if err != nil {
		return nil, err
	}
	for _, asset := range userAssets {
		if asset.Balance == "" || asset.Balance == "0" {
			continue
		}

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		balanceDecimal, _ := decimal.NewFromString(asset.Balance)

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}
		price := decimal.NewFromFloat(tokenPriceMap[key].Price)
		amount := balanceDecimal.Mul(price).Round(2)
		uidType := UidCodeToUidType(asset.UidType)
		if uidType == "" {
			continue
		}
		assets = append(assets, &data.UserDeFiAsset{
			Type:     uidType,
			ValueUsd: amount,
		})
	}

	resp := &pb.UserWalletAssetTypeDistributionResp{
		List: []*pb.UserWalletAssetTypeDistributionResp_UserWalletAssetTypeDistribution{},
	}
	assetsByType := make(map[string][]*data.UserDeFiAsset)
	totalAmountUSD := decimal.Zero
	for _, item := range assets {
		arr := assetsByType[item.Type]
		arr = append(arr, item)
		assetsByType[item.Type] = arr
		totalAmountUSD = totalAmountUSD.Add(item.ValueUsd)
	}

	btcPrice, usdtPrice, err := uc.getBTCAndUsdtPrice()
	if err != nil {
		return nil, err
	}

	//获取 美元/人民币 换算价格
	cnyRate, err := GetCnyRate()
	if err != nil {
		return nil, err
	}

	for assetType, assets := range assetsByType {
		usdAmount := decimal.Zero
		for _, item := range assets {
			usdAmount = usdAmount.Add(item.ValueUsd)
		}
		percentage := decimal.Zero
		if !totalAmountUSD.IsZero() {
			percentage = usdAmount.Div(totalAmountUSD).Round(4)
		}
		value := &pb.UserWalletAssetTypeDistributionResp_UserWalletAssetTypeDistribution{
			Percentage: percentage.String(),
			Amount:     uc.usdToCurrency(usdAmount, cnyRate, btcPrice, usdtPrice),
			AssetType:  assetType,
		}
		resp.List = append(resp.List, value)
	}
	sort.Slice(resp.List, func(i, j int) bool {
		a, _ := decimal.NewFromString(resp.List[i].Amount.Usd)
		b, _ := decimal.NewFromString(resp.List[j].Amount.Usd)
		return a.GreaterThan(b)
	})
	return resp, nil
}
