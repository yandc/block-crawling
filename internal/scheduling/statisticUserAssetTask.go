package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type StatisticUserAssetTask struct {
}

func NewStatisticUserAssetTask() *StatisticUserAssetTask {
	statisticUserAssetTask := &StatisticUserAssetTask{}
	return statisticUserAssetTask
}

func (task *StatisticUserAssetTask) Run() {
	StatisticUserAsset()
}

func StatisticUserAsset() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("StatisticUserAsset error", e)
			} else {
				log.Errore("StatisticUserAsset panic", errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：定时统计用户资产信息失败, error：%s", fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	tm := time.Now()
	nowTime := tm.Unix()
	var dt = utils.GetDayTime(&tm)
	btcUsd, err := biz.GetBTCUSDPrice(context.Background())
	if err != nil {
		panic(err)
	}
	StatisticChainTypeAddressAmount(nowTime, dt)
	StatisticChainTypeAsset(nowTime, dt, btcUsd)
}

func StatisticChainTypeAsset(nowTime, dt int64, btcUsd string) {
	log.Info("统计用户资产金额，处理指标数据开始", zap.Any("dt", dt), zap.Any("nowTime", nowTime))

	var request = &data.AssetRequest{
		UidTypeList:  []int32{1, 2, 3},
		AmountType:   2,
		SelectColumn: "id, chain_name, uid_type, token_address, balance",
		OrderBy:      "id asc",
		PageSize:     data.MAX_PAGE_SIZE,
	}
	var chainTypeAssetList []*data.ChainTypeAsset
	chainTypeAssetMap := make(map[string]map[int8]*data.ChainTypeAsset)
	chainTypeAssetChainNameMap := make(map[string]*data.ChainTypeAsset)
	chainTypeAssetUidTypeMap := make(map[int8]*data.ChainTypeAsset)
	var recordGroupList []*data.UserAsset
	recordGroupMap := make(map[string]*data.UserAsset)
	var err error
	var total int64
	err = data.UserAssetRepoClient.PageListAllCallBack(nil, request, func(userAssets []*data.UserAsset) error {
		total += int64(len(userAssets))
		for _, userAsset := range userAssets {
			key := userAsset.ChainName + strconv.Itoa(int(userAsset.UidType)) + userAsset.TokenAddress
			oldUserAsset, ok := recordGroupMap[key]
			if !ok {
				recordGroupMap[key] = userAsset
			} else {
				balance, _ := decimal.NewFromString(userAsset.Balance)
				oldBalance, _ := decimal.NewFromString(oldUserAsset.Balance)
				oldUserAsset.Balance = oldBalance.Add(balance).String()
			}
		}
		return nil
	})
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：统计用户资产金额，从数据库中查询用户资产信息失败，dt：%d，total：%d", dt, total)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计用户资产金额，从数据库中查询用户资产信息失败", zap.Any("dt", dt), zap.Any("total", total), zap.Any("error", err))
		return
	}
	if total == 0 {
		log.Info("统计用户资产金额开始，从数据库中查询用户资产为空", zap.Any("total", total))
		return
	}
	for _, userAsset := range recordGroupMap {
		recordGroupList = append(recordGroupList, userAsset)
	}

	recordSize := len(recordGroupList)
	if recordSize == 0 {
		log.Info("统计用户资产金额，从数据库中查询用户资产为空", zap.Any("dt", dt), zap.Any("size", recordSize), zap.Any("total", total))
		return
	}

	log.Info("统计用户资产金额，开始执行从nodeProxy中获取代币价格操作", zap.Any("size", recordSize), zap.Any("total", total))
	cnyTokenPriceMap := make(map[string]map[string]string)
	usdTokenPriceMap := make(map[string]map[string]string)
	requestNum := 100
	var chainNameTokenAddressMap = make(map[string][]string)
	for i, userAsset := range recordGroupList {
		tokenAddressList, ok := chainNameTokenAddressMap[userAsset.ChainName]
		if !ok {
			tokenAddressList = make([]string, 0)
		}
		tokenAddressList = append(tokenAddressList, userAsset.TokenAddress)
		chainNameTokenAddressMap[userAsset.ChainName] = tokenAddressList
		if i++; i%requestNum != 0 && i < recordSize {
			continue
		}

		var cnyResultMap map[string]map[string]string
		var usdResultMap map[string]map[string]string
		cnyResultMap, err = biz.GetTokensPriceRetryAlert(nil, biz.CNY, chainNameTokenAddressMap)
		if err != nil {
			log.Error("统计用户资产金额，从nodeProxy中获取代币价格失败", zap.Any("i", i), zap.Any("size", recordSize), zap.Any("total", total), zap.Any("error", err))
			break
		}
		usdResultMap, err = biz.GetTokensPriceRetryAlert(nil, biz.USD, chainNameTokenAddressMap)
		if err != nil {
			log.Error("统计用户资产金额，从nodeProxy中获取代币价格失败", zap.Any("i", i), zap.Any("size", recordSize), zap.Any("total", total), zap.Any("error", err))
			break
		}
		for chainName, tokenAddressPriceMap := range cnyResultMap {
			oldTokenAddressPriceMap, ok := cnyTokenPriceMap[chainName]
			if !ok {
				cnyTokenPriceMap[chainName] = tokenAddressPriceMap
			} else {
				for tokenAddress, price := range tokenAddressPriceMap {
					oldTokenAddressPriceMap[tokenAddress] = price
				}
			}
		}
		for chainName, tokenAddressPriceMap := range usdResultMap {
			oldTokenAddressPriceMap, ok := usdTokenPriceMap[chainName]
			if !ok {
				usdTokenPriceMap[chainName] = tokenAddressPriceMap
			} else {
				for tokenAddress, price := range tokenAddressPriceMap {
					oldTokenAddressPriceMap[tokenAddress] = price
				}
			}
		}
		chainNameTokenAddressMap = make(map[string][]string)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：统计用户资产金额，从nodeProxy中获取代币价格失败，dt：%d", dt)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计用户资产金额，从nodeProxy中获取代币价格失败", zap.Any("dt", dt), zap.Any("error", err))
	}

	if len(cnyTokenPriceMap) == 0 || len(usdTokenPriceMap) == 0 {
		log.Info("统计用户资产金额，从nodeProxy中获取代币价格为空", zap.Any("dt", dt), zap.Any("cnySize", len(cnyTokenPriceMap)), zap.Any("usdSize", len(usdTokenPriceMap)))
		return
	}
	var chainTypeAssetDt = &data.ChainTypeAsset{
		ChainName: "",
		UidType:   0,
		CnyAmount: decimal.Zero,
		UsdAmount: decimal.Zero,
		BtcAmount: decimal.Zero,
		Dt:        dt,
		CreatedAt: nowTime,
		UpdatedAt: nowTime,
	}
	for _, userAsset := range recordGroupList {
		chainName := userAsset.ChainName
		tokenAddress := userAsset.TokenAddress
		var cnyPrice string
		var usdPrice string
		cnyTokenAddressPriceMap := cnyTokenPriceMap[chainName]
		if tokenAddress == "" {
			cnyPrice = cnyTokenAddressPriceMap[chainName]
		} else {
			cnyPrice = cnyTokenAddressPriceMap[tokenAddress]
		}
		usdTokenAddressPriceMap := usdTokenPriceMap[chainName]
		if tokenAddress == "" {
			usdPrice = usdTokenAddressPriceMap[chainName]
		} else {
			usdPrice = usdTokenAddressPriceMap[tokenAddress]
		}
		cnyPrices, _ := decimal.NewFromString(cnyPrice)
		usdPrices, _ := decimal.NewFromString(usdPrice)
		btcUsds, _ := decimal.NewFromString(btcUsd)
		balance := userAsset.Balance
		balances, _ := decimal.NewFromString(balance)
		cnyAmount := cnyPrices.Mul(balances)
		usdAmount := usdPrices.Mul(balances)
		btcAmount := usdAmount.Div(btcUsds)

		typeAssetMap, ok := chainTypeAssetMap[userAsset.ChainName]
		if !ok {
			typeAssetMap = make(map[int8]*data.ChainTypeAsset)
			typeAssetMap[userAsset.UidType] = &data.ChainTypeAsset{
				ChainName: userAsset.ChainName,
				UidType:   userAsset.UidType,
				CnyAmount: cnyAmount,
				UsdAmount: usdAmount,
				BtcAmount: btcAmount,
				Dt:        dt,
				CreatedAt: nowTime,
				UpdatedAt: nowTime,
			}
			chainTypeAssetMap[userAsset.ChainName] = typeAssetMap
		} else {
			chainTypeAsset, tok := typeAssetMap[userAsset.UidType]
			if !tok {
				typeAssetMap[userAsset.UidType] = &data.ChainTypeAsset{
					ChainName: userAsset.ChainName,
					UidType:   userAsset.UidType,
					CnyAmount: cnyAmount,
					UsdAmount: usdAmount,
					BtcAmount: btcAmount,
					Dt:        dt,
					CreatedAt: nowTime,
					UpdatedAt: nowTime,
				}
			} else {
				chainTypeAsset.CnyAmount = chainTypeAsset.CnyAmount.Add(cnyAmount)
				chainTypeAsset.UsdAmount = chainTypeAsset.UsdAmount.Add(usdAmount)
				chainTypeAsset.BtcAmount = chainTypeAsset.BtcAmount.Add(btcAmount)
			}
		}

		chainNameUserAsset, cok := chainTypeAssetChainNameMap[userAsset.ChainName]
		if !cok {
			chainTypeAssetChainNameMap[userAsset.ChainName] = &data.ChainTypeAsset{
				ChainName: userAsset.ChainName,
				UidType:   0,
				CnyAmount: cnyAmount,
				UsdAmount: usdAmount,
				BtcAmount: btcAmount,
				Dt:        dt,
				CreatedAt: nowTime,
				UpdatedAt: nowTime,
			}
		} else {
			chainNameUserAsset.CnyAmount = chainNameUserAsset.CnyAmount.Add(cnyAmount)
			chainNameUserAsset.UsdAmount = chainNameUserAsset.UsdAmount.Add(usdAmount)
			chainNameUserAsset.BtcAmount = chainNameUserAsset.BtcAmount.Add(usdAmount)
		}

		uidTypeUserAsset, uok := chainTypeAssetUidTypeMap[userAsset.UidType]
		if !uok {
			chainTypeAssetUidTypeMap[userAsset.UidType] = &data.ChainTypeAsset{
				ChainName: "",
				UidType:   userAsset.UidType,
				CnyAmount: cnyAmount,
				UsdAmount: usdAmount,
				BtcAmount: btcAmount,
				Dt:        dt,
				CreatedAt: nowTime,
				UpdatedAt: nowTime,
			}
		} else {
			uidTypeUserAsset.CnyAmount = uidTypeUserAsset.CnyAmount.Add(cnyAmount)
			uidTypeUserAsset.UsdAmount = uidTypeUserAsset.UsdAmount.Add(usdAmount)
			uidTypeUserAsset.BtcAmount = uidTypeUserAsset.BtcAmount.Add(btcAmount)
		}

		chainTypeAssetDt.CnyAmount = chainTypeAssetDt.CnyAmount.Add(cnyAmount)
		chainTypeAssetDt.UsdAmount = chainTypeAssetDt.UsdAmount.Add(usdAmount)
		chainTypeAssetDt.BtcAmount = chainTypeAssetDt.BtcAmount.Add(btcAmount)
	}

	for _, typeAssetMap := range chainTypeAssetMap {
		for _, userAsset := range typeAssetMap {
			chainTypeAssetList = append(chainTypeAssetList, userAsset)
		}
	}
	for _, userAsset := range chainTypeAssetChainNameMap {
		chainTypeAssetList = append(chainTypeAssetList, userAsset)
	}
	for _, userAsset := range chainTypeAssetUidTypeMap {
		chainTypeAssetList = append(chainTypeAssetList, userAsset)
	}
	chainTypeAssetList = append(chainTypeAssetList, chainTypeAssetDt)
	_, err = data.ChainTypeAssetRepoClient.PageBatchSaveOrUpdate(nil, chainTypeAssetList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.ChainTypeAssetRepoClient.PageBatchSaveOrUpdate(nil, chainTypeAssetList, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：统计用户资产金额，将数据插入到数据库中失败，dt：%d", dt)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计用户资产金额，将数据插入到数据库中失败", zap.Any("dt", dt), zap.Any("error", err))
	}

	log.Info("统计用户资产金额，处理指标数据结束", zap.Any("dt", dt), zap.Any("nowTime", nowTime))
}

func StatisticChainTypeAddressAmount(nowTime, dt int64) {
	log.Info("统计用户钱包地址数，处理指标数据开始", zap.Any("dt", dt), zap.Any("nowTime", nowTime))

	var request = &data.AssetRequest{
		UidTypeList: []int32{1, 2, 3},
		GroupBy:     "chain_name, uid_type",
	}
	var recordGroupList []*data.UserAssetWrapper
	var chainTypeAddressAmountList []*data.ChainTypeAddressAmount
	userAssets, err := data.UserAssetRepoClient.ListAddressAmountGroup(nil, request)
	if err == nil {
		recordGroupList = append(recordGroupList, userAssets...)
		request.GroupBy = "chain_name"
		userAssets, err = data.UserAssetRepoClient.ListAddressAmountGroup(nil, request)
		if err == nil {
			recordGroupList = append(recordGroupList, userAssets...)
			request.GroupBy = "uid_type"
			userAssets, err = data.UserAssetRepoClient.ListAddressAmountGroup(nil, request)
			if err == nil {
				recordGroupList = append(recordGroupList, userAssets...)
				request.GroupBy = ""
				userAssets, err = data.UserAssetRepoClient.ListAddressAmountGroup(nil, request)
				if err == nil {
					recordGroupList = append(recordGroupList, userAssets...)
				}
			}
		}
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：统计用户钱包地址数，从数据库中查询用户资产信息失败，dt：%d", dt)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计用户钱包地址数，从数据库中查询用户资产信息失败", zap.Any("dt", dt), zap.Any("error", err))
	}

	if len(recordGroupList) == 0 {
		log.Info("统计用户钱包地址数，从数据库中查询用户资产为空", zap.Any("dt", dt), zap.Any("size", len(recordGroupList)))
		return
	}
	for _, userAsset := range recordGroupList {
		var chainTypeAddressAmount = &data.ChainTypeAddressAmount{
			ChainName:     userAsset.ChainName,
			UidType:       userAsset.UidType,
			AddressAmount: userAsset.AddressAmount,
			Dt:            dt,
			CreatedAt:     nowTime,
			UpdatedAt:     nowTime,
		}
		chainTypeAddressAmountList = append(chainTypeAddressAmountList, chainTypeAddressAmount)
	}
	_, err = data.ChainTypeAddressAmountRepoClient.PageBatchSaveOrUpdate(nil, chainTypeAddressAmountList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.ChainTypeAddressAmountRepoClient.PageBatchSaveOrUpdate(nil, chainTypeAddressAmountList, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：统计用户钱包地址数，将数据插入到数据库中失败，dt：%d", dt)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("统计用户钱包地址数，将数据插入到数据库中失败", zap.Any("dt", dt), zap.Any("error", err))
	}

	log.Info("统计用户钱包地址数，处理指标数据结束", zap.Any("dt", dt), zap.Any("nowTime", nowTime))
}
