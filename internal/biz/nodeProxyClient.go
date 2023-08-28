package biz

import (
	v1 "block-crawling/internal/client"
	"block-crawling/internal/common"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/grpc"
)

const (
	ID      = 1
	JSONRPC = "2.0"
)

var TokenInfoMap = &sync.Map{} // make(map[string]types.TokenInfo)
var lock = common.NewSyncronized(0)
var mutex = new(sync.Mutex)

var NftInfoMap = make(map[string]*v1.GetNftReply_NftInfoResp)
var nftLock = common.NewSyncronized(0)
var nftMutex = new(sync.Mutex)

func GetTokenPriceRetryAlert(ctx context.Context, chainName string, currency string, tokenAddress string) (string, error) {
	price, err := GetTokenPrice(ctx, chainName, currency, tokenAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		price, err = GetTokenPrice(ctx, chainName, currency, tokenAddress)
	}
	if err != nil {
		// 调用nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币价格失败，currency:%s，tokenAddress:%s", chainName, currency, tokenAddress)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return price, err
}

func GetTokenPrice(ctx context.Context, chainName string, currency string, tokenAddress string) (string, error) {
	if IsTestNet(chainName) {
		return "", nil
	}

	var getPriceKey string
	var handler string
	if platInfo, ok := PlatInfoMap[chainName]; ok {
		handler = platInfo.Handler
		getPriceKey = platInfo.GetPriceKey
	} else {
		return "", nil
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()
	client := v1.NewTokenlistClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	var reqKey string
	if tokenAddress != "" {
		getPriceKey = ""
		reqKey = handler + "_" + tokenAddress
	}
	priceResp, err := client.GetPrice(ctx, &v1.PriceReq{
		Currency:      currency,
		CoinNames:     getPriceKey,
		CoinAddresses: reqKey,
	})
	if err != nil {
		return "", err
	}

	result := make(map[string]map[string]string)
	err = json.Unmarshal(priceResp.Data, &result)
	if err != nil {
		return "", err
	}
	var price string
	if value, ok := result[getPriceKey]; ok {
		price = value[currency]
	} else if value, ok = result[reqKey]; ok {
		price = value[currency]
	}
	return price, nil
}

func GetTokensPriceRetryAlert(ctx context.Context, currency string, chainNameTokenAddressMap map[string][]string) (map[string]map[string]string, error) {
	price, err := GetTokensPrice(ctx, currency, chainNameTokenAddressMap)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		price, err = GetTokensPrice(ctx, currency, chainNameTokenAddressMap)
	}
	if err != nil {
		// 调用nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：批量查询nodeProxy中代币价格失败，currency:%s", currency)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return price, err
}

func GetTokensPrice(ctx context.Context, currency string, chainNameTokenAddressMap map[string][]string) (map[string]map[string]string, error) {
	var coinNames []string
	var coinNameMap = make(map[string]string)
	var coinAddresses []string
	var getPriceKeyMap = make(map[string][]string)
	var handlerMap = make(map[string]string)
	var resultMap = make(map[string]map[string]string)

	for chainName, tokenAddressList := range chainNameTokenAddressMap {
		if IsTestNet(chainName) {
			continue
		}
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			getPriceKey := platInfo.GetPriceKey
			handler := platInfo.Handler
			chainNames, ok := getPriceKeyMap[getPriceKey]
			if !ok {
				chainNames = make([]string, 0)
			}
			chainNames = append(chainNames, chainName)
			getPriceKeyMap[getPriceKey] = chainNames
			handlerMap[handler] = chainName

			for _, tokenAddress := range tokenAddressList {
				if tokenAddress == "" {
					if _, ok := coinNameMap[getPriceKey]; !ok {
						coinNames = append(coinNames, getPriceKey)
						coinNameMap[getPriceKey] = ""
					}
				} else {
					coinAddresses = append(coinAddresses, handler+"_"+tokenAddress)
				}
			}
		}
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return resultMap, err
	}
	defer conn.Close()
	client := v1.NewCommRPCClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}

	var paramMap = make(map[string]interface{})
	paramMap["currency"] = currency
	paramMap["coin_name"] = coinNames
	paramMap["coin_address"] = coinAddresses
	params, err := utils.JsonEncode(paramMap)
	if err != nil {
		return resultMap, err
	}
	response, err := client.ExecNodeProxyRPC(ctx, &v1.ExecNodeProxyRPCRequest{
		Id:      ID,
		Jsonrpc: JSONRPC,
		Method:  "GetPriceV2",
		Params:  params,
	})
	if err != nil {
		return resultMap, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}

	result := make(map[string]map[string]string)
	err = json.Unmarshal([]byte(response.Result), &result)
	if err != nil {
		return resultMap, err
	}
	for key, value := range result {
		chainNames := getPriceKeyMap[key]
		price := value[currency]
		var tokenAddress string
		if len(chainNames) == 0 {
			handlerTokenAddressList := strings.Split(key, "_")
			handler := handlerTokenAddressList[0]
			chainNames = []string{handlerMap[handler]}
			tokenAddress = handlerTokenAddressList[1]
		}
		for _, chainName := range chainNames {
			tokenAddressPriceMap, ok := resultMap[chainName]
			if !ok {
				tokenAddressPriceMap = make(map[string]string)
				resultMap[chainName] = tokenAddressPriceMap
			}
			if tokenAddress == "" {
				tokenAddressPriceMap[chainName] = price
			} else {
				tokenAddressPriceMap[tokenAddress] = price
			}
		}
	}
	return resultMap, nil
}

func GetTokenInfos(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	var err error
	if tokenAddress == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			decimals := platInfo.Decimal
			symbol := platInfo.NativeCurrency
			tokenInfo = types.TokenInfo{Decimals: int64(decimals), Symbol: symbol}
		}
	} else {
		tokenInfo, err = GetTokenInfo(ctx, chainName, tokenAddress)
	}
	return tokenInfo, err
}

func GetTokenInfoRetryAlert(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo, err := GetTokenInfo(ctx, chainName, tokenAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		tokenInfo, err = GetTokenInfo(ctx, chainName, tokenAddress)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币信息失败，tokenAddress:%s", chainName, tokenAddress)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return tokenInfo, err
}

func GetTokenInfo(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	if tokenAddress == "" {
		return tokenInfo, nil
	}

	var key = chainName + tokenAddress
	// FIX: read-write race
	// var ok bool
	// tokenInfo, ok := TokenInfoMap[key]
	// if ok {
	// 	return tokenInfo, nil
	// }

	lock.Lock(key)
	defer lock.Unlock(key)
	tokenInfoInner, ok := TokenInfoMap.Load(key)
	if ok {
		tokenInfo = *(tokenInfoInner.(*types.TokenInfo))
		return tokenInfo, nil
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return tokenInfo, err
	}
	defer conn.Close()
	client := v1.NewTokenlistClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	response, err := client.GetTokenInfo(ctx, &v1.GetTokenInfoReq{
		Data: []*v1.GetTokenInfoReq_Data{{
			Chain:   chainName,
			Address: tokenAddress,
		}},
	})
	if err != nil {
		return tokenInfo, err
	}

	data := response.Data
	if len(data) > 0 {
		respData := data[0]
		tokenInfo = types.TokenInfo{Address: tokenAddress, Decimals: int64(respData.Decimals), Symbol: respData.Symbol, TokenUri: respData.LogoURI}
		mutex.Lock()
		TokenInfoMap.Store(key, &tokenInfo)
		mutex.Unlock()
		return tokenInfo, nil
	}
	return tokenInfo, nil
}

func GetNftInfo(ctx context.Context, chainName string, tokenAddress string, tokenId string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	if tokenAddress == "" {
		return tokenInfo, nil
	}

	respData, err := GetRawNftInfo(ctx, chainName, tokenAddress, tokenId)
	if err != nil {
		return tokenInfo, err
	}
	if respData == nil {
		return tokenInfo, nil
	}

	tokenInfo.Address = tokenAddress
	tokenInfo.Symbol = respData.Symbol
	tokenInfo.TokenType = respData.TokenType
	tokenInfo.TokenId = respData.TokenId
	tokenInfo.CollectionName = respData.CollectionName
	tokenInfo.ItemName = respData.NftName
	tokenInfo.ItemUri = respData.ImageURL
	return tokenInfo, nil
}

func GetNftInfoDirectlyRetryAlert(ctx context.Context, chainName string, tokenAddress string, tokenId string) (types.TokenInfo, error) {
	tokenInfo, err := GetNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	for i := 0; i < 5 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		tokenInfo, err = GetNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中NFT信息失败，tokenAddress:%s，tokenId:%s", chainName, tokenAddress, tokenId)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return tokenInfo, err
}

func GetNftInfoDirectly(ctx context.Context, chainName string, tokenAddress string, tokenId string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	if tokenAddress == "" {
		return tokenInfo, nil
	}

	respData, err := GetRawNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	if err != nil {
		return tokenInfo, err
	}
	if respData == nil {
		return tokenInfo, nil
	}

	tokenInfo.Address = tokenAddress
	tokenInfo.Symbol = respData.Symbol
	tokenInfo.TokenType = respData.TokenType
	tokenInfo.TokenId = respData.TokenId
	tokenInfo.CollectionName = respData.CollectionName
	tokenInfo.ItemName = respData.NftName
	tokenInfo.ItemUri = respData.ImageURL
	return tokenInfo, nil
}

func GetCollectionInfoDirectlyRetryAlert(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo, err := GetCollectionInfoDirectly(ctx, chainName, tokenAddress)
	for i := 0; i < 5 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		tokenInfo, err = GetCollectionInfoDirectly(ctx, chainName, tokenAddress)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中NFT集合信息失败，tokenAddress:%s", chainName, tokenAddress)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return tokenInfo, err
}

func GetCollectionInfoDirectly(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	if tokenAddress == "" {
		return tokenInfo, nil
	}

	respData, err := GetRawCollectionInfoDirectly(ctx, chainName, tokenAddress)
	if err != nil {
		return tokenInfo, err
	}
	if respData == nil {
		return tokenInfo, nil
	}

	tokenInfo.Address = tokenAddress
	//tokenInfo.Symbol = respData.Symbol
	tokenInfo.TokenUri = respData.ImageURL
	tokenInfo.TokenType = respData.TokenType
	tokenInfo.CollectionName = respData.Name
	return tokenInfo, nil
}

func GetRawNftInfo(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
	if tokenAddress == "" {
		return nil, nil
	}
	var key = chainName + tokenAddress + tokenId
	tokenInfo, ok := NftInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	lock.Lock(key)
	defer lock.Unlock(key)
	tokenInfo, ok = NftInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	tokenInfo, err := GetRawNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	if err != nil {
		return nil, err
	}
	mutex.Lock()
	NftInfoMap[key] = tokenInfo
	mutex.Unlock()
	return tokenInfo, nil
}

func GetRawNftInfoDirectlyRetryAlert(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
	tokenInfo, err := GetRawNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	for i := 0; i < 5 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		tokenInfo, err = GetRawNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中NFT全量信息失败，tokenAddress:%s，tokenId:%s", chainName, tokenAddress, tokenId)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return tokenInfo, nil
}

func GetRawNftInfoDirectly(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
	if tokenAddress == "" {
		return nil, nil
	}

	nftInfoRequestList := []*v1.GetNftInfoRequest_NftInfo{{
		TokenAddress: tokenAddress,
		TokenId:      tokenId,
	}}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := v1.NewNftClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	response, err := client.GetNftInfo(ctx, &v1.GetNftInfoRequest{
		Chain:   chainName,
		NftInfo: nftInfoRequestList,
	})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}

	data := response.Data
	if len(data) > 0 {
		tokenInfo := data[0]
		return tokenInfo, nil
	}
	return nil, nil
}

func GetRawCollectionInfoDirectly(ctx context.Context, chainName string, tokenAddress string) (*v1.GetNftCollectionInfoReply_Data, error) {
	if tokenAddress == "" {
		return nil, nil
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := v1.NewNftClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	response, err := client.GetNftCollectionInfo(ctx, &v1.GetNftCollectionInfoReq{
		Chain:   chainName,
		Address: tokenAddress,
	})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}

	data := response.Data
	return data, nil
}

func GetNftsInfo(ctx context.Context, chainName string, nftAddressMap map[string][]string) ([]*v1.GetNftReply_NftInfoResp, error) {
	var nftInfoRequestList []*v1.GetNftInfoRequest_NftInfo
	for tokenAddress, tokenIdList := range nftAddressMap {
		for _, tokenId := range tokenIdList {
			nftInfoRequest := &v1.GetNftInfoRequest_NftInfo{
				TokenAddress: tokenAddress,
				TokenId:      tokenId,
			}
			nftInfoRequestList = append(nftInfoRequestList, nftInfoRequest)
		}
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := v1.NewNftClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	response, err := client.GetNftInfo(ctx, &v1.GetNftInfoRequest{
		Chain:   chainName,
		NftInfo: nftInfoRequestList,
	})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}

	data := response.Data
	return data, nil
}

func GetTokenNftInfoRetryAlert(ctx context.Context, chainName string, tokenAddress string, tokenId string) (types.TokenInfo, error) {
	tokenInfo, err := GetTokenNftInfo(ctx, chainName, tokenAddress, tokenId)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		tokenInfo, err = GetTokenNftInfo(ctx, chainName, tokenAddress, tokenId)
	}
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币或NFT信息失败，tokenAddress:%s，tokenId:%s", chainName, tokenAddress, tokenId)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return tokenInfo, err
}

func GetTokenNftInfo(ctx context.Context, chainName string, tokenAddress string, tokenId string) (types.TokenInfo, error) {
	tokenInfo, err := GetTokenInfo(ctx, chainName, tokenAddress)
	if err != nil {
		nftInfo, nftErr := GetNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
		if nftErr == nil {
			if nftInfo.TokenType != "" || (nftInfo.TokenType == "" && chainName == "Solana") {
				if nftInfo.TokenType == "" && chainName == "Solana" {
					nftInfo.TokenType = SOLANANFT
				}
				tokenInfo = nftInfo
				err = nil
			}
		}
	} else if tokenInfo.TokenType == "" && tokenInfo.Decimals == 0 && (tokenInfo.Symbol == "" || tokenInfo.Symbol == "Unknown Token") {
		tokenInfo, err = GetNftInfoDirectly(ctx, chainName, tokenAddress, tokenId)
		if err == nil && (tokenInfo.TokenType == "" && chainName == "Solana") {
			tokenInfo.TokenType = SOLANANFT
		}
	}
	return tokenInfo, err
}

func GetCustomChainList(ctx context.Context) (*v1.GetChainNodeInUsedListResp, error) {
	//mock
	//return &v1.GetChainNodeInUsedListResp{
	//	Data: []*v1.GetChainNodeInUsedListResp_Data{
	//		{
	//			Name: "HH",
	//			ChainId: "2222",
	//			Chain: "evm2222",
	//			Urls: []string{
	//				"https://evm.kava.io",
	//				"https://evm2.kava.io",
	//				},
	//			Type: "EVM",
	//		},
	//{
	//	Name: "ETH",
	//	ChainId: 1,
	//
	//	Urls: []string{"https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161", "https://geth.mytokenpocket.vip", "https://rpc.onekey.so/eth"},
	//	Type: "EVM",
	//},
	//},
	//}, nil

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}
	defer conn.Close()
	client := v1.NewChainListClient(conn)

	return client.GetChainNodeInUsedList(ctx, &emptypb.Empty{})
}

func GetPriceFromMarket(tokenAddress []*v1.Tokens, coinIds []string) (*v1.DescribePriceByCoinAddressReply, error) {
	//========================
	//coinss := make([]*v1.DescribePriceByCoinAddressReply_CoinCurrency, 0)
	//for _, ci := range coinIds {
	//	coinss = append(coinss, &v1.DescribePriceByCoinAddressReply_CoinCurrency{
	//		Price: &v1.Currency{
	//			Cny: 1,
	//			Usd: 10,
	//		},
	//		Icon:   "dfdfdfd",
	//		CoinID: ci,
	//	})
	//
	//}
	//tokens := make([]*v1.DescribePriceByCoinAddressReply_Tokens, 0)
	//for _, ta := range tokenAddress {
	//	tokens = append(tokens, &v1.DescribePriceByCoinAddressReply_Tokens{
	//		Price: &v1.Currency{
	//			Cny: 1,
	//			Usd: 10,
	//		},
	//		Icon:    "dfdfdfd",
	//		Chain:   ta.Chain,
	//		Address: ta.Address,
	//		CoinID:  "ddddd",
	//	})
	//}
	//
	//return &v1.DescribePriceByCoinAddressReply{
	//	Coins:  coinss,
	//	Tokens: tokens,
	//}, nil
	//========================

	conn, err := grpc.Dial(AppConfig.MarketRpc, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := v1.NewMarketClient(conn)
	context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
	defer cancel()
	result, err := client.DescribePriceByCoinAddress(context, &v1.DescribePriceByCoinAddressRequest{
		EventId: "100010001000",
		CoinIDs: coinIds,
		Tokens:  tokenAddress,
	})
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：币价信息查询失败, error：%s", fmt.Sprintf("%s", err))
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return result, err

}

func GetPriceFromChain(coinIds []string) (*v1.DescribeCoinsByFieldsReply, error) {
	//========================
	//var demos []*v1.DescribeCoinsByFieldsReply_Coin
	//for _, ci := range coinIds {
	//	dcbf := &v1.DescribeCoinsByFieldsReply_Coin{
	//		CoinID: ci,
	//		Price: &v1.Currency{
	//			Cny: 1,
	//			Usd: 10,
	//		},
	//		Icon: "dfjkdj",
	//	}
	//	demos = append(demos, dcbf)
	//}
	//
	//return &v1.DescribeCoinsByFieldsReply{
	//	Coins: demos,
	//}, nil
	//========================
	conn, err := grpc.Dial(AppConfig.MarketRpc, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := v1.NewMarketClient(conn)
	context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
	defer cancel()

	result, err := client.DescribeCoinsByFields(context, &v1.DescribeCoinsByFieldsRequest{
		EventId: "100010001000",
		CoinIDs: coinIds,
		Fields:  []string{"price"},
	})
	log.Info("调用用户中心", zap.Any("request-coin", coinIds), zap.Any("地址", AppConfig.MarketRpc), zap.Any("result", result), zap.Error(err))
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：币价信息查询失败, error：%s", fmt.Sprintf("%s", err))
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return result, err
}

func GetPriceFromTokenAddress(tokenAddresses []string) (*v1.DescribeTokensByFieldsReply, error) {

	//========================
	//var demos []*v1.DescribeTokensByFieldsReply_Token
	//for _, ci := range tokenAddresses {
	//	dcbf := &v1.DescribeTokensByFieldsReply_Token{
	//		CoinID:  ci,
	//		Address: ci,
	//		Price: &v1.Currency{
	//			Cny: 1,
	//			Usd: 10,
	//		},
	//		Icon: "dfjkdj",
	//	}
	//	demos = append(demos, dcbf)
	//}
	//
	//return &v1.DescribeTokensByFieldsReply{
	//	Tokens: demos,
	//}, nil
	//========================

	conn, err := grpc.Dial(AppConfig.MarketRpc, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := v1.NewMarketClient(conn)
	context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
	defer cancel()

	result, err := client.DescribeTokensByFields(context, &v1.DescribeTokensByFieldsRequest{
		EventId: "100010001000",
		Address: tokenAddresses,
		Fields:  []string{"price"},
	})
	log.Info("调用用户中心", zap.Any("request-token", tokenAddresses), zap.Any("result", result), zap.Error(err))
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：币价信息查询失败, error：%s", fmt.Sprintf("%s", err))
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return result, err

}
func DescribeCoinPriceByTimestamp(tokenAddress, coinId, chainName string, timestamp uint32) (*v1.DescribeCoinPriceByTimestampReply, error) {
	conn, err := grpc.Dial(AppConfig.MarketRpc, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := v1.NewMarketClient(conn)
	context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
	defer cancel()

	result, err := client.DescribeCoinPriceByTimestamp(context, &v1.DescribeCoinPriceByTimestampRequest{
		EventId:      "100010001000",
		CoinID:       coinId,
		Timestamp:    timestamp,
		Chain:        chainName,
		TokenAddress: tokenAddress,
	})
	log.Info("调用历史币价", zap.Any("token", tokenAddress), zap.Any("CoinID", coinId), zap.Any(chainName, result), zap.Error(err))
	if err != nil {
		// nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：币价信息查询失败, error：%s", fmt.Sprintf("%s", err))
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}

	return result, err

}

func GetMethodNameRetryAlert(ctx context.Context, chainName string, contractAddress string, methodId string) (string, error) {
	var methodName string
	contractAbiList, err := GetContractAbi(ctx, chainName, contractAddress, methodId)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		contractAbiList, err = GetContractAbi(ctx, chainName, contractAddress, methodId)
	}
	if err != nil {
		// 调用nodeProxy出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中合约ABI失败，contractAddress:%s，methodId:%s", chainName, contractAddress, methodId)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	} else if len(contractAbiList) > 0 {
		contractAbi := contractAbiList[0]
		methodName = contractAbi.Name
	}
	return methodName, err
}

type ContractAbi struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	StateMutability string `json:"stateMutability"`
	Inputs          []struct {
		InternalType string `json:"internalType"`
		Name         string `json:"name"`
		Type         string `json:"type"`
	} `json:"inputs"`
	Outputs []struct {
		InternalType string `json:"internalType"`
		Name         string `json:"name"`
		Type         string `json:"type"`
	} `json:"outputs"`
}

func GetContractAbi(ctx context.Context, chainName string, contractAddress string, methodId string) ([]*ContractAbi, error) {
	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())

	client := v1.NewCommRPCClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), 10_000*time.Millisecond)
		ctx = context
		defer cancel()
	}

	var paramMap = make(map[string]interface{})
	paramMap["chain"] = chainName
	paramMap["contract"] = contractAddress
	paramMap["method_id"] = methodId
	params, err := utils.JsonEncode(paramMap)
	if err != nil {
		return nil, err
	}
	response, err := client.ExecNodeProxyRPC(ctx, &v1.ExecNodeProxyRPCRequest{
		Id:      ID,
		Jsonrpc: JSONRPC,
		Method:  "GetContractABI",
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}

	var resultList []*ContractAbi
	err = json.Unmarshal([]byte(response.Result), &resultList)
	if err != nil {
		return nil, err
	}
	return resultList, nil
}
