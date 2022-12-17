package biz

import (
	v1 "block-crawling/internal/client"
	"block-crawling/internal/common"
	"block-crawling/internal/types"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var TokenInfoMap = make(map[string]types.TokenInfo)
var lock = common.NewSyncronized(0)
var mutex = new(sync.Mutex)

var NftInfoMap = make(map[string]*v1.GetNftReply_NftInfoResp)
var nftLock = common.NewSyncronized(0)
var nftMutex = new(sync.Mutex)

func GetTokenPrice(ctx context.Context, chainName string, currency string, tokenAddress string) (string, error) {
	if strings.HasSuffix(chainName, "TEST") {
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
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
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
	if value, ok := result[getPriceKey]; ok {
		price := value[currency]
		return price, nil
	} else if value, ok = result[reqKey]; ok {
		price := value[currency]
		return price, nil
	}
	return "", nil
}

func GetTokensPrice(ctx context.Context, currency string, chainNameTokenAddressMap map[string][]string) (map[string]map[string]string, error) {
	var coinNames string
	var coinNameMap = make(map[string]string)
	var coinAddresses string
	var getPriceKeyMap = make(map[string][]string)
	var handlerMap = make(map[string]string)
	var resultMap = make(map[string]map[string]string)

	for chainName, tokenAddressList := range chainNameTokenAddressMap {
		if strings.HasSuffix(chainName, "TEST") {
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
					if coinNames == "" {
						coinNames = getPriceKey
						coinNameMap[getPriceKey] = ""
					} else if _, ok := coinNameMap[getPriceKey]; !ok {
						coinNames = coinNames + "," + getPriceKey
						coinNameMap[getPriceKey] = ""
					}
				} else {
					if coinAddresses == "" {
						coinAddresses = handler + "_" + tokenAddress
					} else {
						coinAddresses = coinAddresses + "," + handler + "_" + tokenAddress
					}
				}
			}
		}
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return resultMap, err
	}
	defer conn.Close()
	client := v1.NewTokenlistClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
		ctx = context
		defer cancel()
	}
	priceResp, err := client.GetPrice(ctx, &v1.PriceReq{
		Currency:      currency,
		CoinNames:     coinNames,
		CoinAddresses: coinAddresses,
	})
	if err != nil {
		return resultMap, err
	}

	result := make(map[string]map[string]string)
	err = json.Unmarshal(priceResp.Data, &result)
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

func GetTokenInfo(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	tokenInfo := types.TokenInfo{}
	if tokenAddress == "" {
		return tokenInfo, nil
	}

	var key = chainName + tokenAddress
	tokenInfo, ok := TokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	lock.Lock(key)
	defer lock.Unlock(key)
	tokenInfo, ok = TokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return tokenInfo, err
	}
	defer conn.Close()
	client := v1.NewTokenlistClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
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
		tokenInfo = types.TokenInfo{Address: tokenAddress, Decimals: int64(respData.Decimals), Symbol: respData.Symbol}
		mutex.Lock()
		TokenInfoMap[key] = tokenInfo
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

	conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := v1.NewNftClient(conn)

	if ctx == nil {
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
		ctx = context
		defer cancel()
	}
	response, err := client.GetNftInfo(ctx, &v1.GetNftInfoRequest{
		Chain: chainName,
		NftInfo: []*v1.GetNftInfoRequest_NftInfo{{
			TokenAddress: tokenAddress,
			TokenId:      tokenId,
		}},
	})
	if err != nil {
		return nil, err
	}

	data := response.Data
	if len(data) > 0 {
		tokenInfo = data[0]
		mutex.Lock()
		NftInfoMap[key] = tokenInfo
		mutex.Unlock()
		return tokenInfo, nil
	}
	return tokenInfo, nil
}

func GetRawNftInfoDirectly(ctx context.Context, chainName string, tokenAddress string, tokenId string) (*v1.GetNftReply_NftInfoResp, error) {
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
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
		ctx = context
		defer cancel()
	}
	response, err := client.GetNftInfo(ctx, &v1.GetNftInfoRequest{
		Chain: chainName,
		NftInfo: []*v1.GetNftInfoRequest_NftInfo{{
			TokenAddress: tokenAddress,
			TokenId:      tokenId,
		}},
	})
	if err != nil {
		return nil, err
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
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
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
		context, cancel := context.WithTimeout(context.Background(), time.Second*3000)
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

	data := response.Data
	return data, nil
}
