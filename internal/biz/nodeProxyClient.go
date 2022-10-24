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
	var coinAddresses string
	var getPriceKeyMap = make(map[string]string)
	var handlerMap = make(map[string]string)
	var resultMap = make(map[string]map[string]string)

	for chainName, tokenAddressList := range chainNameTokenAddressMap {
		if strings.HasSuffix(chainName, "TEST") {
			continue
		}
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			getPriceKey := platInfo.GetPriceKey
			handler := platInfo.Handler
			getPriceKeyMap[getPriceKey] = chainName
			handlerMap[handler] = chainName

			for _, tokenAddress := range tokenAddressList {
				if tokenAddress == "" {
					if coinNames == "" {
						coinNames = getPriceKey
					} else {
						coinNames = coinNames + "," + getPriceKey
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
		chainName := getPriceKeyMap[key]
		price := value[currency]
		var tokenAddress string
		if chainName == "" {
			handlerTokenAddressList := strings.Split(key, "_")
			handler := handlerTokenAddressList[0]
			chainName = handlerMap[handler]
			tokenAddress = handlerTokenAddressList[1]
		}
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
	return resultMap, nil
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
