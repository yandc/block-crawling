package biz

import (
	"block-crawling/internal/client"
	"block-crawling/internal/common"
	"block-crawling/internal/types"
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"time"
)

var TokenInfoMap = make(map[string]types.TokenInfo)
var lock = common.NewSyncronized(0)

func GetTokenPrice(ctx context.Context, chainName string, currency string, tokenAddress string) (string, error) {
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

func GetTokenInfo(ctx context.Context, chainName string, tokenAddress string) (types.TokenInfo, error) {
	var key = chainName + tokenAddress
	tokenInfo, ok := TokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	lock.Lock(key)
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
		TokenInfoMap[key] = tokenInfo
		return tokenInfo, nil
	}
	lock.Unlock(key)
	return tokenInfo, nil
}
