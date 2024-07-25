package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/sui/swap"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type PoolObjectContent struct {
	Type string       `json:"type"`
	Pool *OnChainPool `json:"fields"`
}
type OnChainPool struct {
	ID struct {
		ID string `json:"Id"`
	} `json:"id"`
	CoinA string `json:"coin_a"`
	CoinB string `json:"coin_b"`

	CurrentSqrtPrice string `json:"current_sqrt_price"`

	Type string
}

func (p *OnChainPool) CoinBPrice() decimal.Decimal {
	return decimal.NewFromInt(1).Div(p.BFCPriceInCoinB())
}

func (p *OnChainPool) BFCPriceInCoinB() decimal.Decimal {
	coinA, _ := decimal.NewFromString(p.CurrentSqrtPrice)
	power64, _ := decimal.NewFromString("18446744073709551616") // 2^64
	return coinA.Div(power64).Pow(decimal.NewFromInt(2))
}

func (p *OnChainPool) IntoPool(chainName string, bfcPriceInUSD decimal.Decimal) (*biz.BenfenPool, error) {
	coinA, coinB := p.ParseType()
	coinATokenInfo, err := biz.GetTokenInfo(context.Background(), chainName, coinA)
	if err != nil {
		return nil, err
	}
	coinBTokenInfo, err := biz.GetTokenInfo(context.Background(), chainName, coinB)
	var coinbPriceInBUSD decimal.Decimal
	if coinB == swap.NormalizeBenfenCoinType(chainName, "0xc8::busd::BUSD") {
		coinbPriceInBUSD = decimal.NewFromInt(1)
	} else {
		coinbBFCPrice := p.CoinBPrice()
		coinbPriceInBUSD = coinbBFCPrice.Mul(bfcPriceInUSD)
	}
	coinaAmount, _ := decimal.NewFromString(utils.StringDecimals(p.CoinA, int(coinATokenInfo.Decimals)))
	coinbAmount, _ := decimal.NewFromString(utils.StringDecimals(p.CoinA, int(coinBTokenInfo.Decimals)))

	return &biz.BenfenPool{
		ChainName:             chainName,
		PairAddress:           p.ID.ID,
		TokenBase:             coinA,
		TokenQuote:            coinB,
		TokenBasePriceInBUSD:  bfcPriceInUSD.String(),
		TokenQuotePriceInBUSD: coinbPriceInBUSD.String(),
		TokenBaseBalance:      coinaAmount.String(),
		TokenQuoteBalance:     coinbAmount.String(),
		FDVInBUSD:             coinaAmount.Mul(bfcPriceInUSD).Add(coinbAmount.Mul(coinbPriceInBUSD)).String(),
	}, nil
}

func (p *OnChainPool) ParseType() (string, string) {
	typeArgs := strings.Split(p.Type, "<")[1]
	typeArgs = typeArgs[0 : len(typeArgs)-1]
	coins := strings.Split(typeArgs, ",")
	coinA := swap.NormalizeBenfenCoinType("BenfenTEST", strings.TrimSpace(coins[0]))
	coinB := swap.NormalizeBenfenCoinType("BenfenTEST", strings.TrimSpace(coins[1]))
	return coinA, coinB
}

var benfenPools = map[string]string{
	"BenfenTEST_BUSD": "BFCbb19240e197eeb924b0e5e1a9269fc8c2c985b6379f817b960cf8c4fc0676c0d6f87", // 0.05%
	"BenfenTEST_BJPY": "BFC6d5ff6cf9e6b9bbf521489bab420e48044a73175f76524e5353cb935b82fe300c51b", // 0.05%
	"Benfen_BUSD":     "BFC35b57d9c92815e555bb3c23ebbd7d6578ca5e9e42af4193e16910205e7fb2e50b422", // 0.05%
	"Benfen_BJPY":     "BFCf6ab8d6679332c9c7e5194e19c5035f503f23f4b795511d7d96a47ef1862dd8b9058", // 0.05%

}

type BenfenPoolTask struct {
	chainName string
}

func (t *BenfenPoolTask) Run() {
	if err := t.run(); err != nil {
		log.Error("FAILED TO PUSH POOL TO CMQ", zap.Error(err))
	}
}

func (t *BenfenPoolTask) run() error {
	bfcBUSDPoolID := benfenPools[fmt.Sprint(t.chainName, "_BUSD")]
	bfcBJPYPoolID := benfenPools[fmt.Sprint(t.chainName, "_BJPY")]
	rawBusdPool, err := t.getPoolObject(bfcBUSDPoolID)
	if err != nil {
		return fmt.Errorf("[busdRead] %w", err)
	}
	busdPool := rawBusdPool.Pool
	if busdPool.CoinA == "0" || busdPool.CoinB == "0" {
		return fmt.Errorf("no BFC BUSD price")
	}
	bfcBUSDPrice := busdPool.BFCPriceInCoinB()
	if err := t.pushPool(rawBusdPool, bfcBUSDPrice); err != nil {
		return err
	}

	rawBJPYPool, err := t.getPoolObject(bfcBJPYPoolID)
	if err != nil {
		return fmt.Errorf("[bjpyRead] %w", err)
	}
	if err := t.pushPool(rawBJPYPool, bfcBUSDPrice); err != nil {
		return err
	}

	return nil
}

func (t *BenfenPoolTask) pushPool(rawPool *PoolObjectContent, bfcBUSDPrice decimal.Decimal) error {
	pool := rawPool.Pool
	if pool.CoinA == "0" || pool.CoinB == "0" {
		return nil
	}
	poolID := pool.ID.ID

	if pool, err := pool.IntoPool(t.chainName, bfcBUSDPrice); err != nil {
		return fmt.Errorf("[%sInto] %w", poolID, err)
	} else {
		if err := biz.PushBenfenPool(pool); err != nil {
			return fmt.Errorf("[%sPush] %w", poolID, err)
		}
	}
	return nil
}

func (t *BenfenPoolTask) getPoolObject(objectID string) (*PoolObjectContent, error) {
	rawObj, err := biz.ExecuteRetry(t.chainName, func(client chain.Clienter) (interface{}, error) {
		c := client.(*sui.Client)
		object, err := c.GetObject(objectID, true)
		if err != nil {
			return nil, err
		}
		return object, nil
	})
	if err != nil {
		return nil, err
	}
	object := rawObj.(sui.GetObject)
	var pool *PoolObjectContent
	if err := json.Unmarshal(object.Data.Content, &pool); err != nil {
		return nil, err
	}
	pool.Pool.Type = pool.Type
	log.Info("GOT BENFEN POOL", zap.Any("pool", pool), zap.String("chainName", t.chainName))
	return pool, nil
}

func NewBenfenPoolTask(chainName string) *BenfenPoolTask {
	return &BenfenPoolTask{
		chainName: chainName,
	}
}
