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
	return decimal.NewFromInt(1).Div(p.CoinAPriceInCoinB())
}

func (p *OnChainPool) CoinAPriceInCoinB() decimal.Decimal {
	coinA, _ := decimal.NewFromString(p.CurrentSqrtPrice)
	power64, _ := decimal.NewFromString("18446744073709551616") // 2^64
	return coinA.Div(power64).Pow(decimal.NewFromInt(2))
}

func (p *OnChainPool) IntoPool(chainName string, bfcPriceInUSD decimal.Decimal) (*biz.BenfenPool, error) {
	benfenBUSDCoinType := swap.NormalizeBenfenCoinType(chainName, "0xc8::busd::BUSD")
	benfenBFCCoinType := "BFC000000000000000000000000000000000000000000000000000000000000000268e4::bfc::BFC"

	coinA, coinB := p.ParseType()
	if coinA != benfenBUSDCoinType && coinA != benfenBFCCoinType {
		return nil, fmt.Errorf("[%s] coinA of the pool must be one of BFC or BUSD", coinA)
	}
	coinATokenInfo, err := biz.GetTokenInfo(context.Background(), chainName, coinA)
	if err != nil {
		return nil, err
	}
	coinBTokenInfo, err := biz.GetTokenInfo(context.Background(), chainName, coinB)
	var coinbPriceInBUSD decimal.Decimal
	var coinaPriceInBUSD decimal.Decimal

	if coinA == benfenBUSDCoinType {
		coinaPriceInBUSD = decimal.NewFromInt(1)
		coinbPriceInBUSD = p.CoinBPrice()
	} else {
		// CoinA MUST BE BFC
		coinaPriceInBUSD = bfcPriceInUSD
		if coinB == benfenBUSDCoinType {
			coinbPriceInBUSD = decimal.NewFromInt(1)
		} else {

			coinbBFCPrice := p.CoinBPrice()
			coinbPriceInBUSD = coinbBFCPrice.Mul(bfcPriceInUSD)
		}
	}
	coinaAmount, _ := decimal.NewFromString(utils.StringDecimals(p.CoinA, int(coinATokenInfo.Decimals)))
	coinbAmount, _ := decimal.NewFromString(utils.StringDecimals(p.CoinA, int(coinBTokenInfo.Decimals)))

	return &biz.BenfenPool{
		ChainName:             chainName,
		PairAddress:           p.ID.ID,
		TokenBase:             coinA,
		TokenQuote:            coinB,
		TokenBasePriceInBUSD:  coinaPriceInBUSD.String(),
		TokenQuotePriceInBUSD: coinbPriceInBUSD.String(),
		TokenBaseBalance:      coinaAmount.String(),
		TokenQuoteBalance:     coinbAmount.String(),
		FDVInBUSD:             coinaAmount.Mul(coinaPriceInBUSD).Add(coinbAmount.Mul(coinbPriceInBUSD)).String(),
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
	"Benfen_BUSD":     "BFC39aa609a447497fb9feba26ab0fc7f6ae78e84ac10e53d19194765ef03f66ba9286f", // 0.05%
	"Benfen_BJPY":     "BFCf8507d873507ce3da7a5fd293fa6b03809608b149cc388c2e4615a732fd71c57266b", // 0.05%
}

var benfenBUSDBasePools = map[string]map[string]string{
	"Benfen": {
		"USDC_0.05": "BFC53c66bcb2240bcfb6216683ca829ec74857154e4d2fb1314b20afcd4385f4bead7ec",
		"USDC_0.3":  "BFC725e455d526d2b652746e4cceb19089d3859a52383bdf9ec38b0838302e14d31cab0",
		"USDC_1":    "BFC9cecdd60d5a2b103737bffa795dd05965afc18e5b75ea164e03acd298c27c0a80ada",
		"LONG_0.05": "BFCcc27098eacd4b807d5502deab9ce5e3b7bb079b9b7fa18344fc1d9d98fcde9a3d4ea",
		"LONG_0.3":  "BFC15b9609c8e4d9e415834041fce08ccf0610f3cfbb7b58c56858672ea2f20fd0dd0ca",
		"LONG_1":    "BFC9cecdd60d5a2b103737bffa795dd05965afc18e5b75ea164e03acd298c27c0a80ada",
	},
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
	bfcBUSDPrice := busdPool.CoinAPriceInCoinB() // BFC/BUSD
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

	for name, poolID := range benfenBUSDBasePools[t.chainName] {
		rawPool, err := t.getPoolObject(poolID)
		if err != nil {
			return fmt.Errorf("[%sRead] %w", name, err)
		}
		if err := t.pushPool(rawPool, bfcBUSDPrice); err != nil {
			return err
		}
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
