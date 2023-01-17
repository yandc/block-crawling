package doge

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"fmt"
	"github.com/blockcypher/gobcy"
	"github.com/shopspring/decimal"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	URL       string
	StreamURL string
}

func NewClient(nodeUrl string) Client {
	return Client{nodeUrl, nodeUrl}
}

func GetMempoolTxIds(c *base.Client) ([]string, error) {
	url := c.StreamURL + "/mempool/txids"
	var txIds []string
	err := httpclient.HttpsGetForm(url, nil, &txIds)
	return txIds, err
}

func GetBlockNumber(c *base.Client) (int, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "sync/block_number"
	var height int
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &height)
	return height, err
}

func parseKeyFromNodeURL(nodeURL string) (key, restURL string) {
	parsed, err := url.Parse(nodeURL)
	if err != nil {
		return "", nodeURL
	}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		key = fmt.Sprintf("%s %s", parsed.User.Username(), password)
		parsed.User = nil
		restURL = parsed.String()
		// log.Debug("DOGE PARSED KEY FROM URL", zap.String("key", key), zap.String("url", restURL))
		return
	}
	return "", nodeURL
}

func GetBlockByNumber(number int, c *base.Client) (types.Dogecoin, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "block/" + fmt.Sprintf("%d", number)
	var block types.Dogecoin
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &block)

	return block, err
}

func GetBalance(address string, c *base.Client) (string, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "account/" + address
	var balances []types.Balances
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &balances)
	if err == nil {
		if len(balances) > 0 {
			btcValue := utils.StringDecimals(balances[0].ConfirmedBalance, 8)
			return btcValue, nil
		}
	}
	return "", err
}
func GetTransactionsByTXHash(tx string, c *base.Client) (types.TxInfo, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "tx/" + tx
	var txInfo types.TxInfo
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &txInfo)
	return txInfo, err
}

func GetTxByHashFromChainSo(txhash string, c *base.Client) (types.TX, error) {
	url := c.URL + "api/v2/get_tx/" + c.ChainName + "/" + txhash

	var tx types.TX
	var txInfo types.ChainSOTX
	err := httpclient.HttpsGetForm(url, nil, &txInfo)
	if err != nil || txInfo.Status != "success" {
		return tx, err
	}
	fee, _ := decimal.NewFromString(txInfo.Data.NetworkFee)
	p, _ := decimal.NewFromString("100000000")
	y := fee.Mul(p)

	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_NODE_HEIGHT_KEY + c.ChainName).Result()

	curr, _ := strconv.Atoi(redisHeight)
	blockNumber := curr - txInfo.Data.Confirmations

	var inputs []gobcy.TXInput
	var inputAddress []string
	var outs []gobcy.TXOutput
	var outputAddress []string

	for _, in := range txInfo.Data.Inputs {
		inval, _ := decimal.NewFromString(in.Value)
		am := inval.Mul(p)
		input := gobcy.TXInput{
			OutputValue: int(am.IntPart()),
			Addresses:   append(inputAddress, in.Address),
			PrevHash:    in.FromOutput.Txid,
			OutputIndex: in.FromOutput.OutputNo,
		}
		inputs = append(inputs, input)

	}

	for _, out := range txInfo.Data.Outputs {
		outval, _ := decimal.NewFromString(out.Value)
		amount := outval.Mul(p)

		out := gobcy.TXOutput{
			Value:     *amount.BigInt(),
			Addresses: append(outputAddress, out.Address),
		}
		outs = append(outs, out)
	}

	tx = types.TX{
		BlockHash:   txInfo.Data.Blockhash,
		BlockHeight: blockNumber,
		Hash:        txInfo.Data.Txid,
		Fees:        *y.BigInt(),
		Confirmed:   time.Unix(int64(txInfo.Data.Time), 0),
		Inputs:      inputs,
		Outputs:     outs,
		Error:       "",
	}
	return tx, err

}

func GetMemoryPoolTXByNode(json model.JsonRpcRequest, c *base.Client) (txIds model.MemoryPoolTX, err error) {
	err = httpclient.PostResponse(c.StreamURL, json, &txIds)
	return

}
func GetTransactionByPendingHashByNode(json model.JsonRpcRequest, c *base.Client) (tx model.BTCTX, err error) {
	err = httpclient.PostResponse(c.StreamURL, json, &tx)
	return
}
func GetBlockCount(json model.JsonRpcRequest, c *base.Client) (count model.BTCCount, err error) {
	err = httpclient.PostResponse(c.StreamURL, json, &count)
	return
}
