package doge

import (
	"block-crawling/internal/biz"
	httpclient2 "block-crawling/internal/httpclient"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
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
	err := httpclient2.HttpsGetForm(url, nil, &txIds)
	return txIds, err
}

func GetBlockNumber(index int, c *base.Client) (int, error) {
	url := c.URL + "sync/block_number"
	var height int
	key := biz.AppConfig.GetDogeKey()[index]
	err := httpclient2.HttpsSignGetForm(url, nil, key, &height)
	return height, err
}

func GetBlockByNumber(number int, index int, c *base.Client) (types.Dogecoin, error) {
	url := c.URL + "block/" + fmt.Sprintf("%d", number)
	key := biz.AppConfig.GetDogeKey()[index]
	var block types.Dogecoin
	err := httpclient2.HttpsSignGetForm(url, nil, key, &block)
	return block, err
}

func GetBalance(address string, c *base.Client) (string, error) {
	url := c.URL + "account/" + address
	key := biz.AppConfig.GetDogeKey()[4]
	var balances []types.Balances
	err := httpclient2.HttpsSignGetForm(url, nil, key, &balances)
	if err == nil {
		if len(balances) > 0 {
			return balances[0].ConfirmedBalance, nil
		}
	}
	return "", err
}
func GetTransactionsByTXHash(tx string, c *base.Client) (types.TxInfo, error) {
	url := c.URL + "tx/" + tx
	key := biz.AppConfig.GetDogeKey()[4]
	var txInfo types.TxInfo
	err := httpclient2.HttpsSignGetForm(url, nil, key, &txInfo)
	return txInfo, err
}

func GetMemoryPoolTXByNode(json model.JsonRpcRequest, c *base.Client) (txIds model.MemoryPoolTX, err error) {
	err = postResponse(c.StreamURL, json, &txIds)
	return

}
func GetTransactionByPendingHashByNode(json model.JsonRpcRequest, c *base.Client) (tx model.BTCTX, err error) {
	err = postResponse(c.StreamURL, json, &tx)
	return
}
func GetBlockCount(json model.JsonRpcRequest, c *base.Client) (count model.BTCCount, err error) {
	err = postResponse(c.StreamURL, json, &count)
	return
}

func postResponse(target string, encTarget interface{}, decTarget interface{}) (err error) {
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	if err = enc.Encode(encTarget); err != nil {
		return
	}
	resp, err := http.Post(target, "application/json", &data)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, decTarget)
	if err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
	}
	return
}
