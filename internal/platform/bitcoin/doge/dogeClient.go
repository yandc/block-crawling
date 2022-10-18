package doge

import (
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
	"net/url"
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

func GetBlockNumber(c *base.Client) (int, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "sync/block_number"
	var height int
	err := httpclient2.HttpsSignGetForm(url, nil, key, &height)
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
	err := httpclient2.HttpsSignGetForm(url, nil, key, &block)

	return block, err
}

func GetBalance(address string, c *base.Client) (string, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "account/" + address
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
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "tx/" + tx
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
