package btc

import (
	httpclient2 "block-crawling/internal/httpclient"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/blockcypher/gobcy"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type Client struct {
	URL       string
	StreamURL string
}

var urlMap = map[string]string{
	"https://api.blockcypher.com/v1/btc/main":                     "https://blockstream.info/api",
	"https://api.blockcypher.com/v1/btc/test3":                    "https://blockstream.info/testnet/api",
	"http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332": "http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332",
}

func NewClient(nodeUrl string) Client {
	streamURL := "https://blockstream.info/api"
	if value, ok := urlMap[nodeUrl]; ok {
		streamURL = value
	}
	return Client{nodeUrl, streamURL}
}

func GetBalance(address string, c *base.Client) (string, error) {
	u, err := c.BuildURLBTC("/addrs/"+address+"/balance", "https://api.blockcypher.com/v1/btc/main", nil)
	if err != nil {
		return "", err
	}
	var addr gobcy.Addr
	err = getResponse(u, &addr)
	if err != nil {
		return "", err
	}
	btcValue := utils.BigIntString(&addr.Balance, 8)
	return btcValue, nil
}

func GetBlockNumber(c *base.Client) (int, error) {
	u, err := c.BuildURL("", nil)
	if err != nil {
		return 0, err
	}
	var chain gobcy.Blockchain
	err = getResponse(u, &chain)
	return chain.Height, err
}

func GetBlockHeight(c *base.Client) (int, error) {
	url := c.StreamURL + "/blocks/tip/height"
	var height int
	err := httpclient2.HttpsGetForm(url, nil, &height)
	return height, err
}

func GetMempoolTxIds(c *base.Client) ([]string, error) {
	url := c.StreamURL + "/mempool/txids"
	var txIds []string
	err := httpclient2.HttpsGetForm(url, nil, &txIds)
	return txIds, err
}

func GetBlockHashByNumber(number int, c *base.Client) (string, error) {
	url := c.StreamURL + "/block-height/" + fmt.Sprintf("%d", number)
	return httpclient2.HttpsGetFormString(url, nil)
}

func GetTestBlockByHeight(height int, c *base.Client) (result types.BTCTestBlockerInfo, err error) {
	//get block hash
	hash, err := GetBlockHashByNumber(height, c)
	if err != nil {
		return result, err
	}
	starIndex := 0
	for {
		var block types.BTCTestBlockerInfo
		url := c.StreamURL + "/block/" + hash + "/txs/" + fmt.Sprintf("%d", starIndex*25)
		err = httpclient2.HttpsGetForm(url, nil, &block)
		starIndex++
		result = append(result, block...)
		if len(block) < 25 {
			break
		}
	}

	return
}

func GetTransactionByHash(hash string, c *base.Client) (tx types.TX, err error) {
	tx, err = DoGetTransactionByHash(hash+"?instart=0&outstart=0&limit=500", c)
	if err != nil {
		return
	}
	putsTx := tx
	for (putsTx.NextInputs != "" && len(putsTx.Inputs) > 80) || (putsTx.NextOutputs != "" && len(putsTx.Outputs) > 80) {
		putsTx, err = DoGetTransactionByHash(hash+"?instart="+strconv.Itoa(len(putsTx.Inputs))+"&outstart="+strconv.Itoa(len(putsTx.Outputs))+"&limit=500", c)
		if err != nil {
			return
		}
		for _, input := range putsTx.Inputs {
			tx.Inputs = append(tx.Inputs, input)
		}
		for _, output := range putsTx.Outputs {
			tx.Outputs = append(tx.Outputs, output)
		}
	}
	return
}

func DoGetTransactionByHash(hash string, c *base.Client) (tx types.TX, err error) {
	u, err := c.BuildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return
}

func GetTransactionByPendingHash(hash string, c *base.Client) (tx types.TXByHash, err error) {
	u, err := c.BuildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return
}

func GetTransactionByPendingHashByNode(json model.JsonRpcRequest, c *base.Client) (tx model.BTCTX, err error) {
	err = postResponse(c.StreamURL, json, &tx)
	return
}

//MemoryPoolTX
func GetMemoryPoolTXByNode(json model.JsonRpcRequest, c *base.Client) (txIds model.MemoryPoolTX, err error) {
	err = postResponse(c.StreamURL, json, &txIds)
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

func GetBTCBlockByNumber(number int, c *base.Client) (types.BTCBlockerInfo, error) {
	url := "https://blockchain.info/rawblock/" + fmt.Sprintf("%d", number)
	var block types.BTCBlockerInfo
	err := httpclient2.HttpsGetForm(url, nil, &block)
	return block, err
}

//constructs BlockCypher URLs with parameters for requests
func (c *Client) buildURL(u string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(c.URL + u)
	if err != nil {
		return
	}
	values := target.Query()
	//Set parameters
	for k, v := range params {
		values.Set(k, v)
	}
	//add token to url, if present

	target.RawQuery = values.Encode()
	return
}

//getResponse is a boilerplate for HTTP GET responses.
func getResponse(target *url.URL, decTarget interface{}) (err error) {
	resp, err := http.Get(target.String())
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
