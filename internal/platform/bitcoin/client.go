package bitcoin

import (
	httpclient2 "block-crawling/internal/httpclient"
	"block-crawling/internal/model"
	"block-crawling/internal/types"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/blockcypher/gobcy"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

type Client struct {
	URL       string
	streamURL string
}

var urlMap = map[string]string{
	"https://api.blockcypher.com/v1/btc/main":  "https://blockstream.info/api",
	"https://api.blockcypher.com/v1/btc/test3": "https://blockstream.info/testnet/api",
	"https://api.bixin.com/v1/btc/main":        "http://haotech:phzxiTvtjqHikHTBTnTthqg3@47.244.138.206:8332",
}

func NewClient(nodeUrl string) Client {
	streamURL := "https://blockstream.info/api"
	if value, ok := urlMap[nodeUrl]; ok {
		streamURL = value
	}
	return Client{nodeUrl, streamURL}
}

func (c *Client) GetBlockNumber() (int, error) {
	u, err := c.buildURL("", nil)
	if err != nil {
		return 0, err
	}
	var chain gobcy.Blockchain
	err = getResponse(u, &chain)
	return chain.Height, err
}

func (c *Client) GetBlockHeight() (int, error) {
	url := c.streamURL + "/blocks/tip/height"
	var height int
	err := httpclient2.HttpsGetForm(url, nil, &height)
	return height, err
}

func (c *Client) GetMempoolTxIds() ([]string, error) {
	url := c.streamURL + "/mempool/txids"
	var txIds []string
	err := httpclient2.HttpsGetForm(url, nil, &txIds)
	return txIds, err
}

func (c *Client) GetBlockHashByNumber(number int) (string, error) {
	url := c.streamURL + "/block-height/" + fmt.Sprintf("%d", number)
	return httpclient2.HttpsGetFormString(url, nil)
}

func (c *Client) GetTestBlockByHeight(height int) (result types.BTCTestBlockerInfo, err error) {
	//get block hash
	hash, err := c.GetBlockHashByNumber(height)
	if err != nil {
		return result, err
	}
	starIndex := 0
	for {
		var block types.BTCTestBlockerInfo
		url := c.streamURL + "/block/" + hash + "/txs/" + fmt.Sprintf("%d", starIndex*25)
		err = httpclient2.HttpsGetForm(url, nil, &block)
		starIndex++
		result = append(result, block...)
		if len(block) < 25 {
			break
		}
	}

	return
}

func (c *Client) GetTransactionByHash(hash string) (tx gobcy.TX, err error) {
	u, err := c.buildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return
}

func (c *Client) GetTransactionByPendingHash(hash string) (tx types.TXByHash, err error) {
	u, err := c.buildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return
}

func (c *Client) GetTransactionByPendingHashByNode(json model.JsonRpcRequest) (tx model.BTCTX, err error) {
	err = postResponse(c.streamURL, json, &tx)
	return
}

//MemoryPoolTX
func (c *Client) GetMemoryPoolTXByNode(json model.JsonRpcRequest) (txIds model.MemoryPoolTX, err error) {
	err = postResponse(c.streamURL, json, &txIds)
	return

}

func (c *Client) GetBlockCount(json model.JsonRpcRequest) (count model.BTCCount, err error) {
	err = postResponse(c.streamURL, json, &count)
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		err = respErrorMaker(resp.StatusCode, resp.Body)
		return
	}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(decTarget)
	return
}

func (c *Client) GetBTCBlockByNumber(number int) (types.BTCBlockerInfo, error) {
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
	if resp.StatusCode != http.StatusOK {
		err = respErrorMaker(resp.StatusCode, resp.Body)
		return
	}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(decTarget)
	return
}

//respErrorMaker checks error messages/if they are multiple errors
//serializes them into a single error message
func respErrorMaker(statusCode int, body io.Reader) (err error) {
	status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
	if statusCode == 429 {
		err = errors.New(status)
		return
	}
	type errorJSON struct {
		Err    string `json:"error"`
		Errors []struct {
			Err string `json:"error"`
		} `json:"errors"`
	}
	var msg errorJSON
	dec := json.NewDecoder(body)
	err = dec.Decode(&msg)
	if err != nil {
		return err
	}
	var errtxt string
	errtxt += msg.Err
	for i, v := range msg.Errors {
		if i == len(msg.Errors)-1 {
			errtxt += v.Err
		} else {
			errtxt += v.Err + ", "
		}
	}
	if errtxt == "" {
		err = errors.New(status)
	} else {
		err = errors.New(status + ", Message(s): " + errtxt)
	}
	return
}
