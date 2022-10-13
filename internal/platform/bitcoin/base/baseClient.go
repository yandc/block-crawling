package base

import (
	"block-crawling/internal/model"
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
	ChainName string
}

func (c *Client) BuildURL(u string, params map[string]string) (target *url.URL, err error) {
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


func (c *Client) BuildURLBTC(u string, node string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(node + u)
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
//块信息根据块高
func (c *Client) GetBlockHash(height int) (blockHash model.UTXOBlockHash, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblockhash",
	}

	var pa = make([]interface{}, 0, 1)
	pa = append(pa, height)
	countParam.Params = pa
	err = postResponse(c.StreamURL, countParam, &blockHash)
	return
}

//获取内存池数据
func (c *Client) GetMemoryPoolTXByNode() (txIds model.MemoryPoolTX, err error) {
	memoryTxId := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getrawmempool",
	}
	var pa = make([]interface{}, 0, 0)

	memoryTxId.Params = pa
	err = postResponse(c.StreamURL, memoryTxId, &txIds)
	return
}


func (c *Client) GetUTXOBlock(height int) (utoxBlockInfo model.UTXOBlockInfo, err error) {

	blockHash, err := c.GetBlockHash(height)
	if err != nil {
		return utoxBlockInfo, err
	}
	if blockHash.Error != nil {
		return utoxBlockInfo, errors.New(fmt.Sprintf("%v", blockHash.Error))
	}

	return c.GetUTXOBlockByHash(blockHash.Result)
}


func (c *Client) GetBTCBlock(height int) (btcBlockInfo model.BTCBlockInfo, err error) {
	blockHash, err := c.GetBlockHash(height)

	if err != nil {
		return btcBlockInfo, err
	}
	if blockHash.Error != nil {
		return btcBlockInfo, errors.New(fmt.Sprintf("%v", blockHash.Error))
	}
	btcBlockInfo , err = c.GetBTCBlockByHash(blockHash.Result)
	return btcBlockInfo ,err
}

func (c *Client) GetBTCBlockByHash(blockHash string) (btcBlockInfo model.BTCBlockInfo, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblock",
	}
	var pa = make([]interface{}, 0, 2)
	pa = append(pa, blockHash)
	pa = append(pa, 2)
	countParam.Params = pa

	err = postResponse(c.StreamURL, countParam, &btcBlockInfo)
	return
}

func (c *Client) GetUTXOBlockByHash(blockHash string) (utxoBlockInfo model.UTXOBlockInfo, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblock",
	}
	var pa = make([]interface{}, 0, 1)
	pa = append(pa, blockHash)
	countParam.Params = pa

	err = postResponse(c.StreamURL, countParam, &utxoBlockInfo)
	return
}


//当前块高
func (c *Client) GetBlockCount() (count model.BTCCount, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblockcount",
	}
	var pa = make([]interface{}, 0, 0)
	countParam.Params = pa
	err = postResponse(c.StreamURL, countParam, &count)
	return
}

//未花费信息查询
func (c *Client) GetTxOut(txid string, n int, unconfirmed bool) (utxoList model.UnSpent, err error) {
	param := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "gettxout",
	}
	var pa = make([]interface{}, 0, 2)
	pa = append(pa, txid)
	pa = append(pa, n)
	pa = append(pa, unconfirmed)

	param.Params = pa
	err = postResponse(c.StreamURL, param, &utxoList)
	return
}

func (c *Client) GetTransactionsByTXHash(txid string) (tx model.BTCTX, err error) {
	param := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getrawtransaction",
	}
	var pa = make([]interface{}, 0, 2)
	pa = append(pa, txid)
	pa = append(pa, 1)
	param.Params = pa
	err = postResponse(c.StreamURL, param, &tx)
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
