package starcoin

import (
	"block-crawling/internal/types"
	"context"
	"encoding/json"
	"github.com/starcoinorg/starcoin-go/client"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	ID101 = 101
	ID200 = 200
)

type Client struct {
	URL    string
	client client.StarcoinClient
}

func NewClient(rawUrl string) Client {
	return Client{client: client.NewStarcoinClient(rawUrl), URL: rawUrl}
}

func (c *Client) Call(id int, method string, out interface{}, params []interface{}) error {
	request := types.Request{
		ID:      id,
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
	}
	str, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", c.URL, strings.NewReader(string(str)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var response types.Response
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	if response.Error != nil {
		return response.Error
	}
	if err := json.Unmarshal(response.Result, out); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetTransactionInfoByHash(transactionHash string) (*client.TransactionInfo, error) {
	return c.client.GetTransactionInfoByHash(context.Background(), transactionHash)
}

func (c *Client) GetBlockByNumber(number int) (*types.Block, error) {
	method := "chain.get_block_by_number"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{number, d}
	result := &types.Block{}
	err := c.Call(ID101, method, result, params)
	return result, err
}

func (c *Client) GetBlockTxnInfos(blockHash string) (*[]types.BlockTxnInfos, error) {
	method := "chain.get_block_txn_infos"
	params := []interface{}{blockHash}
	result := &[]types.BlockTxnInfos{}
	err := c.Call(101, method, result, params)
	return result, err
}

func (c *Client) GetTransactionByHash(transactionHash string) (*types.Transaction, error) {
	method := "chain.get_transaction"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{transactionHash, d}
	result := &types.Transaction{}
	err := c.Call(ID101, method, result, params)
	return result, err
}

func (c *Client) GetBlockHeight() (string, error) {
	method := "node.info"
	result := &types.NodeInfo{}
	err := c.Call(ID200, method, result, nil)
	if err != nil {
		return "", err
	}
	return result.PeerInfo.ChainInfo.Header.Height, err
}
