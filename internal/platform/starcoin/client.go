package starcoin

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/starcoinorg/starcoin-go/client"
	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	ID101          = 101
	ID200          = 200
	JSONRPC        = "2.0"
	GAS_TOKEN_CODE = "0x1::STC::STC"
)

type Client struct {
	*common.NodeDefaultIn

	url       string
	chainName string
	client    client.StarcoinClient
}

func NewClient(rawUrl, chainName string) Client {
	return Client{
		client:    client.NewStarcoinClient(rawUrl),
		url:       rawUrl,
		chainName: chainName,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
	}
}

func (c *Client) Detect() error {
	_, err := c.GetBlockHeight()
	return err
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) call(id int, method string, out interface{}, params []interface{}, args ...interface{}) error {
	var resp types.Response
	var header http.Header
	var err error
	if len(args) > 0 {
		header, err = httpclient.HttpsPost(c.url, id, method, JSONRPC, &resp, params, args[0].(int))
	} else {
		header, err = httpclient.HttpsPost(c.url, id, method, JSONRPC, &resp, params)
	}
	_ = header
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	return json.Unmarshal(resp.Result, &out)
}

func (c *Client) GetBalance(address string) (string, error) {
	return c.GetTokenBalance(address, GAS_TOKEN_CODE, 9)
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	method := "state.get_resource"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{address, "0x00000000000000000000000000000001::Account::Balance<" + tokenAddress + ">", d}
	balance := &types.Balance{}
	err := c.call(ID101, method, balance, params)
	if err != nil {
		return "", err
	}
	return utils.BigIntString(big.NewInt(balance.JSON.Token.Value), decimals), nil
}

func (c *Client) GetTransactionInfoByHash(transactionHash string) (*client.TransactionInfo, error) {
	return c.client.GetTransactionInfoByHash(context.Background(), transactionHash)
}

func (c *Client) GetTransactionEventByNumber(number int, typeTags []string) (map[string][]types.Event, error) {
	method := "chain.get_events"
	b := map[string]interface{}{
		"from_block": number,
		"to_block":   number,
		//"limit": number,
	}
	if len(typeTags) > 0 {
		b["type_tags"] = typeTags
	}
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{b, d}
	var result []types.Event
	err := c.call(ID101, method, &result, params)
	if err != nil {
		return nil, err
	}

	eventMap := make(map[string][]types.Event)
	for _, event := range result {
		eventList, ok := eventMap[event.TransactionHash]
		if !ok {
			eventList = make([]types.Event, 0)
		}
		eventList = append(eventList, event)
		eventMap[event.TransactionHash] = eventList
	}
	return eventMap, err
}

func (c *Client) GetTransactionEventByHash(transactionHash string) ([]types.Event, error) {
	method := "chain.get_events_by_txn_hash"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{transactionHash, d}
	var result []types.Event
	err := c.call(ID101, method, &result, params)
	return result, err
}

func (c *Client) GetBlockByNumber(number int) (*types.Block, error) {
	method := "chain.get_block_by_number"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{number, d}
	result := &types.Block{}
	err := c.call(ID101, method, result, params)
	return result, err
}

func (c *Client) GetBlockTxnInfos(blockHash string) (*[]types.BlockTxnInfos, error) {
	method := "chain.get_block_txn_infos"
	params := []interface{}{blockHash}
	result := &[]types.BlockTxnInfos{}
	err := c.call(101, method, result, params)
	return result, err
}

func (c *Client) GetTransactionByHash(transactionHash string) (*types.Transaction, error) {
	method := "chain.get_transaction"
	d := map[string]bool{
		"decode": true,
	}
	params := []interface{}{transactionHash, d}
	result := &types.Transaction{}
	err := c.call(ID101, method, result, params)
	return result, err
}

func (c *Client) GetBlockHeight() (uint64, error) {
	method := "node.info"
	result := &types.NodeInfo{}
	err := c.call(ID200, method, result, nil)
	if err != nil {
		return 0, err
	}
	height, _ := strconv.Atoi(result.PeerInfo.ChainInfo.Header.Height)
	return uint64(height), err
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block, err := c.GetBlockByNumber(int(height))
	if err != nil {
		return nil, err
	}

	var eventMap map[string][]types.Event
	for _, userTransaction := range block.BlockBody.UserTransactions {
		scriptFunction := userTransaction.RawTransaction.DecodedPayload.ScriptFunction
		if !strings.HasPrefix(scriptFunction.Function, "peer_to_peer") {
			eventMap, err = c.GetTransactionEventByNumber(int(height), []string{"0x00000000000000000000000000000001::Account::WithdrawEvent",
				"0x00000000000000000000000000000001::Account::DepositEvent"})
			if err != nil {
				return nil, err
			}
			break
		}
	}
	txs := make([]*chain.Transaction, 0, len(block.BlockBody.UserTransactions))
	for _, utx := range block.BlockBody.UserTransactions {
		events := eventMap[utx.TransactionHash]
		utx.Events = events
		txs = append(txs, &chain.Transaction{
			Hash:        utx.TransactionHash,
			BlockNumber: height,
			Raw:         utx,
			Record:      nil,
		})
	}
	blockHeight, _ := strconv.Atoi(block.BlockHeader.Height)
	if blockHeight == 0 {
		blockHeight = int(height)
	}
	ts, _ := strconv.Atoi(block.BlockHeader.TimeStamp)
	return &chain.Block{
		Hash:         block.BlockHeader.BlockHash,
		ParentHash:   block.BlockHeader.ParentHash,
		Number:       uint64(blockHeight),
		Nonce:        uint64(block.BlockHeader.Nonce),
		BaseFee:      block.BlockHeader.GasUsed,
		Time:         int64(ts),
		Raw:          block,
		Transactions: txs,
	}, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	transactionInfo, err := c.GetTransactionByHash(txHash)
	if err != nil {
		return nil, err
	}

	userTransaction := transactionInfo.UserTransaction
	scriptFunction := userTransaction.RawTransaction.DecodedPayload.ScriptFunction
	if !strings.HasPrefix(scriptFunction.Function, "peer_to_peer") {
		events, err := c.GetTransactionEventByHash(txHash)
		if err != nil {
			return nil, err
		}
		userTransaction.Events = events
	}
	blockNumber, _ := strconv.Atoi(transactionInfo.BlockNumber)
	return &chain.Transaction{
		Hash:        txHash,
		BlockNumber: uint64(blockNumber),
		TxType:      "",
		Raw:         userTransaction,
	}, nil
}
