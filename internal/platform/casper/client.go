package casper

import (
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Client struct {
	*common.NodeDefaultIn

	Url        string
	ChainName  string
	retryAfter time.Time
}

func NewClient(nodeUrl string, chainName string) *Client {
	return &Client{
		Url:       nodeUrl,
		ChainName: chainName,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
	}
}

func (c *Client) RetryAfter() time.Time {
	return c.retryAfter
}

func (c *Client) Detect() error {
	log.Info(c.ChainName+"链节点检测", zap.Any("nodeURL", c.Url))
	_, err := c.GetBlockHeight()
	return err
}

func (c *Client) URL() string {
	return c.Url
}

func (c *Client) rpcCall(method string, params interface{}) (types.RpcResponse, error) {
	body, err := json.Marshal(types.RpcRequest{
		Version: "2.0",
		Method:  method,
		Params:  params,
	})

	if err != nil {
		return types.RpcResponse{}, err
	}

	resp, err := http.Post(c.Url, "application/json", bytes.NewReader(body))
	if err != nil {
		return types.RpcResponse{}, fmt.Errorf("failed to make request: %w", err)
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return types.RpcResponse{}, fmt.Errorf("failed to get response body: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return types.RpcResponse{}, fmt.Errorf("request failed, status code - %d, response - %s", resp.StatusCode, string(b))
	}

	var rpcResponse types.RpcResponse
	err = json.Unmarshal(b, &rpcResponse)
	if err != nil {
		return types.RpcResponse{}, fmt.Errorf("failed to parse response body: %w", err)
	}

	if rpcResponse.Error != nil {
		return rpcResponse, fmt.Errorf("rpc call failed, code - %d, message - %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
	}

	return rpcResponse, nil
}

func (c *Client) GetAccountMainPurseURef(accountHash string) string {
	block, err := c.GetLatestBlock()
	if err != nil {
		return ""
	}

	item, err := c.GetStateItem(block.Header.StateRootHash, accountHash, []string{})
	if err != nil {
		return ""
	}

	return item.Account.MainPurse
}


func (c *Client) GetStateItem(stateRootHash, key string, path []string) (types.StoredValue, error) {
	params := map[string]interface{}{
		"state_root_hash": stateRootHash,
		"key":             key,
	}
	if len(path) > 0 {
		params["path"] = path
	}
	resp, err := c.rpcCall("state_get_item", params)
	if err != nil {
		return types.StoredValue{}, err
	}

	var result types.StoredValueResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return types.StoredValue{}, fmt.Errorf("failed to get result: %w", err)
	}

	return result.StoredValue, nil
}

func (c *Client) GetAccountInfoByAddress(address string)(string,error){
	resp, err := c.rpcCall("state_get_account_info", map[string]string{
		"public_key":      address,
	})
	if err != nil {
		return "", err
	}

	var result types.AccountInfoResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return "big.Int{}", fmt.Errorf("failed to get result: %w", err)
	}

	return result.Account.MainPurse, nil
}


func (c *Client) GetBalance(address string) (big.Int, error) {
	balance := big.Int{}
	block, err := c.GetLatestBlock()
	if err != nil {
		balance.SetString("0", 10)
		return balance,err
	}
	accountResult, err :=c.GetAccountInfoByAddress(address)
	if err != nil {
		balance.SetString("0", 10)
		return balance,err
	}
	return c.GetAccountBalance(block.Header.StateRootHash, accountResult)
}




func (c *Client) GetAccountBalance(stateRootHash, balanceUref string) (big.Int, error) {
	resp, err := c.rpcCall("state_get_balance", map[string]string{
		"state_root_hash": stateRootHash,
		"purse_uref":      balanceUref,
	})
	if err != nil {
		return big.Int{}, err
	}

	var result types.BalanceResponse
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return big.Int{}, fmt.Errorf("failed to get result: %w", err)
	}

	balance := big.Int{}
	balance.SetString(result.BalanceValue, 10)
	return balance, nil
}

func (c *Client) GetLatestBlock() (types.CasperBlockResponse, error) {
	resp, err := c.rpcCall("chain_get_block", nil)
	if err != nil {
		return types.CasperBlockResponse{}, err
	}

	var result types.BlockResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return types.CasperBlockResponse{}, fmt.Errorf("failed to get result: %w", err)
	}

	return result.Block, nil
}

func (c *Client) GetBlockByHeight(height uint64) (types.CasperBlockResponse, error) {
	resp, err := c.rpcCall("chain_get_block",
		types.BlockParams{
			BlockIdentifier: types.BlockIdentifier{
				Height: height,
			}})
	if err != nil {
		return types.CasperBlockResponse{}, err
	}

	var result types.BlockResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return types.CasperBlockResponse{}, fmt.Errorf("failed to get result: %w", err)
	}

	return result.Block, nil
}

func (c *Client) GetBlockByHash(hash string) (types.CasperBlockResponse, error) {
	resp, err := c.rpcCall("chain_get_block",
		types.BlockParams{BlockIdentifier: types.BlockIdentifier{
			Hash: hash,
		}})
	if err != nil {
		return types.CasperBlockResponse{}, err
	}

	var result types.BlockResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return types.CasperBlockResponse{}, fmt.Errorf("failed to get result: %w", err)
	}

	return result.Block, nil
}

func (c *Client) GetBlockHeight() (uint64, error) {
	result, err := c.GetLatestBlock()
	if err != nil {
		return 0, err
	}
	if result.Header.Height != 0 {
		return uint64(result.Header.Height), nil
	} else {
		return 0, errors.New("最新区块出块中。。。。")
	}
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	start := time.Now()
	block, err := c.GetBlockByHeight(height)
	if err != nil || &block == nil {
		log.Debug(
			"RETRIEVED BLOCK FROM CHAIN FAILED WITH ERROR",
			zap.String("chainName", c.ChainName),
			zap.Uint64("height", height),
			zap.String("nodeUrl", c.Url),
			zap.String("elapsed", time.Now().Sub(start).String()),
			zap.Error(err),
		)
		return nil, err
	}
	transactionList, err := c.GetBlockTransfersByHeight(height)
	if err != nil || &transactionList == nil {
		log.Debug(
			"RETRIEVED BLOCK FROM CHAIN FAILED WITH ERROR",
			zap.String("chainName", c.ChainName),
			zap.Uint64("height", height),
			zap.String("nodeUrl", c.Url),
			zap.String("elapsed", time.Now().Sub(start).String()),
			zap.Error(err),
		)
		return nil, err
	}

	var transactions []*chain.Transaction
	for _, transaction := range transactionList {
		//根据hash拿出 交易明细
		txInfo, err := c.GetTxByHash(transaction.DeployHash)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			txInfo, err = c.GetTxByHash(transaction.DeployHash)
		}
		if err != nil {
			log.Info(c.ChainName + "查询txhash失败",zap.Any(transaction.DeployHash,err))
			return nil, err
		}

		transactions = append(transactions, txInfo)
	}
	//tt := block.Header.Timestamp.String()
	//ft1,_ := time.Parse("2006-01-02T15:04:05.000Z",tt)

	tt := block.Header.Timestamp.Unix()

	chainBlock := &chain.Block{
		Hash:         block.Hash,
		ParentHash:   block.Header.ParentHash,
		Number:       height,
		Time:         tt,
		Transactions: transactions,
		//Raw:          block.BlockHeight,
	}
	return chainBlock, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {

	resp, err := c.rpcCall("info_get_deploy", map[string]string{
		"deploy_hash": txHash,
	})
	if err != nil {
		return nil, err
	}

	var result types.DeployResult
	err = json.Unmarshal(resp.Result, &result)

	if err != nil {
		return nil, fmt.Errorf("failed to get result: %w", err)
	}
	//未查出结果
	if len(result.ExecutionResults) != 1 {
		return nil, nil
	}

	bockHash := result.ExecutionResults[0].BlockHash
	block, err1 := c.GetBlockByHash(bockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block result: %w", err1)
	}
	tx := &chain.Transaction{}
	tx.BlockNumber = uint64(block.Header.Height)
	tx.Hash = txHash
	tx.Raw = result
	return tx, err
}

func (c *Client) GetBlockTransfersByHeight(height uint64) ([]types.TransferResponse, error) {
	resp, err := c.rpcCall("chain_get_block_transfers",
		types.BlockParams{BlockIdentifier: types.BlockIdentifier{
			Height: height,
		}})
	if err != nil {
		return nil, err
	}

	var result types.TransferResult
	err = json.Unmarshal(resp.Result, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to get result: %w", err)
	}

	return result.Transfers, nil
}
