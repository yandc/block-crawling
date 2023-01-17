package casper

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"errors"
	"fmt"
	"math/big"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

const (
	JSONRPC = "2.0"
	JSONID  = 1
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
	method := "state_get_item"
	params := map[string]interface{}{
		"state_root_hash": stateRootHash,
		"key":             key,
	}
	if len(path) > 0 {
		params["path"] = path
	}
	var result types.StoredValueResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return types.StoredValue{}, err
	}
	return result.StoredValue, nil
}

func (c *Client) GetAccountInfoByAddress(address string) (string, error) {
	method := "state_get_account_info"
	params := map[string]string{
		"public_key": address,
	}
	var result types.AccountInfoResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return "", err
	}
	return result.Account.MainPurse, nil
}

func (c *Client) GetBalance(address string) (big.Int, error) {
	balance := big.Int{}
	block, err := c.GetLatestBlock()
	if err != nil {
		balance.SetString("0", 10)
		return balance, err
	}
	accountResult, err := c.GetAccountInfoByAddress(address)
	if err != nil {
		balance.SetString("0", 10)
		return balance, err
	}
	return c.GetAccountBalance(block.Header.StateRootHash, accountResult)
}

func (c *Client) GetAccountBalance(stateRootHash, balanceUref string) (big.Int, error) {
	method := "state_get_balance"
	params := map[string]string{
		"state_root_hash": stateRootHash,
		"purse_uref":      balanceUref,
	}
	var result types.BalanceResponse
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return big.Int{}, err
	}
	balance := big.Int{}
	balance.SetString(result.BalanceValue, 10)
	return balance, nil
}

func (c *Client) GetLatestBlock() (types.CasperBlockResponse, error) {
	method := "chain_get_block"
	var result types.BlockResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, nil)
	if err != nil {
		return types.CasperBlockResponse{}, err
	}
	return result.Block, nil
}

func (c *Client) GetBlockByHeight(height uint64) (types.CasperBlockResponse, error) {
	method := "chain_get_block"
	params := types.BlockParams{
		BlockIdentifier: types.BlockIdentifier{
			Height: height,
		}}
	var result types.BlockResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return types.CasperBlockResponse{}, err
	}
	return result.Block, nil
}

func (c *Client) GetBlockByHash(hash string) (types.CasperBlockResponse, error) {
	method := "chain_get_block"
	params := types.BlockParams{BlockIdentifier: types.BlockIdentifier{
		Hash: hash,
	}}
	var result types.BlockResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return types.CasperBlockResponse{}, err
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
			log.Info(c.ChainName+"查询txhash失败", zap.Any(transaction.DeployHash, err))
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
	method := "info_get_deploy"
	params := map[string]string{
		"deploy_hash": txHash,
	}
	var result types.DeployResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return nil, err
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
	method := "chain_get_block_transfers"
	params := types.BlockParams{BlockIdentifier: types.BlockIdentifier{
		Height: height,
	}}
	var result types.TransferResult
	_, err := httpclient.JsonrpcCall(c.Url, JSONID, JSONRPC, method, &result, params)
	if err != nil {
		return nil, err
	}
	return result.Transfers, nil
}
