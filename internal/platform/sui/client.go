package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/sui/stypes"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	JSONRPC = "2.0"
	JSONID  = 1
)

type Client struct {
	*common.NodeDefaultIn

	url       string
	chainName string
}

func NewClient(nodeUrl, chainName string) Client {
	return Client{
		url:       nodeUrl,
		chainName: chainName,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
	}
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) Detect() error {
	_, err := c.GetBlockHeight()
	return err
}

// GetBlockHeight get current block height.
func (c *Client) GetBlockHeight() (uint64, error) {
	height, err := c.GetBlockNumber()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

// GetBlock fetch block data of the given height.
func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	var chainBlock *chain.Block
	block, err := c.GetBlockByNumber(height)
	if err != nil {
		return chainBlock, err
	}
	transactions, err := c.GetTransactionByHashs(block.Transactions)
	if err != nil {
		return chainBlock, err
	}

	var chainTransactions []*chain.Transaction
	for _, rawTx := range transactions {
		chainTransactions = append(chainTransactions, &chain.Transaction{
			Hash:        rawTx.Digest,
			BlockNumber: height,
			TxType:      "",
			FromAddress: rawTx.Transaction.Data.Sender,
			ToAddress:   "",
			Value:       "",
			Raw:         rawTx,
			Record:      nil,
		})
	}
	blkTime, _ := strconv.ParseInt(block.TimestampMs, 10, 64)

	chainBlock = &chain.Block{
		Hash:         block.Digest,
		ParentHash:   block.PreviousDigest,
		Number:       height,
		Raw:          block,
		Time:         blkTime / 1000,
		Transactions: chainTransactions,
	}
	return chainBlock, nil
}

// GetTxByHash get transaction by given tx hash.
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	transaction, err := c.GetTransactionByHash(txHash)
	if err != nil {
		if erro, ok := err.(*types.ErrorObject); ok && strings.HasPrefix(erro.Message, "Could not find the referenced transaction") {
			return nil, common.TransactionNotFound
		}
		if strings.Contains(err.Error(), "Error checking transaction input objects: ObjectNotFound") {
			return nil, common.TransactionNotFound
		}
		log.Error("get transaction by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	}
	/*var transaction *TransactionInfo
	transactions, err := c.GetTransactionByHashs([]string{txHash})
	if err != nil {
		if erro, ok := err.(*types.ErrorObject); ok && (strings.HasPrefix(erro.Message, "Could not find the referenced transaction") {
			return nil, common.TransactionNotFound
		}
		log.Error("get transaction by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	} else {
		if len(transactions) > 0 {
			transaction = transactions[0]
		} else {
			return nil, common.TransactionNotFound
		}
	}*/
	return &chain.Transaction{
		Hash:   txHash,
		Raw:    transaction,
		Record: nil,
	}, nil
}

func (c *Client) GetBalance(address string) (string, error) {
	return c.GetTokenBalance(address, SUI_CODE, 9)
}

type TokenBalance struct {
	CoinType        string      `json:"coinType"`
	CoinObjectCount int         `json:"coinObjectCount"`
	TotalBalance    string      `json:"totalBalance"`
	LockedBalance   interface{} `json:"lockedBalance"`
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	method := "suix_getBalance"
	params := []interface{}{address, tokenAddress}
	var out TokenBalance
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	if err != nil {
		return "0", err
	}
	balance := out.TotalBalance
	balances := utils.StringDecimals(balance, decimals)
	return balances, err
}

type GetObject struct {
	Data struct {
		ObjectId string `json:"objectId"`
		Version  string `json:"version"`
		Digest   string `json:"digest"`
		Type     string `json:"type"`
		Owner    struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"owner"`
	} `json:"data"`
}

func (c *Client) Erc721BalanceByTokenId(address string, tokenAddress string, tokenId string) (string, error) {
	method := "sui_getObject"
	params := []interface{}{tokenId, map[string]bool{
		"showType":                true,
		"showOwner":               true,
		"showPreviousTransaction": false,
		"showDisplay":             false,
		"showContent":             false,
		"showBcs":                 false,
		"showStorageRebate":       false,
	}}
	var out GetObject
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	if err != nil {
		return "0", err
	}
	if out.Data.Owner.AddressOwner != address {
		return "0", nil
	}
	return "1", nil
}

func (c *Client) GetBlockNumber() (int, error) {
	method := "sui_getLatestCheckpointSequenceNumber"
	var out string
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, nil, &timeoutMS)
	if err != nil {
		return 0, err
	}
	blockNumber, err := strconv.Atoi(out)
	return blockNumber, err
}

type BlockerInfo struct {
	Epoch                      string `json:"epoch"`
	SequenceNumber             string `json:"sequenceNumber"`
	Digest                     string `json:"digest"`
	NetworkTotalTransactions   string `json:"networkTotalTransactions"`
	PreviousDigest             string `json:"previousDigest"`
	EpochRollingGasCostSummary struct {
		ComputationCost         string `json:"computationCost"`
		StorageCost             string `json:"storageCost"`
		StorageRebate           string `json:"storageRebate"`
		NonRefundableStorageFee string `json:"nonRefundableStorageFee"`
	} `json:"epochRollingGasCostSummary"`
	TimestampMs           string        `json:"timestampMs"`
	Transactions          []string      `json:"transactions"`
	CheckpointCommitments []interface{} `json:"checkpointCommitments"`
	ValidatorSignature    string        `json:"validatorSignature"`
}

func (c *Client) GetBlockByNumber(number uint64) (BlockerInfo, error) {
	method := "sui_getCheckpoint"
	var out BlockerInfo
	params := []interface{}{strconv.Itoa(int(number))}
	timeoutMS := 5_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	return out, err
}

func (c *Client) GetTransactionByHash(hash string) (*stypes.TransactionInfo, error) {
	method := "sui_getTransactionBlock"
	var out *stypes.TransactionInfo
	params := []interface{}{hash, map[string]bool{
		"showInput":          true,
		"showRawInput":       false,
		"showEffects":        true,
		"showEvents":         true,
		"showObjectChanges":  true,
		"showBalanceChanges": true,
	}}
	timeoutMS := 10_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	if err != nil {
		return nil, err
	}
	if out.Errors != nil {
		return nil, errors.New(utils.GetString(out.Errors))
	}
	return out, nil
}

func (c *Client) GetTransactionByHashs(hashs []string) ([]*stypes.TransactionInfo, error) {
	method := "sui_multiGetTransactionBlocks"
	//multi get transaction input limit is 50
	pageSize := 50
	hashSize := len(hashs)
	start := 0
	stop := pageSize
	if stop > hashSize {
		stop = hashSize
	}
	var result []*stypes.TransactionInfo
	for {
		hs := hashs[start:stop]
		var out []*stypes.TransactionInfo
		params := []interface{}{hs, map[string]bool{
			"showInput":          true,
			"showRawInput":       false,
			"showEffects":        true,
			"showEvents":         true,
			"showObjectChanges":  true,
			"showBalanceChanges": true,
		}}
		timeoutMS := 10_000 * time.Millisecond
		_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
		if err != nil {
			return nil, err
		}
		result = append(result, out...)
		if stop >= hashSize {
			break
		}
		start = stop
		stop += pageSize
		if stop > hashSize {
			stop = hashSize
		}
	}
	return result, nil
}

type TokenParamReq struct {
	Filter  Filter  `json:"filter"`
	Options Options `json:"options"`
}

type Filter struct {
	ChangedObject string `json:"ChangedObject"`
}
type Options struct {
	ShowEffects        bool `json:"showEffects"`
	ShowBalanceChanges bool `json:"showBalanceChanges"`
	ShowObjectChanges  bool `json:"showObjectChanges"`
	ShowInput          bool `json:"showInput"`
}

func (c *Client) GetEventTransfer(tokenId string) (tar stypes.SuiObjectChanges, err error) {
	url := "https://explorer-rpc.testnet.sui.io/"
	if biz.IsTestNet(c.ChainName) {
		url = "https://explorer-rpc.testnet.sui.io/"
	}

	filter := Filter{
		ChangedObject: tokenId,
	}
	op := Options{
		ShowEffects:        true,
		ShowBalanceChanges: true,
		ShowObjectChanges:  true,
		ShowInput:          true,
	}

	tokenParamReq := TokenParamReq{
		Filter:  filter,
		Options: op,
	}

	params := []interface{}{tokenParamReq, nil, 100, true}

	tokenRequest := SuiTokenNftRecordReq{
		Method:  "suix_queryTransactionBlocks",
		Jsonrpc: "2.0",
		Params:  params,
		Id:      "1",
	}
	timeout := 10_000 * time.Millisecond
	err = httpclient.HttpPostJson(url, tokenRequest, &tar, &timeout)
	return
}

type SuiTokenNftRecordReq struct {
	Method  string        `json:"method"`
	Jsonrpc string        `json:"jsonrpc"`
	Params  []interface{} `json:"params"`
	Id      string        `json:"id"`
}
