package polkadot

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/common"
	types2 "block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	API_URL  = "https://Bearer:2c8c9471365c28eb0eef2aadefacc9da@api.polkaholic.io/"
	PROKADOT = "block/0"
	RPC      = "https://rpc.polkadot.io"
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

// GetBlock fetch block data of the given height.
func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	key, baseURL := parseKeyFromNodeURL(c.Url)
	url := baseURL + PROKADOT + "/" + strconv.FormatUint(height, 10)

	var polkBlock types2.PolkadotBlockInfo
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &polkBlock, &timeoutMS)
	if err != nil && strings.Contains(err.Error(), "Not Found") {
		return &chain.Block{
			Number: height,
		}, nil
	}
	if err != nil {
		return nil, err
	}

	if strings.Contains(polkBlock.Error, "Block not found") {
		return &chain.Block{
			Number: height,
		}, nil
	}

	if polkBlock.Error != "" {
		return nil, errors.New(polkBlock.Error)
	}

	var transactions []*chain.Transaction
	for _, transaction := range polkBlock.Extrinsics {
		if transaction.Transfers == nil || len(transaction.Transfers) == 0 {
			continue
		}
		for _, transfer := range transaction.Transfers {
			txInfo := &chain.Transaction{
				Hash:        transaction.ExtrinsicHash,
				Nonce:       uint64(transaction.Nonce),
				BlockNumber: uint64(transaction.BlockNumber),
				FromAddress: transfer.From,
				ToAddress:   transfer.To,
				Value:       strconv.Itoa(int(transfer.RawAmount)),
				Raw:         transaction,
			}
			if transfer.Symbol == "DOT" {
				txInfo.TxType = biz.NATIVE
			} else {
				txInfo.TxType = biz.TRANSFER
			}
			transactions = append(transactions, txInfo)
		}

	}
	return &chain.Block{
		Hash:         polkBlock.Hash,
		ParentHash:   polkBlock.Header.ParentHash,
		Number:       uint64(polkBlock.Header.Number),
		Time:         int64(polkBlock.BlockTS),
		Transactions: transactions,
	}, nil
}

// 块上链时，交易可能未完成。安全区块高度为 6 块，所以这样处理
func (c *Client) GetBlockHeight() (uint64, error) {
	param := model.JsonRpcRequest{
		Jsonrpc: "2.0",
		Id:      "curltest",
		Method:  "chain_getBlock",
	}
	var pa = make([]interface{}, 0, 0)
	param.Params = pa
	var nbi types2.NodeBlockInfo
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.PostResponse(RPC, param, &nbi, &timeoutMS)
	if err != nil {
		return 0, err
	}
	number := nbi.Result.Block.Header.Number
	x, _ := utils.HexStringToInt(number)
	h := x.Uint64()
	if h > 6 {
		h = h - 6
	}
	return h, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	key, baseURL := parseKeyFromNodeURL(c.Url)
	url := baseURL + "tx/" + txHash

	var txInfo types2.PolkadotTxInfo
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &txInfo, &timeoutMS)
	if err != nil {
		return nil, err
	}
	//if txInfo.Error != "" {
	//	return nil, errors.New(txInfo.Error)
	//}

	tx := &chain.Transaction{}
	tx.BlockNumber = uint64(txInfo.BlockNumber)
	tx.Hash = txHash
	tx.Raw = txInfo
	return tx, err
}

func (c *Client) GetBalance(address string) ([]types2.PolkadotAccountInfo, error) {
	key, baseURL := parseKeyFromNodeURL(c.Url)
	url := baseURL + "account/" + address
	var accountInfo []types2.PolkadotAccountInfo
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &accountInfo, &timeoutMS)
	if err != nil {
		return accountInfo, err
	}
	if len(accountInfo) == 1 && accountInfo[0].Error != "" {
		return accountInfo, errors.New(accountInfo[0].Error)
	}

	return accountInfo, nil

}
