package ton

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ton/tonclient"
	"block-crawling/internal/types/tontypes"
	"block-crawling/internal/utils"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Client struct {
	*common.NodeDefaultIn

	chainName string
	url       string
}

// GetBlockHeight implements chain.Clienter
func (c *Client) GetBlockHeight() (uint64, error) {
	var mi *tontypes.MasterchainInfo
	if err := c.get("/masterchainInfo", nil, &mi); err != nil {
		return 0, err
	}
	if err := unwrapErr(mi); err != nil {
		return 0, err
	}
	return uint64(mi.Last.Seqno), nil
}

// URL implements chain.Clienter
func (c *Client) URL() string {
	return c.url
}

// GetBlock implements chain.Clienter
func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block := &chain.Block{
		Number: height,
	}
	startedAt := time.Now()
	txs, err := c.getTransactionsByMasterchainSeqno(height)
	log.Info(
		"RETRIEVED TXS",
		zap.String("chainName", c.chainName), zap.String("nodeUrl", c.url), zap.Int("num", len(txs)),
		zap.String("elapsed", time.Since(startedAt).String()),
		zap.Uint64("height", height),
	)
	if err != nil {
		return nil, err
	}
	for _, tx := range txs {
		block.Transactions = append(block.Transactions, &chain.Transaction{
			BlockNumber: height,
			Raw:         tx,
		})
	}
	return block, nil
}

func (c *Client) getTransactionsByMasterchainSeqno(seqno uint64) (result []tontypes.TX, err error) {
	shards, err := c.getWorkchainShards(int(seqno))
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		if shard.Workchain != 0 {
			continue
		}
		txs, err := c.getTransactions(shard)
		if err != nil {
			return nil, err
		}
		result = append(result, txs...)
	}
	return result, nil
}

func (c *Client) getWorkchainShards(masterchainSeqno int) ([]tontypes.Shard, error) {
	var state *tontypes.ShardState
	params := map[string]string{
		"seqno": strconv.Itoa(masterchainSeqno),
	}
	if err := c.get("/masterchainBlockShardState", params, &state); err != nil {
		return nil, err
	}
	if err := unwrapErr(state); err != nil {
		return nil, err
	}
	return state.Blocks, nil
}

func (c *Client) getTransactions(shard tontypes.Shard) (result []tontypes.TX, err error) {
	var offset int
	var limit int = 128
	query := map[string]string{
		"workchain": strconv.Itoa(shard.Workchain),
		"shard":     shard.Shard,
		"seqno":     strconv.Itoa(shard.Seqno),
		"sort":      "desc",
		"limit":     strconv.Itoa(limit),
	}
	for {
		time.Sleep(time.Second * 2) // rate limit
		query["offset"] = strconv.Itoa(offset)
		var ret *tontypes.TXResult
		if err = c.get("/transactions", query, &ret); err != nil {
			return
		}
		if err := unwrapErr(ret); err != nil {
			return nil, err
		}
		for _, item := range ret.Txs {
			result = append(result, item)
		}
		if len(ret.Txs) < limit {
			return
		}
		offset += limit
	}
}

// GetTxByHash implements chain.Clienter
// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	// 会有获取不到的情况，重试5次
	var results *tontypes.TXResult
	params := map[string]string{
		"direction": "in",
		"msg_hash":  txHash,
		"limit":     "128",
		"offset":    "0",
	}
	if err := c.get("/transactionsByMessage", params, &results); err != nil {
		return nil, err
	}
	if err := unwrapErr(results); err != nil {
		return nil, err
	}
	txs := results.Txs
	if len(txs) > 0 {
		return &chain.Transaction{
			Hash:        txHash,
			Nonce:       0,
			BlockNumber: uint64(txs[0].MCBlockSeqno),
			Raw:         txs,
		}, nil
	}
	return nil, nil
}

func (c *Client) GetAdjacentTxs(txHash string) ([]tontypes.TX, error) {
	results := make([]tontypes.TX, 0, 2)
	txHashes := []string{txHash}
	for len(txHashes) > 0 {
		var nextTxHashes []string
		for _, txHash := range txHashes {
			txs, err := c.getOutTx(txHash)
			if err != nil {
				return nil, err
			}
			for _, tx := range txs {
				results = append(results, tx)
				nextTxHashes = append(nextTxHashes, tx.Hash)
			}
		}
		txHashes = nextTxHashes
	}
	return results, nil
}

func (c *Client) getOutTx(txHash string) (txs []tontypes.TX, err error) {
	var results *tontypes.TXResult
	params := map[string]string{
		"hash":      txHash,
		"direction": "out",
		"limit":     "128",
		"offset":    "0",
		"sort":      "desc",
	}
	err = c.get("/adjacentTransactions", params, &results)
	if err == nil {
		err = unwrapErr(results)
	}
	for i := 0; i <= 6 && err != nil; i++ {
		time.Sleep(time.Second * time.Duration(1<<i))
		err = c.get("/adjacentTransactions", params, &results)
		if err == nil {
			err = unwrapErr(results)
		}
	}
	return results.Txs, err
}

func (c *Client) GetBalance(address string) (string, error) {
	var result *tontypes.Account

	if err := c.get("/account", map[string]string{"address": address}, &result); err != nil {
		return "0", err
	}
	if result != nil {
		if err := unwrapErr(result); err != nil {
			return "0", err
		}
	}
	return utils.StringDecimals(result.Balance, TonDecimals), nil
}

func (c *Client) GetTokenBalance(address string, tokenAddress string, decimals int) (string, error) {
	var result *tontypes.JettonWalletResult
	query := map[string]string{
		"jetton_address": tokenAddress,
		"owner_address":  address,
	}
	if err := c.get("/jetton/wallets", query, &result); err != nil {
		return "0", err
	}
	if err := unwrapErr(result); err != nil {
		return "0", err
	}
	if len(result.JettonWallets) == 0 {
		return "0", nil
	}
	return utils.StringDecimals(result.JettonWallets[0].Balance, decimals), nil
}

func (c *Client) GetNFTNumByTokenId(address string, tokenAddress string, index string) (string, error) {
	var result *tontypes.NFTResult
	query := map[string]string{
		"address":       tokenAddress,
		"owner_address": address,
		"limit":         "128",
		"offset":        "0",
	}
	if err := c.get("/nft/items", query, &result); err != nil {
		return "0", err
	}
	if err := unwrapErr(result); err != nil {
		return "0", err
	}
	var nNFTs int
	for _, item := range result.NFTs {
		if item.Index == index {
			nNFTs++
		}
	}
	return strconv.Itoa(nNFTs), nil
}

func (c *Client) GetNFTTransfers(tokenAddress string) ([]tontypes.NFTTransfer, error) {
	var result *tontypes.NFTTransferResult
	query := map[string]string{
		"item_address": tokenAddress,
		"direction":    "both",
		"limit":        "128",
		"offset":       "0",
		"sort":         "desc",
	}
	if err := c.get("/nft/transfers", query, &result); err != nil {
		return nil, err
	}
	if err := unwrapErr(result); err != nil {
		return nil, err
	}
	return result.Transfers, nil
}

func (c *Client) GetEventFromTonAPI(tx *tontypes.TX) (*tonclient.TonAPIEvent, error) {
	bytsTxHash, err := base64.StdEncoding.DecodeString(tx.InMsg.Hash)
	if err != nil {
		return nil, err
	}
	hexTxHash := hex.EncodeToString(bytsTxHash)

	var out *tonclient.TonAPIEvent
	reqURL := fmt.Sprintf("https://tonapi.io/v2/events/%s", hexTxHash)

	if err := httpclient.GetResponse(reqURL, nil, &out, nil); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) getPrevTx(tx *tontypes.TX) (*tontypes.TX, error) {
	var result *tontypes.TXResult
	params := map[string]string{
		"hash":   tx.PrevTransHash,
		"limit":  "128",
		"offset": "0",
		"sort":   "desc",
	}
	if err := c.get("/transactions", params, &result); err != nil {
		return nil, err
	}
	if err := unwrapErr(result); err != nil {
		return nil, err
	}
	for _, prev := range result.Txs {
		if prev.Hash == tx.PrevTransHash {
			return &prev, nil
		}
	}
	return nil, errors.New("not found")
}

func (c *Client) GetTokenTransfers(tx *tontypes.TX) ([]tontypes.JettonTransfer, error) {
	var result *tontypes.JettonTransferResult
	params := map[string]string{
		"address":   tx.Account,
		"start_lt":  tx.LT, // >=
		"end_lt":    tx.LT, // <=
		"direction": "both",
		"limit":     "128",
		"offset":    "0",
		"sort":      "desc",
	}
	if err := c.get("/jetton/transfers", params, &result); err != nil {
		return nil, err
	}
	if err := unwrapErr(result); err != nil {
		return nil, err
	}
	var results []tontypes.JettonTransfer
	for _, item := range result.Transfers {
		if item.TransactionHash == tx.Hash {
			results = append(results, item)
		}
	}
	return results, nil
}

func (c *Client) get(path string, query map[string]string, out interface{}) error {
	timeout := time.Second * 60
	reqURL := fmt.Sprint(c.url, path)
	if err := httpclient.GetResponse(reqURL, query, out, &timeout); err != nil {
		q := make(url.Values)
		for k, v := range query {
			q.Add(k, v)
		}
		return fmt.Errorf("[requestToncenter %s?%s] %w", reqURL, q.Encode(), err)
	}
	return nil
}

func NewClient(url, chainName string) *Client {
	return &Client{
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName:       chainName,
			RoundRobinProxy: false,
		},
		chainName: chainName,
		url:       url,
	}
}

func unwrapErr(resp tontypes.Response) error {
	if resp == nil {
		return nil
	}
	return resp.UnwrapErr()
}
