package tonclient

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types/tontypes"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type toncenter struct {
	chainName string
	url       string
}

// GetBlockHeight implements chain.Clienter
func (c *toncenter) GetMasterchainSeqno() (uint64, error) {
	var mi *tontypes.MasterchainInfo
	if err := c.get("/masterchainInfo", nil, &mi); err != nil {
		return 0, err
	}
	return uint64(mi.Last.Seqno), nil
}

func (c *toncenter) GetTxsByMasterchainSeqno(seqno uint64) (result []tontypes.TX, err error) {
	var offset int
	var limit int = 128
	query := map[string]string{
		"seqno": fmt.Sprint(seqno),
	}
	for {
		query["offset"] = strconv.Itoa(offset)
		query["limit"] = strconv.Itoa(limit)
		query["sort"] = "desc"
		var ret *tontypes.TXResult
		if err = c.get("/transactionsByMasterchainBlock", query, &ret); err != nil {
			return
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

func (c *toncenter) GetTxsByMessageHash(txHash string) ([]tontypes.TX, error) {
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
	return results.Txs, nil
}

func (c *toncenter) GetAccount(address string) (*tontypes.Account, error) {
	var result *tontypes.Account

	if err := c.get("/account", map[string]string{"address": address}, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *toncenter) GetJettonWallet(address string, tokenAddress string) (*tontypes.JettonWallet, error) {
	var result *tontypes.JettonWalletResult
	query := map[string]string{
		"address":       tokenAddress,
		"owner_address": address,
	}
	if err := c.get("/jetton/wallets", query, &result); err != nil {
		return nil, err
	}
	if len(result.JettonWallets) == 0 {
		return nil, nil
	}
	return &result.JettonWallets[0], nil
}

func (c *toncenter) GetJettonTransfers(tx *tontypes.TX) ([]tontypes.JettonTransfer, error) {
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

	var results []tontypes.JettonTransfer
	for _, item := range result.Transfers {
		if item.TransactionHash == tx.Hash {
			results = append(results, item)
		}
	}
	return results, nil
}

func (c *toncenter) get(path string, query map[string]string, out interface{}) error {
	return get(c.url, path, query, out)
}

func get(apiURL, path string, query map[string]string, out interface{}) error {
	timeout := time.Second * 60
	reqURL := fmt.Sprint(apiURL, path)
	if err := httpclient.GetResponse(reqURL, query, out, &timeout); err != nil {
		q := make(url.Values)
		for k, v := range query {
			q.Add(k, v)
		}
		return fmt.Errorf("[requestToncenter %s?%s] %w", reqURL, q.Encode(), err)
	}
	return nil

}
