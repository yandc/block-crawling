package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"github.com/blockcypher/gobcy"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	URL       string
	StreamURL string
}

var urlMap = map[string]string{
	"https://api.blockcypher.com/v1/btc/main":                                  "https://blockstream.info/api",
	"https://api.blockcypher.com/v1/btc/test3":                                 "https://blockstream.info/testnet/api",
	"http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332": "http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332",
}

const TO_TYPE = "utxo_output"
const FROM_TYPE = "utxo_input"
const FEE_TYPE = "fee"

func NewClient(nodeUrl string) Client {
	streamURL := "https://blockstream.info/api"
	if value, ok := urlMap[nodeUrl]; ok {
		streamURL = value
	}
	return Client{nodeUrl, streamURL}
}

func GetUnspentUtxo(nodeUrl string, address string) ([]types.UbiquityOutput, error) {
	var utxos []types.UbiquityOutput
	key, baseURL := parseKeyFromNodeURL(nodeUrl)
	url := baseURL + "account/" + address + "/utxo"
	param := map[string]string{"spent": "false", "page_size": "100"}
	timeoutMS := 10_000 * time.Millisecond
	var result types.UbiquityUtxo
	err := httpclient.HttpsSignGetForm(url, param, map[string]string{"Authorization": key}, &result, &timeoutMS)
	if err != nil {
		return nil, err
	}

	utxos = result.Data
	for result.Meta != nil {
		param["page_token"] = result.Meta.Paging.NextPageToken
		err = httpclient.HttpsSignGetForm(url, param, map[string]string{"Authorization": key}, &result, &timeoutMS)
		if err != nil {
			return nil, err
		}
		utxos = append(utxos, result.Data...)
	}

	return utxos, nil
}
func GetUnspentUtxoByBlockcypher(chainName string, address string) (types.BlockcypherUtxo, error) {
	url := "https://api.blockcypher.com/v1/" + strings.ToLower(chainName) + "/main/addrs/" + address + "?unspentOnly=true"
	var unspents types.BlockcypherUtxo
	timeout := 10_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &unspents, &timeout)
	return unspents, err
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

func GetBalance(address string, c *base.Client) (string, error) {
	u, err := c.BuildURLBTC("/addrs/"+address+"/balance", "https://api.blockcypher.com/v1/btc/main", nil)
	if err != nil {
		return "", err
	}
	var addr gobcy.Addr
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &addr, &timeoutMS)
	if err != nil {
		return "", err
	}
	btcValue := utils.BigIntString(&addr.Balance, 8)
	return btcValue, nil
}

func GetBlockNumber(c *base.Client) (int, error) {
	u, err := c.BuildURL("", nil)
	if err != nil {
		return 0, err
	}
	var chain gobcy.Blockchain
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &chain, &timeoutMS)
	return chain.Height, err
}

func GetBlockHeight(c *base.Client) (int, error) {
	url := c.StreamURL + "/blocks/tip/height"
	var height int
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &height, &timeoutMS)
	return height, err
}

func GetMempoolTxIds(c *base.Client) ([]string, error) {
	url := c.StreamURL + "/mempool/txids"
	var txIds []string
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &txIds, &timeoutMS)
	return txIds, err
}

func GetBlockHashByNumber(number int, c *base.Client) (string, error) {
	url := c.StreamURL + "/block-height/" + fmt.Sprintf("%d", number)
	return httpclient.HttpsGetFormString(url, nil)
}

func GetTestBlockByHeight(height int, c *base.Client) (result types.BTCTestBlockerInfo, err error) {
	//get block hash
	hash, err := GetBlockHashByNumber(height, c)
	if err != nil {
		return result, err
	}
	starIndex := 0
	pageSize := 25
	timeoutMS := 10_000 * time.Millisecond
	for {
		var block types.BTCTestBlockerInfo
		url := c.StreamURL + "/block/" + hash + "/txs/" + fmt.Sprintf("%d", starIndex*pageSize)
		err = httpclient.HttpsGetForm(url, nil, &block, &timeoutMS)
		starIndex++
		result = append(result, block...)
		if len(block) < pageSize {
			break
		}
	}
	return
}

func GetTransactionByHash(hash string, c *base.Client) (tx types.TX, err error) {
	if c.URL == "https://api.blockcypher.com/v1/btc/main" {
		tx, err = DoGetTransactionByHash(hash+"?instart=0&outstart=0&limit=500", c)
		if err != nil {
			return
		}
		putsTx := tx
		for (putsTx.NextInputs != "" && len(putsTx.Inputs) > 80) || (putsTx.NextOutputs != "" && len(putsTx.Outputs) > 80) {
			putsTx, err = DoGetTransactionByHash(hash+"?instart="+strconv.Itoa(len(putsTx.Inputs))+"&outstart="+strconv.Itoa(len(putsTx.Outputs))+"&limit=500", c)
			if err != nil {
				return
			}
			for _, input := range putsTx.Inputs {
				tx.Inputs = append(tx.Inputs, input)
			}
			for _, output := range putsTx.Outputs {
				tx.Outputs = append(tx.Outputs, output)
			}
		}
		return
	}
	if strings.Contains(c.URL, "ubiquity.api.blockdaemon.com") {
		utxoTxByDD, e := GetTransactionsByTXHash(hash, c)
		if e != nil {
			err = e
			return
		} else {
			if utxoTxByDD.Detail == "The requested resource has not been found" {
				return tx, errors.New(utxoTxByDD.Detail)
			} else {
				var inputs []gobcy.TXInput
				var inputAddress []string
				var outs []gobcy.TXOutput
				var outputAddress []string

				var feeAmount int64

				for _, event := range utxoTxByDD.Events {
					if event.Type == FROM_TYPE {
						input := gobcy.TXInput{
							OutputValue: int(event.Amount),
							Addresses:   append(inputAddress, event.Source),
						}
						inputs = append(inputs, input)
					}
					if event.Type == TO_TYPE {
						out := gobcy.TXOutput{
							Value:     *big.NewInt(event.Amount),
							Addresses: append(outputAddress, event.Destination),
						}
						outs = append(outs, out)
					}
					if event.Type == FEE_TYPE {
						feeAmount = event.Amount
					}
				}

				txTime := time.Unix(int64(utxoTxByDD.Date), 0)
				tx = types.TX{
					BlockHash:   utxoTxByDD.BlockId,
					BlockHeight: utxoTxByDD.BlockNumber,
					Hash:        utxoTxByDD.Id,
					Fees:        *big.NewInt(feeAmount),
					Confirmed:   txTime,
					Inputs:      inputs,
					Outputs:     outs,
					Error:       "",
				}
				return tx, nil
			}
		}
	}
	return
}

func DoGetTransactionByHash(hash string, c *base.Client) (tx types.TX, err error) {
	u, err := c.BuildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &tx, &timeoutMS)
	return
}

func GetTransactionsByTXHash(tx string, c *base.Client) (types.TxInfo, error) {
	key, baseURL := parseKeyFromNodeURL(c.URL)
	url := baseURL + "tx/" + tx
	var txInfo types.TxInfo
	timeoutMS := 10_000 * time.Millisecond
	err := httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": key}, &txInfo, &timeoutMS)
	return txInfo, err
}

func GetTransactionByPendingHash(hash string, c *base.Client) (tx types.TXByHash, err error) {
	u, err := c.BuildURL("/txs/"+hash, nil)
	if err != nil {
		return
	}
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &tx, &timeoutMS)
	return
}

func GetTransactionByPendingHashByNode(json model.JsonRpcRequest, c *base.Client) (tx model.BTCTX, err error) {
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.PostResponse(c.StreamURL, json, &tx, &timeoutMS)
	return
}

// MemoryPoolTX
func GetMemoryPoolTXByNode(json model.JsonRpcRequest, c *base.Client) (txIds model.MemoryPoolTX, err error) {
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.PostResponse(c.StreamURL, json, &txIds, &timeoutMS)
	return
}

func GetBlockCount(json model.JsonRpcRequest, c *base.Client) (count model.BTCCount, err error) {
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.PostResponse(c.StreamURL, json, &count, &timeoutMS)
	return
}

func GetBTCBlockByNumber(number int, c *base.Client) (types.BTCBlockerInfo, error) {
	url := "https://blockchain.info/rawblock/" + fmt.Sprintf("%d", number)
	var block types.BTCBlockerInfo
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &block, &timeoutMS)
	return block, err
}

// constructs BlockCypher URLs with parameters for requests
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
