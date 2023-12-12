package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/blockcypher/gobcy"
)

type Client struct {
	URL       string
	StreamURL string

	node   Noder
	stream Streamer
}

const defaultStreamURL = "https://blockstream.info/api"

var urlMap = map[string]*nodeConf{
	"https://api.blockcypher.com/v1/btc/main": {
		streamURL: defaultStreamURL,
		nodeFactory: func(nodeURL string) Noder {
			return newBlockcypherNode(nodeURL)
		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	"https://api.blockcypher.com/v1/btc/test3": {
		streamURL: "https://blockstream.info/testnet/api",
		nodeFactory: func(nodeURL string) Noder {
			return newBlockcypherNode(nodeURL)

		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	"https://svc.blockdaemon.com/universal/v1/bitcoin/mainnet/": {
		streamURL: "https://blockstream.info/api",
		nodeFactory: func(nodeURL string) Noder {
			return newBlockdaemonNode(nodeURL)
		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	"https://blockstream.info": {
		streamURL: "https://blockstream.info/api",
		nodeFactory: func(nodeURL string) Noder {
			return newBlockstreamNode(nodeURL)
		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	"https://btcscan.org": {
		streamURL: "https://btcscan.org/api",
		nodeFactory: func(nodeURL string) Noder {
			return newBlockstreamNode(nodeURL)
		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	"https://mempool.space": {
		streamURL: "https://mempool.space/api",
		nodeFactory: func(nodeURL string) Noder {
			return newBlockstreamNode(nodeURL)
		},
		streamFactory: func(streamURL string) Streamer {
			return newBTCStream(streamURL)
		},
	},
	// "http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332": "http://haotech:phzxiTvtjqHikHTBTnTthqsUHTY2g3@chain01.openblock.top:8332",
}

type nodeConf struct {
	streamURL     string
	nodeFactory   func(string) Noder
	streamFactory func(string) Streamer
}

const TO_TYPE = "utxo_output"
const FROM_TYPE = "utxo_input"
const FEE_TYPE = "fee"

func NewClient(nodeUrl string) Client {
	_, baseURL := parseKeyFromNodeURL(nodeUrl)
	if value, ok := urlMap[baseURL]; ok {
		return Client{
			URL:       nodeUrl,
			StreamURL: value.streamURL,
			node:      value.nodeFactory(nodeUrl),
			stream:    value.streamFactory(value.streamURL),
		}
	}
	return Client{
		URL:       nodeUrl,
		StreamURL: defaultStreamURL,
	}
}

func GetUnspentUtxo(nodeUrl string, address string) ([]types.UbiquityOutput, error) {
	var utxos []types.UbiquityOutput
	key, baseURL := parseKeyFromNodeURL(nodeUrl)
	url := baseURL + "account/" + address + "/utxo"
	param := map[string]string{"spent": "false", "page_size": "100"}
	timeoutMS := 10_000 * time.Millisecond
	var result types.UbiquityUtxo
	err := httpclient.HttpsSignGetForm(url, param, map[string]string{"X-API-Key": key}, &result, &timeoutMS)
	if err != nil {
		return nil, err
	}

	utxos = result.Data
	for result.Meta != nil {
		param["page_token"] = result.Meta.Paging.NextPageToken
		result = types.UbiquityUtxo{}
		err = httpclient.HttpsSignGetForm(url, param, map[string]string{"X-API-Key": key}, &result, &timeoutMS)
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

func GetBalance(address string, c *Client) (string, error) {
	return c.node.GetBalance(address)
}

type blockChain struct {
	gobcy.Blockchain

	Error string `json:"error"`
}

func GetBlockNumber(c *Client) (int, error) {
	return c.node.GetBlockNumber()
}

func GetBlockHeight(c *Client) (int, error) {
	return c.stream.GetBlockHeight()
}

func GetMempoolTxIds(c *Client) ([]string, error) {
	return c.stream.GetMempoolTxIds()
}

func GetBlockHashByNumber(number int, c *Client) (string, error) {
	return c.stream.GetBlockHashByNumber(number)
}

func GetTestBlockByHeight(height int, c *Client) (result types.BTCTestBlockerInfo, err error) {
	return c.stream.GetTestBlockByHeight(height)
}

func GetTransactionByHash(hash string, c *Client) (tx types.TX, err error) {
	return c.node.GetTransactionByHash(hash)
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
