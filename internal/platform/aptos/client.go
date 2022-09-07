package aptos

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const TYPE_PREFIX = "0x1::coin::CoinStore"

type Client struct {
	URL string
}

func NewClient(nodeUrl string) Client {
	return Client{nodeUrl}
}

type AptosBadResp struct {
	ErrorCode          string `json:"error_code"`
	Message            string `json:"message"`
	AptosLedgerVersion string `json:"aptos_ledger_version"`
}

type AptosBalanceResp struct {
	Data struct {
		Coin struct {
			Value string `json:"value"`
		} `json:"coin"`
	} `json:"data"`
	AptosBadResp
}

func (c *Client) GetAddressIsActive(address string) []interface{} {
	var result []interface{}
	resourceInfo := c.GetResourceByAddress(address)
	if resourceInfo == nil {
		return result
	}
	result = make([]interface{}, 0, len(*resourceInfo))
	for _, resource := range *resourceInfo {
		if strings.HasPrefix(resource.Type, TYPE_PREFIX) {
			result = append(result, resource.Type[len(TYPE_PREFIX+"<"):len(resource.Type)-1])
		}
	}
	return result
}

type AptosResourceResp []struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func (c *Client) GetResourceByAddress(address string) *AptosResourceResp {
	url := fmt.Sprintf("%s/accounts/%s/resources", c.URL, address)
	out := &AptosResourceResp{}
	err := httpclient.HttpsGetForm(url, nil, out)
	if err != nil {
		return nil
	}
	return out
}

func (c *Client) GetBalance(address string) (string, error) {
	activeFlag := c.GetAddressIsActive(address)
	if len(activeFlag) == 0 {
		return "0", nil
	}
	balance, err := c.GetTokenBalance(address, APT_CODE, 0)
	return balance, err
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	resourceType := fmt.Sprintf("%s<%s>", TYPE_PREFIX, tokenAddress)
	url := fmt.Sprintf("%s/accounts/%s/resource/%s", c.URL, address, resourceType)
	out := &AptosBalanceResp{}
	err := httpclient.HttpsGetForm(url, nil, out)
	if err != nil {
		return "", err
	}
	if out.Message != "" {
		return "", errors.New(out.Message)
	}
	balance := utils.StringDecimals(out.Data.Coin.Value, decimals)
	return balance, err
}

type Blockchain struct {
	ChainId             int    `json:"chain_id"`
	Epoch               string `json:"epoch"`
	LedgerVersion       string `json:"ledger_version"`
	OldestLedgerVersion string `json:"oldest_ledger_version"`
	BlockHeight         string `json:"block_height"`
	OldestBlockHeight   string `json:"oldest_block_height"`
	LedgerTimestamp     string `json:"ledger_timestamp"`
	NodeRole            string `json:"node_role"`
	AptosBadResp
}

func (c *Client) GetBlockNumber() (int, error) {
	u, err := c.buildURL("", nil)
	if err != nil {
		return 0, err
	}
	var chain Blockchain
	err = getResponse(u, &chain)
	if err != nil {
		return 0, err
	}
	version, err := strconv.Atoi(chain.BlockHeight)
	if err != nil {
		return 0, err
	}
	return version, err
}

type BlockerInfo struct {
	BlockHeight    string            `json:"block_height"`
	BlockHash      string            `json:"block_hash"`
	BlockTimestamp string            `json:"block_timestamp"`
	FirstVersion   string            `json:"first_version"`
	LastVersion    string            `json:"last_version"`
	Transactions   []TransactionInfo `json:"transactions"`
}

type TransactionInfo struct {
	Type                string `json:"type"`
	Version             string `json:"version"`
	Hash                string `json:"hash"`
	StateRootHash       string `json:"state_root_hash"`
	EventRootHash       string `json:"event_root_hash"`
	GasUsed             string `json:"gas_used"`
	Success             bool   `json:"success"`
	VmStatus            string `json:"vm_status"`
	AccumulatorRootHash string `json:"accumulator_root_hash"`
	Changes             []struct {
		Type         string `json:"type"`
		Address      string `json:"address"`
		StateKeyHash string `json:"state_key_hash"`
		Data         struct {
			Type string      `json:"type"`
			Data interface{} `json:"data"`
		} `json:"data"`
	} `json:"changes"`
	Sender                  string `json:"sender"`
	SequenceNumber          string `json:"sequence_number"`
	MaxGasAmount            string `json:"max_gas_amount"`
	GasUnitPrice            string `json:"gas_unit_price"`
	ExpirationTimestampSecs string `json:"expiration_timestamp_secs"`
	Payload                 struct {
		Type          string        `json:"type"`
		Function      string        `json:"function"`
		TypeArguments []string      `json:"type_arguments"`
		Arguments     []interface{} `json:"arguments"`
	} `json:"payload"`
	Signature struct {
		Type      string `json:"type"`
		PublicKey string `json:"public_key"`
		Signature string `json:"signature"`
	} `json:"signature"`
	Events []struct {
		Key            string `json:"key"`
		SequenceNumber string `json:"sequence_number"`
		Type           string `json:"type"`
		Data           struct {
			Amount string `json:"amount,omitempty"`
		} `json:"data"`
	} `json:"events"`
	Timestamp string `json:"timestamp"`
	AptosBadResp
}

func (c *Client) GetBlockByNumber(number int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/blocks/by_height/"+strconv.Itoa(number)+"?with_transactions=true", nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetBlockByVersion(version int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/blocks/by_version/"+strconv.Itoa(version)+"?with_transactions=true", nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetTransactionByVersion(version int) (tx TransactionInfo, err error) {
	u, err := c.buildURL("/transactions/by_version/"+strconv.Itoa(version), nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetTransactionByHash(hash string) (tx TransactionInfo, err error) {
	u, err := c.buildURL("/transactions/by_hash/"+hash, nil)
	if err != nil {
		return
	}
	err = getResponse(u, &tx)
	return tx, err
}

//constructs BlockCypher URLs with parameters for requests
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

//getResponse is a boilerplate for HTTP GET responses.
func getResponse(target *url.URL, decTarget interface{}) (err error) {
	resp, err := http.Get(target.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, decTarget)
	if err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
	}

	return
}
