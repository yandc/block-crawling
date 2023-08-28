package aptos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const TYPE_PREFIX = "0x1::coin::CoinStore"
const APTOS_DECIMALS = 8

type Client struct {
	*common.NodeDefaultIn

	url string
}

func NewClient(chainName, nodeUrl string) Client {
	return Client{
		url: nodeUrl,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
	}
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

func (c *Client) Detect() error {
	log.Info(c.ChainName+"链节点检测", zap.Any("nodeURL", c.url))
	_, err := c.GetBlockNumber()
	return err
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) GetBlockHeight() (uint64, error) {
	height, err := c.GetBlockNumber()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block, err := c.GetBlockByNumber(int(height))
	if err != nil {
		return nil, err
	}
	txs := make([]*chain.Transaction, 0, len(block.Transactions))
	for _, rawTx := range block.Transactions {
		nonce, _ := strconv.Atoi(rawTx.SequenceNumber)
		txs = append(txs, &chain.Transaction{
			Hash:        rawTx.Hash,
			Nonce:       uint64(nonce),
			BlockNumber: height,
			TxType:      "",
			FromAddress: rawTx.Sender,
			ToAddress:   "",
			Value:       "",
			Raw:         rawTx,
			Record:      nil,
		})
	}
	blkTime, _ := strconv.ParseInt(block.BlockTimestamp, 10, 64)

	return &chain.Block{
		Hash:         block.BlockHash,
		Number:       height,
		Time:         blkTime,
		Raw:          block,
		Transactions: txs,
	}, nil
}

func (c *Client) GetBlockByTxVersion(txVersion int) (*chain.Block, error) {
	block, err := c.GetBlockByVersion(txVersion)
	if err != nil {
		return nil, err
	}
	height, _ := strconv.Atoi(block.BlockHeight)
	txs := make([]*chain.Transaction, 0, len(block.Transactions))
	for _, rawTx := range block.Transactions {
		nonce, _ := strconv.Atoi(rawTx.SequenceNumber)
		txs = append(txs, &chain.Transaction{
			Hash:        rawTx.Hash,
			Nonce:       uint64(nonce),
			BlockNumber: uint64(height),
			TxType:      "",
			FromAddress: rawTx.Sender,
			ToAddress:   "",
			Value:       "",
			Raw:         rawTx,
			Record:      nil,
		})
	}
	blkTime, _ := strconv.ParseInt(block.BlockTimestamp, 10, 64)

	return &chain.Block{
		Hash:         block.BlockHash,
		Number:       uint64(height),
		Time:         blkTime,
		Raw:          block,
		Transactions: txs,
	}, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	rawTx, err := c.GetTransactionByHash(txHash)
	if err != nil {
		log.Error("get transaction by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	}
	nonce, _ := strconv.Atoi(rawTx.SequenceNumber)
	return &chain.Transaction{
		Hash:        txHash,
		Nonce:       uint64(nonce),
		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",
		Raw:         rawTx,
		Record:      nil,
	}, nil
}

func (c *Client) GetTxByVersion(txVersion int) (*chain.Transaction, error) {
	rawTx, err := c.GetTransactionByVersion(txVersion)
	if err != nil {
		return nil, err
	}
	nonce, _ := strconv.Atoi(rawTx.SequenceNumber)
	return &chain.Transaction{
		Hash:        rawTx.Hash,
		Nonce:       uint64(nonce),
		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",
		Raw:         rawTx,
		Record:      nil,
	}, nil
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
	url := fmt.Sprintf("%s/accounts/%s/resources", c.url, address)
	out := &AptosResourceResp{}
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, out, &timeoutMS)
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
	balance, err := c.GetTokenBalance(address, APT_CODE, APTOS_DECIMALS)
	return balance, err
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	resourceType := fmt.Sprintf("%s<%s>", TYPE_PREFIX, tokenAddress)
	url := fmt.Sprintf("%s/accounts/%s/resource/%s", c.url, address, resourceType)
	out := &AptosBalanceResp{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, out, &timeoutMS)
	if err != nil {
		return "", err
	}
	if out.Message != "" {
		return "", errors.New(out.Message)
	}
	balance := utils.StringDecimals(out.Data.Coin.Value, decimals)
	return balance, err
}

type TokenActivitiesResponse struct {
	Data struct {
		TokenActivities []struct {
			TransactionVersion int    `json:"transaction_version"`
			FromAddress        string `json:"from_address"`
			PropertyVersion    int    `json:"property_version"`
			ToAddress          string `json:"to_address"`
			TokenAmount        int    `json:"token_amount"`
			TransferType       string `json:"transfer_type"`
			Typename           string `json:"__typename"`
		} `json:"token_activities"`
	} `json:"data"`
	TokenErrorData
}
type TokenRequest struct {
	OperationName string    `json:"operationName"`
	Variables     Variables `json:"variables"`
	Query         string    `json:"query"`
}
type Variables struct {
	OwnerAddress   string `json:"owner_address,omitempty"`
	CreatorAddress string `json:"creator_address,omitempty"`
	CollectionName string `json:"collection_name,omitempty"`
	Name           string `json:"name,omitempty"`
	TokenId        string `json:"token_id,omitempty"`
	Offset         int    `json:"offset,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}
type TokenVersionRequest struct {
	OperationName string           `json:"operationName"`
	Variables     VariablesVersion `json:"variables"`
	Query         string           `json:"query"`
}
type VariablesVersion struct {
	OwnerAddress    string `json:"owner_address,omitempty"`
	CreatorAddress  string `json:"creator_address,omitempty"`
	CollectionName  string `json:"collection_name,omitempty"`
	Name            string `json:"name,omitempty"`
	TokenId         string `json:"token_id,omitempty"`
	PropertyVersion int    `json:"property_version"`
}
type TokenResponse struct {
	Data struct {
		CurrentTokenOwnerships []struct {
			TokenDataIdHash string `json:"token_data_id_hash"`
			Name            string `json:"name"`
			CollectionName  string `json:"collection_name"`
			CreatorAddress  string `json:"creator_address"`
			TableType       string `json:"table_type"`
			PropertyVersion int    `json:"property_version"`
			Amount          int    `json:"amount"`
			Typename        string `json:"__typename"`
		} `json:"current_token_ownerships"`
	} `json:"data"`
	TokenErrorData
}
type TokenErrorData struct {
	Errors []struct {
		Extensions struct {
			Code string `json:"code"`
			Path string `json:"path"`
		} `json:"extensions"`
		Message string `json:"message"`
	} `json:"errors"`
}

func (c *Client) GetEventTransfer(tokenId string, offset int, limit int, chainName string) (tar TokenActivitiesResponse, err error) {
	url := "https://wqb9q2zgw7i7-mainnet.hasura.app/v1/graphql"
	if biz.IsTestNet(chainName) {
		url = "https://knmpjhsurbz8-testnet.hasura.app/v1/graphql"
	}
	tokenRequest := TokenRequest{
		OperationName: "TokenActivities",
		Variables: Variables{
			TokenId: tokenId,
			Offset:  offset,
			Limit:   limit,
		},
		Query: "query TokenActivities($token_id: String, $limit: Int, $offset: Int) {\n  token_activities(\n    where: {token_data_id_hash: {_eq: $token_id}}\n    order_by: {transaction_version: desc}\n    limit: $limit\n    offset: $offset\n  ) {\n    transaction_version\n    from_address\n    property_version\n    to_address\n    token_amount\n    transfer_type\n    __typename\n  }\n}",
	}
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.HttpPostJson(url, tokenRequest, &tar, &timeoutMS)
	return
}

func (c *Client) Erc1155BalanceByName(address string, creatorAddress string, collectionName string, name string, propertyVersion int) (string, error) {
	url := "https://wqb9q2zgw7i7-mainnet.hasura.app/v1/graphql"
	if biz.IsTestNet(c.ChainName) {
		url = "https://knmpjhsurbz8-testnet.hasura.app/v1/graphql"
	}
	tokenRequest := TokenVersionRequest{
		OperationName: "AccountTokensData",
		Variables: VariablesVersion{
			OwnerAddress:    address,
			CreatorAddress:  creatorAddress,
			CollectionName:  collectionName,
			Name:            name,
			PropertyVersion: propertyVersion,
		},
		Query: "query AccountTokensData($owner_address: String, $creator_address: String, $collection_name: String, $name: String, $property_version: numeric) {\n  current_token_ownerships(\n    where: {owner_address: {_eq: $owner_address}, creator_address: {_eq: $creator_address}, collection_name: {_eq: $collection_name}, name: {_eq: $name}, property_version: {_eq: $property_version}, amount: {_gt: \"0\"}}) {\n    token_data_id_hash\n    name\n    collection_name\n    creator_address\n    table_type\n    property_version\n    amount\n    __typename\n  }\n}",
	}
	out := &TokenResponse{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpPostJson(url, tokenRequest, out, &timeoutMS)
	if err != nil {
		return "", err
	}
	if out.Errors != nil {
		errorsStr, err := utils.JsonEncode(out.Errors)
		if err != nil {
			return "", err
		}
		return "", errors.New(errorsStr)
	}

	currentTokenOwnerships := out.Data.CurrentTokenOwnerships
	if len(currentTokenOwnerships) == 0 {
		return "0", nil
	}
	currentTokenOwnership := currentTokenOwnerships[0]
	amount := currentTokenOwnership.Amount
	return strconv.Itoa(amount), nil
}

func (c *Client) Erc1155BalanceByTokenId(address string, tokenId string, propertyVersion int) (string, error) {
	url := "https://wqb9q2zgw7i7-mainnet.hasura.app/v1/graphql"
	if biz.IsTestNet(c.ChainName) {
		url = "https://knmpjhsurbz8-testnet.hasura.app/v1/graphql"
	}
	tokenRequest := TokenVersionRequest{
		OperationName: "AccountTokensData",
		Variables: VariablesVersion{
			OwnerAddress:    address,
			TokenId:         tokenId,
			PropertyVersion: propertyVersion,
		},
		Query: "query AccountTokensData($owner_address: String, $token_id: String, $property_version: numeric) {\n  current_token_ownerships(\n    where: {owner_address: {_eq: $owner_address}, token_data_id_hash: {_eq: $token_id}, property_version: {_eq: $property_version}, amount: {_gt: \"0\"}}) {\n    token_data_id_hash\n    name\n    collection_name\n    creator_address\n    table_type\n    property_version\n    amount\n    __typename\n  }\n}",
	}
	out := &TokenResponse{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpPostJson(url, tokenRequest, out, &timeoutMS)
	if err != nil {
		return "", err
	}
	if out.Errors != nil {
		errorsStr, err := utils.JsonEncode(out.Errors)
		if err != nil {
			return "", err
		}
		return "", errors.New(errorsStr)
	}

	currentTokenOwnerships := out.Data.CurrentTokenOwnerships
	if len(currentTokenOwnerships) == 0 {
		return "0", nil
	}
	currentTokenOwnership := currentTokenOwnerships[0]
	amount := currentTokenOwnership.Amount
	return strconv.Itoa(amount), nil
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
	err = c.getResponse(u, &chain)
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
	BlockHeight    string             `json:"block_height"`
	BlockHash      string             `json:"block_hash"`
	BlockTimestamp string             `json:"block_timestamp"`
	FirstVersion   string             `json:"first_version"`
	LastVersion    string             `json:"last_version"`
	Transactions   []*TransactionInfo `json:"transactions"`
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
			Type string `json:"type"`
			Data *struct {
				Coin *struct {
					Value string `json:"value"`
				} `json:"coin,omitempty"`
				DepositEvents *struct {
					Counter string `json:"counter"`
					Guid    struct {
						Id struct {
							Addr        string `json:"addr"`
							CreationNum string `json:"creation_num"`
						} `json:"id"`
					} `json:"guid"`
				} `json:"deposit_events,omitempty"`
				Frozen         bool `json:"frozen,omitempty"`
				WithdrawEvents *struct {
					Counter string `json:"counter"`
					Guid    struct {
						Id struct {
							Addr        string `json:"addr"`
							CreationNum string `json:"creation_num"`
						} `json:"id"`
					} `json:"guid"`
				} `json:"withdraw_events,omitempty"`
			} `json:"data"`
		} `json:"data"`
	} `json:"changes"`
	Sender                  string `json:"sender"`
	SequenceNumber          string `json:"sequence_number"`
	MaxGasAmount            string `json:"max_gas_amount"`
	GasUnitPrice            string `json:"gas_unit_price"`
	ExpirationTimestampSecs string `json:"expiration_timestamp_secs"`
	Payload                 *struct {
		Type          string        `json:"type"`
		Function      string        `json:"function"`
		TypeArguments []string      `json:"type_arguments"`
		Arguments     []interface{} `json:"arguments"`
	} `json:"payload,omitempty"`
	Signature *struct {
		Type      string `json:"type"`
		PublicKey string `json:"public_key"`
		Signature string `json:"signature"`
	} `json:"signature,omitempty"`
	Events []struct {
		Guid struct {
			CreationNumber string `json:"creation_number"`
			AccountAddress string `json:"account_address"`
		} `json:"guid"`
		SequenceNumber string      `json:"sequence_number"`
		Type           string      `json:"type"`
		Data           interface{} `json:"data,omitempty"`
		/*Data           struct {
			Amount string      `json:"amount,omitempty"`
			Id     interface{} `json:"id,omitempty"`
			//Id     *struct {
			//	PropertyVersion string `json:"property_version"`
			//	TokenDataId     struct {
			//		Collection string `json:"collection"`
			//		Creator    string `json:"creator"`
			//		Name       string `json:"name"`
			//	} `json:"token_data_id"`
			//} `json:"id,omitempty"`
		} `json:"data"`*/
	} `json:"events"`
	Timestamp string `json:"timestamp"`
	AptosBadResp
}

func (c *Client) GetBlockByNumber(number int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/blocks/by_height/"+strconv.Itoa(number)+"?with_transactions=true", nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetBlockByVersion(version int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/blocks/by_version/"+strconv.Itoa(version)+"?with_transactions=true", nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetTransactionByVersion(version int) (tx *TransactionInfo, err error) {
	u, err := c.buildURL("/transactions/by_version/"+strconv.Itoa(version), nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

func (c *Client) GetTransactionByHash(hash string) (tx *TransactionInfo, err error) {
	u, err := c.buildURL("/transactions/by_hash/"+hash, nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

// constructs BlockCypher URLs with parameters for requests
func (c *Client) buildURL(u string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(c.url + u)
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

// getResponse is a boilerplate for HTTP GET responses.
func (c *Client) getResponse(target *url.URL, decTarget interface{}) (err error) {
	timeoutMS := 5_000 * time.Millisecond
	statusCode, err := httpclient.GetStatusCode(target.String(), nil, &decTarget, &timeoutMS, nil)
	if statusCode == 429 && biz.IsTestNet(c.ChainName) {
		// on test we only sleep for 3 seconds when we meet 429
		c.SetRetryAfter(time.Second * 3)
	}

	return
}
