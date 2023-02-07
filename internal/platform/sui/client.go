package sui

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	JSONRPC        = "2.0"
	JSONID         = 1
	RESULT_SUCCESS = "Exists"
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
	height, err := c.GetTransactionNumber()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

// GetBlock fetch block data of the given height.
func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block, err := c.GetTransactionByNumber(int(height))
	if err != nil {
		return nil, err
	}

	return &chain.Block{
		Hash:   block.Certificate.TransactionDigest,
		Number: height,
		Raw:    block,
		Transactions: []*chain.Transaction{
			{
				Raw: block,
			},
		},
	}, nil
}

// GetTxByHash get transaction by given tx hash.
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	block, err := c.GetTransactionByHash(txHash)
	if err != nil {
		erro, ok := err.(*types.ErrorObject)
		if !ok {
			return nil, err
		}
		block.Error = erro
	}
	return &chain.Transaction{
		Hash:   txHash,
		Raw:    block,
		Record: nil,
	}, nil
}

func (c *Client) GetBalance(address string) (string, error) {
	return c.GetTokenBalance(address, SUI_CODE, 0)
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	resourceType := fmt.Sprintf("%s<%s>", TYPE_PREFIX, tokenAddress)
	balance := 0
	objectIds, err := c.getObjectIds(address, resourceType)
	if err != nil {
		return "0", err
	}
	for _, objectId := range objectIds {
		object, err := c.GetObject(objectId)
		if err != nil {
			return "0", err
		}
		balance += object.Details.Data.Fields.Balance
	}
	balances := utils.StringDecimals(strconv.Itoa(balance), decimals)
	return balances, err
}

func (c *Client) getObjectIds(address, coinType string) ([]string, error) {
	var objectIds []string
	objectInfo, err := c.getObjectsOwnedByAddress(address)
	if err != nil {
		return nil, err
	}
	for _, info := range objectInfo {
		if info.Type == coinType {
			objectIds = append(objectIds, info.ObjectId)
		}
	}
	return objectIds, nil
}

type SuiObjectInfo struct {
	Digest   string `json:"digest"`
	ObjectId string `json:"object_id"`
	Owner    struct {
		AddressOwner string `json:"AddressOwner"`
	} `json:"owner"`
	PreviousTransaction string `json:"previous_transaction"`
	Type                string `json:"type_"`
	Version             int    `json:"version"`
}

func (c *Client) getObjectsOwnedByAddress(address string) ([]SuiObjectInfo, error) {
	method := "sui_getObjectsOwnedByAddress"
	params := []interface{}{address}
	var out []SuiObjectInfo
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	return out, err
}

type SuiObject struct {
	Details struct {
		Data struct {
			DataType string `json:"dataType"`
			Fields   struct {
				Balance int `json:"balance"`
				Id      struct {
					Id string `json:"id"`
				} `json:"id"`
			} `json:"fields"`
			HasPublicTransfer bool   `json:"has_public_transfer"`
			Type              string `json:"type"`
		} `json:"data"`
		Owner struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"owner"`
		PreviousTransaction string `json:"previousTransaction"`
		Reference           struct {
			Digest   string `json:"digest"`
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
		} `json:"reference"`
		StorageRebate int `json:"storageRebate"`
	} `json:"details"`
	Status string `json:"status"`
}

func (c *Client) GetObject(objectId string) (*SuiObject, error) {
	method := "sui_getObject"
	out := &SuiObject{}
	params := []interface{}{objectId}
	timeoutMS := 5_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	if err != nil {
		return out, err
	}
	if out.Status != RESULT_SUCCESS {
		return out, errors.New(out.Status)
	}
	return out, nil
}

func (c *Client) GetTransactionNumber() (int, error) {
	method := "sui_getTotalTransactionNumber"
	var out int
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, nil, &timeoutMS)
	return out, err
}

func (c *Client) getTransactionsInRange(number int) (string, error) {
	method := "sui_getTransactionsInRange"
	var out []interface{}
	params := []interface{}{number, number + 1}
	timeoutMS := 3_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	if err != nil {
		return "", err
	}
	var digest string
	if len(out) > 0 {
		digest = out[0].(string)
	}
	return digest, err
}

type TransactionInfo struct {
	Certificate struct {
		TransactionDigest string `json:"transactionDigest"`
		Data              struct {
			Transactions []*Transaction `json:"transactions"`
			Sender       string         `json:"sender"`
			GasPayment   struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"gasPayment"`
			GasBudget int `json:"gasBudget"`
		} `json:"data"`
		TxSignature  string `json:"txSignature"`
		AuthSignInfo struct {
			Epoch      int         `json:"epoch"`
			Signature  interface{} `json:"signature"`
			SignersMap []int       `json:"signers_map"`
		} `json:"authSignInfo"`
	} `json:"certificate"`
	Effects struct {
		Status struct {
			Status string `json:"status"`
		} `json:"status"`
		GasUsed struct {
			ComputationCost int `json:"computationCost"`
			StorageCost     int `json:"storageCost"`
			StorageRebate   int `json:"storageRebate"`
		} `json:"gasUsed"`
		SharedObjects []struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"sharedObjects"`
		TransactionDigest string `json:"transactionDigest"`
		Created           []struct {
			/*Owner struct {
				AddressOwner string `json:"AddressOwner"`
			} `json:"owner"`*/
			Owner     interface{} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"created"`
		Mutated []struct {
			Owner     interface{} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"mutated"`
		Deleted []struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"deleted"`
		GasObject struct {
			Owner struct {
				AddressOwner string `json:"AddressOwner"`
			} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"gasObject"`
		Events       []*Event `json:"events"`
		Dependencies []string `json:"dependencies"`
	} `json:"effects"`
	TimestampMs int64              `json:"timestamp_ms"`
	ParsedData  interface{}        `json:"parsed_data"`
	Error       *types.ErrorObject `json:"error,omitempty"`
}

type Transaction struct {
	TransferSui *struct {
		Recipient string   `json:"recipient"`
		Amount    *big.Int `json:"amount"`
	} `json:"TransferSui,omitempty"`
	Pay *struct {
		Recipients []string   `json:"recipients"`
		Amounts    []*big.Int `json:"amounts"`
		Coins      []struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"coins"`
	} `json:"Pay,omitempty"`
	PaySui *struct {
		Recipients []string   `json:"recipients"`
		Amounts    []*big.Int `json:"amounts"`
		Coins      []struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"coins"`
	} `json:"PaySui,omitempty"`
	TransferObject *struct {
		Recipient string `json:"recipient"`
		ObjectRef struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"objectRef"`
	} `json:"TransferObject,omitempty"`
	Call *struct {
		Package       string        `json:"package"`
		Module        string        `json:"module"`
		Function      string        `json:"function"`
		TypeArguments []string      `json:"typeArguments"`
		Arguments     []interface{} `json:"arguments"`
	} `json:"Call,omitempty"`
	Publish *struct {
		Disassembled map[string]string `json:"disassembled"`
	} `json:"Publish,omitempty"`
}

type Event struct {
	TransferObject *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		/*Recipient         struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"recipient"`*/
		Recipient  interface{} `json:"recipient"`
		ObjectType string      `json:"objectType"`
		ObjectId   string      `json:"objectId"`
		Version    int         `json:"version"`
	} `json:"transferObject,omitempty"`
	MoveEvent *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		Type              string `json:"type"`
		Fields            struct {
			Creator  string `json:"creator"`
			Name     string `json:"name"`
			ObjectId string `json:"object_id"`

			PoolId    string      `json:"pool_id"`
			InAmount  interface{} `json:"in_amount"`
			OutAmount interface{} `json:"out_amount"`
			XToY      bool        `json:"x_to_y"`

			IsAdded   bool        `json:"is_added"`
			LspAmount interface{} `json:"lsp_amount"`
			XAmount   interface{} `json:"x_amount"`
			YAmount   interface{} `json:"y_amount"`
		} `json:"fields"`
		Bcs string `json:"bcs"`
	} `json:"moveEvent,omitempty"`
	Publish *struct {
		Sender    string `json:"sender"`
		PackageId string `json:"packageId"`
	} `json:"publish,omitempty"`
	DeleteObject *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		ObjectId          string `json:"objectId"`
	} `json:"deleteObject,omitempty"`
	NewObject *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		/*Recipient         struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"recipient"`*/
		Recipient interface{} `json:"recipient"`
		ObjectId  string      `json:"objectId"`
	} `json:"newObject,omitempty"`
	CoinBalanceChange *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		ChangeType        string `json:"changeType"`
		Owner             *struct {
			AddressOwner string
		} `json:"owner"`
		CoinType     string   `json:"coinType"`
		CoinObjectId string   `json:"coinObjectId"`
		Version      int      `json:"version"`
		Amount       *big.Int `json:"amount"`
	} `json:"coinBalanceChange,omitempty"`
	MutateObject *struct {
		PackageId         string `json:"packageId"`
		TransactionModule string `json:"transactionModule"`
		Sender            string `json:"sender"`
		ObjectType        string `json:"objectType"`
		ObjectId          string `json:"objectId"`
		Version           int    `json:"version"`
	} `json:"mutateObject,omitempty"`
}

func (c *Client) GetTransactionByHash(hash string) (TransactionInfo, error) {
	method := "sui_getTransaction"
	var out TransactionInfo
	params := []interface{}{hash}
	timeoutMS := 5_000 * time.Millisecond
	_, err := httpclient.JsonrpcCall(c.url, JSONID, JSONRPC, method, &out, params, &timeoutMS)
	return out, err
}

func (c *Client) GetTransactionByNumber(number int) (TransactionInfo, error) {
	var out TransactionInfo
	hash, err := c.getTransactionsInRange(number)
	if err != nil {
		return out, err
	}
	if hash == "" {
		return out, common.NotFound
	}
	out, err = c.GetTransactionByHash(hash)
	return out, err
}
