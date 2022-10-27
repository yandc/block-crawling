package sui

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

const (
	JSONRPC        = "2.0"
	JSONID         = 1
	RESULT_SUCCESS = "Exists"
)

type Client struct {
	URL string
}

func NewClient(nodeUrl string) Client {
	return Client{nodeUrl}
}

func (c *Client) call(id int, method string, out interface{}, params []interface{}, args ...interface{}) error {
	var resp types.Response
	var err error
	if len(args) > 0 {
		err = httpclient.HttpsPost(c.URL, id, method, JSONRPC, &resp, params, args[0].(int))
	} else {
		err = httpclient.HttpsPost(c.URL, id, method, JSONRPC, &resp, params)
	}
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	return json.Unmarshal(resp.Result, &out)
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
	err := c.call(JSONID, method, &out, params)
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
	err := c.call(JSONID, method, &out, params)
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
	err := c.call(JSONID, method, &out, nil)
	return out, err
}

func (c *Client) getTransactionsInRange(number int) (string, error) {
	method := "sui_getTransactionsInRange"
	var out []interface{}
	params := []interface{}{number, number + 1}
	err := c.call(JSONID, method, &out, params)
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
			Transactions []struct {
				TransferSui *struct {
					Recipient string `json:"recipient"`
					Amount    int    `json:"amount"`
				} `json:"TransferSui,omitempty"`
				TransferObject *struct {
					Recipient string `json:"recipient"`
					ObjectRef struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"objectRef"`
				} `json:"TransferObject,omitempty"`
				Call *struct {
					Package struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"package"`
					Module        string        `json:"module"`
					Function      string        `json:"function"`
					TypeArguments []string      `json:"typeArguments"`
					Arguments     []interface{} `json:"arguments"`
				} `json:"Call,omitempty"`
				Publish *struct {
					Disassembled map[string]string `json:"disassembled"`
				} `json:"Publish,omitempty"`
			} `json:"transactions"`
			Sender     string `json:"sender"`
			GasPayment struct {
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
			/*Owner struct {
				AddressOwner string `json:"AddressOwner"`
			} `json:"owner"`*/
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
		Events []struct {
			TransferObject *struct {
				PackageId         string `json:"packageId"`
				TransactionModule string `json:"transactionModule"`
				Sender            string `json:"sender"`
				Recipient         struct {
					AddressOwner string `json:"AddressOwner"`
				} `json:"recipient"`
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Type     string `json:"type"`
				Amount   int    `json:"amount"`
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

					PoolId    string `json:"pool_id"`
					InAmount  int    `json:"in_amount"`
					OutAmount int    `json:"out_amount"`
					XToY      bool   `json:"x_to_y"`

					//PoolId    string `json:"pool_id"`
					IsAdded   bool `json:"is_added"`
					LspAmount int  `json:"lsp_amount"`
					XAmount   int  `json:"x_amount"`
					YAmount   int  `json:"y_amount"`
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
		} `json:"events"`
		Dependencies []string `json:"dependencies"`
	} `json:"effects"`
	TimestampMs interface{} `json:"timestamp_ms"`
	ParsedData  interface{} `json:"parsed_data"`
}

func (c *Client) GetTransactionByHash(hash string) (TransactionInfo, error) {
	method := "sui_getTransaction"
	var out TransactionInfo
	params := []interface{}{hash}
	err := c.call(JSONID, method, &out, params)
	return out, err
}

func (c *Client) GetTransactionByNumber(number int) (TransactionInfo, error) {
	var out TransactionInfo
	hash, err := c.getTransactionsInRange(number)
	if err != nil {
		return out, err
	}
	if hash == "" {
		return out, errors.New("not found")
	}
	out, err = c.GetTransactionByHash(hash)
	return out, err
}

type Event struct {
	FromAddress  string `json:"fromAddress"`
	ToAddress    string `json:"toAddress"`
	TokenAddress string `json:"tokenAddress"`
	Amount       string `json:"amount,omitempty"`
	ObjectId     string `json:"objectId"`
}
