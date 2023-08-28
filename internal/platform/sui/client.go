package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"

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

type SuiObjectChanges struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Data []struct {
			Digest      string `json:"digest"`
			Transaction struct {
				Data struct {
					MessageVersion string `json:"messageVersion"`
					Transaction    struct {
						Kind   string `json:"kind"`
						Inputs []struct {
							Type                 string `json:"type"`
							ObjectType           string `json:"objectType,omitempty"`
							ObjectId             string `json:"objectId,omitempty"`
							Version              string `json:"version,omitempty"`
							Digest               string `json:"digest,omitempty"`
							ValueType            string `json:"valueType,omitempty"`
							Value                string `json:"value,omitempty"`
							InitialSharedVersion string `json:"initialSharedVersion,omitempty"`
							Mutable              bool   `json:"mutable,omitempty"`
						} `json:"inputs"`
						Transactions []struct {
							TransferObjects []interface{} `json:"TransferObjects,omitempty"`
							SplitCoins      []interface{} `json:"SplitCoins,omitempty"`
							MoveCall        struct {
								Package   string `json:"package"`
								Module    string `json:"module"`
								Function  string `json:"function"`
								Arguments []struct {
									Input        int   `json:"Input,omitempty"`
									NestedResult []int `json:"NestedResult,omitempty"`
								} `json:"arguments"`
							} `json:"MoveCall,omitempty"`
						} `json:"transactions"`
					} `json:"transaction"`
					Sender  string `json:"sender"`
					GasData struct {
						Payment []struct {
							ObjectId string `json:"objectId"`
							Version  int    `json:"version"`
							Digest   string `json:"digest"`
						} `json:"payment"`
						Owner  string `json:"owner"`
						Price  string `json:"price"`
						Budget string `json:"budget"`
					} `json:"gasData"`
				} `json:"data"`
				TxSignatures []string `json:"txSignatures"`
			} `json:"transaction"`
			Effects struct {
				MessageVersion string `json:"messageVersion"`
				Status         struct {
					Status string `json:"status"`
				} `json:"status"`
				ExecutedEpoch string `json:"executedEpoch"`
				GasUsed       struct {
					ComputationCost         string `json:"computationCost"`
					StorageCost             string `json:"storageCost"`
					StorageRebate           string `json:"storageRebate"`
					NonRefundableStorageFee string `json:"nonRefundableStorageFee"`
				} `json:"gasUsed"`
				ModifiedAtVersions []struct {
					ObjectId       string `json:"objectId"`
					SequenceNumber string `json:"sequenceNumber"`
				} `json:"modifiedAtVersions"`
				TransactionDigest string `json:"transactionDigest"`
				Mutated           []struct {
					Owner struct {
						AddressOwner string `json:"AddressOwner,omitempty"`
						Shared       struct {
							InitialSharedVersion int `json:"initial_shared_version"`
						} `json:"Shared,omitempty"`
					} `json:"owner"`
					Reference struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"reference"`
				} `json:"mutated"`
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
				Dependencies  []string `json:"dependencies"`
				SharedObjects []struct {
					ObjectId string `json:"objectId"`
					Version  int    `json:"version"`
					Digest   string `json:"digest"`
				} `json:"sharedObjects,omitempty"`
				Created []struct {
					Owner struct {
						AddressOwner string `json:"AddressOwner,omitempty"`
						ObjectOwner  string `json:"ObjectOwner,omitempty"`
					} `json:"owner"`
					Reference struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"reference"`
				} `json:"created,omitempty"`
				Deleted []struct {
					ObjectId string `json:"objectId"`
					Version  int    `json:"version"`
					Digest   string `json:"digest"`
				} `json:"deleted,omitempty"`
				EventsDigest string `json:"eventsDigest,omitempty"`
			} `json:"effects"`
			ObjectChanges []struct {
				Type   string `json:"type"`
				Sender string `json:"sender"`
				Owner  struct {
					AddressOwner string `json:"AddressOwner,omitempty"`
					Shared       struct {
						InitialSharedVersion int `json:"initial_shared_version"`
					} `json:"Shared,omitempty"`
					ObjectOwner string `json:"ObjectOwner,omitempty"`
				} `json:"owner"`
				ObjectType      string `json:"objectType"`
				ObjectId        string `json:"objectId"`
				Version         string `json:"version"`
				PreviousVersion string `json:"previousVersion,omitempty"`
				Digest          string `json:"digest"`
			} `json:"objectChanges"`
			BalanceChanges []struct {
				Owner struct {
					AddressOwner string `json:"AddressOwner"`
				} `json:"owner"`
				CoinType string `json:"coinType"`
				Amount   string `json:"amount"`
			} `json:"balanceChanges"`
			TimestampMs string `json:"timestampMs"`
			Checkpoint  string `json:"checkpoint"`
		} `json:"data"`
		NextCursor  string `json:"nextCursor"`
		HasNextPage bool   `json:"hasNextPage"`
	} `json:"result"`
	Id string `json:"id"`
}

type TransactionInfo struct {
	Digest      string      `json:"digest"`
	Transaction Transaction `json:"transaction"`
	Effects     struct {
		MessageVersion string `json:"messageVersion"`
		Status         struct {
			Status string `json:"status"`
		} `json:"status"`
		ExecutedEpoch string `json:"executedEpoch"`
		GasUsed       struct {
			ComputationCost         string `json:"computationCost"`
			StorageCost             string `json:"storageCost"`
			StorageRebate           string `json:"storageRebate"`
			NonRefundableStorageFee string `json:"nonRefundableStorageFee"`
		} `json:"gasUsed"`
		/*ModifiedAtVersions []struct {
			ObjectId       string `json:"objectId"`
			SequenceNumber string `json:"sequenceNumber"`
		} `json:"modifiedAtVersions"`
		TransactionDigest string `json:"transactionDigest"`
		Created           []struct {
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
		} `json:"gasObject"`*/
		EventsDigest string `json:"eventsDigest"`
		// Dependencies []string `json:"dependencies"`
	} `json:"effects"`
	// Events        []Event `json:"events"`
	ObjectChanges []struct {
		Type            string      `json:"type"`
		Sender          string      `json:"sender"`
		Owner           interface{} `json:"owner"`
		ObjectType      string      `json:"objectType"`
		ObjectId        string      `json:"objectId"`
		Version         string      `json:"version"`
		PreviousVersion string      `json:"previousVersion,omitempty"`
		Digest          string      `json:"digest"`
	} `json:"objectChanges"`
	BalanceChanges []struct {
		/*Owner struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"owner"`*/
		Owner    interface{} `json:"owner"`
		CoinType string      `json:"coinType"`
		Amount   string      `json:"amount"`
	} `json:"balanceChanges"`
	TimestampMs string             `json:"timestampMs"`
	Checkpoint  string             `json:"checkpoint"`
	Errors      []string           `json:"errors,omitempty"`
	Error       *types.ErrorObject `json:"error,omitempty"`
}

type Transaction struct {
	Data struct {
		MessageVersion string `json:"messageVersion"`
		Transaction    struct {
			Kind   string `json:"kind"`
			Inputs []struct {
				Type                 string      `json:"type"`
				ValueType            string      `json:"valueType,omitempty"`
				Value                interface{} `json:"value,omitempty"`
				ObjectType           string      `json:"objectType,omitempty"`
				ObjectId             string      `json:"objectId,omitempty"`
				InitialSharedVersion string      `json:"initialSharedVersion,omitempty"`
				Mutable              bool        `json:"mutable,omitempty"`
			} `json:"inputs"`
			Transactions []struct {
				MergeCoins      []interface{} `json:"MergeCoins,omitempty"`
				SplitCoins      []interface{} `json:"SplitCoins,omitempty"`
				TransferObjects []interface{} `json:"TransferObjects,omitempty"`
				MoveCall        interface{}   `json:"MoveCall,omitempty"`
			} `json:"transactions"`
		} `json:"transaction"`
		Sender  string `json:"sender"`
		GasData struct {
			/*Payment []struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"payment"`*/
			Owner  string `json:"owner"`
			Price  string `json:"price"`
			Budget string `json:"budget"`
		} `json:"gasData"`
	} `json:"data"`
	//TxSignatures []string `json:"txSignatures"`
}

type Event struct {
	Id struct {
		TxDigest string `json:"txDigest"`
		EventSeq string `json:"eventSeq"`
	} `json:"id"`
	PackageId         string      `json:"packageId"`
	TransactionModule string      `json:"transactionModule"`
	Sender            string      `json:"sender"`
	Type              string      `json:"type"`
	ParsedJson        interface{} `json:"parsedJson"`
	Bcs               string      `json:"bcs"`
}

func (c *Client) GetTransactionByHash(hash string) (*TransactionInfo, error) {
	method := "sui_getTransactionBlock"
	var out *TransactionInfo
	params := []interface{}{hash, map[string]bool{
		"showInput":          true,
		"showRawInput":       false,
		"showEffects":        true,
		"showEvents":         false,
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

func (c *Client) GetTransactionByHashs(hashs []string) ([]*TransactionInfo, error) {
	method := "sui_multiGetTransactionBlocks"
	//multi get transaction input limit is 50
	pageSize := 50
	hashSize := len(hashs)
	start := 0
	stop := pageSize
	if stop > hashSize {
		stop = hashSize
	}
	var result []*TransactionInfo
	for {
		hs := hashs[start:stop]
		var out []*TransactionInfo
		params := []interface{}{hs, map[string]bool{
			"showInput":          true,
			"showRawInput":       false,
			"showEffects":        true,
			"showEvents":         false,
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

func (c *Client) GetEventTransfer(tokenId string) (tar SuiObjectChanges, err error) {
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
