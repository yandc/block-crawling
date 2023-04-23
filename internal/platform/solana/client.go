package solana

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/utils"
	"go.uber.org/zap"
)

const (
	JSONRPC = "2.0"
	JSONID  = 1
)
const SOLANA_DECIMALS = 9

type Client struct {
	*common.NodeDefaultIn

	Url        string
	ChainName  string
	retryAfter time.Time
	stat       common.Stater
}

func NewClient(nodeUrl string, chainName string) *Client {
	return &Client{
		Url:       nodeUrl,
		ChainName: chainName,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
		stat: common.NewStat(),
	}
}

func (c *Client) RetryAfter() time.Time {
	return c.retryAfter
}

func (c *Client) Detect() error {
	log.Info(c.ChainName+"链节点检测", zap.Any("nodeURL", c.Url))
	_, err := c.GetBlockNumber()
	return err
}

func (c *Client) URL() string {
	return c.Url
}

func (c *Client) call(id int, method string, out interface{}, params []interface{}, timeDuration ...*time.Duration) error {
	var timeout *time.Duration
	if len(timeDuration) > 0 {
		timeout = timeDuration[0]
	}
	header, err := httpclient.JsonrpcRequest(c.Url, id, JSONRPC, method, out, params, timeout, nil)
	if err != nil {
		c.ParseRetryAfter(header)
	}
	return err
}

type SolanaBalance struct {
	Context struct {
		ApiVersion string `json:"apiVersion"`
		Slot       int64  `json:"slot"`
	}
	Value int64 `json:"value"`
}

func (c *Client) GetBalance(address string) (string, error) {
	method := "getBalance"
	params := []interface{}{address}
	out := &SolanaBalance{}
	timeoutMS := 3_000 * time.Millisecond
	err := c.call(JSONID, method, out, params, &timeoutMS)
	if err != nil {
		return "", err
	}
	balance := fmt.Sprintf("%d", out.Value)
	return utils.UpdateDecimals(balance, SOLANA_DECIMALS), nil
}

type SolanaTokenAccount struct {
	Context struct {
		APIVersion string `json:"apiVersion"`
		Slot       int    `json:"slot"`
	} `json:"context"`
	Value []struct {
		Account struct {
			Data struct {
				Parsed struct {
					Info struct {
						IsNative    bool   `json:"isNative"`
						Mint        string `json:"mint"`
						Owner       string `json:"owner"`
						State       string `json:"state"`
						TokenAmount struct {
							Amount         string  `json:"amount"`
							Decimals       int     `json:"decimals"`
							UIAmount       float64 `json:"uiAmount"`
							UIAmountString string  `json:"uiAmountString"`
						} `json:"tokenAmount"`
					} `json:"info"`
					Type string `json:"type"`
				} `json:"parsed"`
				Program string `json:"program"`
				Space   int    `json:"space"`
			} `json:"data"`
			Executable bool   `json:"executable"`
			Lamports   int    `json:"lamports"`
			Owner      string `json:"owner"`
			RentEpoch  int    `json:"rentEpoch"`
		} `json:"account"`
		Pubkey string `json:"pubkey"`
	} `json:"value"`
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	method := "getTokenAccountsByOwner"
	params := []interface{}{address, map[string]string{"mint": tokenAddress}, map[string]string{"encoding": "jsonParsed"}}
	out := &SolanaTokenAccount{}
	timeoutMS := 3_000 * time.Millisecond
	err := c.call(JSONID, method, out, params, &timeoutMS)
	if err != nil {
		return "", err
	}

	if len(out.Value) > 0 {
		return out.Value[0].Account.Data.Parsed.Info.TokenAmount.UIAmountString, nil
	}
	return "0", nil
}

func (c *Client) GetBlockHeight() (uint64, error) {
	height, err := c.GetSlotNumber()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

func (c *Client) GetBlockNumber() (int, error) {
	method := "getBlockHeight"
	var out int
	timeoutMS := 3_000 * time.Millisecond
	err := c.call(JSONID, method, &out, nil, &timeoutMS)
	return out, err
}

func (c *Client) GetSlotNumber() (int, error) {
	method := "getSlot"
	var out int
	timeoutMS := 3_000 * time.Millisecond
	err := c.call(JSONID, method, &out, nil, &timeoutMS)
	return out, err
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	defer func() {
		snap := c.stat.Peek()
		if snap.Total%10 == 0 {
			log.Info(
				"RETRIEVE BLOCK ELAPSED",
				zap.String("nodeUrl", c.Url),
				zap.String("avg", snap.Mean.String()),
				zap.String("max", snap.Max.String()),
				zap.String("min", snap.Min.String()),
				zap.Uint64("total", snap.Total),
				zap.Float64("successRate", float64(snap.Success)/float64(snap.Total)),
			)
		}
	}()
	start := time.Now()

	block, err := c.GetBlockByNumber(int(height))
	if err != nil {
		if strings.Contains(fmt.Sprintf("%v", err), " was skipped, or missing ") ||
			strings.HasPrefix(fmt.Sprintf("%v", err), "Block not available for slot") {
			return nil, common.NotFound
		}

		log.Debug(
			"RETRIEVED BLOCK FROM CHAIN FAILED WITH ERROR",
			zap.String("chainName", c.ChainName),
			zap.Uint64("height", height),
			zap.String("nodeUrl", c.Url),
			zap.String("elapsed", time.Now().Sub(start).String()),
			zap.Error(err),
		)
		c.stat.Put(time.Since(start), false)
		return nil, err
	}
	c.stat.Put(time.Since(start), true)
	if block == nil {
		return nil, errors.New("request slot " + strconv.Itoa(int(height)) + " result is null")
	}

	var transactions []*chain.Transaction
	transactionList := block.Transactions
	for _, transaction := range transactionList {
		transactions = append(transactions, &chain.Transaction{
			BlockNumber: height,
			Hash:        transaction.Transaction.Signatures[0],
			Raw:         transaction,
		})
	}
	chainBlock := &chain.Block{
		Hash:         block.Blockhash,
		ParentHash:   block.PreviousBlockhash,
		Number:       height,
		Nonce:        0,
		BaseFee:      "",
		Time:         int64(block.BlockTime),
		Transactions: transactions,
		Raw:          block.BlockHeight,
	}
	return chainBlock, nil
}

type Block struct {
	BlockHeight       int                `json:"blockHeight"`
	BlockTime         int                `json:"blockTime"`
	Blockhash         string             `json:"blockhash"`
	ParentSlot        int                `json:"parentSlot"`
	PreviousBlockhash string             `json:"previousBlockhash"`
	Transactions      []*TransactionInfo `json:"transactions"`
}

func (c *Client) GetBlockByNumber(number int) (*Block, error) {
	method := "getBlock"
	params := []interface{}{number, map[string]interface{}{"encoding": "jsonParsed", "transactionDetails": "full", "maxSupportedTransactionVersion": 0, "rewards": false}}
	result := &Block{}
	timeoutMS := 20_000 * time.Millisecond
	err := c.call(JSONID, method, result, params, &timeoutMS)
	return result, err
}

type TransactionInfo struct {
	BlockTime int64 `json:"blockTime"`
	Meta      struct {
		ComputeUnitsConsumed int         `json:"computeUnitsConsumed"`
		Err                  interface{} `json:"err"`
		Fee                  int64       `json:"fee"`
		InnerInstructions    []struct {
			Index        int `json:"index"`
			Instructions []struct {
				/*Parsed *struct {
					Info map[string]interface{} `json:"info"`
					Type string                 `json:"type"`
				} `json:"parsed,omitempty"`*/
				Parsed    interface{} `json:"parsed"`
				Program   string      `json:"program"`
				ProgramId string      `json:"programId"`

				Accounts []string `json:"accounts,omitempty"`
				Data     string   `json:"data"`
				//ProgramId string `json:"programId"`
			} `json:"instructions"`
		} `json:"innerInstructions"`
		LogMessages       []string   `json:"logMessages"`
		PostBalances      []*big.Int `json:"postBalances"`
		PostTokenBalances []struct {
			AccountIndex  int    `json:"accountIndex"`
			Mint          string `json:"mint"`
			Owner         string `json:"owner"`
			ProgramId     string `json:"programId"`
			UiTokenAmount struct {
				Amount         string  `json:"amount"`
				Decimals       int     `json:"decimals"`
				UiAmount       float64 `json:"uiAmount"`
				UiAmountString string  `json:"uiAmountString"`
			} `json:"uiTokenAmount"`
		} `json:"postTokenBalances"`
		PreBalances      []*big.Int `json:"preBalances"`
		PreTokenBalances []struct {
			AccountIndex  int    `json:"accountIndex"`
			Mint          string `json:"mint"`
			Owner         string `json:"owner"`
			ProgramId     string `json:"programId"`
			UiTokenAmount struct {
				Amount         string  `json:"amount"`
				Decimals       int     `json:"decimals"`
				UiAmount       float64 `json:"uiAmount"`
				UiAmountString string  `json:"uiAmountString"`
			} `json:"uiTokenAmount"`
		} `json:"preTokenBalances"`
		Rewards []interface{} `json:"rewards"`
		Status  struct {
			Ok  interface{} `json:"Ok"`
			Err interface{} `json:"Err"`
		} `json:"status"`
	} `json:"meta"`
	Slot        int `json:"slot"`
	Transaction struct {
		Message struct {
			AccountKeys []struct {
				Pubkey   string `json:"pubkey"`
				Signer   bool   `json:"signer"`
				Source   string `json:"source"`
				Writable bool   `json:"writable"`
			} `json:"accountKeys"`
			AddressTableLookups interface{} `json:"addressTableLookups"`
			Instructions        []struct {
				/*Parsed *struct {
					Info map[string]interface{} `json:"info"`
					Type string                 `json:"type"`
				} `json:"parsed,omitempty"`*/
				Parsed    interface{} `json:"parsed"`
				Program   string      `json:"program"`
				ProgramId string      `json:"programId"`

				Accounts []string `json:"accounts,omitempty"`
				Data     string   `json:"data"`
				//ProgramId string `json:"programId"`
			} `json:"instructions"`
			RecentBlockhash string `json:"recentBlockhash"`
		} `json:"message"`
		Signatures []string `json:"signatures"`
	} `json:"transaction"`
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	transaction, err := c.GetTransactionByHash(txHash)
	if err != nil {
		return nil, err
	}
	if transaction == nil {
		return nil, err
	}
	tx := &chain.Transaction{}
	tx.BlockNumber = uint64(transaction.Slot)
	tx.Hash = txHash
	tx.Raw = transaction
	return tx, err
}

func (c *Client) GetTransactionByHash(txHash string) (*TransactionInfo, error) {
	method := "getTransaction"
	var result *TransactionInfo
	params := []interface{}{txHash, map[string]interface{}{"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}}
	err := c.call(JSONID, method, &result, params)
	return result, err
}
