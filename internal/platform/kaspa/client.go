package kaspa

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"gitlab.bixin.com/mili/node-driver/chain"
	"strconv"
	"strings"
	"time"
)

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

func (c *Client) Detect() error {
	return nil
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) GetBlockHeight() (uint64, error) {
	return 0, nil
}

type BlockInfo struct {
	Header struct {
		Version              int    `json:"version"`
		HashMerkleRoot       string `json:"hashMerkleRoot"`
		AcceptedIdMerkleRoot string `json:"acceptedIdMerkleRoot"`
		UtxoCommitment       string `json:"utxoCommitment"`
		Timestamp            string `json:"timestamp"`
		Bits                 int    `json:"bits"`
		Nonce                string `json:"nonce"`
		DaaScore             string `json:"daaScore"`
		BlueWork             string `json:"blueWork"`
		Parents              []struct {
			ParentHashes []string `json:"parentHashes"`
		} `json:"parents"`
		BlueScore    string `json:"blueScore"`
		PruningPoint string `json:"pruningPoint"`
	} `json:"header"`
	Transactions []struct {
		Inputs []struct {
			PreviousOutpoint struct {
				TransactionId string `json:"transactionId"`
				Index         int    `json:"index"`
			} `json:"previousOutpoint"`
			SignatureScript string `json:"signatureScript"`
			SigOpCount      int    `json:"sigOpCount"`
		} `json:"inputs"`
		Outputs []struct {
			Amount          interface{} `json:"amount"` //有时候是int，有时候是string
			ScriptPublicKey struct {
				ScriptPublicKey string `json:"scriptPublicKey"`
			} `json:"scriptPublicKey"`
			VerboseData struct {
				ScriptPublicKeyType    string `json:"scriptPublicKeyType"`
				ScriptPublicKeyAddress string `json:"scriptPublicKeyAddress"`
			} `json:"verboseData"`
		} `json:"outputs"`
		SubnetworkId string `json:"subnetworkId"`
		VerboseData  struct {
			TransactionId string      `json:"transactionId"`
			Hash          string      `json:"hash"`
			Mass          string      `json:"mass"`
			BlockHash     interface{} `json:"blockHash"` //有时候是string，有时候是[]string
			BlockTime     interface{} `json:"blockTime"` //有时候是int，有时候是string
		} `json:"verboseData"`
	} `json:"transactions"`
	VerboseData struct {
		Hash                string   `json:"hash"`
		Difficulty          float64  `json:"difficulty"`
		SelectedParentHash  string   `json:"selectedParentHash"`
		TransactionIds      []string `json:"transactionIds"`
		BlueScore           string   `json:"blueScore"`
		ChildrenHashes      []string `json:"childrenHashes"`
		MergeSetBluesHashes []string `json:"mergeSetBluesHashes"`
		MergeSetRedsHashes  []string `json:"mergeSetRedsHashes"`
		IsChainBlock        bool     `json:"isChainBlock"`
	} `json:"verboseData"`
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	return nil, nil
}

func (c *Client) GetBlockByHash(blockHash string) (*chain.Block, error) {
	url := fmt.Sprintf("%s/blocks/%s", c.url, blockHash)
	out := &BlockInfo{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, out, &timeoutMS)
	if err != nil {
		return nil, err
	}
	txs := make([]*chain.Transaction, 0, len(out.Transactions))
	height, _ := strconv.Atoi(out.VerboseData.BlueScore)
	for i, rawTx := range out.Transactions {
		if i == 0 {
			continue
		}
		txs = append(txs, &chain.Transaction{
			Hash:        rawTx.VerboseData.TransactionId,
			BlockNumber: uint64(height),
			//TxType:      chain.TxType(blockHash),
			//FromAddress: rawTx.Sender,
			ToAddress: "",
			Value:     "",
			Raw:       rawTx,
			Record:    nil,
		})
	}
	blkTime, _ := strconv.ParseInt(out.Header.Timestamp, 10, 64)
	blkTime /= 1000

	return &chain.Block{
		ParentHash:   out.VerboseData.SelectedParentHash,
		Hash:         blockHash,
		Number:       uint64(height),
		Time:         blkTime,
		Raw:          out,
		Transactions: txs,
	}, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	rawTx, err := c.GetTransactionByHash(txHash)
	if err != nil {
		if err.Error() == "Transaction not found" || strings.Contains(err.Error(), "string does not match regex \"[a-f0-9]{64}\"") {
			return nil, common.TransactionNotFound
		}
		return nil, err
	}
	if len(rawTx.Inputs) > 0 {
		for _, input := range rawTx.Inputs {
			preRawTx, err := c.GetTransactionByHash(input.PreviousOutpointHash)
			if err != nil {
				if err.Error() == "Transaction not found" {
					return nil, common.TransactionNotFound
				}
				return nil, err
			}
			index, _ := strconv.Atoi(input.PreviousOutpointIndex)
			preOutputs := preRawTx.Outputs[index]
			input.PreviousOutpointAmount = preOutputs.Amount
			input.PreviousOutpointScriptPublicKeyAddress = preOutputs.ScriptPublicKeyAddress
		}
	}
	return &chain.Transaction{
		Hash:        txHash,
		Nonce:       0,
		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",
		Raw:         rawTx,
		Record:      nil,
	}, nil
}

type KaspaBadResp struct {
	/*Detail struct {
		Message string `json:"message"`
	} `json:"detail"`*/
	Detail interface{} `json:"detail"`
}

type KaspaBalanceResp struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
	KaspaBadResp
}

func (c *Client) GetBalance(address string) (string, error) {
	url := fmt.Sprintf("%s/addresses/%s/balance", c.url, address)
	out := &KaspaBalanceResp{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, out, &timeoutMS)
	if err != nil {
		return "", err
	}
	if out.Detail != nil {
		return "", errors.New(utils.GetString(out.Detail))
	}
	balance := utils.StringDecimals(strconv.Itoa(int(out.Balance)), 8)
	return balance, err
}

type KaspaUtxoResp struct {
	Address  string `json:"address"`
	Outpoint struct {
		TransactionId string `json:"transactionId"`
		Index         int    `json:"index"`
	} `json:"outpoint"`
	UtxoEntry struct {
		Amount          string `json:"amount"`
		ScriptPublicKey struct {
			ScriptPublicKey string `json:"scriptPublicKey"`
		} `json:"scriptPublicKey"`
		BlockDaaScore string `json:"blockDaaScore"`
	} `json:"utxoEntry"`
}

func (c *Client) GetUtxo(address string) ([]*KaspaUtxoResp, error) {
	url := fmt.Sprintf("%s/addresses/%s/utxos", c.url, address)
	out := []*KaspaUtxoResp{}
	timeoutMS := 20_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, &out, &timeoutMS)
	if err != nil {
		return nil, err
	}
	return out, err
}

func (c *Client) GetTransactionByHash(txHash string) (*types.KaspaTransactionInfo, error) {
	url := fmt.Sprintf("%s/transactions/%s", c.url, txHash)
	out := &types.KaspaTransactionInfo{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, out, &timeoutMS)
	if err != nil {
		return nil, err
	}
	if out.Detail != nil {
		return nil, errors.New(utils.GetString(out.Detail))
	}
	return out, nil
}
