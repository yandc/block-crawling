package tron

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"math/big"
	"net/http"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

var (
	errPendingTx             = errors.New("tx is still pending")
	errIrrationalBlockNumber = errors.New("block number got from chain is less or equal to 0")
)

type Client struct {
	*common.NodeDefaultIn

	url    string
	client *http.Client
}

func NewClient(rawUrl string, chainName string) Client {
	return Client{
		url:    rawUrl,
		client: http.DefaultClient,
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

func (c *Client) GetBlock(height uint64) (block *chain.Block, err error) {
	rawBlock, err := c.GetBlockByNum(int(height))
	if err != nil {
		return nil, err
	}
	var blockNumber uint64
	if rawBlock.BlockHeader.RawData.Number <= 0 {
		log.Debug(
			"GOT IRRATIONAL BLOCK NUMBER",
			zap.String("chainName", c.ChainName),
			zap.Int("gotBlockNumber", rawBlock.BlockHeader.RawData.Number),
			zap.Uint64("expectedBlockNumber", height),
			zap.String("nodeURL", c.url),
		)
		blockNumber = height
	} else {
		blockNumber = uint64(rawBlock.BlockHeader.RawData.Number)
	}
	txs := make([]*chain.Transaction, 0, len(rawBlock.Transactions))
	for _, tx := range rawBlock.Transactions {
		meta := parseTxMeta(&tx)
		txs = append(txs, &chain.Transaction{
			Hash:        tx.TxID,
			BlockNumber: blockNumber,
			TxType:      chain.TxType(meta.txType),
			FromAddress: meta.fromAddr,
			ToAddress:   meta.toAddr,
			Value:       decimal.NewFromInt(int64(meta.amount)).String(),
			Raw: &rawTxWrapper{
				BlockTx:         tx,
				tokenAmount:     meta.tokenAmount,
				contractAddress: meta.contractAddress,
			},
			Record: nil,
		})
	}
	return &chain.Block{
		Hash:         rawBlock.BlockID,
		ParentHash:   rawBlock.BlockHeader.RawData.ParentHash,
		Number:       blockNumber,
		Time:         rawBlock.BlockHeader.RawData.Timestamp,
		Raw:          rawBlock,
		Transactions: txs,
	}, nil
}

type rawTxWrapper struct {
	types.BlockTx

	tokenAmount     string
	contractAddress string
}

type txMeta struct {
	txType          string
	amount          int
	fromAddr        string
	toAddr          string
	tokenAmount     string
	contractAddress string
}

func parseTxMeta(tx *types.BlockTx) *txMeta {
	txType := "native"
	value := tx.RawData.Contract[0].Parameter.Value
	fromAddress := value.OwnerAddress
	var toAddress, contractAddress string
	tokenAmount := "0"
	var amount int
	if value.ContractAddress != "" && len(value.Data) >= 136 {
		methodId := value.Data[:8]
		if methodId == "a9059cbb" {
			txType = "transfer"
			contractAddress = value.ContractAddress
			toAddress = utils.TronHexToBase58(ADDRESS_PREFIX + value.Data[32:72])
			banInt, b := new(big.Int).SetString(value.Data[72:], 16)
			if b {
				tokenAmount = banInt.String()
			}
		}
	} else {
		toAddress = value.ToAddress
		amount = value.Amount
	}
	return &txMeta{
		txType:          txType,
		amount:          amount,
		fromAddr:        fromAddress,
		toAddr:          toAddress,
		tokenAmount:     tokenAmount,
		contractAddress: contractAddress,
	}
}

// GetTxByHash get transaction by given tx hash.
// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	rawTx, err := c.GetTransactionInfoByHash(txHash)
	if err != nil {
		return nil, err
	}

	isPending := len(rawTx.ContractResult) == 0
	if isPending {
		// TxHandler.OnDroppedTx will be invoked.
		return nil, errPendingTx
	}

	result := rawTx.Receipt.Result
	if result != "" && result != "SUCCESS" {
		// TxHandler.OnDroppedTx will be invoked.
		return nil, nil
	}
	if rawTx.BlockNumber <= 0 {
		return nil, errIrrationalBlockNumber
	}
	return &chain.Transaction{
		Hash:        txHash,
		BlockNumber: uint64(rawTx.BlockNumber),
		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",
		Raw:         rawTx,
		Record:      nil,
	}, nil

}

func (c *Client) GetBalance(address string) (string, error) {
	url := c.url + "/wallet/getaccount"
	reqBody := types.BalanceReq{
		Address: address,
		Visible: true,
	}
	out := &types.TronBalance{}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return "", err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return "", errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return "", err
			} else {
				return "", errors.New(e)
			}
		}
	}

	balance := utils.BigIntString(new(big.Int).SetInt64(out.Balance), 6)
	return balance, nil
}

func (c *Client) GetTokenBalance(ownerAddress string, contractAddress string, decimal int) (string, error) {
	url := c.url + "/wallet/triggerconstantcontract"
	addrB := Base58ToHex(ownerAddress)
	parameter := "0000000000000000000000000000000000000000000000000000000000000000"[len(addrB):] + addrB
	out := &types.TronTokenBalanceRes{}
	reqBody := types.TronTokenBalanceReq{
		OwnerAddress:     ownerAddress,
		ContractAddress:  contractAddress,
		FunctionSelector: "balanceOf(address)",
		Parameter:        parameter,
		Visible:          true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return "0", err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return "0", errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return "0", err
			} else {
				return "0", errors.New(e)
			}
		}
	}

	tokenBalance := "0"
	if len(out.ConstantResult) > 0 {
		banInt, b := new(big.Int).SetString(out.ConstantResult[0], 16)
		if b {
			tokenBalance = utils.BigIntString(banInt, decimal)
		}
	}
	return tokenBalance, err
}

func (c *Client) GetBlockHeight() (uint64, error) {
	url := c.url + "/wallet/getnowblock"
	out := &types.NowBlock{}
	err := httpclient.HttpsForm(url, http.MethodGet, nil, nil, out)
	if err != nil {
		return 0, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return 0, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return 0, err
			} else {
				return 0, errors.New(e)
			}
		}
	}

	return uint64(out.BlockHeader.RawData.Number), nil
}

func (c *Client) GetBlockByNum(num int) (*types.BlockResponse, error) {
	url := c.url + "/wallet/getblockbynum"
	out := &types.BlockResponse{}
	reqBody := types.BlockReq{
		Num:     num,
		Visible: true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return nil, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return nil, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return nil, err
			} else {
				return nil, errors.New(e)
			}
		}
	}

	return out, err
}

func (c *Client) GetTransactionInfoByHash(txHash string) (*types.TronTxInfoResponse, error) {
	url := c.url + "/wallet/gettransactioninfobyid"
	out := &types.TronTxInfoResponse{}
	reqBody := types.TronTxReq{
		Value:   txHash,
		Visible: true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return nil, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return nil, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return nil, err
			} else {
				return nil, errors.New(e)
			}
		}
	}

	return out, err
}
