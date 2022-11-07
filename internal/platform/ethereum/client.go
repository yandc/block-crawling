package ethereum

import (
	icommon "block-crawling/internal/common"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/metachris/eth-go-bindings/erc20"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Client struct {
	*ethclient.Client
	*pcommon.NodeDefaultIn

	url       string
	chainName string
}

func NewClient(rawUrl string, chainName string) (*Client, error) {
	client, err := ethclient.Dial(rawUrl)
	if err != nil {
		log.Error("new client error:", zap.Error(err), zap.Any("url", rawUrl))
		return &Client{}, err
	}
	return &Client{
		Client: client,
		NodeDefaultIn: &pcommon.NodeDefaultIn{
			ChainName: chainName,
		},
		url:       rawUrl,
		chainName: chainName,
	}, nil
}

func (c *Client) Detect() error {
	_, err := c.GetBlockNumber(context.Background())
	return err
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) GetBlockHeight() (uint64, error) {
	start := time.Now()
	height, err := c.GetBlockNumber(context.Background())
	if err != nil {
		return 0, err
	}
	log.Debug(
		"RETRIEVED CHAIN HEIGHT FROM NODE",
		zap.Uint64("height", height),
		zap.String("nodeUrl", c.url),
		zap.String("chainName", c.chainName),
		zap.String("elapsed", time.Now().Sub(start).String()),
	)
	return height, nil
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	start := time.Now()

	block, err := c.GetBlockByNumber(context.Background(), big.NewInt(int64(height)))
	if err != nil {
		if err == ethereum.NotFound && isNonstandardEVM(c.chainName) {
			return nil, errors.New("block not found") // retry on next node
		}

		log.Debug(
			"RETRIEVED BLOCK FROM CHAIN FAILED WITH ERROR",
			zap.String("chainName", c.chainName),
			zap.Uint64("height", height),
			zap.String("nodeUrl", c.url),
			zap.String("elapsed", time.Now().Sub(start).String()),
			zap.Error(err),
		)
		return nil, err
	}
	txs := make([]*chain.Transaction, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		txc := &chain.Transaction{
			Hash:        tx.Hash().String(),
			Nonce:       tx.Nonce(),
			BlockNumber: height,
			Raw:         tx,
		}
		c.parseTxMeta(txc, tx)
		txs = append(txs, txc)
	}
	var baseFee string
	if block.BaseFee() != nil {
		baseFee = block.BaseFee().String()
	}
	return &chain.Block{
		Hash:         block.Hash().Hex(),
		ParentHash:   block.ParentHash().Hex(),
		Number:       block.NumberU64(),
		Nonce:        block.Nonce(),
		BaseFee:      baseFee,
		Time:         int64(block.Time()),
		Transactions: txs,
	}, nil
}

func (c *Client) parseTxMeta(txc *chain.Transaction, tx *types2.Transaction) (err error) {
	var from common.Address
	var toAddress string
	from, err = types2.Sender(types2.NewLondonSigner(tx.ChainId()), tx)
	if err != nil {
		from, err = types2.Sender(types2.HomesteadSigner{}, tx)
	}
	if tx.To() != nil {
		toAddress = tx.To().String()
	}
	fromAddress := from.String()
	value := tx.Value().String()
	transactionType := "native"
	if len(tx.Data()) >= 68 && tx.To() != nil {
		methodId := hex.EncodeToString(tx.Data()[:4])
		if methodId == "a9059cbb" || methodId == "095ea7b3" {
			toAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[4:36])).String()
			amount := new(big.Int).SetBytes(tx.Data()[36:])
			if methodId == "a9059cbb" {
				transactionType = "transfer"
			} else {
				transactionType = "approve"
			}
			value = amount.String()
		} else if methodId == "23b872dd" {
			fromAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[36:68])).String()
			amount := new(big.Int).SetBytes(tx.Data()[68:])
			transactionType = "transferfrom"
			value = amount.String()
		}
	}
	txc.FromAddress = fromAddress
	txc.ToAddress = toAddress
	txc.TxType = chain.TxType(transactionType)
	txc.Value = value
	return nil
}

// 3
func (c *Client) GetBlockNumber(ctx context.Context) (uint64, error) {
	return c.BlockNumber(ctx)
}

func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*types2.Block, error) {
	return c.BlockByNumber(ctx, number)
}

// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	txHash = strings.Split(txHash, "#")[0]
	txByHash, _, err := c.GetTransactionByHash(context.Background(), common.HexToHash(txHash))

	if err != nil && err != ethereum.NotFound {
		log.Error(c.chainName+"查询链上数据失败", zap.Any("txHash", txHash), zap.Any("error", err))
		return nil, err
	}

	if txByHash == nil {
		// 兜底
		return nil, nil
	}
	receipt, err := c.GetTransactionReceipt(context.Background(), txByHash.Hash())

	if err != nil && err != ethereum.NotFound {
		log.Error(c.chainName+"查询链上 Receipt 数据失败", zap.Any("txHash", txHash), zap.Any("error", err))
		return nil, err
	}

	if receipt == nil {
		return nil, nil
	}
	intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
	tx = &chain.Transaction{
		Hash:        txByHash.Hash().Hex(),
		Nonce:       uint64(txByHash.Nonce()),
		BlockNumber: uint64(intBlockNumber.Uint64()),

		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",

		Raw:    receipt,
		Record: nil,
	}
	c.parseTxMeta(tx, txByHash)
	return tx, nil
}

func (c *Client) GetBalance(address string) (string, error) {
	account := common.HexToAddress(address)
	balance, err := c.BalanceAt(context.Background(), account, nil)
	if err != nil {
		return "", err
	}
	ethValue := utils.BigIntString(balance, 18)
	return ethValue, err
}

// Receipt represents the results of a transaction.
type Receipt struct {
	// Consensus fields: These fields are defined by the Yellow Paper
	Type              string        `json:"type,omitempty"`
	PostState         string        `json:"root,omitempty"`
	Status            string        `json:"status,omitempty"`
	From              string        `json:"from,omitempty"`
	To                string        `json:"to,omitempty"`
	CumulativeGasUsed string        `json:"cumulativeGasUsed" gencodec:"required"`
	EffectiveGasPrice string        `json:"effectiveGasPrice,omitempty"`
	Bloom             string        `json:"logsBloom" gencodec:"required"`
	Logs              []*types2.Log `json:"logs" gencodec:"required"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          string `json:"transactionHash" gencodec:"required"`
	ContractAddress string `json:"contractAddress,omitempty"`
	GasUsed         string `json:"gasUsed" gencodec:"required"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        string `json:"blockHash,omitempty"`
	BlockNumber      string `json:"blockNumber,omitempty"`
	TransactionIndex string `json:"transactionIndex,omitempty"`
}

// 2
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*Receipt, error) {
	//return c.TransactionReceipt(ctx, txHash)
	var r *Receipt
	rpcClient, err := rpc.DialHTTP(c.url)
	err = rpcClient.CallContext(ctx, &r, "eth_getTransactionReceipt", txHash)
	if err == nil {
		if r == nil {
			return nil, ethereum.NotFound
		}
	}
	return r, err
}

// 1
func (c *Client) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*types2.Transaction, bool, error) {
	return c.TransactionByHash(ctx, txHash)
}

// client.TransactionSender(ctx,tx,block.Hash(),r.TransactionIndex)
func (c *Client) GetTransactionSender(ctx context.Context, tx *types2.Transaction, block common.Hash,
	index uint) (common.Address, error) {
	return c.TransactionSender(ctx, tx, block, index)
}

func (c *Client) BatchTokenBalance(address string, tokenMap map[string]int) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	destAddress := common.HexToAddress(address)
	balanceFun := []byte("balanceOf(address)")
	hash := crypto.NewKeccakState()
	hash.Write(balanceFun)
	methodID := hash.Sum(nil)[:4]
	rpcClient, err := rpc.DialHTTP(c.url)
	if err != nil {
		return result, err
	}
	var tokenAddrs []string
	var be []rpc.BatchElem
	for token, _ := range tokenMap {
		var data []byte
		data = append(data, methodID...)
		tokenAddress := common.HexToAddress(token)
		data = append(data, common.LeftPadBytes(destAddress.Bytes(), 32)...)
		callMsg := map[string]interface{}{
			"from": destAddress,
			"to":   tokenAddress,
			"data": hexutil.Bytes(data),
		}
		be = append(be, rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{callMsg, "latest"},
			Result: new(string),
		})
		tokenAddrs = append(tokenAddrs, token)
	}
	err = rpcClient.BatchCall(be)
	if err != nil {
		return result, err
	}
	for index, b := range be {
		token := tokenAddrs[index]
		hexAmount := b.Result.(*string)
		newHexAmount := *hexAmount
		if len(newHexAmount) > 66 {
			newHexAmount = newHexAmount[0:66]
		}
		bi := new(big.Int).SetBytes(common.FromHex(newHexAmount))
		var balance string
		if tokenMap[token] == 0 {
			balance = bi.String()
		} else {
			balance = utils.BigIntString(bi, tokenMap[token])
		}
		result[token] = balance
	}
	return result, nil
}

var EvmTokenInfoMap = make(map[string]types.TokenInfo)
var lock = icommon.NewSyncronized(0)

func (c *Client) GetEvmTokenInfo(chainName string, tokenAddress string) (types.TokenInfo, error) {
	var key = chainName + tokenAddress
	tokenInfo, ok := EvmTokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	lock.Lock(key)
	defer lock.Unlock(key)
	tokenInfo, ok = EvmTokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	erc20Token, err := erc20.NewErc20(common.HexToAddress(tokenAddress), c)
	if err != nil {
		return tokenInfo, err
	}
	decimals, err := erc20Token.Decimals(nil)
	if err != nil {
		return tokenInfo, err
	}
	symbol, err := erc20Token.Symbol(nil)
	if err != nil {
		return tokenInfo, err
	}
	tokenInfo = types.TokenInfo{Address: tokenAddress, Decimals: int64(decimals), Symbol: symbol}
	EvmTokenInfoMap[key] = tokenInfo
	return tokenInfo, nil
}
