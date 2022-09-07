package ethereum

import (
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"
	"math/big"
)

type Client struct {
	*ethclient.Client
	URL string
}

func NewClient(rawUrl string) (Client, error) {
	client, err := ethclient.Dial(rawUrl)
	if err != nil {
		log.Error("new client error:", zap.Error(err), zap.Any("url", rawUrl))
		return Client{}, err
	}
	return Client{client, rawUrl}, nil
}

//3
func (c *Client) GetBlockNumber(ctx context.Context) (uint64, error) {
	return c.BlockNumber(ctx)
}

func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*types2.Block, error) {
	return c.BlockByNumber(ctx, number)
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

//2
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types2.Receipt, error) {
	return c.TransactionReceipt(ctx, txHash)
}

//1
func (c *Client) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*types2.Transaction, bool, error) {
	return c.TransactionByHash(ctx, txHash)
}

//client.TransactionSender(ctx,tx,block.Hash(),r.TransactionIndex)
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
	rpcClient, err := rpc.DialHTTP(c.URL)
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
		bi := new(big.Int)
		bi.SetBytes(common.FromHex(*hexAmount))
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
