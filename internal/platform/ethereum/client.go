package ethereum

import (
	"block-crawling/internal/log"
	"context"
	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"math/big"
)

type Client struct {
	*ethclient.Client
}

func NewClient(rawUrl string) (Client, error) {
	client, err := ethclient.Dial(rawUrl)
	if err != nil {
		log.Error("new client error:", zap.Error(err), zap.Any("url", rawUrl))
		return Client{}, err
	}
	return Client{client}, nil
}

//3
func (c *Client) GetBlockNumber(ctx context.Context) (uint64, error) {
	return c.BlockNumber(ctx)
}

func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*types2.Block, error) {
	return c.BlockByNumber(ctx, number)
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
