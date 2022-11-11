// Logics copied from go-ethereum to support Arbitrum.
// More details to see:
//
// - https://gitlab.bixin.com/mili/go-ethereum/-/merge_requests/5
// - https://gitlab.bixin.com/mili/go-ethereum/-/merge_requests/3
package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type rpcBlock struct {
	Hash common.Hash `json:"hash"`
	// Transactions []rpcTransaction `json:"transactions"`
	Transactions []json.RawMessage `json:"transactions"`
	UncleHashes  []common.Hash     `json:"uncles"`
}

func (ec *Client) getBlock(ctx context.Context, method string, args ...interface{}) (*types.Block, error) {
	var raw json.RawMessage
	err := ec.c.CallContext(ctx, &raw, method, args...)
	if err != nil {
		return nil, err
	} else if len(raw) == 0 {
		return nil, ethereum.NotFound
	}
	// Decode header and transactions.
	var head *types.Header
	var body rpcBlock
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}

	// Arbitrum 支持了一些内部的交易类型，参见：
	// https://github.com/OffchainLabs/go-ethereum/blob/382f6cd90f60fc082b300ec464dcbabb7e3279ac/core/types/transaction.go#L43-L56
	// 这里为了兼容现存的 EVM 链，忽略这些交易类型，下面将非 EVM 标准交易类型忽略：
	// 1. 首先将 rpcBlock  中的 rawTransction 不进行解析；
	// 2. 然后过滤掉不支持的交易类型通过  json.Unmarshal 进行解码。
	//
	// 避免 ErrTxTypeNotSupported，参见：
	// https://gitlab.bixin.com/mili/go-ethereum/-/blob/master/core/types/transaction.go#L188
	rpcTxs := make([]rpcTransaction, 0, len(body.Transactions))
	for _, rawTx := range body.Transactions {
		var tx rpcTransaction
		err := json.Unmarshal(rawTx, &tx)
		if err == types.ErrTxTypeNotSupported {
			continue
		}
		if err != nil {
			return nil, err
		}
		rpcTxs = append(rpcTxs, tx)
	}

	// Quick-verify transaction and uncle lists. This mostly helps with debugging the server.
	if head.UncleHash == types.EmptyUncleHash && len(body.UncleHashes) > 0 {
		return nil, fmt.Errorf("server returned non-empty uncle list but block header indicates no uncles")
	}
	if head.UncleHash != types.EmptyUncleHash && len(body.UncleHashes) == 0 {
		return nil, fmt.Errorf("server returned empty uncle list but block header indicates uncles")
	}
	if head.TxHash == types.EmptyRootHash && len(rpcTxs) > 0 {
		return nil, fmt.Errorf("server returned non-empty transaction list but block header indicates no transactions")
	}
	if head.TxHash != types.EmptyRootHash && len(rpcTxs) == 0 {
		return nil, fmt.Errorf("server returned empty transaction list but block header indicates transactions")
	}
	// Load uncles because they are not included in the block response.
	var uncles []*types.Header
	if len(body.UncleHashes) > 0 {
		uncles = make([]*types.Header, len(body.UncleHashes))
		reqs := make([]rpc.BatchElem, len(body.UncleHashes))
		for i := range reqs {
			reqs[i] = rpc.BatchElem{
				Method: "eth_getUncleByBlockHashAndIndex",
				Args:   []interface{}{body.Hash, hexutil.EncodeUint64(uint64(i))},
				Result: &uncles[i],
			}
		}
		if err := ec.c.BatchCallContext(ctx, reqs); err != nil {
			return nil, err
		}
		for i := range reqs {
			if reqs[i].Error != nil {
				return nil, reqs[i].Error
			}
			if uncles[i] == nil {
				return nil, fmt.Errorf("got null header for uncle %d of block %x", i, body.Hash[:])
			}
		}
	}
	// Fill the sender cache of transactions in the block.
	txs := make([]*types.Transaction, len(rpcTxs))
	for i, tx := range rpcTxs {
		if tx.From != nil {
			setSenderFromServer(tx.tx, *tx.From, body.Hash)
		}
		txs[i] = tx.tx
	}
	return types.NewBlockWithHeader(head).WithBody(txs, uncles), nil
}

type rpcTransaction struct {
	tx *types.Transaction
	txExtraInfo
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

func (tx *rpcTransaction) UnmarshalJSON(msg []byte) error {
	if err := json.Unmarshal(msg, &tx.tx); err != nil {
		return err
	}
	return json.Unmarshal(msg, &tx.txExtraInfo)
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	pending := big.NewInt(-1)
	if number.Cmp(pending) == 0 {
		return "pending"
	}
	return hexutil.EncodeBig(number)
}
