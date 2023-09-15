// Logics copied from go-ethereum to support Arbitrum.
// More details to see:
//
// - https://gitlab.bixin.com/mili/go-ethereum/-/merge_requests/5
// - https://gitlab.bixin.com/mili/go-ethereum/-/merge_requests/3
package ethereum

import (
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// Arbitrum dapp 跨链时，提取交易，跟普通转账参数一致 20230119
const ArbitrumDepositTxType = 100

// Arbitrum dapp 跨链时，提取交易，跟普通转账参数一致 20230801
//https://arbiscan.io/tx/0xed0b45e9dc70fde48288f21fdcef0d6677e84d7387ac10d5cc5130fcc22f317d
const ArbitrumDistributeTxType = 104

// zkSync 跨链转主币ETH时，跟普通转账参数一致
//https://explorer.zksync.io/tx/0xe2e6a3fc27f5d793b50d4eb80f016fffdb7662278e0c43ab119126206c7876a2
const zkSyncTxType = 255
const zkSyncTxType113 = 113

type rpcBlock struct {
	Hash common.Hash `json:"hash"`
	// Transactions []rpcTransaction `json:"transactions"`
	Transactions []json.RawMessage `json:"transactions"`
	UncleHashes  []common.Hash     `json:"uncles"`
}

// Header represents a block header in the Ethereum blockchain.
type Header struct {
	ParentHash  common.Hash    `json:"parentHash"       gencodec:"required"`
	UncleHash   common.Hash    `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    common.Address `json:"miner"`
	Root        common.Hash    `json:"stateRoot"        gencodec:"required"`
	TxHash      common.Hash    `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash common.Hash    `json:"receiptsRoot"     gencodec:"required"`
	//Bloom       types.Bloom    `json:"logsBloom"` //`json:"logsBloom"        gencodec:"required"`
	Difficulty *big.Int    `json:"difficulty"       gencodec:"required"`
	Number     *big.Int    `json:"number"           gencodec:"required"`
	GasLimit   uint64      `json:"gasLimit"         gencodec:"required"`
	GasUsed    uint64      `json:"gasUsed"          gencodec:"required"`
	Time       uint64      `json:"timestamp"        gencodec:"required"`
	Extra      []byte      `json:"extraData"        gencodec:"required"`
	MixDigest  common.Hash `json:"mixHash"`
	Nonce      uint64      `json:"nonce"`

	// BaseFee was added by EIP-1559 and is ignored in legacy headers.
	BaseFee *big.Int `json:"baseFeePerGas" rlp:"optional"`

	/*
		TODO (MariusVanDerWijden) Add this field once needed
		// Random was added during the merge and contains the BeaconState randomness
		Random common.Hash `json:"random" rlp:"optional"`
	*/

	Hash common.Hash `json:"hash" gencodec:"required"`
}

// A BlockNonce is a 64-bit hash which proves (combined with the
// mix-hash) that a sufficient amount of computation has been carried
// out on a block.

// EncodeNonce converts the given integer to a block nonce.

// UnmarshalJSON unmarshals from JSON.
func (h *Header) UnmarshalJSON(input []byte) error {
	type Header struct {
		ParentHash *common.Hash `json:"parentHash"       gencodec:"required"`
		//UncleHash   *common.Hash    `json:"sha3Uncles"       gencodec:"required"`
		Coinbase    *common.Address `json:"miner"`
		Root        *common.Hash    `json:"stateRoot"        gencodec:"required"`
		TxHash      *common.Hash    `json:"transactionsRoot" gencodec:"required"`
		ReceiptHash *common.Hash    `json:"receiptsRoot"     gencodec:"required"`
		//Bloom       *types.Bloom    `json:"logsBloom"` //`json:"logsBloom"        gencodec:"required"`
		Difficulty      *string        `json:"difficulty"       gencodec:"required"`
		TotalDifficulty *string        `json:"totalDifficulty"`
		Number          *string        `json:"number"           gencodec:"required"`
		GasLimit        *string        `json:"gasLimit"         gencodec:"required"`
		GasUsed         *string        `json:"gasUsed"          gencodec:"required"`
		Time            *string        `json:"timestamp"        gencodec:"required"`
		Extra           *hexutil.Bytes `json:"extraData"        gencodec:"required"`
		MixDigest       *common.Hash   `json:"mixHash"`
		Nonce           *string        `json:"nonce"`
		BaseFee         *string        `json:"baseFeePerGas" rlp:"optional"`

		Hash *common.Hash `json:"hash" gencodec:"required"`
	}
	var e error
	var dec Header
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ParentHash == nil {
		return errors.New("missing required field 'parentHash' for Header")
	}
	h.ParentHash = *dec.ParentHash
	//if dec.UncleHash == nil {
	//	return errors.New("missing required field 'sha3Uncles' for Header")
	//}
	//h.UncleHash = *dec.UncleHash
	if dec.Coinbase != nil {
		h.Coinbase = *dec.Coinbase
	}
	if dec.Root == nil {
		return errors.New("missing required field 'stateRoot' for Header")
	}
	h.Root = *dec.Root
	if dec.TxHash == nil {
		return errors.New("missing required field 'transactionsRoot' for Header")
	}
	h.TxHash = *dec.TxHash
	if dec.ReceiptHash == nil {
		return errors.New("missing required field 'receiptsRoot' for Header")
	}
	h.ReceiptHash = *dec.ReceiptHash
	/*if dec.Bloom == nil {
		return errors.New("missing required field 'logsBloom' for Header")
	}*/
	//if dec.Bloom != nil {
	//	h.Bloom = *dec.Bloom
	//}

	if dec.Difficulty == nil && dec.TotalDifficulty == nil {
		return errors.New("missing required field 'difficulty' for Header")
	}
	if dec.Difficulty != nil {
		h.Difficulty, e = utils.HexStringToBigInt(*dec.Difficulty)
		if e != nil {
			return e
		}
	} else {
		h.Difficulty, e = utils.HexStringToBigInt(*dec.TotalDifficulty)
		if e != nil {
			return e
		}
	}

	if dec.Number == nil {
		return errors.New("missing required field 'number' for Header")
	}
	h.Number, e = utils.HexStringToBigInt(*dec.Number)
	if e != nil {
		return e
	}

	if dec.GasLimit == nil {
		return errors.New("missing required field 'gasLimit' for Header")
	}

	h.GasLimit, e = utils.HexStringToUint64(*dec.GasLimit)
	if e != nil {
		return e
	}

	if dec.GasUsed == nil {
		return errors.New("missing required field 'gasUsed' for Header")
	}
	h.GasUsed, e = utils.HexStringToUint64(*dec.GasUsed)
	if e != nil {
		return e
	}

	if dec.Time == nil {
		return errors.New("missing required field 'timestamp' for Header")
	}
	h.Time, e = utils.HexStringToUint64(*dec.Time)
	if e != nil {
		return e
	}

	if dec.Extra == nil {
		return errors.New("missing required field 'extraData' for Header")
	}
	h.Extra = *dec.Extra
	if dec.MixDigest != nil {
		h.MixDigest = *dec.MixDigest
	}
	if dec.Nonce != nil {
		h.Nonce, e = utils.HexStringToUint64(*dec.Nonce)
		if e != nil {
			return e
		}
	}
	if dec.BaseFee != nil {
		h.BaseFee, e = utils.HexStringToBigInt(*dec.BaseFee)
		if e != nil {
			return e
		}
	}
	if dec.Hash == nil {
		return errors.New("missing required field 'hash' for Header")
	}
	h.Hash = *dec.Hash
	return nil
}

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (c *Client) BlockByHash(ctx context.Context, hash common.Hash) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByHash", hash, true)
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByNumber", toBlockNumArg(number), true)
}

func (c *Client) getBlock(ctx context.Context, method string, args ...interface{}) (*Block, error) {
	var raw json.RawMessage
	err := c.c.CallContext(ctx, &raw, method, args...)
	if err != nil {
		return nil, err
	} else if len(raw) == 0 {
		return nil, ethereum.NotFound
	}
	// Decode header and transactions.
	var head *Header
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
	rpcTxs := make([]*Transaction, 0, len(body.Transactions))
	for _, rawTx := range body.Transactions {
		var tx *Transaction
		err := json.Unmarshal(rawTx, &tx)
		if err == types.ErrInvalidSig || err == types.ErrTxTypeNotSupported ||
			strings.Contains(fmt.Sprintf("%s", err), "missing required field 'nonce' in transaction") ||
			strings.Contains(fmt.Sprintf("%s", err), "missing required field 'v' in transaction") ||
			strings.Contains(fmt.Sprintf("%s", err), "missing required field 'r' in transaction") ||
			strings.Contains(fmt.Sprintf("%s", err), "missing required field 's' in transaction") {
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
	/*if head.UncleHash != types.EmptyUncleHash && len(body.UncleHashes) == 0 {
		return nil, fmt.Errorf("server returned empty uncle list but block header indicates uncles")
	}*/
	if head.TxHash == types.EmptyRootHash && len(rpcTxs) > 0 {
		return nil, fmt.Errorf(BLOCK_NONAL_TRANSCATION)
	}
	if head.TxHash != types.EmptyRootHash && len(rpcTxs) == 0 {
		return nil, fmt.Errorf(BLOCK_NO_TRANSCATION)
	}
	block := &Block{
		header:       head,
		transactions: rpcTxs,
		//uncles:       uncles,
	}
	return block, nil
}

// Block represents an entire block in the Ethereum blockchain.
type Block struct {
	header       *Header
	uncles       []*Header
	transactions []*Transaction

	// caches
	hash common.Hash
	size atomic.Value

	// These fields are used by package eth to track
	// inter-peer block relay.
	ReceivedAt   time.Time
	ReceivedFrom interface{}
}

// Hash returns the keccak256 hash of b's header.
// The hash is computed on the first call and cached thereafter.
func (b *Block) Hash() common.Hash {
	b.hash = b.header.Hash
	return b.hash
}

func (b *Block) Transactions() []*Transaction {
	return b.transactions
}

func (b *Block) BaseFee() *big.Int {
	if b.header.BaseFee == nil {
		return nil
	}
	return new(big.Int).Set(b.header.BaseFee)
}

func (b *Block) ParentHash() common.Hash {
	return b.header.ParentHash
}

func (b *Block) NumberU64() uint64 {
	return b.header.Number.Uint64()
}

func (b *Block) Nonce() uint64 {
	return b.header.Nonce
}

func (b *Block) Time() uint64 {
	return b.header.Time
}

// TransactionByHash returns the transaction with the given hash.
func (c *Client) TransactionByHash(ctx context.Context, hash common.Hash) (tx *Transaction, isPending bool, err error) {
	var json *Transaction
	err = c.c.CallContext(ctx, &json, "eth_getTransactionByHash", hash)
	if err != nil {
		return nil, false, err
	} else if json == nil {
		return nil, false, ethereum.NotFound
	} /* else if _, r, _ := json.RawSignatureValues(); r == nil {
		return nil, false, fmt.Errorf("server returned transaction without signature")
	}*/
	return json, json.BlockNumber == nil, nil
}

// Transaction is an Ethereum transaction.
type Transaction struct {
	inner types.TxData // Consensus contents of a transaction
	time  time.Time    // Time first seen locally (spam avoidance)

	// caches
	//hash atomic.Value
	size atomic.Value
	//from atomic.Value

	hash        common.Hash
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

// txJSON is the JSON representation of transactions.
type txJSON struct {
	Type uint64 `json:"type"`

	// Common transaction fields:
	Nonce                *uint64         `json:"nonce"`
	GasPrice             *big.Int        `json:"gasPrice"`
	MaxPriorityFeePerGas *big.Int        `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *big.Int        `json:"maxFeePerGas"`
	Gas                  *uint64         `json:"gas"`
	Value                *big.Int        `json:"value"`
	Data                 *hexutil.Bytes  `json:"input"`
	V                    *big.Int        `json:"v"`
	R                    *big.Int        `json:"r"`
	S                    *big.Int        `json:"s"`
	To                   *common.Address `json:"to"`

	// Access list transaction fields:
	ChainID    *hexutil.Big      `json:"chainId,omitempty"`
	AccessList *types.AccessList `json:"accessList,omitempty"`

	// Only used for encoding:
	Hash common.Hash `json:"hash"`

	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

func (h *txJSON) UnmarshalJSON(input []byte) error {

	type txJSON struct {
		Type                 *string         `json:"type"`
		Nonce                *string         `json:"nonce"`
		GasPrice             *string         `json:"gasPrice"`
		MaxPriorityFeePerGas *string         `json:"maxPriorityFeePerGas"`
		MaxFeePerGas         *string         `json:"maxFeePerGas"`
		Gas                  *string         `json:"gas"`
		Value                *string         `json:"value"`
		Data                 *hexutil.Bytes  `json:"input"`
		V                    *string         `json:"v"`
		R                    *string         `json:"r"`
		S                    *string         `json:"s"`
		To                   *common.Address `json:"to"`

		// Access list transaction fields:
		ChainID    *hexutil.Big      `json:"chainId,omitempty"`
		AccessList *types.AccessList `json:"accessList,omitempty"`

		// Only used for encoding:
		Hash common.Hash `json:"hash"`

		BlockNumber *string         `json:"blockNumber,omitempty"`
		BlockHash   *common.Hash    `json:"blockHash,omitempty"`
		From        *common.Address `json:"from,omitempty"`
	}

	var e error
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.Data != nil {
		h.Data = dec.Data
	}
	if dec.To != nil {
		h.To = dec.To
	}
	if dec.ChainID != nil {
		h.ChainID = dec.ChainID
	}
	if dec.AccessList != nil {
		h.AccessList = dec.AccessList
	}
	h.Hash = dec.Hash
	if dec.BlockHash != nil {
		h.BlockHash = dec.BlockHash
	}
	if dec.BlockNumber != nil {
		h.BlockNumber = dec.BlockNumber
	}
	if dec.From != nil {
		h.From = dec.From
	}
	if dec.Type != nil {
		h.Type, e = utils.HexStringToUint64(*dec.Type)
		if e != nil {
			return e
		}
	}
	if dec.Nonce != nil {
		nonceV, e := utils.HexStringToUint64(*dec.Nonce)
		h.Nonce = &nonceV
		if e != nil {
			return e
		}
	}
	if dec.GasPrice != nil {
		h.GasPrice, e = utils.HexStringToBigInt(*dec.GasPrice)
		if e != nil {
			return e
		}
	}
	if dec.MaxPriorityFeePerGas != nil {
		h.MaxPriorityFeePerGas, e = utils.HexStringToBigInt(*dec.MaxPriorityFeePerGas)
		if e != nil {
			return e
		}
	}
	if dec.MaxFeePerGas != nil {
		h.MaxFeePerGas, e = utils.HexStringToBigInt(*dec.MaxFeePerGas)
		if e != nil {
			return e
		}
	}
	if dec.Gas != nil {
		gasV, e := utils.HexStringToUint64(*dec.Gas)
		h.Gas = &gasV
		if e != nil {
			return e
		}
	}

	if dec.Value != nil {
		h.Value, e = utils.HexStringToBigInt(*dec.Value)
		if e != nil {
			return e
		}
	}
	if dec.V != nil {
		h.V, e = utils.HexStringToBigInt(*dec.V)
		if e != nil {
			return e
		}
	}
	if dec.R != nil {
		h.R, e = utils.HexStringToBigInt(*dec.R)
		if e != nil {
			return e
		}
	}
	if dec.S != nil {
		h.S, e = utils.HexStringToBigInt(*dec.S)
		if e != nil {
			return e
		}
	}
	return nil
}

// Hash returns the transaction hash.
func (tx *Transaction) Hash() common.Hash {
	return tx.hash
}

// Type returns the transaction type.
func (tx *Transaction) Type() uint8 {
	var txType byte
	if _, ok := tx.inner.(*types.LegacyTx); ok {
		txType = types.LegacyTxType
	} else if _, ok := tx.inner.(*types.AccessListTx); ok {
		txType = types.AccessListTxType
	} else if _, ok := tx.inner.(*types.DynamicFeeTx); ok {
		txType = types.DynamicFeeTxType
	}
	return txType
}

// Nonce returns the sender account nonce of the transaction.
func (tx *Transaction) Nonce() uint64 {
	var nonce uint64
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		nonce = innerTx.Nonce
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		nonce = innerTx.Nonce
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		nonce = innerTx.Nonce
	}
	return nonce
}

// ChainId returns the EIP155 chain ID of the transaction. The return value will always be
// non-nil. For legacy transactions which are not replay-protected, the return value is
// zero.
func (tx *Transaction) ChainId() *big.Int {
	var chainId *big.Int
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		chainId = deriveChainId(innerTx.V)
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		chainId = innerTx.ChainID
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		chainId = innerTx.ChainID
	}
	return chainId
}

// To returns the recipient address of the transaction.
// For contract-creation transactions, To returns nil.
func (tx *Transaction) To() *common.Address {
	var to *common.Address
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		to = innerTx.To
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		to = innerTx.To
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		to = innerTx.To
	}
	return to
}

// Value returns the ether amount of the transaction.
func (tx *Transaction) Value() *big.Int {
	var value *big.Int
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		value = innerTx.Value
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		value = innerTx.Value
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		value = innerTx.Value
	}
	return value
}

// Gas returns the gas limit of the transaction.
func (tx *Transaction) Gas() uint64 {
	//return tx.inner.gas()
	var gas uint64
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		gas = innerTx.Gas
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		gas = innerTx.Gas
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		gas = innerTx.Gas
	}
	return gas
}

// GasPrice returns the gas price of the transaction.
func (tx *Transaction) GasPrice() *big.Int {
	var gasPrice *big.Int
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		gasPrice = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		gasPrice = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		gasPrice = innerTx.GasFeeCap
	}
	return gasPrice
}

// GasFeeCap returns the fee cap per gas of the transaction.
func (tx *Transaction) GasFeeCap() *big.Int {
	var gasFeeCap *big.Int
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		gasFeeCap = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		gasFeeCap = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		gasFeeCap = innerTx.GasFeeCap
	}
	return gasFeeCap
}

// GasTipCap returns the gasTipCap per gas of the transaction.
func (tx *Transaction) GasTipCap() *big.Int {
	//return new(big.Int).Set(tx.inner.gasTipCap())
	var gasTipCap *big.Int
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		gasTipCap = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		gasTipCap = innerTx.GasPrice
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		gasTipCap = innerTx.GasTipCap
	}
	return gasTipCap
}

// Data returns the input data of the transaction.
func (tx *Transaction) Data() []byte {
	var data []byte
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		data = innerTx.Data
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		data = innerTx.Data
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		data = innerTx.Data
	}
	return data
}

// RawSignatureValues returns the V, R, S signature values of the transaction.
// The return values should not be modified by the caller.
func (tx *Transaction) RawSignatureValues() (v, r, s *big.Int) {
	if innerTx, ok := tx.inner.(*types.LegacyTx); ok {
		v, r, s = innerTx.V, innerTx.R, innerTx.S
	} else if innerTx, ok := tx.inner.(*types.AccessListTx); ok {
		v, r, s = innerTx.V, innerTx.R, innerTx.S
	} else if innerTx, ok := tx.inner.(*types.DynamicFeeTx); ok {
		v, r, s = innerTx.V, innerTx.R, innerTx.S
	}
	return v, r, s
}

// UnmarshalJSON unmarshals from JSON.
func (t *Transaction) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}

	// Decode / verify fields according to transaction type.
	var inner types.TxData
	switch dec.Type {
	case types.LegacyTxType, ArbitrumDepositTxType, ArbitrumDistributeTxType, zkSyncTxType, zkSyncTxType113:
		var itx types.LegacyTx
		inner = &itx
		if dec.To != nil {
			itx.To = dec.To
		}
		if dec.Nonce == nil {
			return errors.New("missing required field 'nonce' in transaction")
		}
		itx.Nonce = uint64(*dec.Nonce)
		if dec.GasPrice == nil {
			return errors.New("missing required field 'gasPrice' in transaction")
		}
		itx.GasPrice = (*big.Int)(dec.GasPrice)
		if dec.Gas == nil {
			return errors.New("missing required field 'gas' in transaction")
		}
		itx.Gas = uint64(*dec.Gas)
		if dec.Value == nil {
			return errors.New("missing required field 'value' in transaction")
		}
		itx.Value = (*big.Int)(dec.Value)
		//if dec.Data == nil {
		//	return errors.New("missing required field 'input' in transaction")
		//}
		if dec.Data != nil {
			itx.Data = *dec.Data
		}

		/*if dec.V == nil {
			return errors.New("missing required field 'v' in transaction")
		}*/
		itx.V = (*big.Int)(dec.V)
		/*if dec.R == nil {
			return errors.New("missing required field 'r' in transaction")
		}*/
		itx.R = (*big.Int)(dec.R)
		/*if dec.S == nil {
			return errors.New("missing required field 's' in transaction")
		}*/
		itx.S = (*big.Int)(dec.S)
		/*withSignature := itx.V.Sign() != 0 || itx.R.Sign() != 0 || itx.S.Sign() != 0
		if withSignature {
			if err := sanityCheckSignature(itx.V, itx.R, itx.S, true); err != nil {
				return err
			}
		}*/

	case types.AccessListTxType:
		var itx types.AccessListTx
		inner = &itx
		// Access list is optional for now.
		if dec.AccessList != nil {
			itx.AccessList = *dec.AccessList
		}
		if dec.ChainID == nil {
			return errors.New("missing required field 'chainId' in transaction")
		}
		itx.ChainID = (*big.Int)(dec.ChainID)
		if dec.To != nil {
			itx.To = dec.To
		}
		if dec.Nonce == nil {
			return errors.New("missing required field 'nonce' in transaction")
		}
		itx.Nonce = uint64(*dec.Nonce)
		if dec.GasPrice == nil {
			return errors.New("missing required field 'gasPrice' in transaction")
		}
		itx.GasPrice = (*big.Int)(dec.GasPrice)
		if dec.Gas == nil {
			return errors.New("missing required field 'gas' in transaction")
		}
		itx.Gas = uint64(*dec.Gas)
		if dec.Value == nil {
			return errors.New("missing required field 'value' in transaction")
		}
		itx.Value = (*big.Int)(dec.Value)
		if dec.Data == nil {
			return errors.New("missing required field 'input' in transaction")
		}
		itx.Data = *dec.Data
		/*if dec.V == nil {
			return errors.New("missing required field 'v' in transaction")
		}*/
		itx.V = (*big.Int)(dec.V)
		/*if dec.R == nil {
			return errors.New("missing required field 'r' in transaction")
		}*/
		itx.R = (*big.Int)(dec.R)
		/*if dec.S == nil {
			return errors.New("missing required field 's' in transaction")
		}*/
		itx.S = (*big.Int)(dec.S)
		/*withSignature := itx.V.Sign() != 0 || itx.R.Sign() != 0 || itx.S.Sign() != 0
		if withSignature {
			if err := sanityCheckSignature(itx.V, itx.R, itx.S, false); err != nil {
				return err
			}
		}*/

	case types.DynamicFeeTxType:
		var itx types.DynamicFeeTx
		inner = &itx
		// Access list is optional for now.
		if dec.AccessList != nil {
			itx.AccessList = *dec.AccessList
		}
		if dec.ChainID == nil {
			return errors.New("missing required field 'chainId' in transaction")
		}
		itx.ChainID = (*big.Int)(dec.ChainID)
		if dec.To != nil {
			itx.To = dec.To
		}
		if dec.Nonce == nil {
			return errors.New("missing required field 'nonce' in transaction")
		}
		itx.Nonce = uint64(*dec.Nonce)
		if dec.MaxPriorityFeePerGas == nil {
			return errors.New("missing required field 'maxPriorityFeePerGas' for txdata")
		}
		itx.GasTipCap = (*big.Int)(dec.MaxPriorityFeePerGas)
		if dec.MaxFeePerGas == nil {
			return errors.New("missing required field 'maxFeePerGas' for txdata")
		}
		itx.GasFeeCap = (*big.Int)(dec.MaxFeePerGas)
		if dec.Gas == nil {
			return errors.New("missing required field 'gas' for txdata")
		}
		itx.Gas = uint64(*dec.Gas)
		if dec.Value == nil {
			return errors.New("missing required field 'value' in transaction")
		}
		itx.Value = (*big.Int)(dec.Value)
		if dec.Data == nil {
			return errors.New("missing required field 'input' in transaction")
		}
		itx.Data = *dec.Data
		/*if dec.V == nil {
			return errors.New("missing required field 'v' in transaction")
		}*/
		itx.V = (*big.Int)(dec.V)
		/*if dec.R == nil {
			return errors.New("missing required field 'r' in transaction")
		}*/
		itx.R = (*big.Int)(dec.R)
		/*if dec.S == nil {
			return errors.New("missing required field 's' in transaction")
		}*/
		itx.S = (*big.Int)(dec.S)
		/*withSignature := itx.V.Sign() != 0 || itx.R.Sign() != 0 || itx.S.Sign() != 0
		if withSignature {
			if err := sanityCheckSignature(itx.V, itx.R, itx.S, false); err != nil {
				return err
			}
		}*/

	default:
		return types.ErrTxTypeNotSupported
	}

	// Now set the inner transaction.
	t.setDecoded(inner, 0)

	// TODO: check hash here?
	t.hash = dec.Hash
	t.BlockNumber = dec.BlockNumber
	t.BlockHash = dec.BlockHash
	t.From = dec.From
	return nil
}

// setDecoded sets the inner transaction and size after decoding.
func (tx *Transaction) setDecoded(inner types.TxData, size int) {
	tx.inner = inner
	tx.time = time.Now()
	if size > 0 {
		tx.size.Store(common.StorageSize(size))
	}
}

// deriveChainId derives the chain id from the given v parameter
func deriveChainId(v *big.Int) *big.Int {
	if v.BitLen() <= 64 {
		v := v.Uint64()
		if v == 27 || v == 28 {
			return new(big.Int)
		}
		return new(big.Int).SetUint64((v - 35) / 2)
	}
	v = new(big.Int).Sub(v, big.NewInt(35))
	return v.Div(v, big.NewInt(2))
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (receipt *Receipt, err error) {
	var r *Receipt
	err = c.c.CallContext(ctx, &r, "eth_getTransactionReceipt", txHash)
	if err == nil {
		if r == nil {
			return nil, ethereum.NotFound
		}
	}
	return r, err
}

// Receipt represents the results of a transaction.
type Receipt struct {
	// Consensus fields: These fields are defined by the Yellow Paper
	Type              string `json:"type,omitempty"`
	PostState         string `json:"root,omitempty"`
	Status            string `json:"status,omitempty"`
	From              string `json:"from,omitempty"`
	To                string `json:"to,omitempty"`
	CumulativeGasUsed string `json:"cumulativeGasUsed"` //`json:"cumulativeGasUsed" gencodec:"required"`
	EffectiveGasPrice string `json:"effectiveGasPrice,omitempty"`
	Bloom             string `json:"logsBloom"` //`json:"logsBloom" gencodec:"required"`
	Logs              []*Log `json:"logs" gencodec:"required"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          string `json:"transactionHash" gencodec:"required"`
	ContractAddress string `json:"contractAddress,omitempty"`
	GasUsed         string `json:"gasUsed" gencodec:"required"`
	//Optimism链为16进制string类型，ScrollL2TEST链为10进制int类型
	//Optimism totalFee=(Gas Price * Gas) + (l1GasUsed * l1GasPrice * l1FeeScalar)=(Gas Price * Gas) + l1Fee
	L1Fee interface{} `json:"l1Fee,omitempty"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        string `json:"blockHash,omitempty"`
	BlockNumber      string `json:"blockNumber,omitempty"`
	TransactionIndex string `json:"transactionIndex,omitempty"`
}

//go:generate go run github.com/fjl/gencodec -type Log -field-override logMarshaling -out gen_log_json.go

// Log represents a contract log event. These events are generated by the LOG opcode and
// stored/indexed by the node.
type Log struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address common.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []common.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data []byte `json:"data" gencodec:"required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash common.Hash `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex uint `json:"transactionIndex"`
	// hash of the block in which the transaction was included
	BlockHash common.Hash `json:"blockHash"`
	// index of the log in the block
	Index uint `json:"logIndex"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}

func (l Log) MarshalJSON() ([]byte, error) {
	type Log struct {
		Address     common.Address `json:"address" gencodec:"required"`
		Topics      []common.Hash  `json:"topics" gencodec:"required"`
		Data        []byte         `json:"data" gencodec:"required"`
		BlockNumber uint64         `json:"blockNumber"`
		TxHash      common.Hash    `json:"transactionHash" gencodec:"required"`
		TxIndex     uint           `json:"transactionIndex"`
		BlockHash   common.Hash    `json:"blockHash"`
		Index       uint           `json:"logIndex"`
		Removed     bool           `json:"removed"`
	}
	var enc Log
	enc.Address = l.Address
	enc.Topics = l.Topics
	enc.Data = l.Data

	enc.BlockNumber = l.BlockNumber
	enc.TxHash = l.TxHash
	enc.TxIndex = l.TxIndex
	enc.BlockHash = l.BlockHash
	enc.Index = l.Index
	enc.Removed = l.Removed
	return json.Marshal(&enc)
}

// UnmarshalJSON unmarshals from JSON.
func (l *Log) UnmarshalJSON(input []byte) error {
	type Log struct {
		Address     *common.Address `json:"address" gencodec:"required"`
		Topics      []common.Hash   `json:"topics" gencodec:"required"`
		Data        *hexutil.Bytes  `json:"data" gencodec:"required"`
		BlockNumber *string         `json:"blockNumber"`
		TxHash      *common.Hash    `json:"transactionHash" gencodec:"required"`
		TxIndex     *string         `json:"transactionIndex"`
		BlockHash   *common.Hash    `json:"blockHash"`
		Index       *string         `json:"logIndex"`
		Removed     *bool           `json:"removed"`
	}
	var e error
	var dec Log
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.Address == nil {
		return errors.New("missing required field 'address' for Log")
	}
	l.Address = *dec.Address
	if dec.Topics == nil {
		return errors.New("missing required field 'topics' for Log")
	}
	l.Topics = dec.Topics
	if dec.Data == nil {
		return errors.New("missing required field 'data' for Log")
	}
	l.Data = *dec.Data
	if dec.BlockNumber != nil {
		l.BlockNumber, e = utils.HexStringToUint64(*dec.BlockNumber)
		if e != nil {
			return e
		}
	}
	if dec.TxHash == nil {
		return errors.New("missing required field 'transactionHash' for Log")
	}
	l.TxHash = *dec.TxHash
	if dec.TxIndex != nil {
		ti64, e := utils.HexStringToUint64(*dec.TxIndex)
		if e != nil {
			return e
		}
		l.TxIndex = uint(ti64)
	}
	if dec.BlockHash != nil {
		l.BlockHash = *dec.BlockHash
	}
	if dec.Index != nil {
		i64, e := utils.HexStringToUint64(*dec.Index)
		if e != nil {
			return e
		}
		l.Index = uint(i64)
	}
	if dec.Removed != nil {
		l.Removed = *dec.Removed
	}
	return nil
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
