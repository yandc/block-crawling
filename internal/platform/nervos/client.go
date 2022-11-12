package nervos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/common"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	ccommon "github.com/ethereum/go-ethereum/common"
	"github.com/nervosnetwork/ckb-sdk-go/address"
	"github.com/nervosnetwork/ckb-sdk-go/rpc"
	"github.com/nervosnetwork/ckb-sdk-go/types"
	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	nativeLockCodeHash = "0x9376c3b5811942960a846691e16e477cf43d7c7fa654067c9948dfcd09a32137"
	nativeTypeCodeHash = "0xebafc1ebe95b88cac426f984ed5fce998089ecad0cd2f8b17755c9de4cb02162"
	testSudtCodeHash   = "0xc5e5dcf215925f7ef4dfaf5f4b4f105bc321c02776d6e7d52a1db3fcd9d011a4"
	mainSudtCodeHash   = "0x5e7a36a77e68eecc013dfa2fe6a23f3b6c344b04005808694ae6dd45eea4cfd5"
	zeroHex            = "0x0000000000000000000000000000000000000000000000000000000000000000"
)

type Client struct {
	*common.NodeDefaultIn

	client rpc.Client
	url    string
	mode   address.Mode
}

func NewClient(chainName, nodeURL string) (*Client, error) {
	client, err := rpc.Dial(nodeURL)
	if err != nil {
		return nil, err
	}
	mode := address.Mainnet
	if strings.HasSuffix(chainName, "TEST") {
		mode = address.Testnet
	}
	return &Client{
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
		client: client,
		url:    nodeURL,
		mode:   mode,
	}, nil
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) Detect() error {
	_, err := c.GetBlockHeight()
	return err
}

// GetBlock fetch block data of the given height.
func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block, err := c.client.GetBlockByNumber(context.Background(), height)
	if err != nil {
		return nil, err
	}
	txs, err := c.convertRawTxs(block.Transactions, block.Header.Number)
	if err != nil {
		return nil, err
	}

	return &chain.Block{
		Hash:         block.Header.Hash.Hex(),
		ParentHash:   block.Header.ParentHash.Hex(),
		Number:       block.Header.Number,
		Nonce:        block.Header.Nonce.Uint64(),
		Time:         int64(block.Header.Timestamp / 1000),
		Raw:          block,
		Transactions: txs,
	}, nil
}

type txWrapper struct {
	inputCells        map[string]*cell // input cells loaded from database.
	outputCells       []*cell
	prevOutputs       []*types.OutPoint
	toAddresses       []string
	contractAddresses []string
	txTypes           []string
	amounts           []string
	totalOutput       uint64
	raw               *types.Transaction

	// txByHash
	status types.TransactionStatus
	header *types.Header
}

type cell struct {
	OutPoint *outPoint `json:"outPoint"`
	Capacity uint64    `json:"capacity"`
	Lock     *script   `json:"lock"`
	Type     *script   `json:"type"`
	Data     string    `json:"data"`
}

type outPoint struct {
	TxHash string `json:"txHash"`
	Index  int    `json:"index"`
}

type script struct {
	CodeHash string `json:"codeHash"`
	HashType string `json:"hashType"`
	Args     string `json:"args"`
}

func newCell(rawTx *types.Transaction, index int) *cell {
	output := rawTx.Outputs[index]
	return &cell{
		OutPoint: &outPoint{
			TxHash: rawTx.Hash.Hex(),
			Index:  index,
		},
		Capacity: output.Capacity,
		Lock:     newScript(output.Lock),
		Type:     newScript(output.Type),
		Data:     types.BytesToHash(rawTx.OutputsData[index]).Hex(),
	}
}

func newScript(s *types.Script) *script {
	if s == nil {
		return nil
	}
	return &script{
		CodeHash: s.CodeHash.Hex(),
		HashType: string(s.HashType),
		Args:     ccommon.Bytes2Hex(s.Args),
	}
}

func (c *cell) decodeLock() *types.Script {
	return c.Lock.decode()
}

func (c *cell) decodeType() *types.Script {
	if c.Type != nil {
		return c.Type.decode()
	}
	return nil
}

func (s *script) decode() *types.Script {
	return &types.Script{
		CodeHash: types.HexToHash(s.trimLeftPaddingZero(s.CodeHash)),
		HashType: types.ScriptHashType(s.HashType),
		Args:     ccommon.FromHex(s.trimLeftPaddingZero(s.Args)),
	}
}

func (s *script) trimLeftPaddingZero(v string) string {
	return "0x" + strings.TrimLeft(strings.Replace(v, "0x", "", 1), "0")
}

func (c *Client) convertRawTxs(rawTxs []*types.Transaction, blockNumber uint64) ([]*chain.Transaction, error) {
	txs := make([]*chain.Transaction, 0, len(rawTxs))
	for _, rawTx := range rawTxs {
		var totalOutput uint64
		if len(rawTx.Inputs) == 0 {
			continue
		}
		prevOutputs := make([]*types.OutPoint, 0, len(rawTx.Inputs))
		toAddresses := make([]string, 0, len(rawTx.Outputs))
		outputCells := make([]*cell, 0, len(rawTx.Outputs))
		contractAddresses := make([]string, 0, len(rawTx.Outputs))
		amounts := make([]string, 0, len(rawTx.Outputs))
		txTypes := make([]string, 0, len(rawTx.Outputs))

		txHashs := make(map[string]bool)

		for _, input := range rawTx.Inputs {
			if input.PreviousOutput.TxHash.Hex() == zeroHex {
				continue
			}
			prevOutputs = append(prevOutputs, input.PreviousOutput)
			txHashs[input.PreviousOutput.TxHash.Hex()] = true
		}

		inputCells, err := c.batchLoadCells(txHashs)
		if err != nil {
			return nil, err
		}

		for index, toTxOutput := range rawTx.Outputs {
			toAddr, err := address.ConvertScriptToAddress(c.mode, toTxOutput.Lock)
			if err != nil {
				return nil, err
			}
			amount := fmt.Sprint(toTxOutput.Capacity)
			var contractAddr string
			txType := chain.TxTypeNative
			if !c.isTokenTransfer(toTxOutput.Type) {
				totalOutput += toTxOutput.Capacity
			}

			if c.isTokenTransfer(toTxOutput.Type) {
				txType = string(chain.TxTypeTransfer)
				// https://explorer.nervos.org/sudt/0x797bfd0b7b883bc9dba43678e285999507c6d0b971a2740c76623f70636f4080
				contractAddr = types.BytesToHash(toTxOutput.Type.Args).Hex()
				amount = parseUint128TokenAmount(rawTx.OutputsData[index])
			}

			toAddresses = append(toAddresses, toAddr)
			contractAddresses = append(contractAddresses, contractAddr)
			amounts = append(amounts, amount)
			txTypes = append(txTypes, txType)
			outputCells = append(outputCells, newCell(rawTx, index))
		}
		txs = append(txs, &chain.Transaction{
			Hash:        rawTx.Hash.Hex(),
			BlockNumber: blockNumber,
			TxType:      "",
			FromAddress: "",
			ToAddress:   "",
			Value:       "",
			Raw: &txWrapper{
				inputCells:        inputCells,
				prevOutputs:       prevOutputs,
				toAddresses:       toAddresses,
				outputCells:       outputCells,
				contractAddresses: contractAddresses,
				txTypes:           txTypes,
				amounts:           amounts,
				totalOutput:       totalOutput,
				raw:               rawTx,
			},
			Record: nil,
		})
	}
	return txs, nil
}

func parseHexUint128TokenAmount(data string) string {
	if strings.HasPrefix(data, "0x") {
		data = data[2:]
	}
	byts, _ := hex.DecodeString(data)
	return parseUint128TokenAmount(byts)
}

// According to Rule 1 of the RFC:
//
//		a SUDT cell must store SUDT amount in the first 16 bytes of cell data segment,
//	 the amount should be stored as little endian, 128-bit unsigned integer format.
//
// See also: https://github.com/nervosnetwork/rfcs/blob/master/rfcs/0025-simple-udt/0025-simple-udt.md
func parseUint128TokenAmount(data []byte) string {
	// first 16 bytes.
	data = data[:16]
	fmt.Printf("%v\n", data)

	// reverse to big-endian
	for i := 0; i < len(data)/2; i++ {
		data[i], data[len(data)-i-1] = data[len(data)-i-1], data[i]
	}
	fmt.Printf("%v\n", data)
	bi := new(big.Int).SetBytes(data)
	return bi.String()
}

func (c *Client) fillMissedInputCells(rawTx *txWrapper) error {
	missedTxHashs := make(map[string]bool)
	for _, prevOut := range rawTx.prevOutputs {
		cellKey := c.cellKey(prevOut.TxHash.Hex(), int(prevOut.Index))
		if _, ok := rawTx.inputCells[cellKey]; !ok {
			missedTxHashs[prevOut.TxHash.Hex()] = true
		}
	}
	if len(missedTxHashs) == 0 {
		return nil
	}

	items := make([]types.BatchTransactionItem, 0, len(missedTxHashs))
	for hash, _ := range missedTxHashs {
		items = append(items, types.BatchTransactionItem{
			Hash: types.HexToHash(hash),
		})
	}
	if err := c.client.BatchTransactions(context.Background(), items); err != nil {
		return err
	}
	for _, item := range items {
		if item.Result == nil {
			continue
		}

		for index := range item.Result.Transaction.Outputs {
			rawTx.inputCells[c.cellKey(item.Hash.Hex(), index)] = newCell(item.Result.Transaction, index)
		}
	}
	return nil
}

func (c *Client) convertTxsToMap(txs []*types.Transaction) map[string]bool {
	txHashs := make(map[string]bool)

	for _, rawTx := range txs {
		for _, in := range rawTx.Inputs {
			if in.PreviousOutput.TxHash.Hex() == zeroHex {
				continue
			}
			txHashs[in.PreviousOutput.TxHash.Hex()] = true
		}
	}
	return txHashs
}

// batchLoadCells attempt load cells from database.
func (c *Client) batchLoadCells(txHashs map[string]bool) (map[string]*cell, error) {
	results := make(map[string]*cell)

	hashs := make([]string, 0, len(txHashs))
	for hash := range txHashs {
		hashs = append(hashs, hash)
	}
	records, err := data.NervosCellRecordRepoClient.LoadBatch(context.Background(), hashs)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s 链查找本地交易结果失败, error：%s", c.ChainName, fmt.Sprintf("%s", err))
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		return nil, err
	}
	for _, record := range records {
		cap, _ := strconv.Atoi(record.Capacity)
		var typeScript *script
		if record.TypeCodeHash != "" {
			typeScript = &script{
				CodeHash: record.TypeCodeHash,
				HashType: record.TypeHashType,
				Args:     record.TypeArgs,
			}
		}
		cel := &cell{
			OutPoint: &outPoint{
				TxHash: record.TransactionHash,
				Index:  record.Index,
			},
			Capacity: uint64(cap),
			Lock: &script{
				CodeHash: record.LockCodeHash,
				HashType: record.LockHashType,
				Args:     record.LockArgs,
			},
			Type: typeScript,
			Data: record.Data,
		}
		results[c.cellKey(record.TransactionHash, record.Index)] = cel
	}

	return results, nil

}

func (c *Client) cellKey(txHash string, index int) string {
	return fmt.Sprintf("%s#%d", txHash, index)
}

// GetBlockHeight get current block height.
// 块上链时，交易可能未完成。安全区块高度为 20 块，所以这样处理
func (c *Client) GetBlockHeight() (uint64, error) {
	h,err := c.client.GetTipBlockNumber(context.Background())
	if h > 20{
		h = h - 20
	}
	return h,err
}

// GetTxByHash get transaction by given tx hash.
//
// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	rawTx, err := c.client.GetTransaction(context.Background(), types.HexToHash(txHash))
	if err != nil {
		return nil, err
	}
	if rawTx.TxStatus.Status == types.TransactionStatusRejected {
		// OnDroppedTx will be invoked.
		return nil, nil
	}
	var header *types.Header
	var blockNumber uint64
	if rawTx.TxStatus.BlockHash != nil {
		header, err = c.client.GetHeader(context.Background(), *rawTx.TxStatus.BlockHash)
		if err != nil {
			return nil, err
		}
		blockNumber = header.Number
	}

	txs, err := c.convertRawTxs([]*types.Transaction{rawTx.Transaction}, blockNumber)
	if err != nil {
		return nil, err
	}
	wrapper := txs[0].Raw.(*txWrapper)
	wrapper.header = header
	wrapper.status = rawTx.TxStatus.Status
	return txs[0], nil
}

func (c *Client) GetUTXOByHash(txHash string) (tx *types.TransactionWithStatus, err error) {
	return c.client.GetTransaction(context.Background(), types.HexToHash(txHash))
}

func (c *Client) isTokenTransfer(typeScript *types.Script) bool {
	if typeScript == nil {
		return false
	}

	if c.mode == address.Mainnet && typeScript.CodeHash.Hex() == mainSudtCodeHash {
		return true
	}
	if c.mode == address.Testnet && typeScript.CodeHash.Hex() == testSudtCodeHash {
		return true
	}
	return false
}

func (c *Client) isNativeTransfer(lock, typeScript *types.Script) bool {
	if typeScript == nil {
		return true
	}
	return lock.CodeHash.Hex() == nativeLockCodeHash && typeScript.CodeHash.Hex() == nativeTypeCodeHash
}
