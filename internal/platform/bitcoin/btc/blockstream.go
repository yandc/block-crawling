package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"math/big"
	"strconv"
	"time"

	"github.com/blockcypher/gobcy"
)

type blockstreamNode struct {
	nodeURL string
}

// GetBalance implements Noder
func (node *blockstreamNode) GetBalance(address string) (string, error) {
	u, err := base.BuildURLBTC("/api/address/"+address, node.nodeURL, nil)
	if err != nil {
		return "", err
	}
	var addr blockstreamAddr
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &addr, &timeoutMS)
	if err != nil {
		return "", err
	}
	balance := strconv.Itoa(int(addr.ChainStats.FundedTxoSum - addr.ChainStats.SpentTxoSum))
	btcValue := utils.StringDecimals(balance, 8)
	return btcValue, nil
}

type blockstreamAddr struct {
	Address      string               `json:"address"`
	ChainStats   blockstreamAddrStats `json:"chain_stats"`
	MempoolStats blockstreamAddrStats `json:"mempool_stats"`
}

type blockstreamAddrStats struct {
	FundedTxoCount uint64 `json:"funded_txo_count"`
	FundedTxoSum   uint64 `json:"funded_txo_sum"`
	SpentTxoCount  uint64 `json:"spent_txo_count"`
	SpentTxoSum    uint64 `json:"spent_txo_sum"`
	TxCount        uint64 `json:"tx_count"`
}

// GetBlockNumber implements Noder
func (node *blockstreamNode) GetBlockNumber() (int, error) {
	u, err := base.BuildURLBTC("/api/blocks/tip/height", node.nodeURL, nil)
	if err != nil {
		return 0, err
	}
	content, err := httpclient.HttpsGetFormString(u.String(), nil)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(content, 10, 64)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

// GetTransactionByHash implements Noder
func (node *blockstreamNode) GetTransactionByHash(hash string) (tx types.TX, err error) {
	u, err := base.BuildURLBTC("/api/tx/"+hash, node.nodeURL, nil)
	if err != nil {
		return types.TX{}, err
	}
	var rawTx types.BTCTestBlock
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &rawTx, &timeoutMS)
	if err != nil {
		return types.TX{}, err
	}
	var inputs []gobcy.TXInput
	var inputAddress []string
	var outs []gobcy.TXOutput
	var outputAddress []string
	for _, in := range rawTx.Vin {
		input := gobcy.TXInput{
			OutputValue: int(in.Prevout.Value),
			Addresses:   append(inputAddress, in.Prevout.ScriptpubkeyAddress),
		}
		inputs = append(inputs, input)
	}
	for _, out := range rawTx.Vout {
		out := gobcy.TXOutput{
			Value:     *big.NewInt(int64(out.Value)),
			Addresses: append(outputAddress, out.ScriptpubkeyAddress),
		}
		outs = append(outs, out)
	}
	return types.TX{
		BlockHash:   rawTx.Status.BlockHash,
		BlockHeight: rawTx.Status.BlockHeight,
		Hash:        hash,
		Fees:        *big.NewInt(int64(rawTx.Fee)),
		Size:        rawTx.Size,
		Confirmed:   time.Unix(int64(rawTx.Status.BlockTime), 0),
		LockTime:    rawTx.Locktime,
		VinSize:     len(rawTx.Vin),
		VoutSize:    len(rawTx.Vout),
		Inputs:      inputs,
		Outputs:     outs,
		Error:       "",
	}, nil
}

func newBlockstreamNode(nodeURL string) *blockstreamNode {
	return &blockstreamNode{
		nodeURL: nodeURL,
	}
}
