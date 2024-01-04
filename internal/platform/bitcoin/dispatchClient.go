package bitcoin

import (
	"block-crawling/internal/log"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/platform/bitcoin/btc"
	"block-crawling/internal/platform/bitcoin/doge"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"time"

	"github.com/blockcypher/gobcy"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

const TO_TYPE = "utxo_output"
const FROM_TYPE = "utxo_input"
const FEE_TYPE = "fee"
const SUCCESS_STATUS = "completed"

// errIrrationalHeight height is irrational.
var errIrrationalHeight = errors.New("irrational height, zero or less than zero")

type Client struct {
	*common.NodeDefaultIn
	oklinkClient   OklinkBtcClient
	nodeURL        string
	chainName      string
	DispatchClient base.Client

	btcClient *btc.Client
}

func NewClient(nodeUrl string, chainName string) Client {
	var r base.Client
	var btcClient *btc.Client
	if chainName == "BTC" {
		c := btc.NewClient(nodeUrl)
		utils.CopyProperties(c, &r)
		btcClient = &c
	} else if chainName == "LTC" || chainName == "DOGE" {
		utils.CopyProperties(doge.NewClient(nodeUrl), &r)
	}
	r.ChainName = chainName
	return Client{
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName: chainName,
		},
		nodeURL:        nodeUrl,
		chainName:      chainName,
		DispatchClient: r,
		oklinkClient:   NewOklinkClient(chainName, nodeUrl),
		btcClient:      btcClient,
	}
}

func (c *Client) URL() string {
	return c.nodeURL
}

func (c *Client) Detect() error {
	height, err := c.GetBlockNumber()
	if err != nil {
		return err
	}
	if height <= 0 {
		return errIrrationalHeight
	}
	return err
}

func (c *Client) GetBlockHeight() (uint64, error) {
	start := time.Now()
	if c.ChainName == "BTC" {
		btcHeigth, err := btc.GetBlockNumber(c.btcClient)
		return uint64(btcHeigth), err
	} else {
		result, err := c.oklinkClient.GetBlockHeight()
		if err != nil {
			return 0, err
		}
		log.Debug(
			"RETRIEVED CHAIN HEIGHT FROM NODE",
			zap.Uint64("height", result),
			zap.String("nodeUrl", c.nodeURL),
			zap.String("chainName", c.chainName),
			zap.String("elapsed", time.Now().Sub(start).String()),
		)
		return result, err
	}
}

func (c *Client) GetBlock(height uint64) (block *chain.Block, err error) {
	if c.chainName == "DOGE" || c.chainName == "LTC" {
		return c.oklinkClient.GetBlock(height)
	}
	block, err = c.getBlock(height)
	if err != nil {
		return nil, err
	}

	// Block number may return 0.
	if block.Number == 0 {
		block.Number = height
	}
	return block, nil
}

func (c *Client) getBlock(height uint64) (*chain.Block, error) {
	block, err := c.GetBTCBlockByNumber(int(height))
	if err != nil {
		return nil, err
	}
	txs := make([]*chain.Transaction, 0, len(block.Tx))
	for _, tx := range block.Tx {
		txs = append(txs, &chain.Transaction{
			Hash:        tx.Hash,
			Nonce:       uint64(block.Nonce),
			BlockNumber: uint64(block.Height),
			FromAddress: "",
			ToAddress:   "",
			Value:       "",
			Raw:         tx,
			Record:      nil,
		})
	}
	return &chain.Block{
		Hash:         block.Hash,
		ParentHash:   block.PrevBlock,
		Number:       uint64(block.Height),
		Nonce:        uint64(block.Nonce),
		Time:         int64(block.Time),
		Transactions: txs,
	}, nil
}

// GetTxByHash get transaction by given tx hash.
//
// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	if c.chainName == "DOGE" || c.chainName == "LTC" {
		return c.oklinkClient.GetTxByHash(txHash)
	}
	rawTx, err := c.GetTransactionByHash(txHash)

	if err != nil {
		log.Error("get transaction by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	}

	// Tx has been dropped, return nil without error will invoke OnDroppedTx.
	if strings.HasSuffix(rawTx.Error, " not found.") {
		return nil, nil
	}

	return &chain.Transaction{
		Hash:        txHash,
		BlockNumber: uint64(rawTx.BlockHeight),
		Raw:         rawTx,
		Record:      nil,
	}, nil
}

// chainso 源
func (c *Client) GetBalance(address string) (string, error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetBalance(address, c.btcClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return c.oklinkClient.GetBalance(address)
	}
	return "", nil
}
func (c *Client) GetBlockNumber() (int, error) {
	if c.chainName == "DOGE" || c.chainName == "LTC" {
		result, err := c.oklinkClient.GetBlockHeight()
		return int(result), err
	}
	countInfo, err := c.DispatchClient.GetBlockCount()
	if err != nil {
		return 0, err

	}
	if countInfo.Error != nil {
		return 0, errors.New(fmt.Sprintf("%v", countInfo.Error))
	}

	return countInfo.Result, nil

}

func (c *Client) GetMempoolTxIds() ([]string, error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetMempoolTxIds(c.btcClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetMempoolTxIds(&c.DispatchClient)
	}
	return nil, nil
}

func (c *Client) GetTestBlockByHeight(height int) (result types.BTCTestBlockerInfo, err error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTestBlockByHeight(height, c.btcClient)
	}
	return nil, err
}

func (c *Client) GetTransactionByHash(hash string) (tx types.TX, err error) {
	if c.chainName == "DOGE" || c.chainName == "LTC" {
		return c.oklinkClient.GetTransactionByHash(hash)
	}

	if c.DispatchClient.URL == "https://chain.so/" {
		return doge.GetTxByHashFromChainSo(hash, &c.DispatchClient)
	}
	//UTXOTX --- utxo
	if c.DispatchClient.ChainName == "BTC" {
		tx, err = btc.GetTransactionByHash(hash, c.btcClient)
		if strings.Contains(tx.Error, "Limits reached") {
			//抛出异常
			return tx, errors.New(tx.Error)
		}
		return tx, err
	} else {
		utxoTxByDD, e := doge.GetTransactionsByTXHash(hash, &c.DispatchClient)
		if e != nil {
			return tx, e
		} else {
			if utxoTxByDD.Detail == "The requested resource has not been found" {
				return tx, errors.New(utxoTxByDD.Detail)
			} else {
				var inputs []gobcy.TXInput
				var inputAddress []string
				var outs []gobcy.TXOutput
				var outputAddress []string

				var feeAmount int64

				for _, event := range utxoTxByDD.Events {
					if event.Type == FROM_TYPE {
						input := gobcy.TXInput{
							OutputValue: int(event.Amount),
							Addresses:   append(inputAddress, event.Source),
						}
						inputs = append(inputs, input)
					}
					if event.Type == TO_TYPE {
						out := gobcy.TXOutput{
							Value:     *big.NewInt(event.Amount),
							Addresses: append(outputAddress, event.Destination),
						}
						outs = append(outs, out)
					}
					if event.Type == FEE_TYPE {
						feeAmount = event.Amount
					}
				}

				txTime := time.Unix(int64(utxoTxByDD.Date), 0)
				tx = types.TX{
					BlockHash:   utxoTxByDD.BlockId,
					BlockHeight: utxoTxByDD.BlockNumber,
					Hash:        utxoTxByDD.Id,
					Fees:        *big.NewInt(feeAmount),
					Confirmed:   txTime,
					Inputs:      inputs,
					Outputs:     outs,
					Error:       "",
				}
				return tx, nil
			}

		}
	}
	//if c.DispatchClient.ChainName == "BTC" {
	//	return btc.GetTransactionByHash(hash, &c.DispatchClient)
	//} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
	//	utxoTxByDD, e := doge.GetTransactionsByTXHash(hash, &c.DispatchClient)
	//	if e != nil {
	//		return tx, e
	//	} else {
	//		if utxoTxByDD.Detail == "The requested resource has not been found" {
	//			tx = types.TX{
	//				Error: " not found.",
	//			}
	//			return tx, nil
	//		} else {
	//			var inputs []gobcy.TXInput
	//			var inputAddress []string
	//			var outs []gobcy.TXOutput
	//			var outputAddress []string
	//
	//			var feeAmount int64
	//
	//			for _, event := range utxoTxByDD.Events {
	//				if event.Type == FROM_TYPE {
	//					input := gobcy.TXInput{
	//						OutputValue: int(event.Amount),
	//						Addresses:   append(inputAddress, event.Source),
	//					}
	//					inputs = append(inputs, input)
	//				}
	//				if event.Type == TO_TYPE {
	//					out := gobcy.TXOutput{
	//						Value:     *big.NewInt(event.Amount),
	//						Addresses: append(outputAddress, event.Destination),
	//					}
	//					outs = append(outs, out)
	//				}
	//				if event.Type == FEE_TYPE {
	//					feeAmount = event.Amount
	//				}
	//			}
	//
	//			txTime := time.Unix(int64(utxoTxByDD.Date), 0)
	//			tx = types.TX{
	//				BlockHash:   utxoTxByDD.BlockId,
	//				BlockHeight: utxoTxByDD.BlockNumber,
	//				Hash:        utxoTxByDD.Id,
	//				Fees:        *big.NewInt(feeAmount),
	//				Confirmed:   txTime,
	//				Inputs:      inputs,
	//				Outputs:     outs,
	//				Error:       "",
	//			}
	//			return tx, nil
	//		}
	//
	//	}
	//
	//}
	//return tx, err

}
func (c *Client) GetMemoryPoolTXByNode() (txIds model.MemoryPoolTX, err error) {
	//if c.DispatchClient.ChainName == "BTC" {
	//	return btc.GetMemoryPoolTXByNode(json, &c.DispatchClient)
	//} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
	//	return doge.GetMemoryPoolTXByNode(json, &c.DispatchClient)
	//}
	//return txIds, err
	return c.DispatchClient.GetMemoryPoolTXByNode()
}

//	func (c *Client) GetBlockCount(json model.JsonRpcRequest) (count model.BTCCount, err error) {
//		//dongdong已去掉
//		if c.DispatchClient.ChainName == "BTC" {
//			return doge.GetBlockCount(json, &c.DispatchClient)
//		} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
//		}
//		return model.BTCCount{}, err
//	}
func (c *Client) GetBTCBlockByNumber(number int) (types.BTCBlockerInfo, error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetBTCBlockByNumber(number, &c.DispatchClient)

		//btcBlockInfo, err := c.DispatchClient.GetBTCBlock(number)
		//if err != nil {
		//	return block, err
		//}
		//if btcBlockInfo.Error != nil {
		//	return block, errors.New(fmt.Sprintf("%v", btcBlockInfo.Error))
		//}
		//
		//var txs []types.Tx
		//for index, utxoTx := range btcBlockInfo.Result.Tx {
		//	var inputs []types.Inputs
		//	var outs []types.Out
		//	valueNumFee := decimal.NewFromFloat(utxoTx.Fee)
		//	// 需要乘以100000000(10的8次方)转成整型
		//	feeAmount := valueNumFee.Mul(utxo).IntPart()
		//
		//	if index == 0 {
		//		//coinbase 不记录
		//		continue
		//	}
		//	for _, vin := range utxoTx.Vin {
		//		voutIndex := vin.Vout
		//		btcPreTx, err1 := c.DispatchClient.GetTransactionsByTXHash(vin.Txid)
		//		if err1 != nil || btcPreTx.Error != nil {
		//			continue
		//		}
		//		pvout := btcPreTx.Result.Vout[voutIndex]
		//		valueNum := decimal.NewFromFloat(pvout.Value)
		//		// 需要乘以100000000(10的8次方)转成整型
		//		amount := valueNum.Mul(utxo)
		//		val := amount.IntPart()
		//		input := types.Inputs{
		//			PrevOut: types.PrevOut{
		//				Value: int(val),
		//				Addr:  pvout.ScriptPubKey.Address,
		//			},
		//		}
		//		inputs = append(inputs, input)
		//	}
		//
		//	for _, vout := range utxoTx.Vout {
		//		outValue := decimal.NewFromFloat(vout.Value)
		//		// 需要乘以100000000(10的8次方)转成整型
		//		outAmount := outValue.Mul(utxo).IntPart()
		//		out := types.Out{
		//			Value: int(outAmount),
		//			Addr:  vout.ScriptPubKey.Address,
		//		}
		//		outs = append(outs, out)
		//	}
		//	tx := types.Tx{
		//		Hash:        utxoTx.Txid,
		//		Fee:         int(feeAmount),
		//		DoubleSpend: false,
		//		Time:        btcBlockInfo.Result.Time,
		//		BlockIndex:  btcBlockInfo.Result.Height,
		//		BlockHeight: btcBlockInfo.Result.Height,
		//		Inputs:      inputs,
		//		Out:         outs,
		//	}
		//
		//	txs = append(txs, tx)
		//
		//}
		//
		//block = types.BTCBlockerInfo{
		//	Hash:      btcBlockInfo.Result.Hash,
		//	PrevBlock: btcBlockInfo.Result.Previousblockhash,
		//	Time:      btcBlockInfo.Result.Time,
		//	NTx:       btcBlockInfo.Result.NTx,
		//	MainChain: true,
		//	Height:    btcBlockInfo.Result.Height,
		//	Tx:        txs,
		//}
		//return block, err

		//return btc.GetBTCBlockByNumber(number, &c.DispatchClient)

	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		block, err := doge.GetBlockByNumber(number, &c.DispatchClient)
		var txs []types.Tx
		for _, utxoTx := range block.Txs {
			var inputs []types.Inputs
			var outs []types.Out
			var feeAmount int
			for _, event := range utxoTx.Events {
				if event.Type == FROM_TYPE {
					input := types.Inputs{
						PrevOut: types.PrevOut{
							Value: int(event.Amount),
							Addr:  event.Source,
						},
					}
					inputs = append(inputs, input)
				}
				if event.Type == TO_TYPE {
					out := types.Out{
						Value: int(event.Amount),
						Addr:  event.Destination,
					}
					outs = append(outs, out)
				}
				if event.Type == FEE_TYPE {
					feeAmount = int(event.Amount)
				}
			}

			tx := types.Tx{
				Hash:        utxoTx.Id,
				Fee:         feeAmount,
				DoubleSpend: false,
				Time:        block.Date,
				BlockIndex:  block.Number,
				BlockHeight: block.Number,
				Inputs:      inputs,
				Out:         outs,
			}

			txs = append(txs, tx)
		}

		blockInfo := types.BTCBlockerInfo{
			Hash:      block.Id,
			PrevBlock: block.ParentId,
			Time:      block.Date,
			NTx:       block.NumTxs,
			MainChain: true,
			Height:    block.Number,
			Tx:        txs,
		}
		return blockInfo, err
	}
	return types.BTCBlockerInfo{}, nil
}
