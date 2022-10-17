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
	"strings"

	"time"

	"github.com/blockcypher/gobcy"
	"github.com/shopspring/decimal"
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
	*common.NodeRecoverIn
	nodeURL        string
	chainName      string
	DispatchClient base.Client
}

func NewClient(nodeUrl string, chainName string) Client {
	var r base.Client
	if chainName == "BTC" {
		utils.CopyProperties(btc.NewClient(nodeUrl), &r)
	} else if chainName == "LTC" || chainName == "DOGE" {
		utils.CopyProperties(doge.NewClient(nodeUrl), &r)
	}
	r.ChainName = chainName
	return Client{
		NodeRecoverIn: &common.NodeRecoverIn{
			ChainName: chainName,
		},
		nodeURL:        nodeUrl,
		chainName:      chainName,
		DispatchClient: r,
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
		btcHeigth, err := btc.GetBlockNumber(&c.DispatchClient)
		return uint64(btcHeigth), err
	} else {
		result, err := c.DispatchClient.GetBlockCount()
		if err != nil {
			return 0, err
		}
		height := result.Result
		log.Debug(
			"RETRIEVED CHAIN HEIGHT FROM NODE",
			zap.Int("height", height),
			zap.String("nodeUrl", c.nodeURL),
			zap.String("chainName", c.chainName),
			zap.String("elapsed", time.Now().Sub(start).String()),
		)
		return uint64(height), err
	}
}

func (c *Client) GetBlock(height uint64) (block *chain.Block, err error) {
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
	rawTx, err := c.GetTransactionByHash(txHash)
	if err != nil {
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

func (c *Client) GetBalance(address string) (string, error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetBalance(address, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetBalance(address, &c.DispatchClient)
	}
	return "", nil
}
func (c *Client) GetBlockNumber() (int, error) {
	//if c.DispatchClient.ChainName == "BTC" {
	//	return btc.GetBlockNumber(&c.DispatchClient)
	//} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
	//	return doge.GetBlockNumber(0, &c.DispatchClient)
	//}
	//return 0, nil

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
		return btc.GetMempoolTxIds(&c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetMempoolTxIds(&c.DispatchClient)
	}
	return nil, nil
}

func (c *Client) GetTestBlockByHeight(height int) (result types.BTCTestBlockerInfo, err error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTestBlockByHeight(height, &c.DispatchClient)
	}
	return nil, err
}
func (c *Client) GetTransactionByPendingHash(hash string) (tx types.TXByHash, err error) {
	//20220905 未用到
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTransactionByPendingHash(hash, &c.DispatchClient)
	}
	return tx, err
}
func (c *Client) GetTransactionByPendingHashByNode(json model.JsonRpcRequest) (tx model.BTCTX, err error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTransactionByPendingHashByNode(json, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetTransactionByPendingHashByNode(json, &c.DispatchClient)
	}
	return tx, err
}
func (c *Client) GetTransactionByHash(hash string) (tx types.TX, err error) {

	//UTXOTX --- utxo
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTransactionByHash(hash, &c.DispatchClient)
	} else {

		utxoTx, err := c.DispatchClient.GetTransactionsByTXHash(hash)

		var inputs []gobcy.TXInput
		var inputAddress []string
		var outs []gobcy.TXOutput
		var outputAddress []string

		vinAmount := decimal.Zero
		voutAmount := decimal.Zero
		utxo := decimal.NewFromInt(100000000)

		if err != nil {
			return tx, err
		}
		if utxoTx.Error != nil {
			return tx, errors.New(fmt.Sprintf("%v", utxoTx.Error))
		}
		for _, vin := range utxoTx.Result.Vin {
			voutIndex := vin.Vout
			btcPreTx, err1 := c.DispatchClient.GetTransactionsByTXHash(vin.Txid)
			if err1 != nil || btcPreTx.Error != nil {
				continue
			}
			pvout := btcPreTx.Result.Vout[voutIndex]
			valueNum := decimal.NewFromFloat(pvout.Value)
			// 需要乘以100000000(10的8次方)转成整型
			amount := valueNum.Mul(utxo)
			vinAmount = vinAmount.Add(amount)
			val := amount.IntPart()
			if c.DispatchClient.ChainName == "BTC" {
				inputAddress = append(inputAddress, pvout.ScriptPubKey.Address)
			}

			if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
				inputAddress = pvout.ScriptPubKey.Addresses
			}
			input := gobcy.TXInput{
				OutputValue: int(val),
				Addresses:   inputAddress,
			}
			inputs = append(inputs, input)
		}

		for _, vout := range utxoTx.Result.Vout {
			outValue := decimal.NewFromFloat(vout.Value)
			// 需要乘以100000000(10的8次方)转成整型
			vu := outValue.Mul(utxo)
			voutAmount = voutAmount.Add(vu)
			outAmount := vu.BigInt()

			if c.DispatchClient.ChainName == "BTC" {
				outputAddress = append(outputAddress, vout.ScriptPubKey.Address)
			}

			if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
				outputAddress = vout.ScriptPubKey.Addresses
			}

			out := gobcy.TXOutput{
				Value:     *outAmount,
				Addresses: outputAddress,
			}
			outs = append(outs, out)
		}
		feeAmount := vinAmount.Sub(voutAmount).BigInt()

		log.Info(hash, zap.Any("vinAmount", vinAmount), zap.Any("voutAmount", voutAmount), zap.Any("feeAmount", feeAmount))

		txTime := time.Unix(int64(utxoTx.Result.Time), 0)
		utxoBlock, err := c.DispatchClient.GetUTXOBlockByHash(utxoTx.Result.Blockhash)
		tx = types.TX{
			BlockHash:   utxoTx.Result.Blockhash,
			BlockHeight: utxoBlock.Result.Height,
			Hash:        hash,
			Fees:        *feeAmount,
			Confirmed:   txTime,
			Inputs:      inputs,
			Outputs:     outs,
			Error:       tx.Error,
		}

		return tx, nil
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
	var block types.BTCBlockerInfo
	utxo := decimal.NewFromInt(100000000)
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

		utoxBlockInfo, err := c.DispatchClient.GetUTXOBlock(number)
		if err != nil {
			return block, err
		}
		if utoxBlockInfo.Error != nil {
			return block, errors.New(fmt.Sprintf("%v", utoxBlockInfo.Error))
		}
		var txs []types.Tx
		txIds := utoxBlockInfo.Result.Tx
		if len(txIds) <= 0 {
			return block, err
		}

		for index, txId := range txIds {
			if index == 0 {
				continue
			}

			var inputs []types.Inputs
			var outs []types.Out
			vinAmount := decimal.Zero
			voutAmount := decimal.Zero
			utxoTx, err1 := c.DispatchClient.GetTransactionsByTXHash(txId)
			if err1 != nil || utxoTx.Error != nil {
				log.Debug(
					"RETRIEVE TX BY HASH FAILED",
					zap.String("txId", txId),
					zap.Any("err1", err1),
					zap.Any("utxotx.Error", utxoTx.Error),
					zap.String("chainName", c.DispatchClient.ChainName),
				)
				continue
			}
			for _, vin := range utxoTx.Result.Vin {
				voutIndex := vin.Vout
				btcPreTx, err1 := c.DispatchClient.GetTransactionsByTXHash(vin.Txid)
				if err1 != nil || btcPreTx.Error != nil {
					log.Debug(
						"RETRIEVE TX BY HASH FAILED",
						zap.String("txId", vin.Txid),
						zap.Any("err1", err1),
						zap.Any("utxotx.Error", utxoTx.Error),
						zap.String("chainName", c.DispatchClient.ChainName),
					)
					continue
				}
				pvout := btcPreTx.Result.Vout[voutIndex]
				valueNum := decimal.NewFromFloat(pvout.Value)
				// 需要乘以100000000(10的8次方)转成整型
				amount := valueNum.Mul(utxo)
				vinAmount = vinAmount.Add(amount)
				val := amount.IntPart()
				pAdds := pvout.ScriptPubKey.Addresses
				var pAddr string
				if len(pAdds) > 0 {
					pAddr = pAdds[0]
				}
				input := types.Inputs{
					PrevOut: types.PrevOut{
						Value: int(val),
						Addr:  pAddr,
					},
				}
				inputs = append(inputs, input)
			}

			for _, vout := range utxoTx.Result.Vout {
				outValue := decimal.NewFromFloat(vout.Value)
				// 需要乘以100000000(10的8次方)转成整型
				vu := outValue.Mul(utxo)
				voutAmount = voutAmount.Add(vu)
				outAmount := vu.IntPart()
				adds := vout.ScriptPubKey.Addresses
				var addr string
				if len(adds) > 0 {
					addr = adds[0]
				}
				out := types.Out{
					Value: int(outAmount),
					Addr:  addr,
				}
				outs = append(outs, out)
			}
			feeAmount := vinAmount.Sub(voutAmount).IntPart()

			tx := types.Tx{
				Hash:        txId,
				Fee:         int(feeAmount),
				DoubleSpend: false,
				Time:        utxoTx.Result.Time,
				BlockIndex:  utoxBlockInfo.Result.Height,
				BlockHeight: utoxBlockInfo.Result.Height,
				Inputs:      inputs,
				Out:         outs,
			}

			txs = append(txs, tx)
		}

		block = types.BTCBlockerInfo{
			Hash:      utoxBlockInfo.Result.Hash,
			PrevBlock: utoxBlockInfo.Result.Previousblockhash,
			Time:      utoxBlockInfo.Result.Time,
			NTx:       utoxBlockInfo.Result.NTx,
			MainChain: true,
			Height:    utoxBlockInfo.Result.Height,
			Tx:        txs,
		}
		return block, err
	}
	return types.BTCBlockerInfo{}, nil
}
