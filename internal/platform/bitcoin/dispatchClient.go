package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/platform/bitcoin/btc"
	"block-crawling/internal/platform/bitcoin/doge"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"github.com/blockcypher/gobcy"
	"math/big"
	"time"
)

const TO_TYPE = "utxo_output"
const FROM_TYPE = "utxo_input"
const FEE_TYPE = "fee"
const SUCCESS_STATUS = "completed"

type Client struct {
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
		DispatchClient: r,
	}
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
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetBlockNumber(&c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetBlockNumber(0, &c.DispatchClient)
	}
	return 0, nil
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
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetTransactionByHash(hash, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		utxoTxByDD, e := doge.GetTransactionsByTXHash(hash, &c.DispatchClient)
		if e != nil {
			return tx, e
		} else {
			if utxoTxByDD.Detail == "The requested resource has not been found" {
				tx = types.TX{
					Error: " not found.",
				}
				return tx, nil
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
	return tx, err

}
func (c *Client) GetMemoryPoolTXByNode(json model.JsonRpcRequest) (txIds model.MemoryPoolTX, err error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetMemoryPoolTXByNode(json, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		return doge.GetMemoryPoolTXByNode(json, &c.DispatchClient)
	}
	return txIds, err
}
func (c *Client) GetBlockCount(json model.JsonRpcRequest) (count model.BTCCount, err error) {
	//dongdong已去掉
	if c.DispatchClient.ChainName == "BTC" {
		return doge.GetBlockCount(json, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
	}
	return model.BTCCount{}, err
}
func (c *Client) GetBTCBlockByNumber(number int) (types.BTCBlockerInfo, error) {
	if c.DispatchClient.ChainName == "BTC" {
		return btc.GetBTCBlockByNumber(number, &c.DispatchClient)
	} else if c.DispatchClient.ChainName == "LTC" || c.DispatchClient.ChainName == "DOGE" {
		block, err := doge.GetBlockByNumber(number, 0, &c.DispatchClient)
		for i := 1; i < len(biz.AppConfig.GetDogeKey()) && err != nil; i++ {
			block, err = doge.GetBlockByNumber(number, i, &c.DispatchClient)
		}
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
