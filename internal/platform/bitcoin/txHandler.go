package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txHandler struct {
	chainName   string
	block       *chain.Block
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords      []*data.BtcTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	return h.onNewTx(tx.Raw.(types.Tx), int(tx.BlockNumber), int(h.chainHeight))
}

func (h *txHandler) onNewTx(tx types.Tx, curHeight int, height int) (err error) {
	status := biz.PENDING
	if tx.BlockHeight > 0 {
		status = biz.SUCCESS
	}
	var amount decimal.Decimal
	var fromAddress, toAddress, fromUid, toUid string
	var fromAddressExist, toAddressExist bool

	//openblock钱包转账中tx.Inputs只会有一笔
	if len(tx.Inputs) > 0 {
		fromAddress = tx.Inputs[0].PrevOut.Addr
	} else {
		log.Error(h.chainName+"扫块，txInputs为0", zap.Any("current", curHeight), zap.Any("new", height))
	}
	if fromAddress != "" {
		fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
		if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
	}

	index, _ := h.txHashIndexMap[tx.Hash]

	for _, out := range tx.Out {
		toAddress = out.Addr
		if fromAddress == toAddress {
			continue
		}
		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
		}

		if fromAddressExist || toAddressExist {
			index++
			h.txHashIndexMap[tx.Hash] = index
			var txHash string
			if index == 1 {
				txHash = tx.Hash
			} else {
				txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", index)
			}
			amount = decimal.NewFromInt(int64(out.Value))
			btcTransactionRecord := &data.BtcTransactionRecord{
				BlockHash:       h.block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: txHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       decimal.NewFromInt(int64(tx.Fee)),
				Amount:          amount,
				Status:          status,
				TxTime:          int64(tx.Time),
				ConfirmCount:    int32(height - curHeight),
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, btcTransactionRecord)
		}
	}
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	curHeight := h.curHeight
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("btc主网扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, txRecords)
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, chainTx *chain.Transaction, rawTx interface{}) (err error) {
	tx := rawTx.(types.TX)
	record := chainTx.Record.(*data.BtcTransactionRecord)

	if strings.HasSuffix(tx.Error, " No such mempool or blockchain transaction") {
		nowTime := time.Now().Unix()
		if record.CreatedAt+300 > nowTime {
			status := biz.NO_STATUS
			record.Status = status
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
		} else {
			status := biz.FAIL
			record.Status = status
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
		}
		return nil
	}

	isPending := tx.BlockHeight <= 0
	if isPending {
		return nil
	}

	curHeight := tx.BlockHeight
	status := biz.PENDING
	if tx.BlockHeight > 0 {
		status = biz.SUCCESS
	}
	var amount decimal.Decimal
	var fromAddress, toAddress, fromUid, toUid string
	var fromAddressExist, toAddressExist bool

	//openblock钱包转账中tx.Inputs只会有一笔
	fromAddress = tx.Inputs[0].Addresses[0]
	if fromAddress != "" {
		fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
		if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
			/*//更新 redis中的块高和区块hash
			data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
			data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)*/

			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
			return
		}
	}

	index := 0
	for _, out := range tx.Outputs {
		toAddress = out.Addresses[0]
		if fromAddress == toAddress {
			continue
		}
		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				/*//更新 redis中的块高和区块hash
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+h.chainName, preHeight, 0)
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+h.chainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)*/

				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
				return
			}
		}

		if fromAddressExist || toAddressExist {
			index++
			var txHash string
			if index == 1 {
				txHash = tx.Hash
			} else {
				txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", index)
			}
			amount = decimal.NewFromBigInt(&out.Value, 0)
			btcTransactionRecord := &data.BtcTransactionRecord{
				BlockHash:       tx.BlockHash,
				BlockNumber:     curHeight,
				TransactionHash: txHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       decimal.NewFromInt(tx.Fees.Int64()),
				Amount:          amount,
				Status:          status,
				TxTime:          tx.Confirmed.Unix(),
				//ConfirmCount:    int32(height - curHeight),
				DappData:   "",
				ClientData: "",
				CreatedAt:  h.now,
				UpdatedAt:  h.now,
			}
			h.txRecords = append(h.txRecords, btcTransactionRecord)
		}
	}

	return nil
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.BtcTransactionRecord)
	nowTime := time.Now().Unix()
	if record.CreatedAt+300 > nowTime {
		status := biz.NO_STATUS
		record.Status = status
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
	} else {
		status := biz.FAIL
		record.Status = status
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
	}
	return nil
}
