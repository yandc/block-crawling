package kaspa

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txHandler struct {
	chainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords []*data.KasTransactionRecord
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	height := int(h.chainHeight)
	curHeight := int(chainTx.BlockNumber)
	tx := chainTx.Raw.(*types.KaspaTransactionInfo)
	transactionHash := tx.TransactionId
	status := biz.PENDING
	if tx.AcceptingBlockBlueScore > 0 {
		status = biz.SUCCESS
	}
	var feeAmount decimal.Decimal
	var fromAddress, toAddress, fromUid, toUid string
	var fromAddressExist, toAddressExist bool

	//openblock钱包转账中tx.Inputs只会有一笔
	if len(tx.Inputs) > 0 {
		fromAddress = tx.Inputs[0].PreviousOutpointScriptPublicKeyAddress
	} else {
		log.Error("扫块，txInputs为0", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash))
	}
	if fromAddress != "" {
		fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
		if err != nil {
			log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
			return
		}
	}

	var index int
	for _, out := range tx.Outputs {
		toAddress = out.ScriptPublicKeyAddress
		if fromAddress == toAddress {
			continue
		}
		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if !fromAddressExist && !toAddressExist {
			continue
		}

		index++
		var txHash string
		if index == 1 {
			txHash = transactionHash
		} else {
			txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
		}
		var inAmount, outAmount, fee int64
		for _, input := range tx.Inputs {
			inAmount += input.PreviousOutpointAmount
		}
		for _, output := range tx.Outputs {
			outAmount += output.Amount
		}
		fee = inAmount - outAmount
		feeAmount = decimal.NewFromInt(fee)
		amountValue := decimal.NewFromInt(out.Amount)

		kasTransactionRecord := &data.KasTransactionRecord{
			BlockHash:       chainBlock.Hash,
			BlockNumber:     curHeight,
			TransactionHash: txHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         fromUid,
			ToUid:           toUid,
			FeeAmount:       feeAmount,
			Amount:          amountValue,
			Status:          status,
			TxTime:          tx.BlockTime / 1000,
			ConfirmCount:    int32(height - curHeight),
			DappData:        "",
			ClientData:      "",
			CreatedAt:       h.now,
			UpdatedAt:       h.now,
		}
		h.txRecords = append(h.txRecords, kasTransactionRecord)
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	transactionInfo := tx.Raw.(*types.KaspaTransactionInfo)

	var block *chain.Block
	blockHashs := transactionInfo.BlockHash
	if len(blockHashs) == 1 {
		block = &chain.Block{
			Hash: blockHashs[0],
		}
	} else {
		for _, blockHash := range blockHashs {
			result, err := ExecuteRetry(h.chainName, func(client Client) (interface{}, error) {
				return client.GetBlockByHash(blockHash)
			})
			if err != nil {
				return err
			}
			block = result.(*chain.Block)
			blockInfo := block.Raw.(*BlockInfo)
			if blockInfo.VerboseData.IsChainBlock {
				break
			}
		}
	}

	err = h.OnNewTx(c, block, tx)
	return nil
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.KasTransactionRecord)
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

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *client, txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}
