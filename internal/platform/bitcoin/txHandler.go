package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"context"
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
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords      []*data.BtcTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, block *chain.Block, chainTx *chain.Transaction) (err error) {
	height := int(h.chainHeight)
	curHeight := int(chainTx.BlockNumber)
	tx := chainTx.Raw.(types.Tx)
	transactionHash := tx.Hash
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
		log.Error("扫块，txInputs为0", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash))
	}
	if fromAddress != "" {
		fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
		if err != nil {
			log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
			return
		}
	}

	index, _ := h.txHashIndexMap[transactionHash]

	for _, out := range tx.Out {
		toAddress = out.Addr
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
		h.txHashIndexMap[transactionHash] = index
		var txHash string
		if index == 1 {
			txHash = transactionHash
		} else {
			txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
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
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	curHeight := h.curHeight
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("error", err))
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

func (h *txHandler) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) (err error) {
	tx := txByHash.Raw.(types.TX)
	record := txByHash.Record.(*data.BtcTransactionRecord)

	if strings.HasSuffix(tx.Error, " No such mempool or blockchain transaction") {
		nowTime := time.Now().Unix()
		if record.CreatedAt+300 > nowTime {
			if record.Status == biz.PENDING {
				status := biz.NO_STATUS
				record.Status = status
				record.UpdatedAt = h.now
				h.txRecords = append(h.txRecords, record)
			}
		} else {
			status := biz.FAIL
			record.Status = status
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)

			//交易失败后重置 input 状态
			go h.ResetDroppedTxInputs(tx.Hash, record.FromAddress)
		}
		return nil
	}

	isPending := tx.BlockHeight <= 0
	if isPending {
		common.SetTxResult(txByHash, record)
		return nil
	}

	curHeight := tx.BlockHeight
	transactionHash := tx.Hash
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
		fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
		if err != nil {
			log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("txHash", transactionHash), zap.Any("error", err))
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
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("txHash", transactionHash), zap.Any("error", err))
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

		//交易失败后重置 input 状态
		go h.ResetDroppedTxInputs(tx.Hash, record.FromAddress)
	}
	return nil
}

func (h *txHandler) ResetDroppedTxInputs(txHash, address string) error {
	if txHash == "" {
		return nil
	}

	ctx := context.Background()
	utxos, err := data.UtxoUnspentRecordRepoClient.FindBySpentTxHash(ctx, txHash)
	if err != nil {
		return err
	}

	if len(utxos) != 0 {
		//更新状态为未花费，置空 spent tx hash
		for _, utxo := range utxos {
			utxo.Unspent = data.UtxoStatusUnSpend
			utxo.SpentTxHash = ""
		}

		_, err = data.UtxoUnspentRecordRepoClient.BatchSaveOrUpdate(ctx, utxos)
		if err != nil {
			return err
		}
	} else {
		err := RefreshUserUTXO(h.chainName, address, true)
		if err != nil {
			return err
		}
	}

	return nil
}
