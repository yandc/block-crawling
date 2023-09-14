package polkadot

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"fmt"
	"github.com/shopspring/decimal"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	ChainName   string
	block       *chain.Block
	chainHeight uint64
	now         time.Time
	newTxs      bool
	txRecords   []*data.DotTransactionRecord
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	curHeight := block.Number
	txTime := block.Time
	transaction := tx.Raw.(types.ExtrinsicInfo)
	status := biz.SUCCESS
	if transaction.Err != "" {
		status = biz.FAIL
	}
	fromAddress := tx.FromAddress
	toAddress := tx.ToAddress
	amount := tx.Value
	a, _ := decimal.NewFromString(amount)
	nonce := tx.Nonce

	p1 := decimal.NewFromInt(10000000000)
	f, _ := decimal.NewFromString(fmt.Sprintf("%f", transaction.Fee))
	fee := f.Mul(p1).Round(0)
	user, err := pCommon.MatchUser(fromAddress, toAddress, h.ChainName)
	if err != nil {
		return err
	}
	if !(user.MatchTo || user.MatchFrom) {
		return nil
	}

	dotTransactionRecord := &data.DotTransactionRecord{
		BlockHash:       block.Hash,
		BlockNumber:     int(curHeight),
		Nonce:           int64(nonce),
		TransactionHash: tx.Hash,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		FromUid:         user.FromUid,
		ToUid:           user.ToUid,
		FeeAmount:       fee,
		Amount:          a,
		Status:          status,
		TxTime:          txTime,
		ContractAddress: "",
		TransactionType: biz.NATIVE,
		DappData:        "",
		ClientData:      "",
		CreatedAt:       h.now.Unix(),
		UpdatedAt:       h.now.Unix(),
	}
	h.txRecords = append(h.txRecords, dotTransactionRecord)

	return nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	var err error
	if txRecords != nil && len(txRecords) > 0 {
		if h.newTxs {
			err = BatchSaveOrUpdate(txRecords, biz.GetTableName(h.ChainName))
		} else {
			_, err = data.DotTransactionRecordRepoClient.UpdateStatus(nil, biz.GetTableName(h.ChainName), txRecords)
		}

		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.ChainName), zap.Any("current", h.block.Number), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.ChainName, *(client.(*Client)), txRecords)
		} else {
			go HandlePendingRecord(h.ChainName, *(client.(*Client)), txRecords)
		}
	}
	return nil
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, tx *chain.Transaction) error {
	transaction := tx.Raw.(types.PolkadotTxInfo)
	if transaction.Error != "" {
		dotTransactionRecord := &data.DotTransactionRecord{
			TransactionHash: transaction.ExtrinsicHash,
			Status:          biz.FAIL,
			UpdatedAt:       h.now.Unix(),
		}
		h.txRecords = append(h.txRecords, dotTransactionRecord)
	} else {
		for _, transfer := range transaction.Transfers {
			txInfo := &chain.Transaction{
				Hash:        transaction.ExtrinsicHash,
				Nonce:       uint64(transaction.Nonce),
				BlockNumber: uint64(transaction.BlockNumber),
				FromAddress: transfer.From,
				ToAddress:   transfer.To,
				Value:       transfer.RawAmount.String(),
				Raw:         transaction,
			}
			if transfer.Symbol == "DOT" {
				txInfo.TxType = biz.NATIVE
			} else {
				txInfo.TxType = biz.TRANSFER
			}

			fromAddress := transfer.From
			toAddress := transfer.To
			user, err := pCommon.MatchUser(fromAddress, toAddress, h.ChainName)
			if err != nil {
				return err
			}
			if !(user.MatchTo || user.MatchFrom) {
				continue
			}
			status := biz.SUCCESS
			if transaction.Error != "" {
				status = biz.FAIL
			}
			amount := transfer.RawAmount.String()
			a, _ := decimal.NewFromString(amount)
			p1 := decimal.NewFromInt(10000000000)
			f, _ := decimal.NewFromString(fmt.Sprintf("%f", transaction.Fee))
			fee := f.Mul(p1).Round(0)
			nonce := transaction.Nonce
			dotTransactionRecord := &data.DotTransactionRecord{
				BlockHash:       transaction.BlockHash,
				BlockNumber:     transaction.BlockNumber,
				Nonce:           int64(nonce),
				TransactionHash: transaction.ExtrinsicHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         user.FromUid,
				ToUid:           user.ToUid,
				FeeAmount:       fee,
				Amount:          a,
				Status:          status,
				TxTime:          int64(transaction.Ts),
				ContractAddress: "",
				TransactionType: biz.NATIVE,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, dotTransactionRecord)
		}
	}
	return nil
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	// client := c.(*Client)
	record := tx.Record.(*data.DotTransactionRecord)

	log.Info(
		"PENDING TX COULD NOT FOUND ON THE CHAIN",
		zap.String("chainName", h.ChainName),
		zap.Uint64("height", tx.BlockNumber),
		zap.String("nodeUrl", c.URL()),
		zap.String("txHash", tx.Hash),
		zap.String("fromUid", record.FromUid),
		zap.String("toUid", record.ToUid),
		zap.String("fromAddress", record.FromAddress),
		zap.String("toAddress", record.ToAddress),
	)

	nowTime := time.Now().Unix()
	if record.CreatedAt+300 > nowTime {
		record.Status = biz.NO_STATUS
		h.txRecords = append(h.txRecords, record)
	} else {
		record.Status = biz.FAIL
		h.txRecords = append(h.txRecords, record)
	}

	return nil
}
