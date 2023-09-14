package casper

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	ChainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	now         time.Time
	newTxs      bool
	txRecords   []*data.CsprTransactionRecord
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	curHeight := block.Number
	txTime := block.Time
	transaction := tx.Raw.(types.DeployResult)
	feeAmount := decimal.Zero
	var status string
	if len(transaction.ExecutionResults) == 0 {
		status = biz.FAIL
	} else {
		if transaction.ExecutionResults[0].Result.Success.Transfers == nil || len(transaction.ExecutionResults[0].Result.Failure.ErrorMessage) > 0 {
			status = biz.FAIL
		} else {
			feeArgs := transaction.ExecutionResults[0].Result.Success.Cost
			fa, _ := decimal.NewFromString(feeArgs)
			feeAmount = fa
			status = biz.SUCCESS
		}
	}
	fromAddress := transaction.Deploy.Header.Account
	//非转账交易
	if transaction.Deploy.Session.Transfer.Args == nil {
		return nil
	}
	toAddress := transaction.Deploy.Session.Transfer.Args[1][1].(map[string]interface{})["parsed"].(string)

	amountOriginal := transaction.Deploy.Session.Transfer.Args[0][1].(map[string]interface{})["parsed"].(string)
	amount, _ := decimal.NewFromString(amountOriginal)

	user, err := pCommon.MatchUser(fromAddress, toAddress, h.ChainName)
	if err != nil {
		return err
	}
	if !(user.MatchTo || user.MatchFrom) {
		return nil
	}
	csprTransactionRecord := &data.CsprTransactionRecord{
		BlockHash:       block.Hash,
		BlockNumber:     int(curHeight),
		TransactionHash: transaction.Deploy.Hash,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		FromUid:         user.FromUid,
		ToUid:           user.ToUid,
		FeeAmount:       feeAmount,
		Amount:          amount,
		Status:          status,
		TxTime:          txTime,
		TransactionType: biz.NATIVE,
		DappData:        "",
		ClientData:      "",
		CreatedAt:       h.now.Unix(),
		UpdatedAt:       h.now.Unix(),
	}
	h.txRecords = append(h.txRecords, csprTransactionRecord)

	return nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.ChainName), zap.Any("current", h.block.Number), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		go HandleRecord(h.ChainName, *(client.(*Client)), txRecords)

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

func (h *txDecoder) OnSealedTx(c chain.Clienter, tx *chain.Transaction) error {
	var err error
	client := c.(*Client)
	var block *chain.Block

	block, err = client.GetBlock(tx.BlockNumber)
	if err != nil {
		return err
	}

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	// client := c.(*Client)
	record := tx.Record.(*data.CsprTransactionRecord)

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
